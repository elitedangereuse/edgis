"""Replay archived Journal.Docked EDDN messages into the stations feeder."""

from __future__ import annotations

import argparse
import bz2
import json
import os
import re
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv


DOCKED_ARCHIVE_NAME = re.compile(
    r"^Journal\.Docked-(?P<date>\d{4}-\d{2}-\d{2})\.jsonl\.bz2$"
)


def archive_sort_key(path: Path) -> tuple[int, str, str]:
    """Sort recognised Docked archive files by their day, then by path."""
    match = DOCKED_ARCHIVE_NAME.match(path.name)
    return (0, match.group("date"), str(path)) if match else (1, path.name, str(path))


def ordered_archives(paths: Iterable[Path]) -> list[Path]:
    """Validate and order a set of local Journal.Docked archive files."""
    archives = sorted(paths, key=archive_sort_key)
    if not archives:
        raise ValueError("at least one Journal.Docked .jsonl.bz2 file is required")
    invalid = [path for path in archives if not DOCKED_ARCHIVE_NAME.match(path.name)]
    if invalid:
        names = ", ".join(str(path) for path in invalid)
        raise ValueError(f"not a Journal.Docked archive file: {names}")
    missing = [path for path in archives if not path.is_file()]
    if missing:
        names = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(names)
    return archives


def market_id_from_envelope(envelope: dict[str, Any]) -> int | None:
    """Return a stable station ID for a valid Docked archive record."""
    payload = envelope.get("message") or {}
    if payload.get("event") != "Docked":
        return None
    try:
        return int(payload.get("MarketID"))
    except (TypeError, ValueError):
        return None


def market_id_occurrences(path: Path, limit: int | None) -> Counter[int]:
    """Count MarketIDs in the part of one archive eligible for replay.

    Replaying only a MarketID's last line preserves the newest observation in
    a chronological archive without retaining whole EDDN messages in memory.
    """
    occurrences: Counter[int] = Counter()
    seen = 0
    with bz2.open(path, mode="rt", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            seen += 1
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError:
                pass
            else:
                market_id = market_id_from_envelope(envelope)
                if market_id is not None:
                    occurrences[market_id] += 1
            if limit is not None and seen >= limit:
                break
    return occurrences


def docked_log_label(envelope: dict[str, Any]) -> str:
    """Build a useful progress label without depending on feeder acceptance."""
    payload = envelope.get("message") or {}
    station_name = (
        payload.get("StationName_Localised")
        or payload.get("StationName")
        or "Unknown station"
    )
    market_id = payload.get("MarketID", "?")
    system_name = payload.get("StarSystem") or payload.get("SystemName")
    system_id64 = payload.get("SystemAddress")
    if system_name and system_id64 is not None:
        location = f"{system_name} [{system_id64}]"
    elif system_name:
        location = str(system_name)
    elif system_id64 is not None:
        location = str(system_id64)
    else:
        location = "unknown system"
    return f"Docked: {station_name} [{market_id}] in {location}"


def replay_archives(
    paths: Iterable[Path],
    connection: Any,
    process: Callable[..., Any],
    *,
    dry_run: bool = False,
    commit_every: int = 10_000,
    limit: int | None = None,
    verbose: bool = True,
) -> dict[str, int]:
    """Stream archived Docked envelopes through the live station processor."""
    if commit_every < 1:
        raise ValueError("commit_every must be positive")
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")

    counts = {
        "seen": 0,
        "with_host": 0,
        "processed": 0,
        "duplicates": 0,
        "skipped": 0,
        "invalid": 0,
    }
    committed_since_last = 0

    for path in ordered_archives(paths):
        print(f"Replaying {path}")
        remaining_limit = None if limit is None else limit - counts["seen"]
        if remaining_limit is not None and remaining_limit < 1:
            break
        remaining_market_ids = market_id_occurrences(path, remaining_limit)
        with bz2.open(path, mode="rt", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                counts["seen"] += 1
                try:
                    envelope = json.loads(line)
                except json.JSONDecodeError:
                    counts["invalid"] += 1
                    continue

                market_id = market_id_from_envelope(envelope)
                if market_id is not None:
                    remaining_market_ids[market_id] -= 1
                    if remaining_market_ids[market_id] > 0:
                        counts["duplicates"] += 1
                        continue

                payload = envelope.get("message") or {}
                host_body = payload.get("Body")
                if isinstance(host_body, str) and host_body.strip():
                    counts["with_host"] += 1

                outcome = process(
                    envelope,
                    connection=connection,
                    # The replayer owns progress logging so every retained
                    # archive row is visible, including rejected old schemas.
                    verbose=False,
                    commit=False,
                    record_metrics=False,
                )
                if verbose:
                    outcome_reason = getattr(outcome, "reason", None)
                    reason = f" ({outcome_reason})" if outcome_reason else ""
                    print(f"{docked_log_label(envelope)} -> {outcome.status}{reason}")
                if outcome.status == "success":
                    counts["processed"] += 1
                    committed_since_last += 1
                else:
                    counts["skipped"] += 1

                if not dry_run and committed_since_last >= commit_every:
                    connection.commit()
                    committed_since_last = 0
                if limit is not None and counts["seen"] >= limit:
                    break
        if limit is not None and counts["seen"] >= limit:
            break

    if dry_run:
        connection.rollback()
    else:
        connection.commit()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay local Journal.Docked EDDN archive files into stations."
    )
    parser.add_argument(
        "paths", nargs="+", type=Path,
        help="Journal.Docked-YYYY-MM-DD.jsonl.bz2 files (processed by date)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Process messages and roll back all database changes",
    )
    parser.add_argument(
        "--commit-every", type=int, default=10_000,
        help="Commit after this many processed messages (default: 10000)",
    )
    parser.add_argument("--limit", type=int, help="Process at most this many rows")
    parser.add_argument(
        "--quiet", action="store_true",
        help="Do not print each retained Docked station and its system name",
    )
    args = parser.parse_args()

    load_dotenv()
    try:
        from feeders import eddn_stations_feeder
    except ModuleNotFoundError:  # Allow direct execution from feeders/.
        import sys

        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from feeders import eddn_stations_feeder

    with psycopg.connect(
        host=os.getenv("DB_HOST"), port=5432, dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
    ) as connection:
        counts = replay_archives(
            args.paths,
            connection,
            eddn_stations_feeder.process_message,
            dry_run=args.dry_run,
            commit_every=args.commit_every,
            limit=args.limit,
            verbose=not args.quiet,
        )
    mode = "Dry run:" if args.dry_run else "Imported:"
    print(
        f"{mode} scanned {counts['seen']}, found {counts['with_host']} Docked "
        f"host reference(s), processed {counts['processed']}, skipped "
        f"{counts['skipped']}, deduplicated {counts['duplicates']}, and ignored "
        f"{counts['invalid']} invalid row(s)."
    )


if __name__ == "__main__":
    main()
