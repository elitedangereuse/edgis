"""Backfill empty station parent chains from an EDSM stations dump."""

from __future__ import annotations

import argparse
import gzip
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import ijson
import psycopg
from dotenv import load_dotenv
from tqdm import tqdm

try:
    from feeders.station_ingestion import station_parent_chain
except ModuleNotFoundError:  # Allow direct execution from feeders/.
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from feeders.station_ingestion import station_parent_chain


def station_host_reference(station: dict[str, Any]) -> tuple[int, int, str] | None:
    """Return the station and host identity exposed by one EDSM dump row."""
    market_id = station.get("marketId")
    system_id64 = station.get("systemId64")
    body = station.get("body")
    body_name = body.get("name") if isinstance(body, dict) else None
    if not isinstance(body_name, str) or not body_name.strip():
        return None
    try:
        return int(market_id), int(system_id64), body_name.strip()
    except (TypeError, ValueError):
        return None


def lookup_parent_chain(
    cursor: Any, system_id64: int, body_name: str,
) -> list[dict[str, Any]]:
    """Resolve an EDSM host name to its in-system body parent chain."""
    cursor.execute(
        """
        SELECT b.body_id, bt.name, b.parents
        FROM bodies b
        INNER JOIN body_types bt ON bt.id = b.body_type_id
        WHERE b.system_id64 = %s
          AND LOWER(b.body_name) = LOWER(%s)
        LIMIT 1
        """,
        (system_id64, body_name),
    )
    host = cursor.fetchone()
    return station_parent_chain(*host) if host else []


def update_empty_station_parents(
    cursor: Any, market_id: int, system_id64: int, parents: list[dict[str, Any]],
) -> bool:
    """Write only an empty parent chain; never replace an existing one."""
    cursor.execute(
        """
        UPDATE stations
        SET parents = %s::jsonb
        WHERE market_id = %s
          AND system_id64 = %s
          AND (parents IS NULL OR parents = '[]'::jsonb)
        """,
        (json.dumps(parents), market_id, system_id64),
    )
    return cursor.rowcount > 0


def reconstruct(
    path: Path, connection: Any, *, dry_run: bool = False,
    commit_every: int = 10_000, limit: int | None = None,
) -> dict[str, int]:
    """Stream an EDSM dump and backfill parent chains for matched stations."""
    if commit_every < 1:
        raise ValueError("commit_every must be positive")

    counts = {
        "seen": 0,
        "with_host": 0,
        "host_resolved": 0,
        "updated": 0,
    }
    with connection.cursor() as cursor, path.open("rb") as compressed:
        @lru_cache(maxsize=100_000)
        def cached_parent_chain(system_id64: int, body_name: str) -> tuple[str, ...]:
            parents = lookup_parent_chain(cursor, system_id64, body_name)
            return tuple(json.dumps(parent, sort_keys=True) for parent in parents)

        with gzip.GzipFile(fileobj=compressed) as stream, tqdm(
            total=path.stat().st_size,
            unit="B",
            unit_scale=True,
            desc="Reconstructing station parents",
        ) as progress:
            for station in ijson.items(stream, "item"):
                counts["seen"] += 1
                reference = station_host_reference(station)
                if reference is not None:
                    counts["with_host"] += 1
                    market_id, system_id64, body_name = reference
                    encoded_parents = cached_parent_chain(system_id64, body_name)
                    if encoded_parents:
                        counts["host_resolved"] += 1
                        parents = [json.loads(parent) for parent in encoded_parents]
                        if not dry_run and update_empty_station_parents(
                            cursor, market_id, system_id64, parents
                        ):
                            counts["updated"] += 1
                if not dry_run and counts["seen"] % commit_every == 0:
                    connection.commit()
                progress.update(compressed.tell() - progress.n)
                if limit is not None and counts["seen"] >= limit:
                    break
    if not dry_run:
        connection.commit()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill empty stations.parents values from an EDSM dump."
    )
    parser.add_argument("path", type=Path, help="Path to EDSM stations.json.gz")
    parser.add_argument(
        "--dry-run", action="store_true", help="Resolve hosts without updating rows"
    )
    parser.add_argument(
        "--commit-every", type=int, default=10_000,
        help="Commit after this many dump rows (default: 10000)",
    )
    parser.add_argument(
        "--limit", type=int, help="Process at most this many dump rows"
    )
    args = parser.parse_args()

    load_dotenv()
    with psycopg.connect(
        host=os.getenv("DB_HOST"), port=5432, dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
    ) as connection:
        counts = reconstruct(
            args.path, connection, dry_run=args.dry_run,
            commit_every=args.commit_every, limit=args.limit,
        )
    if args.dry_run:
        print(
            f"Dry run: scanned {counts['seen']}, found {counts['with_host']} "
            f"EDSM host reference(s), and resolved {counts['host_resolved']} "
            "host body/bodies; no rows updated."
        )
    else:
        print(
            f"Updated {counts['updated']} station(s); scanned {counts['seen']}, "
            f"found {counts['with_host']} EDSM host reference(s), and resolved "
            f"{counts['host_resolved']} host body/bodies."
        )


if __name__ == "__main__":
    main()
