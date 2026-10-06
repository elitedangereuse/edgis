"""Reconcile old station locations against Spansh's station API.

Fleet carriers move often enough that an old EDDN observation should not stay
attached to a system indefinitely.  This tool deliberately updates only rows
whose EDGIS ``last_seen_at`` is older than a configurable threshold.  It never
deletes a station: a successful Spansh lookup can safely move it to the system
reported by Spansh, while an unavailable record remains untouched.
"""

from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

try:
    from feeders.station_ingestion import station_from_spansh_api, upsert_station
except ModuleNotFoundError:  # Allow direct execution from feeders/.
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from feeders.station_ingestion import station_from_spansh_api, upsert_station


DEFAULT_SYSTEM_NAMES = ("HIP 58832", "HD 105341")
SPANSH_API_BASE = "https://spansh.co.uk/api"
HTTP_TIMEOUT_SECONDS = 30
DEFAULT_REQUEST_DELAY_SECONDS = 2.0


@dataclass(frozen=True)
class StaleStation:
    market_id: int
    name: str
    system_id64: int
    system_name: str
    last_seen_at: datetime


def find_system(cursor: Any, name: str) -> tuple[int, str] | None:
    cursor.execute(
        """
        SELECT id64, name
        FROM systems_big
        WHERE LOWER(name) = LOWER(%s)
        LIMIT 1
        """,
        (name,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return int(row[0]), str(row[1])


def find_stale_stations(
    cursor: Any, system_id64: int, system_name: str, cutoff: datetime
) -> list[StaleStation]:
    cursor.execute(
        """
        SELECT market_id, name, system_id64, last_seen_at
        FROM stations
        WHERE system_id64 = %s
          AND last_seen_at < %s
        ORDER BY last_seen_at, market_id
        """,
        (system_id64, cutoff),
    )
    return [
        StaleStation(
            market_id=int(market_id),
            name=str(name),
            system_id64=int(row_system_id64),
            system_name=system_name,
            last_seen_at=last_seen_at,
        )
        for market_id, name, row_system_id64, last_seen_at in cursor.fetchall()
    ]


def fetch_spansh_station(
    session: requests.Session, market_id: int
) -> dict[str, Any] | None:
    """Return a current Spansh station record, or ``None`` for a 404."""
    response = session.get(
        f"{SPANSH_API_BASE}/station/{market_id}", timeout=HTTP_TIMEOUT_SECONDS
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    record = payload.get("record") if isinstance(payload, dict) else None
    return record if isinstance(record, dict) else None


def describe_update(station: StaleStation, normalized: dict[str, Any]) -> str:
    target_system_id64 = int(normalized["system_id64"])
    if target_system_id64 == station.system_id64:
        return f"refresh {station.name} [{station.market_id}] in {station.system_name}"
    return (
        f"move {station.name} [{station.market_id}] from {station.system_name} "
        f"({station.system_id64}) to system {target_system_id64}"
    )


def refresh_stations(
    connection: Any,
    system_names: Iterable[str],
    *,
    older_than_days: int,
    apply: bool,
    request_delay_seconds: float,
    session: requests.Session | None = None,
    now: datetime | None = None,
) -> dict[str, int]:
    """Check stale rows and optionally apply timestamp-protected upserts."""
    if older_than_days < 1:
        raise ValueError("older_than_days must be at least one day")
    if request_delay_seconds < 0:
        raise ValueError("request_delay_seconds cannot be negative")

    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=older_than_days)
    counts = {
        "systems": 0,
        "candidates": 0,
        "moved": 0,
        "refreshed": 0,
        "not_found": 0,
        "invalid": 0,
    }
    client = session or requests.Session()
    owns_session = session is None
    try:
        with connection.cursor() as cursor:
            candidates: list[StaleStation] = []
            for system_name in system_names:
                system = find_system(cursor, system_name)
                if system is None:
                    print(f"System not found in EDGIS: {system_name}")
                    continue
                system_id64, resolved_name = system
                counts["systems"] += 1
                candidates.extend(
                    find_stale_stations(cursor, system_id64, resolved_name, cutoff)
                )

            counts["candidates"] = len(candidates)
            for index, station in enumerate(candidates):
                try:
                    record = fetch_spansh_station(client, station.market_id)
                except requests.RequestException as exc:
                    print(f"Failed {station.name} [{station.market_id}]: {exc}")
                    counts["invalid"] += 1
                else:
                    normalized = station_from_spansh_api(record) if record else None
                    if normalized is None:
                        label = "not found" if record is None else "invalid record"
                        print(f"Skip {station.name} [{station.market_id}]: {label}")
                        counts["not_found" if record is None else "invalid"] += 1
                    else:
                        update_description = describe_update(station, normalized)
                        print(("Apply " if apply else "Would ") + update_description)
                        if int(normalized["system_id64"]) == station.system_id64:
                            counts["refreshed"] += 1
                        else:
                            counts["moved"] += 1
                        if apply:
                            upsert_station(cursor, normalized)

                if index < len(candidates) - 1 and request_delay_seconds:
                    time.sleep(request_delay_seconds)

        if apply:
            connection.commit()
        else:
            connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        if owns_session:
            client.close()
    return counts


def open_database_connection() -> Any:
    import psycopg

    return psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=5432,
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile stale EDGIS station locations from Spansh."
    )
    parser.add_argument(
        "--system",
        action="append",
        dest="system_names",
        help="System name to check; repeatable (default: HIP 58832 and HD 105341)",
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=7,
        help="Only check stations last seen before this age (default: 7)",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY_SECONDS,
        help="Seconds to wait between Spansh requests (default: 2)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit updates; without this flag the script is a dry run",
    )
    args = parser.parse_args()

    from dotenv import load_dotenv

    load_dotenv()
    system_names = args.system_names or list(DEFAULT_SYSTEM_NAMES)
    with open_database_connection() as connection:
        counts = refresh_stations(
            connection,
            system_names,
            older_than_days=args.older_than_days,
            apply=args.apply,
            request_delay_seconds=args.request_delay,
        )
    mode = "Applied" if args.apply else "Dry run"
    print(
        f"{mode}: {counts['candidates']} stale station(s) across "
        f"{counts['systems']} system(s); {counts['moved']} move(s), "
        f"{counts['refreshed']} unchanged refresh(es), "
        f"{counts['not_found']} unavailable, {counts['invalid']} invalid/error(s)."
    )


if __name__ == "__main__":
    main()
