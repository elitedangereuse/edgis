"""Synchronize every station listed by Spansh for one system.

Unlike ``refresh_stale_stations``, this importer discovers stations that are
not already present in EDGIS.  Spansh's system endpoint supplies market IDs;
the individual station endpoint supplies the timestamped fields needed for a
safe station upsert.
"""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Iterable
from typing import Any

import requests

try:
    from feeders.station_ingestion import (
        parse_timestamp,
        station_from_spansh,
        station_from_spansh_api,
        upsert_station,
    )
except ModuleNotFoundError:  # Allow direct execution from feeders/.
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from feeders.station_ingestion import (
        parse_timestamp,
        station_from_spansh,
        station_from_spansh_api,
        upsert_station,
    )


SPANSH_API_BASE = "https://spansh.co.uk/api"
HTTP_TIMEOUT_SECONDS = 30
DEFAULT_REQUEST_DELAY_SECONDS = 1.0


def fetch_system_record(
    session: requests.Session, system_id64: int
) -> dict[str, Any]:
    """Return the record from Spansh's system endpoint."""
    response = session.get(
        f"{SPANSH_API_BASE}/system/{system_id64}", timeout=HTTP_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    payload = response.json()
    record = payload.get("record") if isinstance(payload, dict) else None
    if not isinstance(record, dict):
        raise ValueError("Unexpected response shape from Spansh system API")
    return record


def fetch_station_record(
    session: requests.Session, market_id: int
) -> dict[str, Any] | None:
    """Return a station record, or ``None`` when Spansh no longer has it."""
    response = session.get(
        f"{SPANSH_API_BASE}/station/{market_id}", timeout=HTTP_TIMEOUT_SECONDS
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    record = payload.get("record") if isinstance(payload, dict) else None
    return record if isinstance(record, dict) else None


def station_market_ids(system_record: dict[str, Any]) -> list[int]:
    """Extract distinct valid market IDs from a Spansh system record."""
    station_summaries = system_record.get("stations")
    if not isinstance(station_summaries, list):
        return []

    market_ids: list[int] = []
    seen: set[int] = set()
    for summary in station_summaries:
        if not isinstance(summary, dict):
            continue
        try:
            market_id = int(summary["market_id"])
        except (KeyError, TypeError, ValueError):
            continue
        if market_id < 0 or market_id in seen:
            continue
        seen.add(market_id)
        market_ids.append(market_id)
    return market_ids


def station_from_system_summary(
    summary: dict[str, Any], system_id64: int, system_updated_at: Any
) -> dict[str, Any] | None:
    """Normalize a system-listing entry when station detail is unavailable."""
    normalized = station_from_spansh(
        {
            "id": summary.get("market_id"),
            "name": summary.get("name"),
            "type": summary.get("type"),
            "distanceToArrival": summary.get("distance_to_arrival"),
            "landingPads": {
                "large": summary.get("large_pads"),
                "medium": summary.get("medium_pads"),
                "small": summary.get("small_pads"),
            },
            "services": summary.get("services"),
            "controllingFaction": summary.get("controlling_minor_faction"),
            "controllingFactionState": summary.get(
                "controlling_minor_faction_state"
            ),
        },
        system_id64,
        parse_timestamp(system_updated_at),
    )
    if normalized is not None:
        normalized["last_source"] = "spansh_system_api"
    return normalized


def station_system_id(cursor: Any, market_id: int) -> int | None:
    cursor.execute(
        "SELECT system_id64 FROM stations WHERE market_id = %s",
        (market_id,),
    )
    row = cursor.fetchone()
    return int(row[0]) if row is not None else None


def sync_system_stations(
    connection: Any,
    system_id64: int,
    *,
    apply: bool,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    session: requests.Session | None = None,
    summary_only: bool = False,
) -> dict[str, int]:
    """Fetch and upsert every station currently listed for ``system_id64``.

    A dry run makes no writes.  Applied upserts remain timestamp protected, so
    an older Spansh record cannot overwrite a newer EDDN observation.
    """
    if system_id64 <= 0:
        raise ValueError("system_id64 must be positive")
    if request_delay_seconds < 0:
        raise ValueError("request_delay_seconds cannot be negative")

    client = session or requests.Session()
    owns_session = session is None
    counts = {
        "listed": 0,
        "fetched": 0,
        "created": 0,
        "updated": 0,
        "not_found": 0,
        "invalid": 0,
        "moved": 0,
        "existing": 0,
        "located_elsewhere": 0,
        "summary_fallback": 0,
    }
    try:
        system_record = fetch_system_record(client, system_id64)
        market_ids = station_market_ids(system_record)
        summaries = {
            int(summary["market_id"]): summary
            for summary in system_record.get("stations") or []
            if isinstance(summary, dict)
            and isinstance(summary.get("market_id"), (int, str))
            and str(summary["market_id"]).isdigit()
        }
        counts["listed"] = len(market_ids)

        with connection.cursor() as cursor:
            for index, market_id in enumerate(market_ids):
                stored_system_id = station_system_id(cursor, market_id)
                # Existing rows already in the requested system were recently
                # reconciled separately, so avoid querying Spansh again.
                if stored_system_id == system_id64:
                    counts["existing"] += 1
                    continue
                # System station lists can lag behind carrier movement.  A
                # one-request summary import must never move a known carrier
                # back to this system on the strength of a stale listing.
                if summary_only and stored_system_id is not None:
                    counts["located_elsewhere"] += 1
                    continue

                normalized = None
                if not summary_only:
                    try:
                        record = fetch_station_record(client, market_id)
                    except requests.RequestException as exc:
                        print(f"Failed station [{market_id}]: {exc}")
                    else:
                        normalized = station_from_spansh_api(record) if record else None

                if normalized is None:
                    normalized = station_from_system_summary(
                        summaries.get(market_id, {}),
                        system_id64,
                        system_record.get("updated_at"),
                    )
                    if normalized is not None:
                        counts["summary_fallback"] += 1
                if normalized is None:
                    print(f"Skip station [{market_id}]: invalid system summary")
                    counts["invalid"] += 1
                    continue

                counts["fetched"] += 1
                current_system_id64 = int(normalized["system_id64"])
                if current_system_id64 != system_id64:
                    counts["moved"] += 1

                if apply:
                    is_new = upsert_station(cursor, normalized)
                else:
                        is_new = stored_system_id is None
                counts["created" if is_new else "updated"] += 1

                action = "Apply" if apply else "Would"
                location = (
                    "in system"
                    if current_system_id64 == system_id64
                    else f"now in system {current_system_id64}"
                )
                result = "create" if is_new else "refresh"
                print(
                    f"{action} {result} {normalized['name']} [{market_id}] {location}"
                )

                if index < len(market_ids) - 1 and request_delay_seconds:
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
        description="Synchronize every Spansh-listed station for one system."
    )
    parser.add_argument("system_id64", type=int, help="System id64 to synchronize")
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY_SECONDS,
        help="Seconds to wait between Spansh station requests (default: 1.0)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit updates; without this flag the command is a dry run",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Use only the single Spansh system response; do not call station endpoints",
    )
    args = parser.parse_args()

    from dotenv import load_dotenv

    load_dotenv()
    with open_database_connection() as connection:
        counts = sync_system_stations(
            connection,
            args.system_id64,
            apply=args.apply,
            request_delay_seconds=args.request_delay,
            summary_only=args.summary_only,
        )
    mode = "Applied" if args.apply else "Dry run"
    print(
        f"{mode}: {counts['listed']} listed, {counts['fetched']} fetched; "
        f"{counts['created']} created, {counts['existing']} already here, "
        f"{counts['located_elsewhere']} known elsewhere, "
        f"{counts['updated']} refreshed, "
        f"{counts['summary_fallback']} summary-only, {counts['moved']} moved during sync, "
        f"{counts['not_found']} unavailable, "
        f"{counts['invalid']} invalid/error(s)."
    )


if __name__ == "__main__":
    main()
