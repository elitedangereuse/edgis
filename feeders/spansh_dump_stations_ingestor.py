"""Stream Spansh's galaxy dump into the sparse stations tables."""

from __future__ import annotations

import argparse
import gzip
import os
from typing import Any

import ijson
import psycopg
from dotenv import load_dotenv
from tqdm import tqdm

try:
    from feeders.station_ingestion import (
        normalize_allegiance,
        parse_timestamp,
        station_parent_chain,
        station_from_spansh,
        upsert_station,
        upsert_system_allegiance,
    )
except ModuleNotFoundError:  # Allow direct execution from feeders/.
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from feeders.station_ingestion import (
        normalize_allegiance,
        parse_timestamp,
        station_parent_chain,
        station_from_spansh,
        upsert_station,
        upsert_system_allegiance,
    )

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
COMMIT_EVERY_SYSTEMS = 1_000


def _ingest_station(
    cursor: Any, station: dict[str, Any], system_id64: int,
    system_updated_at, *, body_id: int | None = None,
    body_name: str | None = None, parents: list[dict[str, Any]] | None = None,
) -> bool:
    normalized = station_from_spansh(
        station, system_id64, system_updated_at,
        body_id=body_id, body_name=body_name, parents=parents,
    )
    return bool(normalized and upsert_station(cursor, normalized))


def ingest_streaming(path: str) -> tuple[int, int]:
    """Return the number of stations seen and newly created."""
    total_bytes = os.path.getsize(path)
    seen = 0
    created = 0
    with psycopg.connect(
        host=DB_HOST, port=5432, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD,
    ) as connection:
        with connection.cursor() as cursor, gzip.open(path, "rb") as stream:
            with tqdm(
                total=total_bytes, unit="B", unit_scale=True,
                desc="Ingesting stations",
            ) as progress:
                for system_count, system in enumerate(ijson.items(stream, "item"), 1):
                    system_id64 = system.get("id64")
                    if system_id64 is None:
                        continue
                    system_id64 = int(system_id64)
                    updated_at = parse_timestamp(system.get("date"))
                    allegiance = normalize_allegiance(system.get("allegiance"))
                    if allegiance and updated_at:
                        upsert_system_allegiance(
                            cursor, system_id64, allegiance, updated_at,
                            "spansh_dump",
                        )

                    # The system-level list includes every station. Some
                    # dump variants also nest surface stations in their body;
                    # process both, letting market_id make this idempotent.
                    for station in system.get("stations") or []:
                        if _ingest_station(cursor, station, system_id64, updated_at):
                            created += 1
                        seen += 1
                    for body in system.get("bodies") or []:
                        body_id = body.get("bodyId")
                        try:
                            body_id = int(body_id) if body_id is not None else None
                        except (TypeError, ValueError):
                            body_id = None
                        parents = station_parent_chain(
                            body_id, body.get("type"), body.get("parents")
                        )
                        for station in body.get("stations") or []:
                            if _ingest_station(
                                cursor, station, system_id64, updated_at,
                                body_name=body.get("name"), parents=parents,
                            ):
                                created += 1
                            seen += 1
                    if system_count % COMMIT_EVERY_SYSTEMS == 0:
                        connection.commit()
                    progress.update(stream.tell() - progress.n)
        connection.commit()
    return seen, created


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Spansh dump stations and sparse allegiances."
    )
    parser.add_argument("path", help="Path to a Spansh galaxy JSON gzip dump")
    args = parser.parse_args()
    seen, created = ingest_streaming(args.path)
    print(f"Done. Inserted/updated {seen} stations ({created} new).")


if __name__ == "__main__":
    main()
