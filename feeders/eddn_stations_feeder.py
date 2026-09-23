"""Ingest station placement and basic metadata from EDDN journal events."""

from __future__ import annotations

import json
import os
import zlib
from datetime import datetime, timezone
from typing import Literal, NamedTuple, Optional

import psycopg
import zmq
from dotenv import load_dotenv

try:
    from feeders.station_ingestion import (
        station_from_eddn,
        system_allegiance_from_eddn,
        upsert_station,
        upsert_system_allegiance,
    )
except ModuleNotFoundError:  # Allow direct execution from feeders/.
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from feeders.station_ingestion import (
        station_from_eddn,
        system_allegiance_from_eddn,
        upsert_station,
        upsert_system_allegiance,
    )

TRUSTED_CLIENTS = {
    "EDDI", "EDDiscovery", "EDDLite", "E:D Market Connector [Linux]",
    "E:D Market Connector [Windows]", "EDO Materials Helper",
}
SUPPORTED_EVENTS = {"Docked", "Location", "CarrierJump", "FSDJump"}
INACTIVITY_TIMEOUT_SECONDS = int(os.getenv("EDDN_INACTIVITY_TIMEOUT", "900"))

load_dotenv()
conn = psycopg.connect(
    host=os.getenv("DB_HOST"), port=5432, dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
)
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://eddn.edcd.io:9500")
socket.setsockopt_string(zmq.SUBSCRIBE, "")


class StreamStalledError(RuntimeError):
    """Raised when no EDDN messages arrive before the watchdog expires."""


class ProcessOutcome(NamedTuple):
    status: Literal["success", "skipped"]
    reason: Optional[str] = None


def is_trusted_source(software_name: str | None) -> bool:
    return software_name in TRUSTED_CLIENTS


def record_stations_processed(cur, amount: int = 1, is_new: bool = False) -> None:
    bucket = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if is_new:
        cur.execute(
            """
            INSERT INTO eddn_stations_metrics (bucket, stations_processed, stations_new)
            VALUES (%s, 0, %s)
            ON CONFLICT (bucket) DO UPDATE
            SET stations_new = eddn_stations_metrics.stations_new + EXCLUDED.stations_new;
            """,
            (bucket, amount),
        )
    else:
        cur.execute(
            """
            INSERT INTO eddn_stations_metrics (bucket, stations_processed, stations_new)
            VALUES (%s, %s, 0)
            ON CONFLICT (bucket) DO UPDATE
            SET stations_processed = eddn_stations_metrics.stations_processed + EXCLUDED.stations_processed;
            """,
            (bucket, amount),
        )


def process_message(
    message: dict, *, connection: Optional[psycopg.Connection] = None,
    verbose: bool = True,
) -> ProcessOutcome:
    """Persist a station observation and/or sparse system allegiance."""
    header = message.get("header") or {}
    payload = message.get("message") or {}
    event = payload.get("event")
    if event not in SUPPORTED_EVENTS:
        return ProcessOutcome("skipped", "unsupported_event")
    if not is_trusted_source(header.get("softwareName")):
        return ProcessOutcome("skipped", "untrusted_source")

    db_conn = connection or conn
    station = None
    if event in {"Docked", "CarrierJump"}:
        station = station_from_eddn(payload)
    elif event == "Location" and payload.get("Docked") is True:
        station = station_from_eddn(payload)
    allegiance = system_allegiance_from_eddn(payload)
    if station is None and allegiance is None:
        return ProcessOutcome("skipped", "no_station_or_allegiance")

    with db_conn.cursor() as cursor:
        if allegiance is not None:
            system_id64, value, updated_at = allegiance
            upsert_system_allegiance(
                cursor, system_id64, value, updated_at, "eddn_journal"
            )
        if station is not None:
            is_new = upsert_station(cursor, station)
            record_stations_processed(cursor, is_new=is_new)
    db_conn.commit()
    if verbose and station is not None:
        print(
            f"{event}: {station['name']} [{station['market_id']}] "
            f"in {station['system_id64']}"
        )
    return ProcessOutcome("success")


def recv_with_watchdog(sock: zmq.Socket, timeout_seconds: int) -> bytes:
    if timeout_seconds <= 0:
        return sock.recv()
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    events = dict(poller.poll(timeout_seconds * 1000))
    if events.get(sock) == zmq.POLLIN:
        return sock.recv()
    raise StreamStalledError(
        f"No EDDN station events received in {timeout_seconds} seconds"
    )


def stream_events() -> None:
    try:
        while True:
            compressed = recv_with_watchdog(socket, INACTIVITY_TIMEOUT_SECONDS)
            process_message(json.loads(zlib.decompress(compressed).decode("utf-8")))
    except StreamStalledError as exc:
        print(f"Watchdog detected stalled EDDN stations feed: {exc}")
        raise SystemExit(2) from exc
    except KeyboardInterrupt:
        print("Stopping stations feeder listener...")
    finally:
        conn.close()
        socket.close(0)
        context.term()


if __name__ == "__main__":
    print("Listening for EDDN station events from trusted clients...")
    stream_events()
