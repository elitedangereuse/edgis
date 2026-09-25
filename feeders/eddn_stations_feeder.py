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
        reconstruct_station_parents,
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
        reconstruct_station_parents,
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
PARENT_LOOKUP_FIELDS = (
    "Body",
    "BodyName",
    "BodyID",
    "Parents",
    "StationName",
    "StationName_Localised",
    "StationType",
    "DistFromStarLS",
    "Latitude",
    "Longitude",
)

load_dotenv()


def open_database_connection() -> psycopg.Connection:
    """Create the connection used by the live EDDN listener."""
    return psycopg.connect(
        host=os.getenv("DB_HOST"), port=5432, dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
    )


def open_subscription() -> tuple[zmq.Context, zmq.Socket]:
    """Connect a subscriber only when the live listener is started."""
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://eddn.edcd.io:9500")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    return context, socket


class StreamStalledError(RuntimeError):
    """Raised when no EDDN messages arrive before the watchdog expires."""


class ProcessOutcome(NamedTuple):
    status: Literal["success", "skipped"]
    reason: Optional[str] = None


def is_trusted_source(software_name: str | None) -> bool:
    return software_name in TRUSTED_CLIENTS


def system_name_from_message(message: dict) -> str | None:
    """Return the Journal-provided system name when the event supplies one."""
    system_name = message.get("StarSystem") or message.get("SystemName")
    if not isinstance(system_name, str):
        return None
    system_name = system_name.strip()
    return system_name or None


def parent_lookup_context(message: dict) -> str:
    """Format only Journal fields useful for resolving a station host."""
    fields = []
    for field in PARENT_LOOKUP_FIELDS:
        value = message.get(field, "<missing>")
        if value == "<missing>":
            fields.append(f"{field}=<missing>")
        else:
            fields.append(f"{field}={value!r}")
    return "  parent lookup context: " + ", ".join(fields)


def host_body_name_from_message(message: dict, station: dict) -> str | None:
    """Prefer an explicit host body name over a Body value naming the station."""
    body_name = message.get("BodyName")
    if isinstance(body_name, str) and body_name.strip():
        return body_name.strip()
    body = message.get("Body")
    if not isinstance(body, str) or not body.strip():
        return None
    if body.strip().casefold() == str(station["name"]).strip().casefold():
        return None
    return body.strip()


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
    message: dict,
    *,
    connection: Optional[psycopg.Connection] = None,
    verbose: bool = True,
    commit: bool = True,
    record_metrics: bool = True,
) -> ProcessOutcome:
    """Persist a station observation and/or sparse system allegiance."""
    header = message.get("header") or {}
    payload = message.get("message") or {}
    event = payload.get("event")
    if event not in SUPPORTED_EVENTS:
        return ProcessOutcome("skipped", "unsupported_event")
    if not is_trusted_source(header.get("softwareName")):
        return ProcessOutcome("skipped", "untrusted_source")

    owns_connection = connection is None
    db_conn = connection or open_database_connection()
    station = None
    parents_reconstructed = False
    host_body_name = None
    try:
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
                if event in {"Docked", "Location"}:
                    host_body_name = host_body_name_from_message(payload, station)
                    parents_reconstructed = reconstruct_station_parents(
                        cursor,
                        station,
                        host_body_name,
                        distance_from_arrival_ls=payload.get("DistFromStarLS"),
                        verbose=False,
                    )
                is_new = upsert_station(cursor, station)
                if record_metrics:
                    record_stations_processed(cursor, is_new=is_new)
        if commit:
            db_conn.commit()
    finally:
        if owns_connection:
            close = getattr(db_conn, "close", None)
            if close is not None:
                close()
    if verbose and station is not None:
        system_name = system_name_from_message(payload)
        system_label = (
            f"{system_name} [{station['system_id64']}]"
            if system_name
            else str(station["system_id64"])
        )
        print(
            f"{event}: {station['name']} [{station['market_id']}] "
            f"in {system_label}"
        )
        if event in {"Docked", "Location"}:
            if parents_reconstructed:
                resolution = station.get("_parent_resolution") or {}
                if resolution.get("source") == "arrival_distance":
                    print(
                        f"  parents: {json.loads(station['parents'])} "
                        f"(inferred from {resolution['body_name']} at "
                        f"{resolution['body_distance']:.6f} ls; "
                        f"delta {resolution['distance_delta']:.6f} ls)"
                    )
                else:
                    print(f"  parents: {json.loads(station['parents'])}")
            else:
                host_label = host_body_name if host_body_name is not None else "<none>"
                print(f"  parents: can't find host {host_label!r}")
                print(parent_lookup_context(payload))
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
    conn = open_database_connection()
    context, socket = open_subscription()
    try:
        while True:
            compressed = recv_with_watchdog(socket, INACTIVITY_TIMEOUT_SECONDS)
            process_message(
                json.loads(zlib.decompress(compressed).decode("utf-8")),
                connection=conn,
            )
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
