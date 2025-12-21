import json
import os
import zlib
from datetime import datetime
from typing import Optional

import psycopg
import zmq
from barycenter_renamer import (
    insert_barycenter_first_pass,
    rename_barycenters_second_pass,
)
from dotenv import load_dotenv

# Load env before reading configuration
load_dotenv()

# Trusted clients list kept in sync with other feeders
TRUSTED_CLIENTS = {
    "EDDI",
    "EDDiscovery",
    "EDDLite",
    "E:D Market Connector [Linux]",
    "E:D Market Connector [Windows]",
    "EDO Materials Helper",
}

INACTIVITY_TIMEOUT_SECONDS = int(os.getenv("EDDN_INACTIVITY_TIMEOUT", "900"))
TARGET_EVENT = "FSSAllBodiesFound"

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def _connect_db() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST,
        port=5432,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


conn = _connect_db()

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://eddn.edcd.io:9500")
socket.setsockopt_string(zmq.SUBSCRIBE, "")

print(f"Listening for EDDN {TARGET_EVENT} events from trusted clients...")


class StreamStalledError(RuntimeError):
    """Raised when no events are received for an extended period."""


def recv_with_watchdog(sock: zmq.Socket, timeout_seconds: int) -> bytes:
    if timeout_seconds <= 0:
        return sock.recv()
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    events = dict(poller.poll(timeout_seconds * 1000))
    if events.get(sock) == zmq.POLLIN:
        return sock.recv()
    raise StreamStalledError(
        f"No EDDN barycenter events received in {timeout_seconds} seconds"
    )


def is_trusted_source(software_name: Optional[str]) -> bool:
    return software_name in TRUSTED_CLIENTS


def _parse_timestamp(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _run_barycenter_renamer(
    system_id64: int, system_name: Optional[str]
) -> None:
    global conn
    try:
        insert_barycenter_first_pass(conn, system_id64, debug=False)
        updates = rename_barycenters_second_pass(
            conn, system_id64, debug=False
        )
        conn.commit()  # finish any open transaction from SELECT-only runs
        if updates is None:
            renamed = 0
        elif isinstance(updates, int):
            renamed = updates
        else:
            try:
                renamed = len(updates)
            except Exception:
                renamed = 0
        system_label = system_name or str(system_id64)
        print(
            f"Barycenter renamer completed for {system_label} [{system_id64}] "
            f"({renamed} barycenter(s) renamed)"
        )
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(
            f"Error running barycenter renamer for {system_name or system_id64}: {exc}"
        )
        if conn.closed:
            conn = _connect_db()


def _handle_fss_all_bodies_found(msg_data: dict) -> None:
    system_address_raw = msg_data.get("SystemAddress")
    system_name = msg_data.get("StarSystem") or msg_data.get("SystemName")
    timestamp_raw = msg_data.get("timestamp")

    try:
        system_address = int(system_address_raw)
    except (TypeError, ValueError):
        return

    if _parse_timestamp(timestamp_raw) is None:
        return

    _run_barycenter_renamer(system_address, system_name)


def _process_event(message: dict) -> None:
    header = message.get("header", {})
    msg_data = message.get("message", {})
    event = msg_data.get("event")

    software_name = header.get("softwareName")
    if not is_trusted_source(software_name):
        return
    if event != TARGET_EVENT:
        return

    _handle_fss_all_bodies_found(msg_data)


def stream_events() -> None:
    try:
        while True:
            try:
                compressed = recv_with_watchdog(
                    socket, INACTIVITY_TIMEOUT_SECONDS
                )
                decompressed = zlib.decompress(compressed)
                message = json.loads(decompressed.decode("utf-8"))
                _process_event(message)
            except StreamStalledError:
                raise
            except Exception as exc:
                print(f"Error processing message: {exc}")
                continue
    except StreamStalledError as stalled:
        print(f"Watchdog detected stalled EDDN barycenter feed: {stalled}")
        raise SystemExit(2) from stalled
    except KeyboardInterrupt:
        print("Stopping barycenter EDDN listener...")
    finally:
        conn.close()
        socket.close(0)
        context.term()


if __name__ == "__main__":
    stream_events()
