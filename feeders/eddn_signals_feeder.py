"""Ingest durable FSS signals from EDDN into the signals table."""

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
    from feeders.signal_ingestion import signal_rows_from_eddn, upsert_signal
except ModuleNotFoundError:  # Allow direct execution from feeders/.
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from feeders.signal_ingestion import signal_rows_from_eddn, upsert_signal


load_dotenv()

TRUSTED_CLIENTS = {
    "EDDI", "EDDiscovery", "EDDLite", "E:D Market Connector [Linux]",
    "E:D Market Connector [Windows]", "EDO Materials Helper",
}
TARGET_EVENT = "FSSSignalDiscovered"
INACTIVITY_TIMEOUT_SECONDS = int(os.getenv("EDDN_INACTIVITY_TIMEOUT", "900"))


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
    signals_processed: int = 0
    signals_new: int = 0


def is_trusted_source(software_name: str | None) -> bool:
    return software_name in TRUSTED_CLIENTS


def record_signals_processed(cur, amount: int = 1, is_new: bool = False) -> None:
    """Record one or more signal observations in the minute metrics bucket."""
    bucket = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if is_new:
        cur.execute(
            """
            INSERT INTO eddn_signals_metrics (bucket, signals_processed, signals_new)
            VALUES (%s, 0, %s)
            ON CONFLICT (bucket) DO UPDATE
            SET signals_new = eddn_signals_metrics.signals_new + EXCLUDED.signals_new;
            """,
            (bucket, amount),
        )
    else:
        cur.execute(
            """
            INSERT INTO eddn_signals_metrics (bucket, signals_processed, signals_new)
            VALUES (%s, %s, 0)
            ON CONFLICT (bucket) DO UPDATE
            SET signals_processed = eddn_signals_metrics.signals_processed
                + EXCLUDED.signals_processed;
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
    """Persist durable FSS signal observations from one EDDN message."""
    header = message.get("header") or {}
    payload = message.get("message") or {}
    if payload.get("event") != TARGET_EVENT:
        return ProcessOutcome("skipped", "unsupported_event")
    if not is_trusted_source(header.get("softwareName")):
        return ProcessOutcome("skipped", "untrusted_source")

    signals = signal_rows_from_eddn(payload)
    if not signals:
        return ProcessOutcome("skipped", "no_durable_signals")

    owns_connection = connection is None
    db_conn = connection or open_database_connection()
    signals_new = 0
    try:
        with db_conn.cursor() as cursor:
            for signal in signals:
                is_new = upsert_signal(cursor, signal)
                signals_new += is_new
                if record_metrics:
                    record_signals_processed(cursor, is_new=is_new)
        if commit:
            db_conn.commit()
    finally:
        if owns_connection:
            close = getattr(db_conn, "close", None)
            if close is not None:
                close()

    if verbose:
        for signal in signals:
            label = signal["signal_type"] or "signal"
            print(
                f"FSS: {signal['signal_name']} ({label}) "
                f"in {signal['system_id64']}"
            )
    return ProcessOutcome(
        "success", signals_processed=len(signals), signals_new=signals_new
    )


def recv_with_watchdog(sock: zmq.Socket, timeout_seconds: int) -> bytes:
    if timeout_seconds <= 0:
        return sock.recv()
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    events = dict(poller.poll(timeout_seconds * 1000))
    if events.get(sock) == zmq.POLLIN:
        return sock.recv()
    raise StreamStalledError(
        f"No EDDN signal events received in {timeout_seconds} seconds"
    )


def stream_events() -> None:
    """Run the live EDDN signal listener until interrupted or stalled."""
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
        print(f"Watchdog detected stalled EDDN signals feed: {exc}")
        raise SystemExit(2) from exc
    except KeyboardInterrupt:
        print("Stopping signals EDDN listener...")
    finally:
        conn.close()
        socket.close(0)
        context.term()


if __name__ == "__main__":
    print("Listening for EDDN FSS signals from trusted clients...")
    stream_events()
