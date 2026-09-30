"""Normalization and persistence for durable EDDN FSS signals."""

from __future__ import annotations

from datetime import datetime
from typing import Any


SIGNAL_UPSERT = """
    INSERT INTO signals (
        system_id64, signal_name, signal_type, is_station,
        first_seen_at, last_seen_at, last_source
    ) VALUES (
        %(system_id64)s, %(signal_name)s, %(signal_type)s, %(is_station)s,
        %(observed_at)s, %(observed_at)s, %(last_source)s
    )
    ON CONFLICT (system_id64, signal_name) DO UPDATE SET
        signal_type = CASE
            WHEN EXCLUDED.last_seen_at >= signals.last_seen_at
            THEN COALESCE(EXCLUDED.signal_type, signals.signal_type)
            ELSE signals.signal_type
        END,
        is_station = signals.is_station OR EXCLUDED.is_station,
        first_seen_at = LEAST(signals.first_seen_at, EXCLUDED.first_seen_at),
        last_seen_at = GREATEST(signals.last_seen_at, EXCLUDED.last_seen_at),
        last_source = CASE
            WHEN EXCLUDED.last_seen_at >= signals.last_seen_at
            THEN EXCLUDED.last_source ELSE signals.last_source
        END
    RETURNING (xmax = 0) AS is_new;
"""


def parse_timestamp(value: Any) -> datetime | None:
    """Parse the ISO 8601 timestamp emitted by EDDN."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def signal_rows_from_eddn(message: dict[str, Any]) -> list[dict[str, Any]]:
    """Return persistent FSS signals represented by an EDDN message.

    USS records are intentionally excluded: they are transient and EDDN omits
    their expiry time.  The remaining named FSS signals are stable enough to
    retain at system scope, even though FSS does not provide a physical
    location or host body.
    """
    try:
        system_id64 = int(message["SystemAddress"])
    except (KeyError, TypeError, ValueError):
        return []

    signals = message.get("signals")
    if not isinstance(signals, list):
        return []

    rows = []
    message_timestamp = message.get("timestamp")
    for signal in signals:
        if not isinstance(signal, dict) or signal.get("USSType"):
            continue
        name = signal.get("SignalName")
        if not isinstance(name, str) or not (name := name.strip()):
            continue
        observed_at = parse_timestamp(signal.get("timestamp") or message_timestamp)
        if observed_at is None:
            continue
        signal_type = signal.get("SignalType")
        rows.append(
            {
                "system_id64": system_id64,
                "signal_name": name,
                "signal_type": (
                    signal_type.strip()
                    if isinstance(signal_type, str) and signal_type.strip()
                    else None
                ),
                "is_station": signal.get("IsStation") is True,
                "observed_at": observed_at,
                "last_source": "eddn_fss",
            }
        )
    return rows


def upsert_signal(cursor: Any, signal: dict[str, Any]) -> bool:
    """Persist a signal observation and return whether it was newly created."""
    cursor.execute(SIGNAL_UPSERT, signal)
    result = cursor.fetchone()
    return bool(result and result[0])
