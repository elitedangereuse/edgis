"""Shared, source-aware persistence for Spansh and EDDN station records."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any


# Operations Runner is a transient, game-managed object. These three reserved
# market IDs are reused rather than identifying persistent stations.
EXCLUDED_MARKET_IDS = frozenset({127000000, 127000256, 127000512})

STATION_UPSERT = """
    INSERT INTO stations (
        market_id, system_id64, body_id, body_name, attachment_source,
        parents,
        name, station_type, is_carrier, is_planetary,
        distance_from_arrival_ls, latitude, longitude,
        large_pads, medium_pads, small_pads, services, economies,
        primary_economy, government, allegiance, controlling_faction,
        controlling_faction_state, station_state,
        location_updated_at, details_updated_at, last_seen_at, last_source
    ) VALUES (
        %(market_id)s, %(system_id64)s, %(body_id)s, %(body_name)s,
        %(attachment_source)s, %(parents)s::jsonb,
        %(name)s, %(station_type)s, %(is_carrier)s,
        %(is_planetary)s, %(distance_from_arrival_ls)s, %(latitude)s,
        %(longitude)s, %(large_pads)s, %(medium_pads)s, %(small_pads)s,
        %(services)s::jsonb, %(economies)s::jsonb, %(primary_economy)s,
        %(government)s, %(allegiance)s, %(controlling_faction)s,
        %(controlling_faction_state)s, %(station_state)s,
        %(location_updated_at)s, %(details_updated_at)s, %(last_seen_at)s,
        %(last_source)s
    )
    ON CONFLICT (market_id) DO UPDATE SET
        system_id64 = CASE
            WHEN EXCLUDED.location_updated_at >= COALESCE(stations.location_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.system_id64 ELSE stations.system_id64 END,
        body_id = CASE
            WHEN EXCLUDED.location_updated_at >= COALESCE(stations.location_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.body_id ELSE stations.body_id END,
        body_name = CASE
            WHEN EXCLUDED.location_updated_at >= COALESCE(stations.location_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.body_name ELSE stations.body_name END,
        attachment_source = CASE
            WHEN EXCLUDED.location_updated_at >= COALESCE(stations.location_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.attachment_source ELSE stations.attachment_source END,
        parents = CASE
            WHEN jsonb_array_length(EXCLUDED.parents) > 0
             AND EXCLUDED.location_updated_at >= COALESCE(stations.location_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.parents ELSE stations.parents END,
        location_updated_at = GREATEST(stations.location_updated_at, EXCLUDED.location_updated_at),
        name = CASE
            WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.name ELSE stations.name END,
        station_type = CASE
            WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.station_type ELSE stations.station_type END,
        is_carrier = CASE
            WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.is_carrier ELSE stations.is_carrier END,
        is_planetary = CASE
            WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz)
            THEN EXCLUDED.is_planetary ELSE stations.is_planetary END,
        distance_from_arrival_ls = CASE
            WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz)
            THEN COALESCE(EXCLUDED.distance_from_arrival_ls, stations.distance_from_arrival_ls)
            ELSE stations.distance_from_arrival_ls END,
        latitude = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.latitude, stations.latitude) ELSE stations.latitude END,
        longitude = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.longitude, stations.longitude) ELSE stations.longitude END,
        large_pads = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.large_pads, stations.large_pads) ELSE stations.large_pads END,
        medium_pads = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.medium_pads, stations.medium_pads) ELSE stations.medium_pads END,
        small_pads = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.small_pads, stations.small_pads) ELSE stations.small_pads END,
        services = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN EXCLUDED.services ELSE stations.services END,
        economies = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN EXCLUDED.economies ELSE stations.economies END,
        primary_economy = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.primary_economy, stations.primary_economy) ELSE stations.primary_economy END,
        government = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.government, stations.government) ELSE stations.government END,
        allegiance = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.allegiance, stations.allegiance) ELSE stations.allegiance END,
        controlling_faction = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.controlling_faction, stations.controlling_faction) ELSE stations.controlling_faction END,
        controlling_faction_state = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.controlling_faction_state, stations.controlling_faction_state) ELSE stations.controlling_faction_state END,
        station_state = CASE WHEN EXCLUDED.details_updated_at >= COALESCE(stations.details_updated_at, '-infinity'::timestamptz) THEN COALESCE(EXCLUDED.station_state, stations.station_state) ELSE stations.station_state END,
        details_updated_at = GREATEST(stations.details_updated_at, EXCLUDED.details_updated_at),
        last_seen_at = GREATEST(stations.last_seen_at, EXCLUDED.last_seen_at),
        last_source = CASE WHEN EXCLUDED.last_seen_at >= stations.last_seen_at THEN EXCLUDED.last_source ELSE stations.last_source END
    RETURNING (xmax = 0) AS is_new;
"""

SYSTEM_ALLEGIANCE_UPSERT = """
    INSERT INTO system_allegiances (system_id64, allegiance, updated_at, source)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (system_id64) DO UPDATE SET
        allegiance = EXCLUDED.allegiance,
        updated_at = EXCLUDED.updated_at,
        source = EXCLUDED.source
    WHERE EXCLUDED.updated_at >= system_allegiances.updated_at;
"""


def parse_timestamp(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def normalize_token(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if stripped.startswith("$") and stripped.endswith(";"):
        stripped = stripped[1:-1]
        if "_" in stripped:
            stripped = stripped.split("_", 1)[1]
        stripped = stripped.replace("_", " ")
    return stripped or None


def normalize_allegiance(value: Any) -> str | None:
    allegiance = normalize_token(value)
    if not allegiance or allegiance.lower() in {"none", "null"}:
        return None
    return allegiance


def _station_faction(value: Any) -> str | None:
    if isinstance(value, dict):
        return value.get("Name") or value.get("name")
    return value if isinstance(value, str) else None


def _journal_station_name(message: dict[str, Any]) -> str | None:
    """Return a displayable station name from a Journal/EDDN message.

    Journal events may contain an internal localisation token in
    ``StationName`` and its translated text in ``StationName_Localised``.
    Keep player-provided names verbatim, but turn the known generated-name
    token shape into a readable fallback when the translated field is absent.
    """
    localized = message.get("StationName_Localised")
    if isinstance(localized, str) and localized.strip():
        return localized.strip()

    raw_name = message.get("StationName")
    if not isinstance(raw_name, str) or not raw_name.strip():
        return None
    raw_name = raw_name.strip()
    if not raw_name.startswith("$"):
        return raw_name

    token = raw_name[1:].split(":", 1)[0]
    if token.endswith("_Name"):
        fallback = re.sub(r"_+", " ", token[:-5]).strip()
        return fallback or None
    return None


def _economies_from_journal(entries: Any) -> dict[str, float]:
    if not isinstance(entries, list):
        return {}
    economies: dict[str, float] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = normalize_token(entry.get("Name") or entry.get("name"))
        share = entry.get("Proportion", entry.get("share"))
        if not name or share is None:
            continue
        try:
            economies[name] = float(share) * (100 if float(share) <= 1 else 1)
        except (TypeError, ValueError):
            continue
    return economies


def _is_planetary(station_type: str | None) -> bool:
    normalized = (station_type or "").lower()
    return any(token in normalized for token in ("planet", "settlement", "surface"))


def _is_carrier(station_type: str | None) -> bool:
    return "carrier" in (station_type or "").lower()


def station_from_spansh(
    station: dict[str, Any], system_id64: int, system_updated_at: datetime | None,
    *, body_id: int | None = None, body_name: str | None = None,
) -> dict[str, Any] | None:
    market_id = station.get("id")
    name = station.get("name")
    station_type = station.get("type")
    if market_id is None or not name or not station_type:
        return None
    market_id = int(market_id)
    if market_id in EXCLUDED_MARKET_IDS:
        return None
    updated_at = parse_timestamp(station.get("updateTime")) or system_updated_at
    if updated_at is None:
        return None
    pads = station.get("landingPads") or {}
    source_body_name = body_name or station.get("bodyName")
    return {
        "market_id": market_id, "system_id64": system_id64,
        "body_id": body_id, "body_name": source_body_name,
        "attachment_source": (
            "spansh_body_id" if body_id is not None
            else "spansh_body_name" if source_body_name else "unresolved"
        ),
        "parents": json.dumps([]),
        "name": str(name), "station_type": str(station_type),
        "is_carrier": _is_carrier(station_type),
        "is_planetary": _is_planetary(station_type),
        "distance_from_arrival_ls": station.get("distanceToArrival"),
        "latitude": station.get("latitude"), "longitude": station.get("longitude"),
        "large_pads": pads.get("large"), "medium_pads": pads.get("medium"),
        "small_pads": pads.get("small"),
        "services": json.dumps(station.get("services") or []),
        "economies": json.dumps(station.get("economies") or {}),
        "primary_economy": station.get("primaryEconomy"),
        "government": station.get("government"),
        "allegiance": normalize_allegiance(station.get("allegiance")),
        "controlling_faction": station.get("controllingFaction"),
        "controlling_faction_state": station.get("controllingFactionState"),
        "station_state": station.get("state"),
        "location_updated_at": updated_at, "details_updated_at": updated_at,
        "last_seen_at": updated_at, "last_source": "spansh_dump",
    }


def station_from_eddn(message: dict[str, Any]) -> dict[str, Any] | None:
    market_id = message.get("MarketID")
    system_id64 = message.get("SystemAddress")
    name = _journal_station_name(message)
    station_type = message.get("StationType")
    updated_at = parse_timestamp(message.get("timestamp"))
    if (
        market_id is None or system_id64 is None or not name or not station_type
        or updated_at is None
    ):
        return None
    market_id = int(market_id)
    if market_id in EXCLUDED_MARKET_IDS:
        return None
    pads = message.get("LandingPads") or {}
    body_id = message.get("BodyID")
    body_name = message.get("Body")
    return {
        "market_id": market_id, "system_id64": int(system_id64),
        "body_id": int(body_id) if body_id is not None else None,
        "body_name": body_name,
        "attachment_source": "eddn_body_id" if body_id is not None else "unresolved",
        "parents": json.dumps([]),
        "name": str(name), "station_type": str(station_type),
        "is_carrier": _is_carrier(station_type),
        "is_planetary": _is_planetary(station_type),
        "distance_from_arrival_ls": message.get("DistFromStarLS"),
        "latitude": message.get("Latitude"), "longitude": message.get("Longitude"),
        "large_pads": pads.get("Large"), "medium_pads": pads.get("Medium"),
        "small_pads": pads.get("Small"),
        "services": json.dumps(message.get("StationServices") or []),
        "economies": json.dumps(_economies_from_journal(message.get("StationEconomies"))),
        "primary_economy": normalize_token(message.get("StationEconomy")),
        "government": normalize_token(message.get("StationGovernment")),
        "allegiance": normalize_allegiance(message.get("StationAllegiance")),
        "controlling_faction": _station_faction(message.get("StationFaction")),
        "controlling_faction_state": normalize_token(message.get("FactionState")),
        "station_state": message.get("StationState"),
        "location_updated_at": updated_at, "details_updated_at": updated_at,
        "last_seen_at": updated_at, "last_source": "eddn_journal",
    }


def reconstruct_station_parents(
    cursor: Any, station: dict[str, Any], host_body_name: Any,
) -> bool:
    """Populate a station's parent chain from the body named by Docked.Body.

    ``stations.body_id`` remains the station's own BodyID.  The first parent
    instead identifies the celestial body that hosts the station.
    """
    if not isinstance(host_body_name, str) or not host_body_name.strip():
        return False

    cursor.execute(
        """
        SELECT b.body_id, bt.name, b.parents
        FROM bodies b
        INNER JOIN body_types bt ON bt.id = b.body_type_id
        WHERE b.system_id64 = %s
          AND LOWER(b.body_name) = LOWER(%s)
        LIMIT 1
        """,
        (station["system_id64"], host_body_name.strip()),
    )
    host = cursor.fetchone()
    if host is None:
        return False

    host_body_id, host_type, host_parents = host
    if host_body_id is None or not isinstance(host_type, str) or not host_type:
        return False
    if isinstance(host_parents, str):
        try:
            host_parents = json.loads(host_parents)
        except json.JSONDecodeError:
            host_parents = []
    if not isinstance(host_parents, list):
        host_parents = []

    station["parents"] = json.dumps([{host_type: int(host_body_id)}, *host_parents])
    return True


def system_allegiance_from_eddn(message: dict[str, Any]) -> tuple[int, str, datetime] | None:
    system_id64 = message.get("SystemAddress")
    updated_at = parse_timestamp(message.get("timestamp"))
    allegiance = normalize_allegiance(message.get("SystemAllegiance"))
    if system_id64 is None or allegiance is None or updated_at is None:
        return None
    return int(system_id64), allegiance, updated_at


def upsert_station(cursor: Any, station: dict[str, Any]) -> bool:
    cursor.execute(STATION_UPSERT, station)
    result = cursor.fetchone()
    return bool(result and result[0])


def upsert_system_allegiance(
    cursor: Any, system_id64: int, allegiance: str, updated_at: datetime, source: str
) -> None:
    cursor.execute(SYSTEM_ALLEGIANCE_UPSERT, (system_id64, allegiance, updated_at, source))
