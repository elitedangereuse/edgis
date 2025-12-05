"""Ingest a single Spansh system by id64 via the public API."""

from __future__ import annotations

import argparse
import json
from collections import deque
from datetime import datetime
from typing import Any

import requests
from psycopg import Connection as PGConnection
from requests import HTTPError

try:
    from feeders.spansh_dump_bodies_ingestor import (
        SpanshBodyIngestSession,
        conn,
        parse_timestamp,
    )
except (
    ModuleNotFoundError
):  # Allow running as a script from repo root or feeders/
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from feeders.spansh_dump_bodies_ingestor import (
        SpanshBodyIngestSession,
        conn,
        parse_timestamp,
    )

API_BASE = "https://spansh.co.uk/api"


def fetch_json(path: str) -> dict[str, Any]:
    response = requests.get(f"{API_BASE}{path}", timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected response shape from Spansh API")
    return payload


def map_name_share_sequence(entries: Any) -> dict[str, float]:
    result: dict[str, float] = {}
    if isinstance(entries, dict):
        for name, value in entries.items():
            if name and value is not None:
                result[str(name)] = float(value)
        return result
    if isinstance(entries, list):
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            share = entry.get("share")
            if name and share is not None:
                result[str(name)] = float(share)
    return result


def map_parents(parents: Any) -> list[dict[str, int]] | None:
    if not isinstance(parents, list):
        return None
    mapped: list[dict[str, int]] = []
    for parent in parents:
        if not isinstance(parent, dict):
            continue
        ptype = parent.get("type")
        pid = parent.get("id64")
        if ptype and pid is not None:
            mapped.append({ptype: pid})
    return mapped or None


def remap_parent_ids(
    parents: list[dict[str, int]] | None, id_map: dict[int, int]
) -> list[dict[str, int]] | None:
    if not parents:
        return None
    remapped: list[dict[str, int]] = []
    for parent in parents:
        if not isinstance(parent, dict):
            continue
        key, value = next(iter(parent.items()))
        if value is None:
            continue
        mapped_value = id_map.get(int(value))
        if mapped_value is None:
            continue
        remapped.append({key: mapped_value})
    return remapped or None


def map_rings(rings: Any) -> list[dict[str, Any]]:
    if not isinstance(rings, list):
        return []
    normalized: list[dict[str, Any]] = []
    for ring in rings:
        if not isinstance(ring, dict):
            continue
        normalized.append(
            {
                "name": ring.get("name"),
                "type": ring.get("class") or ring.get("type"),
                "innerRadius": ring.get("inner_radius"),
                "outerRadius": ring.get("outer_radius"),
                "mass": ring.get("mass") or ring.get("mass_mt"),
                "updateTime": ring.get("updated_at"),
            }
        )
    return normalized


def normalize_body(
    detail: dict[str, Any], summary: dict[str, Any]
) -> dict[str, Any]:
    """Convert API payload into the dump-style body shape."""

    body = detail.copy()
    # Fill occasionally missing summary fields.
    for key in ("subtype", "terraforming_state", "distance_to_arrival"):
        body.setdefault(key, summary.get(key))

    normalized: dict[str, Any] = {
        "bodyId": body.get("body_id"),
        "name": body.get("name"),
        "type": body.get("type"),
        "subType": body.get("subtype"),
        "distanceToArrival": body.get("distance_to_arrival"),
        "isLandable": body.get("is_landable"),
        "isMainStar": body.get("is_main_star"),
        "terraformingState": body.get("terraforming_state"),
        "atmosphereType": body.get("atmosphere"),
        "atmosphereComposition": map_name_share_sequence(
            body.get("atmosphere_composition")
        ),
        "materials": map_name_share_sequence(body.get("materials")),
        "solidComposition": map_name_share_sequence(
            body.get("solid_composition")
        ),
        "radius": body.get("radius"),
        "earthMasses": body.get("earth_masses"),
        "gravity": body.get("gravity"),
        "surfaceTemperature": body.get("surface_temperature"),
        "surfacePressure": body.get("surface_pressure"),
        "volcanismType": body.get("volcanism_type"),
        "axialTilt": body.get("axis_tilt"),
        "rotationalPeriod": body.get("rotational_period"),
        "rotationalPeriodTidallyLocked": body.get(
            "is_rotational_period_tidally_locked"
        ),
        "semiMajorAxis": body.get("semi_major_axis"),
        "orbitalEccentricity": body.get("orbital_eccentricity"),
        "orbitalInclination": body.get("orbital_inclination"),
        "argOfPeriapsis": body.get("arg_of_periapsis"),
        "meanAnomaly": body.get("mean_anomaly"),
        "orbitalPeriod": body.get("orbital_period"),
        "ascendingNode": body.get("ascending_node"),
        "age": body.get("age"),
        "absoluteMagnitude": body.get("absolute_magnitude"),
        "luminosity": body.get("luminosity_class"),
        "solarMasses": body.get("solar_masses"),
        "solarRadius": body.get("solar_radius"),
        "spectralClass": body.get("spectral_class"),
        "parents": map_parents(body.get("parents")),
        "rings": map_rings(body.get("rings")),
        "updateTime": body.get("updated_at"),
    }
    return normalized


def ingest_system(
    system_id64: int,
    verbose: bool = False,
    connection: PGConnection | None = None,
) -> None:
    system_payload = fetch_json(f"/system/{system_id64}")
    system_record = system_payload.get("record") or {}
    bodies = system_record.get("bodies") or []
    if not bodies:
        raise SystemExit(f"No bodies found for system {system_id64}.")

    session_conn = connection or conn
    session = SpanshBodyIngestSession(session_conn, verbose=verbose)
    system_updated = parse_timestamp(system_record.get("updated_at"))
    id64_to_body_id: dict[int, int] = {}
    pending_bodies: list[tuple[dict[str, Any], datetime | None]] = []
    queue: deque[tuple[int, dict[str, Any]]] = deque()
    enqueued: set[int] = set()

    for summary in bodies:
        body_id64 = summary.get("id64") or summary.get("id")
        if body_id64 is None:
            continue
        try:
            body_id64_int = int(body_id64)
        except (TypeError, ValueError):
            continue
        queue.append((body_id64_int, summary))
        enqueued.add(body_id64_int)

    try:
        while queue:
            body_id64, summary = queue.popleft()
            try:
                detail_payload = fetch_json(f"/body/{body_id64}")
            except HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    if verbose:
                        print(f"Skipping parent {body_id64}: not found on Spansh")
                    continue
                raise
            detail_record = detail_payload.get("record") or {}
            normalized = normalize_body(detail_record, summary or {})
            if normalized.get("bodyId") is None:
                if verbose:
                    print(
                        "Summary payload:",
                        json.dumps(summary, indent=2, sort_keys=True),
                    )
                    print(
                        "Detail payload:",
                        json.dumps(detail_record, indent=2, sort_keys=True),
                    )
                raise ValueError(
                    f"Missing body id for {normalized.get('name')} ({body_id64})."
                )

            detail_id64 = detail_record.get("id64") or body_id64
            if detail_id64 is not None:
                try:
                    id64_key = int(detail_id64)
                    id64_to_body_id[id64_key] = int(normalized["bodyId"])
                except (TypeError, ValueError):
                    pass

            body_updatetime = (
                parse_timestamp(detail_record.get("updated_at")) or system_updated
            )
            pending_bodies.append((normalized, body_updatetime))

            for parent_info in detail_record.get("parents") or []:
                parent_id64 = parent_info.get("id64")
                if parent_id64 is None:
                    continue
                try:
                    parent_id64_int = int(parent_id64)
                except (TypeError, ValueError):
                    continue
                if parent_id64_int in enqueued:
                    continue
                queue.append((parent_id64_int, {}))
                enqueued.add(parent_id64_int)

        for normalized, body_updatetime in pending_bodies:
            normalized["parents"] = remap_parent_ids(
                normalized.get("parents"), id64_to_body_id
            )
            if verbose:
                print(
                    "Normalized body:",
                    json.dumps(normalized, indent=2, sort_keys=True),
                )
            session.process_body(normalized, system_id64, body_updatetime)

        session.flush_batches()
        session_conn.commit()
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upsert a single system from the Spansh API into the database."
    )
    parser.add_argument("system_id64", type=int, help="System id64 to ingest")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print parsed payloads and UPSERT values",
    )
    args = parser.parse_args()
    ingest_system(args.system_id64, verbose=args.verbose)


if __name__ == "__main__":
    main()
