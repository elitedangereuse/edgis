import os
import logging
import httpx
from decimal import Decimal
from fastapi import HTTPException
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg
from pydantic import BaseModel
from typing import Optional, Any

try:
    from .edts.edtslib import system  # type: ignore[attr-defined]
except ImportError:
    import sys
    from pathlib import Path

    _local_edts = Path(__file__).resolve().parent / "edts"
    if _local_edts.exists():
        sys.path.insert(0, str(_local_edts))
        from edtslib import system  # type: ignore[attr-defined]
    else:
        raise
import asyncio
from dotenv import load_dotenv

load_dotenv()

NEIGHBORS_CONCURRENCY_LIMIT = 2
neighbors_semaphore = asyncio.Semaphore(NEIGHBORS_CONCURRENCY_LIMIT)
app = FastAPI()
SYSTEM_NOT_FOUND = "System not found"
logger = logging.getLogger(__name__)

# Enable CORS for your app, you can restrict it to specific domains (origins)


def _load_cors_origins() -> list[str]:
    configured = os.getenv("CORS_ORIGINS", "")
    if not configured:
        return []
    return [
        origin.strip() for origin in configured.split(",") if origin.strip()
    ]


origins = _load_cors_origins()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Only allow specified origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Database connection parameters
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
UVICORN_PORT = int(os.getenv("UVICORN_PORT") or "8383")

REDIS_HOST = os.getenv("REDIS_HOST") or "localhost"
REDIS_PORT = int(os.getenv("REDIS_PORT") or "6379")
ONE_DAY_SECONDS = 60 * 60 * 24
AUTOCOMPLETE_LIMIT = 15
AUTOCOMPLETE_STATEMENT_TIMEOUT_MS = int(
    os.getenv("AUTOCOMPLETE_STATEMENT_TIMEOUT_MS") or "500"
)
AUTOCOMPLETE_TIMEOUT_RETRY_MS = int(
    os.getenv("AUTOCOMPLETE_TIMEOUT_RETRY_MS") or "1500"
)

from bisect import bisect_left
from aiocache import cached
from aiocache.serializers import PickleSerializer
from aiocache.backends.redis import RedisCache


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="coords_batch",
    serializer=PickleSerializer(),
)
async def fetch_coords_for_systems(id64_list: list[int]):
    if not id64_list:
        return {}

    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()

    query = f"""
        SELECT id64, ST_AsText(coords) AS coordinates
        FROM systems_big
        WHERE id64 = ANY(%s)
    """
    cursor.execute(query, (id64_list,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    coords_map = {}
    for row in rows:
        coords_str = row[1].replace("POINT Z (", "").replace(")", "").split()
        coords_map[row[0]] = {
            "x": float(coords_str[0]),
            "y": float(coords_str[1]),
            "z": float(coords_str[2]),
        }

    return coords_map


AUTOCOMPLETE_QUERY = """
    SELECT name
    FROM systems_big
    WHERE lower(name) LIKE %s
    ORDER BY name
    LIMIT %s
"""


_MANUAL_AUTOCOMPLETE_CACHE: tuple[list[str], list[str]] | None = None


def _load_manual_system_names() -> tuple[list[str], list[str]]:
    """Load the manually renamed systems shipped with EDTS."""

    global _MANUAL_AUTOCOMPLETE_CACHE
    if _MANUAL_AUTOCOMPLETE_CACHE is not None:
        return _MANUAL_AUTOCOMPLETE_CACHE

    from edtslib import id64data, pgnames  # type: ignore[attr-defined]

    entries: list[tuple[str, str]] = []
    for raw_name in id64data.known_systems.keys():
        canonical = pgnames.get_canonical_name(raw_name)
        cleaned = canonical if canonical else raw_name.strip().title()
        if not cleaned:
            continue
        entries.append((cleaned.lower(), cleaned))

    entries.sort()
    if entries:
        lowers, canonicals = zip(*entries)
        _MANUAL_AUTOCOMPLETE_CACHE = (list(lowers), list(canonicals))
    else:
        _MANUAL_AUTOCOMPLETE_CACHE = ([], [])
    return _MANUAL_AUTOCOMPLETE_CACHE


def _manual_name_suggestions(term: str, limit: int) -> list[str]:
    if limit <= 0:
        return []
    prefix = term.strip().lower()
    if len(prefix) < 2:
        return []

    lowers, canonicals = _load_manual_system_names()
    start = bisect_left(lowers, prefix)
    results: list[str] = []
    seen: set[str] = set()
    idx = start
    while idx < len(lowers) and lowers[idx].startswith(prefix):
        name = canonicals[idx]
        if name not in seen:
            results.append(name)
            seen.add(name)
            if len(results) >= limit:
                break
        idx += 1
    return results


def _fetch_system_names_from_db(term: str, limit: int) -> list[str]:
    if limit <= 0:
        return []
    prefix = term.strip().lower()
    if len(prefix) < 2:
        return []

    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    try:
        cursor = conn.cursor()
        try:
            like_pattern = f"{prefix}%"
            rows: list[tuple[str]] = []
            timeouts: list[int] = []
            primary_timeout = max(0, AUTOCOMPLETE_STATEMENT_TIMEOUT_MS)
            timeouts.append(primary_timeout)
            retry_timeout = max(0, AUTOCOMPLETE_TIMEOUT_RETRY_MS)
            if retry_timeout and retry_timeout != primary_timeout:
                timeouts.append(retry_timeout)

            for attempt, timeout_ms in enumerate(timeouts, start=1):
                timeout_statement = (
                    f"SET LOCAL statement_timeout = {timeout_ms}"
                )
                logger.debug(
                    "autocomplete DB lookup",
                    extra={
                        "autocomplete_prefix": prefix,
                        "autocomplete_limit": limit,
                        "autocomplete_timeout_ms": timeout_ms,
                        "autocomplete_attempt": attempt,
                    },
                )
                try:
                    cursor.execute(timeout_statement)
                    cursor.execute(AUTOCOMPLETE_QUERY, (like_pattern, limit))
                    rows = cursor.fetchall()
                    break
                except psycopg.errors.QueryCanceled:
                    logger.warning(
                        "autocomplete DB lookup canceled",
                        extra={
                            "autocomplete_prefix": prefix,
                            "autocomplete_limit": limit,
                            "autocomplete_timeout_ms": timeout_ms,
                            "autocomplete_attempt": attempt,
                        },
                    )
                    conn.rollback()
                    if attempt == len(timeouts):
                        return []
                    continue
            else:
                return []
        except Exception:
            logger.exception(
                "autocomplete DB lookup failed",
                extra={
                    "autocomplete_prefix": prefix,
                    "autocomplete_limit": limit,
                },
            )
            raise
        finally:
            cursor.close()
    finally:
        conn.close()

    return [row[0] for row in rows if row and row[0]]


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=600,
    namespace="systems_autocomplete",
    serializer=PickleSerializer(),
)
async def fetch_system_name_suggestions(term: str) -> list[str]:
    manual = _manual_name_suggestions(term, AUTOCOMPLETE_LIMIT)
    if len(manual) >= AUTOCOMPLETE_LIMIT:
        return manual[:AUTOCOMPLETE_LIMIT]

    remaining = AUTOCOMPLETE_LIMIT - len(manual)
    db_results = _fetch_system_names_from_db(term, remaining)

    seen = set(manual)
    for name in db_results:
        if not name or name in seen:
            continue
        manual.append(name)
        seen.add(name)
        if len(manual) >= AUTOCOMPLETE_LIMIT:
            break

    return manual


TOTAL_SYSTEMS_QUERY = """
    SELECT c.reltuples::bigint AS total_systems
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname IN ('systems_big')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
"""


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="stats_total_systems",
    serializer=PickleSerializer(),
)
async def fetch_total_systems_from_db() -> int:
    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()
    cursor.execute(TOTAL_SYSTEMS_QUERY)
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row or row[0] is None:
        raise RuntimeError("Total systems count unavailable")
    return int(row[0])


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,  # or your Redis host
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,  # one day cache
    namespace="neighbors",
    serializer=PickleSerializer(),  # Or JsonSerializer if you prefer
)
async def fetch_neighbors_from_db(x: float, y: float, z: float, radius: float):
    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()

    query = """
        WITH ref AS (
            SELECT ST_SetSRID(ST_MakePoint(%s, %s, %s), 0) AS geom
        ), candidates AS (
            SELECT
                s.id64,
                s.name,
                s.mainstar,
                ST_AsText(s.coords) AS coordinates,
                ST_3DDistance(s.coords, ref.geom) AS distance
            FROM systems_big s, ref
            WHERE ST_3DDWithin(s.coords, ref.geom, %s)
        )
        SELECT *
        FROM candidates
        ORDER BY distance
        LIMIT 100000;
    """
    cursor.execute(query, (x, y, z, radius))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    results = []
    for row in rows:
        coords = row[3].replace("POINT Z (", "").replace(")", "").split()
        results.append(
            {
                "id64": row[0],
                "name": row[1],
                "mainstar": row[2],
                "coords": {
                    "x": float(coords[0]),
                    "y": float(coords[1]),
                    "z": float(coords[2]),
                },
                "distance": row[4],
            }
        )
    return results


@app.get("/neighbors")
async def get_neighbors(
    x: float = Query(...),
    y: float = Query(...),
    z: float = Query(...),
    radius: float = Query(10.0),
):
    if radius < 0:
        return JSONResponse(
            content={"error": "Radius must be positive"}, status_code=200
        )
    async with neighbors_semaphore:
        try:
            results = await fetch_neighbors_from_db(x, y, z, radius)
            return JSONResponse(content=results)
        except Exception as e:
            return JSONResponse(content={"error": str(e)}, status_code=500)


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,  # cache for one day
    namespace="coords",
    serializer=PickleSerializer(),
)
async def fetch_system_from_db(name_or_id: str):
    import psycopg

    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()

    if name_or_id.isdigit() or (
        name_or_id.startswith("-") and name_or_id[1:].isdigit()
    ):
        query = """
            SELECT id64, name, mainstar, ST_AsText(coords) AS coordinates
            FROM systems_big
            WHERE id64 = %s
            LIMIT 1;
        """
        cursor.execute(query, (name_or_id,))
    else:
        query = """
            SELECT id64, name, mainstar, ST_AsText(coords) AS coordinates
            FROM systems_big
            WHERE LOWER(name) = LOWER(%s)
            LIMIT 1;
        """
        cursor.execute(query, (name_or_id,))

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row is None:
        return None

    point_coordinates = row[3]
    coords = (
        point_coordinates.replace("POINT Z (", "").replace(")", "").split()
    )
    x_coord = float(coords[0])
    y_coord = float(coords[1])
    z_coord = float(coords[2])

    return {
        "id64": row[0],
        "name": row[1],
        "mainstar": row[2],
        "coords": {"x": x_coord, "y": y_coord, "z": z_coord},
    }


def _format_neutron_result(row: Optional[tuple[Any, Any, Any]]):
    if row is None:
        return None

    neutron_id64, neutron_name, distance_ly = row
    if distance_ly is None:
        formatted_distance: Optional[float] = None
    elif isinstance(distance_ly, (int, float, Decimal)):
        formatted_distance = float(distance_ly)
    else:
        formatted_distance = None

    return {
        "neutron_id64": int(neutron_id64)
        if neutron_id64 is not None
        else None,
        "neutron_name": neutron_name,
        "distance_ly": formatted_distance,
    }


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="nearest_neutron_system",
    serializer=PickleSerializer(),
)
async def fetch_nearest_neutron_star(system_name: str):
    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM nearest_neutron_star(%s);", (system_name,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return _format_neutron_result(row)


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="nearest_neutron_coords",
    serializer=PickleSerializer(),
)
async def fetch_nearest_neutron_star_at_coords(x: float, y: float, z: float):
    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM nearest_neutron_star_at_coords(%s, %s, %s);",
        (x, y, z),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return _format_neutron_result(row)


def _is_star(record: dict[str, Any]) -> bool:
    body_type = str(record.get("type", "")).lower()
    if body_type == "star":
        return True
    return "star_type" in record


def _apply_mode_scaling(
    records: list[dict[str, Any]], mode: Optional[str]
) -> list[dict[str, Any]]:
    result_records = records
    should_scale = mode == "edsm"

    if should_scale:
        conversions = _build_conversion_map()
        numeric_types = (int, float, Decimal)

        for record in result_records:
            _scale_radius(record, numeric_types)
            _scale_fields(record, conversions, numeric_types)

    return result_records


def _build_conversion_map() -> dict[str, float]:
    return {
        "gravity": 9.807,
        "surface_gravity": 9.807,
        "semiMajorAxis": 149597870700,
        "semi_major_axis": 149597870700,
        "surfacePressure": 101325,
        "surface_pressure": 101325,
    }


def _scale_radius(
    record: dict[str, Any], numeric_types: tuple[type, ...]
) -> None:
    radius = record.get("radius")
    if not isinstance(radius, numeric_types):
        return
    radius_value = float(radius) if isinstance(radius, Decimal) else radius
    divisor = 695500000 if _is_star(record) else 1000
    record["radius"] = radius_value / divisor


def _scale_fields(
    record: dict[str, Any],
    conversions: dict[str, float],
    numeric_types: tuple[type, ...],
) -> None:
    for key, divisor in conversions.items():
        value = record.get(key)
        if not isinstance(value, numeric_types):
            continue
        scalar = float(value) if isinstance(value, Decimal) else value
        record[key] = scalar / divisor


def fetch_bodies_from_db(
    name_or_id: str, mode: Optional[str] = None, body_id: Optional[int] = None
):
    import psycopg

    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()
    identifier = name_or_id.strip()
    system_id64: Optional[int] = None
    is_numeric = identifier.isdigit() or (
        identifier.startswith("-") and identifier[1:].isdigit()
    )

    if is_numeric:
        system_id64 = int(identifier)
    else:
        cursor.execute(
            """
            SELECT id64
            FROM systems_big
            WHERE LOWER(name) = LOWER(%s)
            LIMIT 1
            """,
            (identifier,),
        )
        row = cursor.fetchone()
        if row is not None:
            system_id64 = int(row[0])

    if system_id64 is None:
        cursor.close()
        conn.close()
        return None

    # Always query bodies by their resolved id64 to avoid ambiguous name lookups
    body_filter = ""
    params: list[Any] = [system_id64]
    if body_id is not None:
        body_filter = " AND b.body_id = %s"
        params.append(body_id)

    query = f"""
            SELECT
            b.system_id64,
            b.body_id,
            b.body_name,
            bt.name AS type,
            pc.name AS planet_class,
            ts.name AS terraform_state,
            at.name AS atmosphere_type,
            a.name AS atmosphere,
            v.name AS volcanism,
            rc.name AS ring_class,
            b.ring_inner_rad,
            b.ring_outer_rad,
            b.ring_mass_mt,
            b.radius,
            b.mass_em,
            b.surface_gravity,
            b.surface_temperature,
            b.surface_pressure,
            b.axial_tilt,
            b.semi_major_axis,
            b.eccentricity,
            b.orbital_inclination,
            b.periapsis,
            b.mean_anomaly,
            b.orbital_period,
            b.rotation_period,
            b.ascending_node,
            b.distance_from_arrival_ls,
            b.age_my,
            b.absolute_magnitude,
            l.name AS luminosity,
            st.name AS star_type,
            b.subclass,
            b.stellar_mass,
            b.composition_ice,
            b.composition_metal,
            b.composition_rock,
            -- Normalized atmosphere_composition
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'Name', g.name,
                        'Percent', ba.percent
                    ) ORDER BY ba.percent DESC
                ) FILTER (WHERE ba.gas_id IS NOT NULL),
                '[]'::jsonb
            ) AS atmosphere_composition,
            -- New: aggregate materials from body_materials + material_names
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'Name', mn.name,
                        'Percent', bm.percent
                    ) ORDER BY bm.percent DESC
                ) FILTER (WHERE bm.material_id IS NOT NULL),
                '[]'::jsonb
            ) AS materials,

            b.parents,
            b.tidally_locked,
            b.landable,
            b.updatetime

        FROM bodies b
        INNER JOIN systems_big s ON s.id64 = b.system_id64
        LEFT JOIN body_types bt         ON b.body_type_id = bt.id
        LEFT JOIN planet_classes pc     ON b.planet_class_id = pc.id
        LEFT JOIN terraform_states ts   ON b.terraform_state_id = ts.id
        LEFT JOIN atmosphere_types at   ON b.atmosphere_type_id = at.id
        LEFT JOIN atmospheres a         ON b.atmosphere_id = a.id
        LEFT JOIN volcanisms v          ON b.volcanism_id = v.id
        LEFT JOIN ring_classes rc       ON b.ring_class_id = rc.id
        LEFT JOIN luminosities l        ON b.luminosity_id = l.id
        LEFT JOIN star_types st         ON b.star_type_id = st.id
        LEFT JOIN body_materials bm     ON b.system_id64 = bm.system_id64 AND b.body_id = bm.body_id
        LEFT JOIN material_names mn     ON bm.material_id = mn.id
        LEFT JOIN body_atmospheres ba ON b.system_id64 = ba.system_id64 AND b.body_id = ba.body_id
        LEFT JOIN atmosphere_gases g ON ba.gas_id = g.id
        WHERE b.system_id64 = %s{body_filter}
    GROUP BY
            b.system_id64, b.body_id, b.body_name,
            bt.name, pc.name, ts.name, at.name, a.name,
            v.name, rc.name, l.name, st.name
        ORDER BY b.body_id;
    """

    cursor.execute(query, tuple(params))

    rows = cursor.fetchall()
    if not rows:
        cursor.close()
        conn.close()
        return None

    # get column names from cursor.description
    col_names = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    # build array of dicts, filtering out None values
    results = [
        {col: val for col, val in zip(col_names, row) if val is not None}
        for row in rows
    ]

    return _apply_mode_scaling(results, mode)


# @cached(
#     cache=RedisCache,
#     endpoint="localhost",
#     port=6379,
#     ttl=86400,  # one day cache
#     namespace="bodies",
#     serializer=PickleSerializer(),
# )
@app.get("/bodies", include_in_schema=True)
def bodies(
    name_or_id: str = Query(..., description="The name or id64 of the system"),
    body_id: Optional[int] = Query(
        None, description="Optional body_id to narrow to a single body"
    ),
    mode: Optional[str] = Query(
        None, description="Optional response mode adjustments"
    ),
):
    if body_id is None:
        result = fetch_bodies_from_db(name_or_id, mode=mode)
    else:
        result = fetch_bodies_from_db(name_or_id, mode=mode, body_id=body_id)
    if result is None:
        return JSONResponse(
            content={"error": SYSTEM_NOT_FOUND}, status_code=404
        )
    return result


@app.get("/edsm/bodies", include_in_schema=False)
@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,  # cache for 1 day
    namespace="edsm_bodies",
    serializer=PickleSerializer(),
)
async def proxy_edsm_bodies(
    systemName: str = Query(..., description="The name of the system")
):
    url = "https://www.edsm.net/api-system-v1/bodies"

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url, params={"systemName": systemName})

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail="Failed to fetch system bodies from EDSM",
            )

        return response.json()

    except httpx.ReadTimeout:
        # EDSM took too long to respond
        raise HTTPException(
            status_code=504,
            detail=f"Upstream timeout when querying system '{systemName}' from EDSM",
        )

    except httpx.RequestError as exc:
        # Any other network error (DNS, connection refused, etc.)
        raise HTTPException(
            status_code=502,
            detail=f"Error connecting to EDSM: {exc}",
        )


@app.get("/coords")
async def get_coords(name_or_id: str = Query(..., alias="q")):
    result = await fetch_system_from_db(name_or_id)
    if result is None:
        return JSONResponse(
            content={"error": SYSTEM_NOT_FOUND}, status_code=404
        )
    return result


class Coords(BaseModel):
    x: float
    y: float
    z: float


class SystemResponse(BaseModel):
    id64: Optional[int]
    name: str
    mainstar: Optional[str]
    coords: Coords
    prediction: Optional[bool] = False


class NeutronStarResponse(BaseModel):
    neutron_id64: Optional[int]
    neutron_name: Optional[str]
    distance_ly: Optional[float]


@app.get("/coords/predict", response_model=SystemResponse)
async def get_coords(name_or_id: str = Query(..., alias="q")):
    try:
        if name_or_id.isdigit() or (
            name_or_id.startswith("-") and name_or_id[1:].isdigit()
        ):
            sys_obj = system.from_id64(int(name_or_id), allow_known=False)
        else:
            sys_obj = system.from_name(name_or_id, allow_known=False)
        if sys_obj is None:
            return JSONResponse(
                content={"error": SYSTEM_NOT_FOUND}, status_code=404
            )

        return SystemResponse(
            id64=getattr(sys_obj, "id64", None),
            name=getattr(sys_obj, "name", "Unknown"),
            mainstar=getattr(sys_obj, "mainstar", None),
            coords=Coords(
                x=sys_obj.position[0],
                y=sys_obj.position[1],
                z=sys_obj.position[2],
            ),
            prediction=True,
        )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/nearest-neutron-star", response_model=NeutronStarResponse)
async def get_nearest_neutron_star(
    system_name: str = Query(
        ..., description="Exact system name used to seed the search"
    )
):
    result = await fetch_nearest_neutron_star(system_name)
    if result is None:
        return JSONResponse(
            content={"error": "No neutron star found"}, status_code=404
        )
    return result


@app.get(
    "/nearest-neutron-star/coords",
    response_model=NeutronStarResponse,
)
async def get_nearest_neutron_star_from_coords(
    x: float = Query(..., description="Cartesian X coordinate"),
    y: float = Query(..., description="Cartesian Y coordinate"),
    z: float = Query(..., description="Cartesian Z coordinate"),
):
    result = await fetch_nearest_neutron_star_at_coords(x, y, z)
    if result is None:
        return JSONResponse(
            content={"error": "No neutron star found"}, status_code=404
        )
    return result


@app.get("/spansh/system/{system_id}", include_in_schema=False)
async def proxy_spansh_system(system_id: int):
    url = f"https://spansh.co.uk/api/system/{system_id}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            # If you want only bodies, extract here:
            return data.get("record", {}).get("bodies", data)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/spansh/faction_presence", include_in_schema=False)
@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="faction_presence",
    serializer=PickleSerializer(),
)
async def proxy_spansh_faction_presence(
    faction: str = Query(..., description="The minor faction name")
):
    SAVE_URL = "https://spansh.co.uk/api/systems/search/save"
    RECALL_URL = "https://spansh.co.uk/api/systems/search/recall/"
    MAX_PAGE_SIZE = 500  # Maximum results per page

    payload = {
        "filters": {
            "minor_faction_presences": [{"name": {"value": [faction]}}]
        },
        "sort": [],
        "size": MAX_PAGE_SIZE,
        "page": 0,
    }

    async with httpx.AsyncClient() as client:
        # Step 1: Save search
        save_response = await client.post(SAVE_URL, json=payload)
        if save_response.status_code != 200:
            raise HTTPException(
                status_code=save_response.status_code,
                detail="Failed to save search",
            )

        save_data = save_response.json()
        search_reference = save_data.get("search_reference")
        if not search_reference:
            raise HTTPException(
                status_code=500, detail="No search_reference returned"
            )

        # Step 2: Recall search (handle pagination)
        all_results = []
        page = 0
        total_count = None

        while True:
            recall_response = await client.get(
                f"{RECALL_URL}{search_reference}", params={"page": page}
            )
            if recall_response.status_code != 200:
                raise HTTPException(
                    status_code=recall_response.status_code,
                    detail=f"Failed to recall search on page {page}",
                )

            recall_data = recall_response.json()
            results = recall_data.get("results", [])
            all_results.extend(results)

            if total_count is None:
                total_count = recall_data.get("count", len(results))

            page += 1
            if len(all_results) >= total_count:
                break

    # Step 3: Build simplified results with is_controlling
    simplified_results = [
        {
            "id64": system.get("id64"),
            "name": system.get("name"),
            "is_controlling": system.get("controlling_minor_faction")
            == faction,
        }
        for system in all_results
    ]

    id64_list = [s["id64"] for s in simplified_results if s.get("id64")]

    # Fetch coordinates
    coords_map = await fetch_coords_for_systems(id64_list)

    for s in simplified_results:
        s["coords"] = coords_map.get(s["id64"])

    return {"results": simplified_results}


@app.get(
    "/spansh/autocomplete_controlling_minor_faction",
    include_in_schema=False,
)
@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="controlling_faction_autocomplete",
    serializer=PickleSerializer(),
)
async def proxy_spansh_autocomplete_controlling_minor_faction(
    q: str = Query(
        ..., description="Search fragment for the controlling faction name"
    ),
):
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query must not be empty")

    url = (
        "https://spansh.co.uk/api/systems/field_values/"
        "autocomplete_controlling_minor_faction"
    )

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url, params={"q": query})
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or "values" not in payload:
                raise HTTPException(
                    status_code=502,
                    detail="Unexpected response from Spansh",
                )
            return payload
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/systems/autocomplete")
async def autocomplete_systems(
    q: str = Query(..., description="System name prefix")
):
    query = q.strip()
    if len(query) < 2:
        return {"suggestions": []}
    try:
        suggestions = await fetch_system_name_suggestions(query)
        return {"suggestions": suggestions}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail="Autocomplete failed"
        ) from exc


@app.get("/stats/total-systems")
async def get_total_systems():
    try:
        total = await fetch_total_systems_from_db()
        return {"total_systems": total}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail="Failed to retrieve total systems"
        ) from exc


from fastapi.staticfiles import StaticFiles

app.mount("/static", StaticFiles(directory="static"), name="static")

from fastapi.responses import FileResponse


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.png")


# Route to serve index.html
@app.get("/", include_in_schema=False)
def read_index():
    return FileResponse(os.path.join("static", "index.html"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=UVICORN_PORT)
