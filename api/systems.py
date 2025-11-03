from aiocache import cached
from aiocache import caches
from aiocache.backends.redis import RedisCache
from aiocache.serializers import PickleSerializer
from decimal import Decimal
from fastapi import FastAPI, Query
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Any
import heapq
import httpx
import json
import logging
import math
import os
import psycopg
import time

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

# Enable CORS for your app, you can restrict it to specific domains (origins)


def _load_cors_origins() -> list[str]:
    configured = os.getenv("CORS_ORIGINS", "")
    if not configured:
        return []
    return [origin.strip() for origin in configured.split(",") if origin.strip()]


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


async def _get_coords_by_name(conn, name: str):
    cur = conn.cursor()
    cur.execute(
        "SELECT id64, name, ST_X(coords), ST_Y(coords), ST_Z(coords) "
        "FROM systems_big WHERE lower(name)=lower(%s) LIMIT 1;",
        (name,),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return {
        "id64": row[0],
        "name": row[1],
        "x": float(row[2]),
        "y": float(row[3]),
        "z": float(row[4]),
    }


@cached(
    cache=RedisCache,
    endpoint="localhost",
    port=6379,
    ttl=7 * 86400,  # 1 week
    namespace="neighbors_cache",
    serializer=PickleSerializer(),
)
async def cached_neighbors(
    conn,
    system_id64: int,
    cx: float,
    cy: float,
    cz: float,
    gx: float,
    gy: float,
    gz: float,
    radius: float,
    limit: int = 150,
):
    # This wraps the same SQL you have
    return await _neighbors(conn, cx, cy, cz, gx, gy, gz, radius, limit)


async def _neighbors(
    conn,
    cx: float,
    cy: float,
    cz: float,
    gx: float,
    gy: float,
    gz: float,
    radius: float,
    limit: int = 150,
):
    cur = conn.cursor()
    cur.execute(
        """
        WITH cur AS (
          SELECT ST_MakePoint(%s,%s,%s)::geometry(PointZ) AS geom,
                 ST_MakePoint(%s,%s,%s)::geometry(PointZ) AS goal_geom
        )
        SELECT
          s.id64, s.name,
          ST_X(s.coords) AS x,
          ST_Y(s.coords) AS y,
          ST_Z(s.coords) AS z,
          ST_3DDistance(s.coords, cur.geom) AS dist_from_current,
          ST_3DDistance(s.coords, cur.goal_geom) AS dist_to_goal
        FROM systems_big s, cur
        WHERE ST_3DDWithin(s.coords, cur.geom, %s)
          AND ST_3DDistance(s.coords, cur.geom) > 0
        ORDER BY dist_from_current DESC, dist_to_goal ASC
        LIMIT %s;
        """,
        (cx, cy, cz, gx, gy, gz, radius, limit),
    )
    rows = cur.fetchall()
    cur.close()

    return [
        {
            "id64": r[0],
            "name": r[1],
            "x": r[2],
            "y": r[3],
            "z": r[4],
            "dist": float(r[5]),
            "goal_dist": float(r[6]),
        }
        for r in rows
    ]


def _dist(a, b):
    return math.sqrt(
        (a["x"] - b["x"]) ** 2 + (a["y"] - b["y"]) ** 2 + (a["z"] - b["z"]) ** 2
    )


async def cache_route_segment(
    start_id64: int, goal_id64: int, jump_range: float, path: list[dict]
):
    key = f"route_cache:{start_id64}:{goal_id64}:{int(jump_range)}"
    redis = caches.get("default")
    await redis.set(key, json.dumps(path), ttl=30 * 86400)  # 30 days


async def fetch_cached_segment(start_id64: int, goal_id64: int, jump_range: float):
    key = f"route_cache:{start_id64}:{goal_id64}:{int(jump_range)}"
    redis = caches.get("default")
    data = await redis.get(key)
    if data:
        return json.loads(data)
    return None


logger = logging.getLogger("route_debug")
logging.basicConfig(level=logging.INFO)


async def astar_route(
    conn,
    start,
    goal,
    jump_range: float,
    neighbor_limit: int = 50,
    max_expansions: int = 5000,
    log_every: int = 50,
):
    openq = [(0.0, start["id64"], start)]
    gscore = {start["id64"]: 0.0}
    came_from = {}
    visited = set()
    expansions = 0
    start_time = time.time()

    while openq:
        _, cid, current = heapq.heappop(openq)
        if cid in visited:
            continue
        visited.add(cid)
        expansions += 1

        if expansions % log_every == 0:
            logger.info(
                f"[A*] expanded={expansions} open={len(openq)} visited={len(visited)} "
                f"dist_to_goal={_dist(current, goal):.1f} ly"
            )

        # Goal check
        if _dist(current, goal) <= jump_range:
            logger.info(f"[A*] Reached goal vicinity after {expansions} expansions.")

            # Reconstruct safely
            path = [current]
            step = cid
            while step in came_from:
                prev = came_from[step]
                path.append(prev)
                step = prev["id"]
            path.reverse()

            logger.info(
                f"[A*] Path length={len(path)} nodes; time={time.time() - start_time:.2f}s"
            )
            return path

        if expansions > max_expansions:
            logger.warning(f"[A*] Aborted after {expansions} expansions.")
            return []

        neighbors = await cached_neighbors(
            conn,
            current["id64"],
            current["x"],
            current["y"],
            current["z"],
            goal["x"],
            goal["y"],
            goal["z"],
            jump_range,
            limit=100000,
        )

        for nb in neighbors:
            nid = nb["id64"]
            tentative = gscore[cid] + nb["dist"]
            if tentative < gscore.get(nid, float("inf")):
                gscore[nid] = tentative
                came_from[nid] = {"id": cid, **current}
                f = tentative + _dist(nb, goal)
                heapq.heappush(openq, (f, nid, nb))

    logger.warning(f"[A*] Queue exhausted after {expansions} expansions.")
    return []


@app.get("/route")
async def get_route(
    start: str = Query(...),
    goal: str = Query(...),
    jump_range: float = Query(70.0),
):
    conn = None
    try:
        conn = psycopg.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )

        s = await _get_coords_by_name(conn, start)
        g = await _get_coords_by_name(conn, goal)
        if not s or not g:
            return JSONResponse({"error": "Start or goal not found"}, 404)

        logger.info(
            f"[route] Starting A* from {s['name']} -> {g['name']} "
            f"(range {jump_range} ly)"
        )

        cached = await fetch_cached_segment(s["id64"], g["id64"], jump_range)
        if cached:
            logger.info(f"[route] Using cached route {s['name']} -> {g['name']}")
            return {"count": len(cached), "route": cached}

        path = await astar_route(conn, s, g, jump_range)
        if not path:
            return JSONResponse({"error": "No route found"}, 404)

        await cache_route_segment(s["id64"], g["id64"], jump_range, path)
        await cache_route_segment(
            g["id64"], s["id64"], jump_range, list(reversed(path))
        )

        total_distance = sum(p.get("dist", 0) for p in path[1:])
        avg_jump = total_distance / (len(path) - 1) if len(path) > 1 else 0

        return {
            "count": len(path),
            "total_distance": round(total_distance, 2),
            "avg_jump": round(avg_jump, 2),
            "route": path,
        }

    except Exception as e:
        logger.exception("[route] Exception in /route")
        return JSONResponse({"error": str(e)}, 500)

    finally:
        if conn:
            conn.close()


@cached(
    cache=RedisCache,
    endpoint="localhost",
    port=6379,
    ttl=86400,
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

    query = """
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


@cached(
    cache=RedisCache,
    endpoint="localhost",  # or your Redis host
    port=6379,
    ttl=86400,  # one day cache
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
    endpoint="localhost",
    port=6379,
    ttl=86400,  # cache for one day
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
    coords = point_coordinates.replace("POINT Z (", "").replace(")", "").split()
    x_coord = float(coords[0])
    y_coord = float(coords[1])
    z_coord = float(coords[2])

    return {
        "id64": row[0],
        "name": row[1],
        "mainstar": row[2],
        "coords": {"x": x_coord, "y": y_coord, "z": z_coord},
    }


def _is_star(record: dict[str, Any]) -> bool:
    body_type = str(record.get("type", "")).lower()
    if body_type == "star":
        return True
    return "star_type" in record


def _apply_mode_scaling(
    records: list[dict[str, Any]], mode: Optional[str]
) -> list[dict[str, Any]]:
    if mode == "edsm":
        conversions = {
            "gravity": 9.807,
            "surface_gravity": 9.807,
            "semiMajorAxis": 149597870700,
            "semi_major_axis": 149597870700,
            "surfacePressure": 101325,
            "surface_pressure": 101325,
        }

        numeric_types = (int, float, Decimal)

        for record in records:
            radius = record.get("radius")
            if isinstance(radius, numeric_types):
                if isinstance(radius, Decimal):
                    radius = float(radius)
                divisor = 695500000 if _is_star(record) else 1000
                record["radius"] = radius / divisor

            for key, divisor in conversions.items():
                value = record.get(key)
                if isinstance(value, numeric_types):
                    if isinstance(value, Decimal):
                        value = float(value)
                    record[key] = value / divisor

    return records


def fetch_bodies_from_db(name_or_id: str, mode: Optional[str] = None):
    import psycopg

    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()

    if name_or_id.isdigit() or (
        name_or_id.startswith("-") and name_or_id[1:].isdigit()
    ):
        query = """
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

            WHERE b.system_id64 = %s
            GROUP BY
                b.system_id64, b.body_id, b.body_name,
                bt.name, pc.name, ts.name, at.name, a.name,
                v.name, rc.name, l.name, st.name
            ORDER BY b.body_id;
        """
        cursor.execute(query, (name_or_id,))
    else:
        query = """
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
            WHERE LOWER(s.name) = LOWER(%s)
            AND s.id64 = b.system_id64
            GROUP BY
                b.system_id64, b.body_id, b.body_name,
                bt.name, pc.name, ts.name, at.name, a.name,
                v.name, rc.name, l.name, st.name
            ORDER by body_id;
        """
        cursor.execute(query, (name_or_id,))

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
    mode: Optional[str] = Query(None, description="Optional response mode adjustments"),
):
    result = fetch_bodies_from_db(name_or_id, mode=mode)
    if result is None:
        return JSONResponse(content={"error": SYSTEM_NOT_FOUND}, status_code=404)
    return result


@app.get("/edsm/bodies", include_in_schema=False)
@cached(
    cache=RedisCache,
    endpoint="localhost",
    port=6379,
    ttl=86400,  # cache for 1 day
    namespace="edsm_bodies",
    serializer=PickleSerializer(),
)
async def proxy_edsm_bodies(
    systemName: str = Query(..., description="The name of the system"),
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
        return JSONResponse(content={"error": SYSTEM_NOT_FOUND}, status_code=404)
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


@app.get("/coords/predict", response_model=SystemResponse)
async def get_coords_prediction(name_or_id: str = Query(..., alias="q")):
    try:
        if name_or_id.isdigit() or (
            name_or_id.startswith("-") and name_or_id[1:].isdigit()
        ):
            sys_obj = system.from_id64(int(name_or_id), allow_known=False)
        else:
            sys_obj = system.from_name(name_or_id, allow_known=False)
        if sys_obj is None:
            return JSONResponse(content={"error": SYSTEM_NOT_FOUND}, status_code=404)

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
    endpoint="localhost",
    port=6379,
    ttl=86400,
    namespace="faction_presence",
    serializer=PickleSerializer(),
)
async def proxy_spansh_faction_presence(
    faction: str = Query(..., description="The minor faction name"),
):
    SAVE_URL = "https://spansh.co.uk/api/systems/search/save"
    RECALL_URL = "https://spansh.co.uk/api/systems/search/recall/"
    MAX_PAGE_SIZE = 500  # Maximum results per page

    payload = {
        "filters": {"minor_faction_presences": [{"name": {"value": [faction]}}]},
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
            raise HTTPException(status_code=500, detail="No search_reference returned")

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
            "is_controlling": system.get("controlling_minor_faction") == faction,
        }
        for system in all_results
    ]

    id64_list = [s["id64"] for s in simplified_results if s.get("id64")]

    # Fetch coordinates
    coords_map = await fetch_coords_for_systems(id64_list)

    for s in simplified_results:
        s["coords"] = coords_map.get(s["id64"])

    return {"results": simplified_results}


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.png")


# Route to serve index.html
@app.get("/", include_in_schema=False)
def read_index():
    return FileResponse(os.path.join("static", "index.html"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8383)
