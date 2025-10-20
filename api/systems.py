import os
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

from aiocache import cached
from aiocache.serializers import PickleSerializer
from aiocache.backends.redis import RedisCache


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


# @cached(
#     cache=RedisCache,
#     endpoint="localhost",
#     port=6379,
#     ttl=86400,  # one day cache
#     namespace="bodies",
#     serializer=PickleSerializer(),
# )
async def fetch_bodies_from_db(name_or_id: str, mode: Optional[str] = None):
    import psycopg

    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()

    if name_or_id.isdigit() or (
        name_or_id.startswith("-") and name_or_id[1:].isdigit()
    ):
        query = """
            SELECT system_id64, body_id, body_name, bt.name as type, pc.name as planet_class, ts.name as terraform_state, at.name as atmosphere_type, atmosphere_composition, a.name as atmosphere, v.name as volcanism, rc.name as ring_class, ring_inner_rad, ring_outer_rad, ring_mass_mt, radius, mass_em, surface_gravity, surface_temperature, surface_pressure, axial_tilt, semi_major_axis, eccentricity, orbital_inclination, periapsis, mean_anomaly, orbital_period, rotation_period, ascending_node, distance_from_arrival_ls, age_my, absolute_magnitude, l.name as luminosity, st.name as star_type, subclass, stellar_mass, composition_ice, composition_metal, composition_rock, materials, parents, tidally_locked, landable, updatetime
            FROM bodies b
            LEFT JOIN body_types bt ON b.body_type_id = bt.id
            LEFT JOIN planet_classes pc ON b.planet_class_id = pc.id
            LEFT JOIN terraform_states ts ON b.terraform_state_id = ts.id
            LEFT JOIN atmosphere_types at ON b.atmosphere_type_id = at.id
            LEFT JOIN atmospheres a ON b.atmosphere_id = a.id
            LEFT JOIN volcanisms v ON b.volcanism_id = v.id
            LEFT JOIN ring_classes rc ON b.ring_class_id = rc.id
            LEFT JOIN luminosities l ON b.luminosity_id = l.id
            LEFT JOIN star_types st ON b.star_type_id = st.id
            WHERE system_id64 = %s
            ORDER by body_id;
        """
        cursor.execute(query, (name_or_id,))
    else:
        query = """
            SELECT system_id64, body_id, body_name, bt.name as type, pc.name as planet_class, ts.name as terraform_state, at.name as atmosphere_type, atmosphere_composition, a.name as atmosphere, v.name as volcanism, rc.name as ring_class, ring_inner_rad, ring_outer_rad, ring_mass_mt, radius, mass_em, surface_gravity, surface_temperature, surface_pressure, axial_tilt, semi_major_axis, eccentricity, orbital_inclination, periapsis, mean_anomaly, orbital_period, rotation_period, ascending_node, distance_from_arrival_ls, age_my, absolute_magnitude, l.name as luminosity, st.name as star_type, subclass, stellar_mass, composition_ice, composition_metal, composition_rock, materials, parents, tidally_locked, landable, b.updatetime
            FROM bodies b
            INNER JOIN systems_big s ON s.id64 = b.system_id64
            LEFT JOIN body_types bt ON b.body_type_id = bt.id
            LEFT JOIN planet_classes pc ON b.planet_class_id = pc.id
            LEFT JOIN terraform_states ts ON b.terraform_state_id = ts.id
            LEFT JOIN atmosphere_types at ON b.atmosphere_type_id = at.id
            LEFT JOIN atmospheres a ON b.atmosphere_id = a.id
            LEFT JOIN volcanisms v ON b.volcanism_id = v.id
            LEFT JOIN ring_classes rc ON b.ring_class_id = rc.id
            LEFT JOIN luminosities l ON b.luminosity_id = l.id
            LEFT JOIN star_types st ON b.star_type_id = st.id
            WHERE LOWER(s.name) = LOWER(%s)
            AND s.id64 = b.system_id64
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


async def fetch_bodies_from_db2(name_or_id: str, mode: Optional[str] = None):
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
                b.atmosphere_composition,
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
                b.atmosphere_composition,
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


@app.get("/bodies", include_in_schema=True)
async def bodies(
    name_or_id: str = Query(..., description="The name or id64 of the system"),
    mode: Optional[str] = Query(None, description="Optional response mode adjustments"),
):
    result = await fetch_bodies_from_db(name_or_id, mode=mode)
    if result is None:
        return JSONResponse(content={"error": SYSTEM_NOT_FOUND}, status_code=404)
    return result


@app.get("/bodies2", include_in_schema=True)
async def bodies2(
    name_or_id: str = Query(..., description="The name or id64 of the system"),
    mode: Optional[str] = Query(None, description="Optional response mode adjustments"),
):
    result = await fetch_bodies_from_db2(name_or_id, mode=mode)
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
async def get_coords(name_or_id: str = Query(..., alias="q")):
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
    faction: str = Query(..., description="The minor faction name")
):
    SAVE_URL = "https://spansh.co.uk/api/systems/search/save"
    RECALL_URL = "https://spansh.co.uk/api/systems/search/recall/"
    MAX_PAGE_SIZE = 500  # Maximum results per page

    payload = {
        "filters": {"minor_faction_presences": {"name": {"value": [faction]}}},
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


from fastapi.staticfiles import StaticFiles

app.mount("/static", StaticFiles(directory="static"), name="static")

from fastapi.responses import FileResponse


@app.get("/favicon.ico")
async def favicon():
    return FileResponse("static/favicon.png")


# Route to serve index.html
@app.get("/")
def read_index():
    return FileResponse(os.path.join("static", "index.html"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8383)
