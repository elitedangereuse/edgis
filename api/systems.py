import os
import httpx
from fastapi import HTTPException
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg
from pydantic import BaseModel
from typing import Optional
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

NEIGHBORS_CONCURRENCY_LIMIT = 2
neighbors_semaphore = asyncio.Semaphore(NEIGHBORS_CONCURRENCY_LIMIT)
app = FastAPI()

# Enable CORS for your app, you can restrict it to specific domains (origins)
origins = [
    "https://elitedangereuse.hobeika.fr",
    "https://elitedangereuse.fr",
    "https://elitedangereuse.com",
    "https://cutter.elitedangereuse.fr",
    "https://beta.elitedangereuse.fr",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Only allow specified origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Database connection parameters
load_dotenv()
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


# @cached(
#     cache=RedisCache,
#     endpoint="localhost",
#     port=6379,
#     ttl=86400,  # one day cache
#     namespace="bodies",
#     serializer=PickleSerializer(),
# )
async def fetch_bodies_from_db(name_or_id: str):
    import psycopg

    conn = psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()

    if name_or_id.isdigit() or (
        name_or_id.startswith("-") and name_or_id[1:].isdigit()
    ):
        query = """
            SELECT *
            FROM bodies
            WHERE system_id64 = %s
            ORDER BY body_id;
        """
        cursor.execute(query, (name_or_id,))
    else:
        query = """
            SELECT b.*
            FROM bodies b, systems_big s
            WHERE LOWER(s.name) = LOWER(%s)
            AND s.id64 = b.system_id64
            ORDER BY b.body_id;
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

    return results


@app.get("/bodies", include_in_schema=True)
async def bodies(
    name_or_id: str = Query(..., description="The name or id64 of the system")
):
    result = await fetch_bodies_from_db(name_or_id)
    if result is None:
        return JSONResponse(
            content={"error": "System not found"}, status_code=404
        )
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
        return JSONResponse(
            content={"error": "System not found"}, status_code=404
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
                content={"error": "System not found"}, status_code=404
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
