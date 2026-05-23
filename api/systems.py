import os
import logging
import time
import base64
import json
from contextlib import contextmanager
from queue import Empty, LifoQueue
from threading import Lock
import httpx
from decimal import Decimal
from fastapi import HTTPException
from fastapi import FastAPI, Query
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg
from pydantic import BaseModel
from typing import Callable, Iterable, Optional, Any, Sequence
from urllib.parse import quote

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

_SPANSH_IMPORT_ERROR: str | None = None
try:
    from feeders.spansh_system_ingestor import (
        ingest_system as spansh_ingest_system,
    )
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    _repo_root = Path(__file__).resolve().parents[1]
    if str(_repo_root) not in sys.path:
        sys.path.append(str(_repo_root))
    try:
        from feeders.spansh_system_ingestor import (
            ingest_system as spansh_ingest_system,
        )
    except ModuleNotFoundError as exc:
        spansh_ingest_system = None
        _SPANSH_IMPORT_ERROR = str(exc)
except Exception as exc:  # pragma: no cover - defensive import fallback
    spansh_ingest_system = None
    _SPANSH_IMPORT_ERROR = str(exc)

load_dotenv()

EXTERNAL_USER_AGENT = os.getenv("EDGIS_USER_AGENT", "EDGIS")
NEIGHBORS_BULK_CONCURRENCY_LIMIT = max(
    1, int(os.getenv("NEIGHBORS_BULK_CONCURRENCY_LIMIT") or "1")
)
NEIGHBORS_PAGED_CONCURRENCY_LIMIT = max(
    1, int(os.getenv("NEIGHBORS_PAGED_CONCURRENCY_LIMIT") or "4")
)
_neighbors_bulk_semaphore: asyncio.Semaphore | None = None
_neighbors_bulk_semaphore_loop: asyncio.AbstractEventLoop | None = None
_neighbors_paged_semaphore: asyncio.Semaphore | None = None
_neighbors_paged_semaphore_loop: asyncio.AbstractEventLoop | None = None
NEIGHBORS_MAX_RADIUS = max(
    0.0, float(os.getenv("NEIGHBORS_MAX_RADIUS") or "3000")
)
NEIGHBORS_DEFAULT_LIMIT = max(
    1, int(os.getenv("NEIGHBORS_DEFAULT_LIMIT") or "100000")
)
NEIGHBORS_RESULT_LIMIT = max(
    NEIGHBORS_DEFAULT_LIMIT,
    int(os.getenv("NEIGHBORS_RESULT_LIMIT") or "100000"),
)
NEIGHBORS_PAGE_SIZE_DEFAULT = max(
    1, int(os.getenv("NEIGHBORS_PAGE_SIZE_DEFAULT") or "100")
)
NEIGHBORS_PAGE_SIZE_MAX = max(
    NEIGHBORS_PAGE_SIZE_DEFAULT,
    int(os.getenv("NEIGHBORS_PAGE_SIZE_MAX") or "500"),
)
NEIGHBORS_STATEMENT_TIMEOUT_MS = max(
    0, int(os.getenv("NEIGHBORS_STATEMENT_TIMEOUT_MS") or "30000")
)
NEIGHBORS_SEEDED_RADII = tuple(
    radius
    for radius in (
        20.0,
        50.0,
        100.0,
    )
    if radius > 0
)
app = FastAPI()
SYSTEM_NOT_FOUND = "System not found"
logger = logging.getLogger(__name__)
request_logger = logging.getLogger("uvicorn.error")

# Enable CORS for your app, you can restrict it to specific domains (origins)


def _load_cors_origins() -> list[str]:
    configured = os.getenv("CORS_ORIGINS", "")
    if not configured:
        return []
    return [
        origin.strip() for origin in configured.split(",") if origin.strip()
    ]


def _allow_cors_credentials(cors_origins: Sequence[str]) -> bool:
    return "*" not in cors_origins


origins = _load_cors_origins()
allow_cors_credentials = _allow_cors_credentials(origins)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    # Wildcard CORS cannot be combined with credentialed requests.
    allow_credentials=allow_cors_credentials,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)


def _truncate_header_value(value: str | None, max_length: int = 300) -> str:
    if not value:
        return ""
    if len(value) <= max_length:
        return value
    return f"{value[:max_length]}..."


def _get_neighbors_semaphore(paged: bool) -> asyncio.Semaphore:
    global _neighbors_bulk_semaphore, _neighbors_bulk_semaphore_loop
    global _neighbors_paged_semaphore, _neighbors_paged_semaphore_loop

    loop = asyncio.get_running_loop()
    if paged:
        if (
            _neighbors_paged_semaphore is None
            or _neighbors_paged_semaphore_loop is not loop
        ):
            _neighbors_paged_semaphore = asyncio.Semaphore(
                NEIGHBORS_PAGED_CONCURRENCY_LIMIT
            )
            _neighbors_paged_semaphore_loop = loop
        return _neighbors_paged_semaphore

    if (
        _neighbors_bulk_semaphore is None
        or _neighbors_bulk_semaphore_loop is not loop
    ):
        _neighbors_bulk_semaphore = asyncio.Semaphore(
            NEIGHBORS_BULK_CONCURRENCY_LIMIT
        )
        _neighbors_bulk_semaphore_loop = loop
    return _neighbors_bulk_semaphore


@app.middleware("http")
async def log_get_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)

    if request.method.upper() == "GET":
        client_host = request.client.host if request.client else "unknown"
        ua = _truncate_header_value(request.headers.get("user-agent"))
        referer = _truncate_header_value(request.headers.get("referer"))
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        request_logger.info(
            'request_details method=%s path="%s" query="%s" status=%d ip=%s ua="%s" referer="%s" duration_ms=%s',
            request.method,
            request.url.path,
            request.url.query,
            response.status_code,
            f"{client_host}:{request.client.port if request.client else 0}",
            ua,
            referer,
            duration_ms,
        )

    return response


# Database connection parameters
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
UVICORN_PORT = int(os.getenv("UVICORN_PORT") or "8383")
INDEX_HTML_FILENAME = os.path.basename(
    os.getenv("INDEX_HTML_FILENAME") or "index.html"
)
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
INDEX_HTML_PATH = os.path.join(STATIC_DIR, INDEX_HTML_FILENAME)

REDIS_HOST = os.getenv("REDIS_HOST") or "localhost"
REDIS_PORT = int(os.getenv("REDIS_PORT") or "6379")
ONE_DAY_SECONDS = 60 * 60 * 24
AUTOCOMPLETE_LIMIT = 15
DB_POOL_MAX_SIZE = max(1, int(os.getenv("DB_POOL_MAX_SIZE") or "12"))
DB_POOL_WAIT_TIMEOUT_SECONDS = max(
    0.1, float(os.getenv("DB_POOL_WAIT_TIMEOUT_SECONDS") or "30")
)
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


def _create_db_connection():
    return psycopg.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )


class _SyncConnectionPool:
    """Small thread-safe pool for sync psycopg connections."""

    def __init__(
        self,
        *,
        factory: Callable[[], Any],
        max_size: int,
        wait_timeout_seconds: float,
    ) -> None:
        self._factory = factory
        self._max_size = max_size
        self._wait_timeout_seconds = wait_timeout_seconds
        self._available: LifoQueue[Any] = LifoQueue(maxsize=max_size)
        self._lock = Lock()
        self._open_count = 0

    def acquire(self):
        try:
            return self._available.get_nowait()
        except Empty:
            pass

        with self._lock:
            if self._open_count < self._max_size:
                conn = self._factory()
                self._open_count += 1
                return conn

        try:
            return self._available.get(timeout=self._wait_timeout_seconds)
        except Empty as exc:
            raise RuntimeError("Database connection pool exhausted") from exc

    def release(self, conn) -> None:
        if conn is None:
            return

        if getattr(conn, "closed", False) or getattr(conn, "broken", False):
            self._discard(conn)
            return

        try:
            # Reset the transaction before the next borrower sees this connection.
            conn.rollback()
        except Exception:
            self._discard(conn)
            return

        if getattr(conn, "closed", False) or getattr(conn, "broken", False):
            self._discard(conn)
            return

        self._available.put_nowait(conn)

    def _discard(self, conn) -> None:
        try:
            conn.close()
        except Exception:
            pass
        with self._lock:
            self._open_count = max(0, self._open_count - 1)

    def closeall(self) -> None:
        while True:
            try:
                conn = self._available.get_nowait()
            except Empty:
                break
            try:
                conn.close()
            except Exception:
                pass
        with self._lock:
            self._open_count = 0


_db_pool = _SyncConnectionPool(
    factory=_create_db_connection,
    max_size=DB_POOL_MAX_SIZE,
    wait_timeout_seconds=DB_POOL_WAIT_TIMEOUT_SECONDS,
)


@contextmanager
def _db_connection():
    conn = _db_pool.acquire()
    try:
        yield conn
    finally:
        _db_pool.release(conn)


async def _run_db_task(func: Callable[..., Any], *args: Any) -> Any:
    return await asyncio.to_thread(func, *args)


def _reset_db_pool_for_testing() -> None:
    _db_pool.closeall()


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

    return await _run_db_task(_fetch_coords_for_systems_sync, id64_list)


def _fetch_coords_for_systems_sync(
    id64_list: list[int],
) -> dict[int, dict[str, float]]:
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
            query = """
                SELECT id64, ST_AsText(coords) AS coordinates
                FROM systems_big
                WHERE id64 = ANY(%s)
            """
            cursor.execute(query, (id64_list,))
            rows = cursor.fetchall()
        finally:
            cursor.close()

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

    with _db_connection() as conn:
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
    spansh_results = await _fetch_spansh_autocomplete(term)
    if spansh_results:
        return spansh_results

    edsm_results = await _fetch_edsm_autocomplete(term)
    if edsm_results:
        return edsm_results

    return await _run_db_task(_local_system_name_suggestions, term)


def _local_system_name_suggestions(term: str) -> list[str]:
    db_results = _fetch_system_names_from_db(term, AUTOCOMPLETE_LIMIT)
    db_suggestions = _dedupe_autocomplete_names(db_results, AUTOCOMPLETE_LIMIT)
    if db_suggestions:
        return db_suggestions

    manual = _manual_name_suggestions(term, AUTOCOMPLETE_LIMIT)
    return _dedupe_autocomplete_names(manual, AUTOCOMPLETE_LIMIT)


def _dedupe_autocomplete_names(names: Iterable[str], limit: int) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for name in names:
        if not name:
            continue
        cleaned = name.strip()
        if not cleaned:
            continue
        normalized = cleaned.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(cleaned)
        if len(unique) >= limit:
            break
    return unique


async def _fetch_edsm_autocomplete(term: str) -> list[str]:
    query = term.strip()
    if len(query) < 2:
        return []

    encoded = quote(query, safe="")
    url = f"https://www.edsm.net/typeahead/systems/query/{encoded}"

    try:
        async with httpx.AsyncClient(
            timeout=2.0, headers={"User-Agent": EXTERNAL_USER_AGENT}
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
    except httpx.HTTPError:
        return []
    except ValueError:
        return []

    names: list[str] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, dict):
                candidate = (
                    item.get("value") or item.get("name") or item.get("label")
                )
                if candidate:
                    names.append(candidate)

    return _dedupe_autocomplete_names(names, AUTOCOMPLETE_LIMIT)


async def _fetch_spansh_autocomplete(term: str) -> list[str]:
    query = term.strip()
    if len(query) < 2:
        return []

    url = "https://spansh.co.uk/api/systems"

    try:
        async with httpx.AsyncClient(
            timeout=2.0, headers={"User-Agent": EXTERNAL_USER_AGENT}
        ) as client:
            response = await client.get(url, params={"q": query})
            response.raise_for_status()
            payload = response.json()
    except httpx.HTTPError:
        return []
    except ValueError:
        return []

    raw_entries = _extract_spansh_results(payload)
    return _dedupe_autocomplete_names(raw_entries, AUTOCOMPLETE_LIMIT)


def _extract_spansh_results(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        raw = payload.get("results")
    else:
        raw = payload

    if not isinstance(raw, list):
        return []

    names: list[str] = []
    for item in raw:
        if isinstance(item, str):
            names.append(item)
            continue
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("value")
        if name:
            names.append(name)
    return names


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
    return await _run_db_task(_fetch_total_systems_from_db_sync)


def _fetch_total_systems_from_db_sync() -> int:
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(TOTAL_SYSTEMS_QUERY)
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row or row[0] is None:
        raise RuntimeError("Total systems count unavailable")
    return int(row[0])


def _normalize_neighbor_row(
    row: Sequence[Any], include_facets: bool = False
) -> dict[str, Any]:
    coords = str(row[3]).replace("POINT Z (", "").replace(")", "").split()
    distance = row[4]
    if isinstance(distance, (Decimal, int, float)):
        normalized_distance: Any = float(distance)
    else:
        normalized_distance = distance
    normalized = {
        "id64": row[0],
        "name": row[1],
        "mainstar": row[2],
        "coords": {
            "x": float(coords[0]),
            "y": float(coords[1]),
            "z": float(coords[2]),
        },
        "distance": normalized_distance,
    }
    if include_facets:
        atmosphere_gases = row[5] if len(row) > 5 else []
        materials = row[6] if len(row) > 6 else []
        normalized["atmosphere_gases"] = list(atmosphere_gases or [])
        normalized["materials"] = list(materials or [])
    return normalized


def _neighbors_seeded_radii_for_request(radius: float) -> list[float]:
    radii: list[float] = [
        seeded_radius
        for seeded_radius in NEIGHBORS_SEEDED_RADII
        if seeded_radius < radius
    ]
    radii.append(radius)
    deduped: list[float] = []
    for candidate in radii:
        if deduped and deduped[-1] == candidate:
            continue
        deduped.append(candidate)
    return deduped


def _normalize_optional_filter(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _encode_neighbors_cursor(item: dict[str, Any]) -> str:
    payload = {
        "d": format(float(item["distance"]), ".17g"),
        "n": str(item.get("name") or ""),
        "i": int(item["id64"]),
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_neighbors_cursor(cursor: str) -> tuple[float, str, int]:
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        payload = json.loads(
            base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        )
        distance = float(payload["d"])
        name = str(payload["n"])
        id64 = int(payload["i"])
    except (
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
        UnicodeDecodeError,
    ) as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc
    return distance, name, id64


def _neighbors_page_payload(
    rows: Sequence[Sequence[Any]],
    page_size: int,
    include_facets: bool = False,
) -> dict[str, Any]:
    items = [
        _normalize_neighbor_row(row, include_facets=include_facets)
        for row in rows[:page_size]
    ]
    has_more = len(rows) > page_size
    next_cursor = (
        _encode_neighbors_cursor(items[-1]) if has_more and items else None
    )
    return {
        "items": items,
        "has_more": has_more,
        "next_cursor": next_cursor,
    }


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,  # or your Redis host
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,  # one day cache
    namespace="neighbors",
    serializer=PickleSerializer(),  # Or JsonSerializer if you prefer
)
async def fetch_neighbors_from_db(
    x: float,
    y: float,
    z: float,
    radius: float,
    limit: int,
    atmosphere_gas: str | None = None,
    material: str | None = None,
    include_facets: bool = False,
):
    return await _run_db_task(
        _fetch_neighbors_from_db_sync,
        x,
        y,
        z,
        radius,
        limit,
        atmosphere_gas,
        material,
        include_facets,
    )


def _fetch_neighbors_from_db_sync(
    x: float,
    y: float,
    z: float,
    radius: float,
    limit: int,
    atmosphere_gas: str | None = None,
    material: str | None = None,
    include_facets: bool = False,
) -> list[dict[str, Any]]:
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
            if NEIGHBORS_STATEMENT_TIMEOUT_MS > 0:
                cursor.execute(
                    f"SET LOCAL statement_timeout = {int(NEIGHBORS_STATEMENT_TIMEOUT_MS)};"
                )

            where_clauses: list[str] = [
                "ST_3DDWithin(s.coords, ref.geom, %s)"
            ]
            params: list[Any] = [x, y, z, radius]

            if atmosphere_gas:
                where_clauses.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM body_atmospheres ba
                        JOIN atmosphere_gases ag ON ag.id = ba.gas_id
                        WHERE ba.system_id64 = s.id64
                          AND ag.name ILIKE %s
                    )
                    """.strip()
                )
                params.append(f"%{atmosphere_gas}%")

            if material:
                where_clauses.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM body_materials bm
                        JOIN material_names mn ON mn.id = bm.material_id
                        WHERE bm.system_id64 = s.id64
                          AND mn.name ILIKE %s
                    )
                    """.strip()
                )
                params.append(f"%{material}%")

            facet_select = ""
            facet_joins = ""
            if include_facets:
                facet_select = """
                        ,
                        COALESCE(atm.atmosphere_gases, ARRAY[]::text[]) AS atmosphere_gases,
                        COALESCE(mat.materials, ARRAY[]::text[]) AS materials
                """
                facet_joins = """
                    LEFT JOIN LATERAL (
                        SELECT array_agg(DISTINCT ag.name ORDER BY ag.name) AS atmosphere_gases
                        FROM body_atmospheres ba
                        JOIN atmosphere_gases ag ON ag.id = ba.gas_id
                        WHERE ba.system_id64 = s.id64
                    ) atm ON true
                    LEFT JOIN LATERAL (
                        SELECT array_agg(DISTINCT mn.name ORDER BY mn.name) AS materials
                        FROM body_materials bm
                        JOIN material_names mn ON mn.id = bm.material_id
                        WHERE bm.system_id64 = s.id64
                    ) mat ON true
                """

            query = f"""
                WITH ref AS (
                    SELECT ST_SetSRID(ST_MakePoint(%s, %s, %s), 0) AS geom
                ), candidates AS (
                    SELECT
                        s.id64,
                        COALESCE(s.name, '') AS name,
                        s.mainstar,
                        ST_AsText(s.coords) AS coordinates,
                        ST_3DDistance(s.coords, ref.geom) AS distance
                        {facet_select}
                    FROM systems_big s, ref
                    {facet_joins}
                    WHERE {" AND ".join(where_clauses)}
                )
                SELECT *
                FROM candidates
                ORDER BY distance, name, id64
                LIMIT %s;
            """
            cursor.execute(query, tuple(params + [limit]))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [
        _normalize_neighbor_row(row, include_facets=include_facets)
        for row in rows
    ]


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="neighbors_paged_exact_v1",
    serializer=PickleSerializer(),
)
async def fetch_neighbors_page_from_db(
    x: float,
    y: float,
    z: float,
    radius: float,
    page_size: int,
    cursor_distance: float | None,
    cursor_name: str | None,
    cursor_id64: int | None,
    atmosphere_gas: str | None = None,
    material: str | None = None,
    include_facets: bool = False,
):
    return await _run_db_task(
        _fetch_neighbors_page_from_db_sync,
        x,
        y,
        z,
        radius,
        page_size,
        cursor_distance,
        cursor_name,
        cursor_id64,
        atmosphere_gas,
        material,
        include_facets,
    )


def _fetch_neighbors_page_from_db_sync(
    x: float,
    y: float,
    z: float,
    radius: float,
    page_size: int,
    cursor_distance: float | None,
    cursor_name: str | None,
    cursor_id64: int | None,
    atmosphere_gas: str | None = None,
    material: str | None = None,
    include_facets: bool = False,
) -> dict[str, Any]:
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
            if NEIGHBORS_STATEMENT_TIMEOUT_MS > 0:
                cursor.execute(
                    f"SET LOCAL statement_timeout = {int(NEIGHBORS_STATEMENT_TIMEOUT_MS)};"
                )

            where_clauses: list[str] = [
                "ST_3DDWithin(s.coords, ref.geom, %s)"
            ]
            params_prefix: list[Any] = [x, y, z, radius]

            if atmosphere_gas:
                where_clauses.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM body_atmospheres ba
                        JOIN atmosphere_gases ag ON ag.id = ba.gas_id
                        WHERE ba.system_id64 = s.id64
                          AND ag.name ILIKE %s
                    )
                    """.strip()
                )
                params_prefix.append(f"%{atmosphere_gas}%")

            if material:
                where_clauses.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM body_materials bm
                        JOIN material_names mn ON mn.id = bm.material_id
                        WHERE bm.system_id64 = s.id64
                          AND mn.name ILIKE %s
                    )
                    """.strip()
                )
                params_prefix.append(f"%{material}%")

            facet_select = ""
            facet_joins = ""
            if include_facets:
                facet_select = """
                        ,
                        COALESCE(atm.atmosphere_gases, ARRAY[]::text[]) AS atmosphere_gases,
                        COALESCE(mat.materials, ARRAY[]::text[]) AS materials
                """
                facet_joins = """
                    LEFT JOIN LATERAL (
                        SELECT array_agg(DISTINCT ag.name ORDER BY ag.name) AS atmosphere_gases
                        FROM body_atmospheres ba
                        JOIN atmosphere_gases ag ON ag.id = ba.gas_id
                        WHERE ba.system_id64 = s.id64
                    ) atm ON true
                    LEFT JOIN LATERAL (
                        SELECT array_agg(DISTINCT mn.name ORDER BY mn.name) AS materials
                        FROM body_materials bm
                        JOIN material_names mn ON mn.id = bm.material_id
                        WHERE bm.system_id64 = s.id64
                    ) mat ON true
                """

            base_query = f"""
                WITH ref AS (
                    SELECT ST_SetSRID(ST_MakePoint(%s, %s, %s), 0) AS geom
                ), candidates AS (
                    SELECT
                        s.id64,
                        COALESCE(s.name, '') AS name,
                        s.mainstar,
                        ST_AsText(s.coords) AS coordinates,
                        ST_3DDistance(s.coords, ref.geom) AS distance
                        {facet_select}
                    FROM systems_big s, ref
                    {facet_joins}
                    WHERE {" AND ".join(where_clauses)}
                )
            """

            if cursor_distance is None:
                query = (
                    base_query
                    + """
                SELECT *
                FROM candidates
                ORDER BY distance, name, id64
                LIMIT %s;
                """
                )
                params = tuple(params_prefix + [page_size + 1])
            else:
                query = (
                    base_query
                    + """
                SELECT *
                FROM candidates
                WHERE (
                    distance > %s
                    OR (distance = %s AND name > %s)
                    OR (distance = %s AND name = %s AND id64 > %s)
                )
                ORDER BY distance, name, id64
                LIMIT %s;
                """
                )
                params = tuple(
                    params_prefix
                    + [
                        cursor_distance,
                        cursor_distance,
                        cursor_name,
                        cursor_distance,
                        cursor_name,
                        cursor_id64,
                        page_size + 1,
                    ]
                )

            cursor.execute(query, params)
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return _neighbors_page_payload(
        rows, page_size, include_facets=include_facets
    )


async def fetch_neighbors_seeded_page_from_db(
    x: float,
    y: float,
    z: float,
    radius: float,
    page_size: int,
    atmosphere_gas: str | None = None,
    material: str | None = None,
    include_facets: bool = False,
) -> dict[str, Any]:
    fallback_page: dict[str, Any] | None = None
    for candidate_radius in _neighbors_seeded_radii_for_request(radius):
        page = await fetch_neighbors_page_from_db(
            x,
            y,
            z,
            candidate_radius,
            page_size,
            None,
            None,
            None,
            atmosphere_gas,
            material,
            include_facets,
        )
        fallback_page = page
        if page["has_more"] or len(page["items"]) >= page_size:
            return page
    return fallback_page or {
        "items": [],
        "has_more": False,
        "next_cursor": None,
    }


@app.get("/neighbors")
async def get_neighbors(
    x: float = Query(...),
    y: float = Query(...),
    z: float = Query(...),
    radius: float = Query(10.0),
    limit: int = Query(
        NEIGHBORS_DEFAULT_LIMIT,
        ge=1,
        le=NEIGHBORS_RESULT_LIMIT,
        description="Maximum number of nearby systems to return",
    ),
    page_size: int
    | None = Query(
        None,
        ge=1,
        le=NEIGHBORS_PAGE_SIZE_MAX,
        description="Optional page size for paginated neighbors browsing",
    ),
    cursor: str
    | None = Query(
        None,
        description="Opaque cursor returned by the previous paginated neighbors page",
    ),
    atmosphere_gas: str
    | None = Query(
        None,
        description="Optional atmosphere gas filter (matches systems containing at least one body with this gas)",
    ),
    material: str
    | None = Query(
        None,
        description="Optional body material filter (matches systems containing at least one body with this material)",
    ),
    include_facets: bool
    | None = Query(
        False,
        description="Include per-system atmosphere/material facet arrays",
    ),
):
    if radius <= 0:
        return JSONResponse(
            content={"error": "Radius must be positive"}, status_code=400
        )

    if NEIGHBORS_MAX_RADIUS and radius > NEIGHBORS_MAX_RADIUS:
        return JSONResponse(
            content={
                "error": f"Radius too large (max {NEIGHBORS_MAX_RADIUS:g} ly)",
            },
            status_code=400,
        )
    if cursor and page_size is None:
        return JSONResponse(
            content={"error": "cursor requires page_size"}, status_code=400
        )

    paged = page_size is not None
    normalized_atmosphere_gas = _normalize_optional_filter(atmosphere_gas)
    normalized_material = _normalize_optional_filter(material)
    async with _get_neighbors_semaphore(paged):
        try:
            if paged:
                if cursor:
                    (
                        cursor_distance,
                        cursor_name,
                        cursor_id64,
                    ) = _decode_neighbors_cursor(cursor)
                    results = await fetch_neighbors_page_from_db(
                        x,
                        y,
                        z,
                        radius,
                        page_size,
                        cursor_distance,
                        cursor_name,
                        cursor_id64,
                        normalized_atmosphere_gas,
                        normalized_material,
                        bool(include_facets),
                    )
                else:
                    results = await fetch_neighbors_seeded_page_from_db(
                        x,
                        y,
                        z,
                        radius,
                        page_size,
                        normalized_atmosphere_gas,
                        normalized_material,
                        bool(include_facets),
                    )
                return JSONResponse(content=results)

            results = await fetch_neighbors_from_db(
                x,
                y,
                z,
                radius,
                limit,
                normalized_atmosphere_gas,
                normalized_material,
                bool(include_facets),
            )
            return JSONResponse(content=results)
        except HTTPException:
            raise
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
    return await _run_db_task(_fetch_system_from_db_sync, name_or_id)


def _fetch_system_from_db_sync(name_or_id: str):
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
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
        finally:
            cursor.close()

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


def _parse_point_wkt(point_wkt: Optional[str]) -> Optional[dict[str, float]]:
    if not point_wkt or not isinstance(point_wkt, str):
        return None

    cleaned = point_wkt.strip()
    if cleaned.upper().startswith("POINT Z"):
        cleaned = cleaned[7:].strip()
    elif cleaned.upper().startswith("POINT"):
        cleaned = cleaned[5:].strip()

    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = cleaned[1:-1].strip()

    parts = cleaned.split()
    if len(parts) != 3:
        return None
    try:
        x, y, z = (float(part) for part in parts)
    except ValueError:
        return None

    return {"x": x, "y": y, "z": z}


def _format_neutron_result(row: Optional[Sequence[Any]]):
    if row is None:
        return None

    neutron_id64: Optional[int]
    neutron_name: Optional[str]
    coords_wkt: Optional[str] = None

    if len(row) >= 5:
        neutron_id64 = row[0]
        neutron_name = row[1]
        coords_wkt = row[3]
        distance_ly = row[4]
    elif len(row) >= 3:
        neutron_id64 = row[0]
        neutron_name = row[1]
        distance_ly = row[2]
    else:
        return None

    if distance_ly is None:
        formatted_distance: Optional[float] = None
    elif isinstance(distance_ly, (int, float, Decimal)):
        formatted_distance = float(distance_ly)
    else:
        formatted_distance = None

    coords = _parse_point_wkt(coords_wkt)

    payload: dict[str, Any] = {
        "neutron_id64": int(neutron_id64)
        if neutron_id64 is not None
        else None,
        "neutron_name": neutron_name,
        "distance_ly": formatted_distance,
    }
    if coords is not None:
        payload["coords"] = coords

    return payload


def _format_neutron_results(rows: Optional[Sequence[Sequence[Any]]]):
    if not rows:
        return None

    formatted: list[dict[str, Any]] = []
    for row in rows:
        payload = _format_neutron_result(row)
        if payload is not None:
            formatted.append(payload)

    return formatted or None


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="nearest_neutron_system_v2",
    serializer=PickleSerializer(),
)
async def fetch_nearest_neutron_star(system_name: str):
    return await _run_db_task(_fetch_nearest_neutron_star_sync, system_name)


def _fetch_nearest_neutron_star_sync(system_name: str):
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    neutron_id64,
                    neutron_name,
                    type,
                    ST_AsText(coordinates) AS coordinates_wkt,
                    distance_ly
                FROM nearest_neutron_star_ten_results(%s);
                """,
                (system_name,),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
    return _format_neutron_results(rows)


@cached(
    cache=RedisCache,
    endpoint=REDIS_HOST,
    port=REDIS_PORT,
    ttl=ONE_DAY_SECONDS,
    namespace="nearest_neutron_coords_v2",
    serializer=PickleSerializer(),
)
async def fetch_nearest_neutron_star_at_coords(x: float, y: float, z: float):
    return await _run_db_task(
        _fetch_nearest_neutron_star_at_coords_sync, x, y, z
    )


def _fetch_nearest_neutron_star_at_coords_sync(x: float, y: float, z: float):
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    neutron_id64,
                    neutron_name,
                    type,
                    ST_AsText(coordinates) AS coordinates_wkt,
                    distance_ly
                FROM nearest_neutron_star_at_coords_ten_results(%s, %s, %s);
                """,
                (x, y, z),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
    return _format_neutron_results(rows)


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
        "rotation_period": 86400,
        "orbital_period": 86400,
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
    with _db_connection() as conn:
        cursor = conn.cursor()
        identifier = name_or_id.strip()
        system_id64: Optional[int] = None
        is_numeric = identifier.isdigit() or (
            identifier.startswith("-") and identifier[1:].isdigit()
        )

        try:
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
                return None

            # get column names from cursor.description
            col_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()

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


@app.post("/bodies/{system_id64}/spansh-refresh", include_in_schema=False)
async def refresh_bodies_from_spansh(system_id64: int):
    if system_id64 <= 0:
        raise HTTPException(
            status_code=400, detail="system_id64 must be positive"
        )
    if spansh_ingest_system is None:
        detail = "Spansh ingestor unavailable"
        if _SPANSH_IMPORT_ERROR:
            detail = f"{detail}: {_SPANSH_IMPORT_ERROR}"
        raise HTTPException(status_code=501, detail=detail)

    def _run_refresh() -> None:
        with psycopg.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        ) as connection:
            spansh_ingest_system(system_id64, connection=connection)

    try:
        await asyncio.to_thread(_run_refresh)
    except SystemExit:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # pragma: no cover - defensive logging path
        logger.exception(
            "Failed to refresh system %s from Spansh", system_id64
        )
        raise HTTPException(
            status_code=500, detail="Failed to refresh system from Spansh"
        ) from exc

    return {"status": "ok", "system_id64": system_id64}


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

    headers = {"User-Agent": EXTERNAL_USER_AGENT}
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0), headers=headers
        ) as client:
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
    coords: Optional[Coords] = None


@app.get("/coords/predict", response_model=SystemResponse)
async def get_coords(name_or_id: str = Query(..., alias="q")):
    loop = asyncio.get_event_loop()
    try:
        if name_or_id.isdigit() or (
            name_or_id.startswith("-") and name_or_id[1:].isdigit()
        ):
            sys_obj = await loop.run_in_executor(
                None, system.from_id64, int(name_or_id), False, False
            )
        else:
            sys_obj = await loop.run_in_executor(
                None, system.from_name, name_or_id, False, False
            )
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


@app.get("/nearest-neutron-star", response_model=list[NeutronStarResponse])
async def get_nearest_neutron_star(
    system_name: str = Query(
        ...,
        min_length=1,
        description="Exact system name used to seed the search",
    )
):
    trimmed_name = system_name.strip()
    if not trimmed_name:
        raise HTTPException(status_code=400, detail="System name is required")

    try:
        result = await fetch_nearest_neutron_star(trimmed_name)
    except Exception:
        raise HTTPException(status_code=404, detail="System not found")
    if not result:
        return JSONResponse(
            content={"error": "No neutron star found"}, status_code=404
        )
    return result


@app.get(
    "/nearest-neutron-star/coords",
    response_model=list[NeutronStarResponse],
)
async def get_nearest_neutron_star_from_coords(
    x: float = Query(..., description="Cartesian X coordinate"),
    y: float = Query(..., description="Cartesian Y coordinate"),
    z: float = Query(..., description="Cartesian Z coordinate"),
):
    result = await fetch_nearest_neutron_star_at_coords(x, y, z)
    if not result:
        return JSONResponse(
            content={"error": "No neutron star found"}, status_code=404
        )
    return result


@app.get("/spansh/system/{system_id}", include_in_schema=False)
async def proxy_spansh_system(system_id: int):
    url = f"https://spansh.co.uk/api/system/{system_id}"
    headers = {"User-Agent": EXTERNAL_USER_AGENT}
    try:
        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
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

    headers = {"User-Agent": EXTERNAL_USER_AGENT}
    async with httpx.AsyncClient(headers=headers) as client:
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

    headers = {"User-Agent": EXTERNAL_USER_AGENT}
    try:
        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
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

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

from fastapi.responses import FileResponse


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(os.path.join(STATIC_DIR, "favicon.png"))


# Route to serve index.html
@app.get("/", include_in_schema=False)
def read_index():
    return FileResponse(INDEX_HTML_PATH)


@app.on_event("shutdown")
def close_db_pool() -> None:
    _db_pool.closeall()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=UVICORN_PORT)
