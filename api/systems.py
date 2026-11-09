import os
import asyncio
import logging
import time
import math
import hmac
import base64
import json
from contextlib import contextmanager
from queue import Empty, LifoQueue
from threading import Lock, Thread
import httpx
from decimal import Decimal
from fastapi import HTTPException
from fastapi import FastAPI, Query, Depends
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import psycopg
from pydantic import BaseModel, Field
from typing import Callable, Iterable, Optional, Any, Sequence
from urllib.parse import quote

try:
    from .edts.edtslib import pgnames, sector, system  # type: ignore[attr-defined]
except ImportError:
    import sys
    from pathlib import Path

    _local_edts = Path(__file__).resolve().parent / "edts"
    if _local_edts.exists():
        sys.path.insert(0, str(_local_edts))
        from edtslib import pgnames, sector, system  # type: ignore[attr-defined]
    else:
        raise
from dotenv import load_dotenv

try:
    from . import routing, entitlements, patreon_auth, permit_zones, fsd
except ImportError:  # running as a script / tests with api/ on sys.path
    import routing  # type: ignore[no-redef]
    import entitlements  # type: ignore[no-redef]
    import patreon_auth  # type: ignore[no-redef]
    import permit_zones  # type: ignore[no-redef]
    import fsd  # type: ignore[no-redef]

# Patreon identity (Layer 2) backs the entitlement gate: patrons who linked on
# the main site are recognised here via the shared cookie. No-op until
# EDGIS_IDENTITY_URL is configured.
entitlements.set_patron_resolver(patreon_auth.resolve_patron)

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
BOXELS_DEFAULT_LIMIT = max(
    1, int(os.getenv("BOXELS_DEFAULT_LIMIT") or "2400")
)
BOXELS_RESULT_LIMIT = max(
    BOXELS_DEFAULT_LIMIT,
    int(os.getenv("BOXELS_RESULT_LIMIT") or "40000"),
)
BOXELS_FIXED_MAX_ITERATIONS = max(
    1, int(os.getenv("BOXELS_FIXED_MAX_ITERATIONS") or "3000000")
)
BOXELS_ADAPTIVE_MAX_COARSE_ITERATIONS = max(
    1,
    int(os.getenv("BOXELS_ADAPTIVE_MAX_COARSE_ITERATIONS") or "800000"),
)
PREDICTED_SYSTEMS_DEFAULT_LIMIT = max(
    1, int(os.getenv("PREDICTED_SYSTEMS_DEFAULT_LIMIT") or "2500")
)
PREDICTED_SYSTEMS_RESULT_LIMIT = max(
    PREDICTED_SYSTEMS_DEFAULT_LIMIT,
    int(os.getenv("PREDICTED_SYSTEMS_RESULT_LIMIT") or "12000"),
)
PREDICTED_SYSTEMS_MAX_ITERATIONS = max(
    1, int(os.getenv("PREDICTED_SYSTEMS_MAX_ITERATIONS") or "1200000")
)
PREDICTED_SYSTEMS_AUTO_MCODE_MAX_ITERATIONS = max(
    1, int(os.getenv("PREDICTED_SYSTEMS_AUTO_MCODE_MAX_ITERATIONS") or "450000")
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
ADMIN_HTML_FILENAME = os.path.basename(
    os.getenv("ADMIN_HTML_FILENAME") or "admin.html"
)
ADMIN_HTML_PATH = os.path.join(STATIC_DIR, ADMIN_HTML_FILENAME)

REDIS_HOST = os.getenv("REDIS_HOST") or "localhost"
REDIS_PORT = int(os.getenv("REDIS_PORT") or "6379")
ONE_DAY_SECONDS = 60 * 60 * 24
# TTL for the per-system /bodies and /system-map caches (env-tunable). Short by
# default so a re-scanned system refreshes quickly; first view is always a miss.
BODIES_CACHE_TTL = int(os.getenv("BODIES_CACHE_TTL") or str(10 * 60))
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


class AdminActionRequest(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)


def _get_admin_token() -> str:
    return (os.getenv("ADMIN_TOKEN") or "").strip()


def _extract_admin_token(request: Request) -> str:
    bearer = (request.headers.get("authorization") or "").strip()
    if bearer.lower().startswith("bearer "):
        return bearer[7:].strip()
    return (request.headers.get("x-admin-token") or "").strip()


def _require_admin_token(request: Request) -> None:
    configured_token = _get_admin_token()
    if not configured_token:
        raise HTTPException(status_code=503, detail="Admin actions disabled")

    provided_token = _extract_admin_token(request)
    if not provided_token:
        raise HTTPException(status_code=401, detail="Missing admin token")

    if not hmac.compare_digest(provided_token, configured_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")


def _coerce_positive_int_param(
    params: dict[str, Any], key: str
) -> int:
    raw_value = params.get(key)
    if raw_value is None or (
        isinstance(raw_value, str) and not raw_value.strip()
    ):
        raise HTTPException(status_code=400, detail=f"{key} is required")

    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"{key} must be an integer"
        ) from exc

    if value <= 0:
        raise HTTPException(
            status_code=400, detail=f"{key} must be positive"
        )

    return value


async def _run_admin_spansh_refresh(
    params: dict[str, Any]
) -> dict[str, Any]:
    system_id64 = _coerce_positive_int_param(params, "system_id64")
    return await refresh_bodies_from_spansh(system_id64=system_id64)


ADMIN_ACTIONS = {
    "bodies-spansh-refresh": {
        "id": "bodies-spansh-refresh",
        "label": "Refresh bodies from Spansh",
        "description": (
            "Re-run the Spansh body ingestor for a system and upsert the"
            " refreshed data into Postgres."
        ),
        "parameters": [
            {
                "name": "system_id64",
                "label": "System ID64",
                "type": "number",
                "required": True,
                "placeholder": "10477373803",
                "description": "Positive system ID64 to refresh.",
            }
        ],
    }
}

ADMIN_ACTION_HANDLERS: dict[
    str, Callable[[dict[str, Any]], Any]
] = {
    "bodies-spansh-refresh": _run_admin_spansh_refresh,
}


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
    ORDER BY lower(name) USING ~<~
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
    # Local DB first: idx_systems_big_name_lower_prefix answers any prefix in
    # ~0.2-6 ms (ORDER BY ... USING ~<~ keeps it index-native), so there is no
    # reason to round-trip to spansh/EDSM on every keystroke. Fall back to the
    # external typeaheads only when our own data has nothing to offer (e.g. a
    # system not yet ingested).
    db_results = await _run_db_task(_local_system_name_suggestions, term)
    if db_results:
        return db_results

    spansh_results = await _fetch_spansh_autocomplete(term)
    if spansh_results:
        return spansh_results

    return await _fetch_edsm_autocomplete(term)


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
    row: Sequence[Any],
    include_facets: bool = False,
    include_allegiance: bool = False,
    include_controlling_power: bool = False,
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
    if include_allegiance:
        allegiance_index = 7 if include_facets else 5
        allegiance = row[allegiance_index] if len(row) > allegiance_index else None
        normalized["allegiance"] = allegiance or None
    if include_controlling_power:
        power_index = 5 + (2 if include_facets else 0) + (1 if include_allegiance else 0)
        controlling_power = row[power_index] if len(row) > power_index else None
        normalized["controlling_power"] = controlling_power or None
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
    include_allegiance: bool = False,
    include_controlling_power: bool = False,
) -> dict[str, Any]:
    items = [
        _normalize_neighbor_row(
            row,
            include_facets=include_facets,
            include_allegiance=include_allegiance,
            include_controlling_power=include_controlling_power,
        )
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
    include_allegiance: bool = False,
    include_controlling_power: bool = False,
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
        include_allegiance,
        include_controlling_power,
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
    include_allegiance: bool = False,
    include_controlling_power: bool = False,
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

            allegiance_select = ", sa.allegiance" if include_allegiance else ""
            allegiance_join = (
                "LEFT JOIN system_allegiances sa ON sa.system_id64 = s.id64"
                if include_allegiance
                else ""
            )
            controlling_power_select = (
                ", scp.controlling_power" if include_controlling_power else ""
            )
            controlling_power_join = (
                "LEFT JOIN system_controlling_powers scp ON scp.system_id64 = s.id64"
                if include_controlling_power
                else ""
            )

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
                        {allegiance_select}
                        {controlling_power_select}
                    FROM systems_big s
                    {allegiance_join}
                    {controlling_power_join}
                    {facet_joins}
                    CROSS JOIN ref
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
        _normalize_neighbor_row(
            row,
            include_facets=include_facets,
            include_allegiance=include_allegiance,
            include_controlling_power=include_controlling_power,
        )
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
    include_allegiance: bool = False,
    include_controlling_power: bool = False,
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
        include_allegiance,
        include_controlling_power,
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
    include_allegiance: bool = False,
    include_controlling_power: bool = False,
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

            allegiance_select = ", sa.allegiance" if include_allegiance else ""
            allegiance_join = (
                "LEFT JOIN system_allegiances sa ON sa.system_id64 = s.id64"
                if include_allegiance
                else ""
            )
            controlling_power_select = (
                ", scp.controlling_power" if include_controlling_power else ""
            )
            controlling_power_join = (
                "LEFT JOIN system_controlling_powers scp ON scp.system_id64 = s.id64"
                if include_controlling_power
                else ""
            )

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
                        {allegiance_select}
                        {controlling_power_select}
                    FROM systems_big s
                    {allegiance_join}
                    {controlling_power_join}
                    {facet_joins}
                    CROSS JOIN ref
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
        rows,
        page_size,
        include_facets=include_facets,
        include_allegiance=include_allegiance,
        include_controlling_power=include_controlling_power,
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
    include_allegiance: bool = False,
    include_controlling_power: bool = False,
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
            include_allegiance,
            include_controlling_power,
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
    include_allegiance: bool
    | None = Query(
        False,
        description="Include the latest observed system allegiance when available",
    ),
    include_controlling_power: bool
    | None = Query(
        False,
        description="Include the latest observed Powerplay controlling power when available",
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
    affiliation_args = (
        (bool(include_allegiance), bool(include_controlling_power))
        if include_allegiance or include_controlling_power
        else ()
    )
    async with _get_neighbors_semaphore(paged):
        try:
            if paged:
                if cursor:
                    (
                        cursor_distance,
                        cursor_name,
                        cursor_id64,
                    ) = _decode_neighbors_cursor(cursor)
                    page_args = (
                        x, y, z, radius, page_size, cursor_distance, cursor_name,
                        cursor_id64, normalized_atmosphere_gas,
                        normalized_material, bool(include_facets),
                    )
                    results = await fetch_neighbors_page_from_db(
                        *page_args,
                        *affiliation_args,
                    )
                else:
                    seeded_page_args = (
                        x, y, z, radius, page_size, normalized_atmosphere_gas,
                        normalized_material, bool(include_facets),
                    )
                    results = await fetch_neighbors_seeded_page_from_db(
                        *seeded_page_args,
                        *affiliation_args,
                    )
                return JSONResponse(content=results)

            if affiliation_args:
                results = await fetch_neighbors_from_db(
                    x,
                    y,
                    z,
                    radius,
                    limit,
                    normalized_atmosphere_gas,
                    normalized_material,
                    bool(include_facets),
                    *affiliation_args,
                )
            else:
                # Preserve the legacy call shape for integrations that monkeypatch
                # or wrap the neighborhood query.
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
                    SELECT s.id64, s.name, s.mainstar, ST_AsText(s.coords) AS coordinates
                    FROM systems_big s
                    WHERE LOWER(s.name) = LOWER(%s)
                    -- Names are not unique in systems_big. Prefer the record for
                    -- which body data exists; maps cannot be rendered otherwise.
                    ORDER BY EXISTS (
                        SELECT 1
                        FROM bodies b
                        WHERE b.system_id64 = s.id64
                    ) DESC, s.id64
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


def _sphere_intersects_aabb(
    cx: float,
    cy: float,
    cz: float,
    radius: float,
    ox: float,
    oy: float,
    oz: float,
    size: float,
) -> bool:
    max_x = ox + size
    max_y = oy + size
    max_z = oz + size
    closest_x = min(max(cx, ox), max_x)
    closest_y = min(max(cy, oy), max_y)
    closest_z = min(max(cz, oz), max_z)
    dx = cx - closest_x
    dy = cy - closest_y
    dz = cz - closest_z
    return (dx * dx + dy * dy + dz * dz) <= (radius * radius)


def _normalize_boxel_mcode(
    raw_mcode: str | None, *, default: str = "d"
) -> str:
    normalized = str(raw_mcode or default).strip().lower()
    if normalized not in {"a", "b", "c", "d", "e", "f", "g", "h"}:
        raise HTTPException(
            status_code=400,
            detail="mcode must be one of: a,b,c,d,e,f,g,h",
        )
    return normalized


def _sphere_contains_aabb(
    cx: float,
    cy: float,
    cz: float,
    radius: float,
    ox: float,
    oy: float,
    oz: float,
    size: float,
) -> bool:
    max_x = ox + size
    max_y = oy + size
    max_z = oz + size
    dx = max(abs(cx - ox), abs(cx - max_x))
    dy = max(abs(cy - oy), abs(cy - max_y))
    dz = max(abs(cz - oz), abs(cz - max_z))
    return (dx * dx + dy * dy + dz * dz) <= (radius * radius)


def _resolve_sector_name_for_coords(x: float, y: float, z: float) -> str | None:
    coords = (x, y, z)
    sector_obj: Any = None

    # EDTS signature compatibility: older variants may not support get_name.
    try:
        sector_obj = pgnames.get_sector(coords, allow_ha=False, get_name=True)
    except TypeError:
        try:
            sector_obj = pgnames.get_sector(coords, False)
        except TypeError:
            sector_obj = pgnames.get_sector(coords)
    except Exception as exc:  # pragma: no cover - defensive safety for lookup edge-cases
        logger.warning("Boxel sector lookup failed at %s: %s", coords, exc)
        return None

    if sector_obj is None:
        return None
    if isinstance(sector_obj, str):
        return sector_obj
    return getattr(sector_obj, "name", None)


def _estimate_boxel_iteration_count(
    x: float,
    y: float,
    z: float,
    radius: float,
    mcode: str,
) -> int:
    cube_width = float(sector.get_mcode_cube_width(mcode))
    if cube_width <= 0:
        return 0
    min_x = x - radius
    min_y = y - radius
    min_z = z - radius
    max_x = x + radius
    max_y = y + radius
    max_z = z + radius
    start_origin = pgnames.get_boxel_origin((min_x, min_y, min_z), mcode)
    if start_origin is None:
        return 0
    nx = int(math.floor((max_x - start_origin.x) / cube_width)) + 1
    ny = int(math.floor((max_y - start_origin.y) / cube_width)) + 1
    nz = int(math.floor((max_z - start_origin.z) / cube_width)) + 1
    return max(nx, 0) * max(ny, 0) * max(nz, 0)


def _infer_prediction_mcode_for_radius(
    x: float,
    y: float,
    z: float,
    radius: float,
) -> str:
    """
    Select the finest viable mcode for this view, bounded by an iteration budget.
    This gives denser predicted overlays while keeping requests responsive.
    """
    for candidate in ("a", "b", "c", "d", "e", "f", "g", "h"):
        estimated = _estimate_boxel_iteration_count(x, y, z, radius, candidate)
        if (
            estimated > 0
            and estimated <= PREDICTED_SYSTEMS_AUTO_MCODE_MAX_ITERATIONS
            and estimated <= PREDICTED_SYSTEMS_MAX_ITERATIONS
        ):
            return candidate
    return "h"


def _materialize_predicted_name(proto_name: str) -> str:
    # pgnames returns the boxel prefix; the per-system sequence number N2 is
    # appended here (the boxel's representative is the first system, N2 = 0).
    # With N1 present the proto ends "...d1-"; without N1 it ends in the mass-code
    # letter "...d". Both need the "0" suffix -- so append it whenever the name
    # doesn't already end in a digit (a complete name like "...d5-54" is left as
    # is). Previously only the "ends with -" case was handled, so no-N1 boxels
    # produced invalid names like "Pru Theia FK-A b" (missing the trailing 0).
    name = str(proto_name or "").strip()
    if name and not name[-1].isdigit():
        return f"{name}0"
    return name


@app.get("/predicted-systems")
async def get_predicted_systems(
    x: float = Query(...),
    y: float = Query(...),
    z: float = Query(...),
    radius: float = Query(20.0, gt=0),
    mcode: str | None = Query(
        None, description="Optional mcode override (a..h)."
    ),
    limit: int = Query(
        PREDICTED_SYSTEMS_DEFAULT_LIMIT,
        ge=1,
        description="Maximum number of predicted systems to return.",
    ),
):
    effective_limit = min(int(limit), PREDICTED_SYSTEMS_RESULT_LIMIT)
    normalized_mcode = _normalize_boxel_mcode(
        mcode,
        default=_infer_prediction_mcode_for_radius(x, y, z, radius),
    )
    cube_width = float(sector.get_mcode_cube_width(normalized_mcode))
    if cube_width <= 0:
        raise HTTPException(status_code=400, detail="Invalid mcode size")

    min_x = x - radius
    min_y = y - radius
    min_z = z - radius
    max_x = x + radius
    max_y = y + radius
    max_z = z + radius

    start_origin = pgnames.get_boxel_origin((min_x, min_y, min_z), normalized_mcode)
    if start_origin is None:
        raise HTTPException(status_code=400, detail="Invalid coordinates")

    nx = int(math.floor((max_x - start_origin.x) / cube_width)) + 1
    ny = int(math.floor((max_y - start_origin.y) / cube_width)) + 1
    nz = int(math.floor((max_z - start_origin.z) / cube_width)) + 1
    estimated_iterations = max(nx, 0) * max(ny, 0) * max(nz, 0)
    if estimated_iterations > PREDICTED_SYSTEMS_MAX_ITERATIONS:
        raise HTTPException(
            status_code=400,
            detail="Requested predicted grid is too broad; reduce radius or use a coarser mcode",
        )

    items: list[dict[str, Any]] = []
    truncated = False

    ox = float(start_origin.x)
    while ox <= max_x:
        oy = float(start_origin.y)
        while oy <= max_y:
            oz = float(start_origin.z)
            while oz <= max_z:
                if _sphere_intersects_aabb(x, y, z, radius, ox, oy, oz, cube_width):
                    center_x = ox + (cube_width * 0.5)
                    center_y = oy + (cube_width * 0.5)
                    center_z = oz + (cube_width * 0.5)
                    center_distance = math.sqrt(
                        ((center_x - x) ** 2)
                        + ((center_y - y) ** 2)
                        + ((center_z - z) ** 2)
                    )
                    proto = pgnames.get_system(
                        (center_x, center_y, center_z),
                        normalized_mcode,
                        allow_ha=False,
                    )
                    proto_name = _materialize_predicted_name(
                        str(getattr(proto, "name", "") or "")
                    )
                    if proto_name:
                        items.append(
                            {
                                "name": proto_name,
                                "coords": {
                                    "x": center_x,
                                    "y": center_y,
                                    "z": center_z,
                                },
                                "distance": center_distance,
                                "mcode": normalized_mcode,
                                "uncertainty": cube_width * 0.5,
                            }
                        )
                        if len(items) >= effective_limit:
                            truncated = True
                            break
                oz += cube_width
            if truncated:
                break
            oy += cube_width
        if truncated:
            break
        ox += cube_width

    items.sort(key=lambda item: (item["distance"], item["name"]))
    return JSONResponse(
        content={
            "items": items,
            "truncated": truncated,
            "mcode": normalized_mcode,
            "size": cube_width,
            "estimated_iterations": estimated_iterations,
            "requested_limit": int(limit),
            "applied_limit": effective_limit,
            "max_limit": PREDICTED_SYSTEMS_RESULT_LIMIT,
        }
    )


@app.get("/boxels")
async def get_boxels(
    x: float = Query(...),
    y: float = Query(...),
    z: float = Query(...),
    radius: float = Query(20.0, gt=0),
    mode: str = Query(
        "adaptive",
        description="Grid extraction mode: adaptive (octree-like) or fixed",
    ),
    mcode: str | None = Query(
        None,
        description="Used only with mode=fixed",
    ),
    min_mcode: str = Query(
        "b",
        description="Smallest cube size for adaptive mode (a finest .. h coarsest)",
    ),
    max_mcode: str = Query(
        "g",
        description="Largest cube size for adaptive mode (a finest .. h coarsest)",
    ),
    resolve_sector: bool = Query(
        False,
        description="Whether to resolve sector names for each returned cell",
    ),
    fixed_mcode: str = Query(
        "d",
        description="Deprecated alias of mcode for fixed mode compatibility",
    ),
    limit: int = Query(
        BOXELS_DEFAULT_LIMIT,
        ge=1,
        le=BOXELS_RESULT_LIMIT,
        description="Maximum number of cells to return",
    ),
):
    normalized_mode = str(mode or "adaptive").strip().lower()
    if normalized_mode not in {"adaptive", "fixed"}:
        raise HTTPException(
            status_code=400,
            detail="mode must be one of: adaptive,fixed",
        )

    min_x = x - radius
    min_y = y - radius
    min_z = z - radius
    max_x = x + radius
    max_y = y + radius
    max_z = z + radius

    if normalized_mode == "fixed":
        normalized_mcode = _normalize_boxel_mcode(
            mcode if mcode else fixed_mcode, default="d"
        )
        cube_width = float(sector.get_mcode_cube_width(normalized_mcode))
        if cube_width <= 0:
            raise HTTPException(status_code=400, detail="Invalid boxel size")

        start_origin = pgnames.get_boxel_origin(
            (min_x, min_y, min_z), normalized_mcode
        )
        if start_origin is None:
            raise HTTPException(
                status_code=400, detail="Invalid coordinates"
            )

        nx = int(math.floor((max_x - start_origin.x) / cube_width)) + 1
        ny = int(math.floor((max_y - start_origin.y) / cube_width)) + 1
        nz = int(math.floor((max_z - start_origin.z) / cube_width)) + 1
        estimated_iterations = max(nx, 0) * max(ny, 0) * max(nz, 0)

        if estimated_iterations > BOXELS_FIXED_MAX_ITERATIONS:
            raise HTTPException(
                status_code=400,
                detail="Requested radius/boxel size is too broad; increase mcode or reduce radius",
            )

        boxels: list[dict[str, Any]] = []
        truncated = False

        ox = float(start_origin.x)
        while ox <= max_x:
            oy = float(start_origin.y)
            while oy <= max_y:
                oz = float(start_origin.z)
                while oz <= max_z:
                    if _sphere_intersects_aabb(
                        x, y, z, radius, ox, oy, oz, cube_width
                    ):
                        center_x = ox + (cube_width * 0.5)
                        center_y = oy + (cube_width * 0.5)
                        center_z = oz + (cube_width * 0.5)
                        center_distance = math.sqrt(
                            ((center_x - x) ** 2)
                            + ((center_y - y) ** 2)
                            + ((center_z - z) ** 2)
                        )
                        sector_name = (
                            _resolve_sector_name_for_coords(
                                center_x, center_y, center_z
                            )
                            if resolve_sector
                            else None
                        )
                        boxels.append(
                            {
                                "id": f"{ox:.3f}_{oy:.3f}_{oz:.3f}_{normalized_mcode}",
                                "origin": {"x": ox, "y": oy, "z": oz},
                                "center": {
                                    "x": center_x,
                                    "y": center_y,
                                    "z": center_z,
                                },
                                "size": cube_width,
                                "mcode": normalized_mcode,
                                "sector": sector_name,
                                "distance": center_distance,
                            }
                        )
                        if len(boxels) >= limit:
                            truncated = True
                            break
                    oz += cube_width
                if truncated:
                    break
                oy += cube_width
            if truncated:
                break
            ox += cube_width

        boxels.sort(key=lambda item: (item["distance"], item["id"]))
        return JSONResponse(
            content={
                "items": boxels,
                "truncated": truncated,
                "mode": normalized_mode,
                "mcode": normalized_mcode,
                "size": cube_width,
                "estimated_iterations": estimated_iterations,
            }
        )

    normalized_min_mcode = _normalize_boxel_mcode(min_mcode, default="b")
    normalized_max_mcode = _normalize_boxel_mcode(max_mcode, default="g")
    if normalized_min_mcode > normalized_max_mcode:
        raise HTTPException(
            status_code=400,
            detail="min_mcode must be finer or equal to max_mcode (a..h)",
        )

    finest_width = float(sector.get_mcode_cube_width(normalized_min_mcode))
    coarsest_width = float(
        sector.get_mcode_cube_width(normalized_max_mcode)
    )
    if finest_width <= 0 or coarsest_width <= 0:
        raise HTTPException(status_code=400, detail="Invalid mcode range")

    start_origin = pgnames.get_boxel_origin(
        (min_x, min_y, min_z), normalized_max_mcode
    )
    if start_origin is None:
        raise HTTPException(status_code=400, detail="Invalid coordinates")

    nx = int(math.floor((max_x - start_origin.x) / coarsest_width)) + 1
    ny = int(math.floor((max_y - start_origin.y) / coarsest_width)) + 1
    nz = int(math.floor((max_z - start_origin.z) / coarsest_width)) + 1
    coarse_iterations = max(nx, 0) * max(ny, 0) * max(nz, 0)
    if coarse_iterations > BOXELS_ADAPTIVE_MAX_COARSE_ITERATIONS:
        raise HTTPException(
            status_code=400,
            detail="Requested adaptive grid is too broad; reduce radius or increase max_mcode",
        )

    boxels: list[dict[str, Any]] = []
    truncated = False
    visited_nodes = 0

    stack: list[tuple[float, float, float, str, float]] = []
    ox = float(start_origin.x)
    while ox <= max_x:
        oy = float(start_origin.y)
        while oy <= max_y:
            oz = float(start_origin.z)
            while oz <= max_z:
                stack.append(
                    (
                        ox,
                        oy,
                        oz,
                        normalized_max_mcode,
                        coarsest_width,
                    )
                )
                oz += coarsest_width
            oy += coarsest_width
        ox += coarsest_width

    while stack:
        cell_ox, cell_oy, cell_oz, cell_mcode, cell_size = stack.pop()
        visited_nodes += 1

        if not _sphere_intersects_aabb(
            x, y, z, radius, cell_ox, cell_oy, cell_oz, cell_size
        ):
            continue

        if len(boxels) >= limit:
            truncated = True
            break

        center_x = cell_ox + (cell_size * 0.5)
        center_y = cell_oy + (cell_size * 0.5)
        center_z = cell_oz + (cell_size * 0.5)
        center_distance = math.sqrt(
            ((center_x - x) ** 2)
            + ((center_y - y) ** 2)
            + ((center_z - z) ** 2)
        )

        can_subdivide = (
            cell_mcode > normalized_min_mcode and cell_size > finest_width
        )
        fully_inside = _sphere_contains_aabb(
            x, y, z, radius, cell_ox, cell_oy, cell_oz, cell_size
        )
        if (not can_subdivide) or fully_inside:
            sector_name = (
                _resolve_sector_name_for_coords(center_x, center_y, center_z)
                if resolve_sector
                else None
            )
            boxels.append(
                {
                    "id": f"{cell_ox:.3f}_{cell_oy:.3f}_{cell_oz:.3f}_{cell_mcode}",
                    "origin": {"x": cell_ox, "y": cell_oy, "z": cell_oz},
                    "center": {
                        "x": center_x,
                        "y": center_y,
                        "z": center_z,
                    },
                    "size": cell_size,
                    "mcode": cell_mcode,
                    "sector": sector_name,
                    "distance": center_distance,
                }
            )
            continue

        child_mcode = chr(ord(cell_mcode) - 1)
        child_size = cell_size * 0.5
        for dx in (0.0, child_size):
            for dy in (0.0, child_size):
                for dz in (0.0, child_size):
                    stack.append(
                        (
                            cell_ox + dx,
                            cell_oy + dy,
                            cell_oz + dz,
                            child_mcode,
                            child_size,
                        )
                    )

    boxels.sort(key=lambda item: (item["distance"], item["id"]))
    return JSONResponse(
        content={
            "items": boxels,
            "truncated": truncated,
            "mode": normalized_mode,
            "min_mcode": normalized_min_mcode,
            "max_mcode": normalized_max_mcode,
            "size_min": finest_width,
            "size_max": coarsest_width,
            "coarse_iterations": coarse_iterations,
            "visited_nodes": visited_nodes,
        }
    )


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
                    SELECT s.id64
                    FROM systems_big s
                    WHERE LOWER(s.name) = LOWER(%s)
                    -- A duplicate system name can exist in imported EDDN data.
                    -- Prefer the candidate that can actually serve /bodies.
                    ORDER BY EXISTS (
                        SELECT 1
                        FROM bodies b
                        WHERE b.system_id64 = s.id64
                    ) DESC, s.id64
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


def fetch_system_map_from_db(
    name_or_id: str, mode: Optional[str] = None
) -> dict[str, Any] | None:
    """Return map-only system metadata, bodies and sparse station records."""
    body_records = fetch_bodies_from_db(name_or_id, mode=mode)
    if not body_records:
        return None
    system_id64 = int(body_records[0]["system_id64"])
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT s.id64, s.name, sa.allegiance
                FROM systems_big s
                LEFT JOIN system_allegiances sa ON sa.system_id64 = s.id64
                WHERE s.id64 = %s
                """,
                (system_id64,),
            )
            system_row = cursor.fetchone()
            if system_row is None:
                return None
            cursor.execute(
                """
                SELECT market_id, system_id64, body_id, body_name, parents, name,
                       station_type, is_carrier, is_planetary,
                       distance_from_arrival_ls, latitude, longitude,
                       large_pads, medium_pads, small_pads, services, economies,
                       primary_economy, government, allegiance,
                       controlling_faction, controlling_faction_state,
                       station_state, last_seen_at
                FROM stations
                WHERE system_id64 = %s
                ORDER BY is_carrier, name, market_id
                """,
                (system_id64,),
            )
            station_rows = cursor.fetchall()
            station_columns = [entry[0] for entry in cursor.description]
        finally:
            cursor.close()
    return {
        "system": {
            "id64": system_row[0],
            "name": system_row[1],
            "allegiance": system_row[2],
        },
        "bodies": body_records,
        "stations": [
            {
                column: value
                for column, value in zip(station_columns, row)
                if value is not None
            }
            for row in station_rows
        ],
    }


_bodies_cache = RedisCache(
    endpoint=REDIS_HOST, port=REDIS_PORT, namespace="bodies",
    serializer=PickleSerializer(),
)


async def _fetch_bodies_cached(
    name_or_id: str, mode: Optional[str], body_id: Optional[int]
):
    """Best-effort Redis cache for the heavy per-system bodies query.

    Keyed by (name_or_id, mode, body_id). If Redis is unavailable the query
    still runs (we skip the cache), so a cache outage can never take the
    endpoint down. Not-found (None) results are not cached. The first view of a
    system is a miss (fresh); later views within BODIES_CACHE_TTL hit Redis.
    """
    cache_key = f"{name_or_id}|{mode}|{body_id}"
    try:
        cached_value = await _bodies_cache.get(cache_key)
    except Exception:
        logger.warning("bodies cache read failed; using DB", exc_info=True)
        cached_value = None
    if cached_value is not None:
        return cached_value
    result = await _run_db_task(fetch_bodies_from_db, name_or_id, mode, body_id)
    if result is not None:
        try:
            await _bodies_cache.set(cache_key, result, ttl=BODIES_CACHE_TTL)
        except Exception:
            logger.warning("bodies cache write failed", exc_info=True)
    return result


@app.get("/bodies", include_in_schema=True)
async def bodies(
    name_or_id: str = Query(..., description="The name or id64 of the system"),
    body_id: Optional[int] = Query(
        None, description="Optional body_id to narrow to a single body"
    ),
    mode: Optional[str] = Query(
        None, description="Optional response mode adjustments"
    ),
):
    result = await _fetch_bodies_cached(name_or_id, mode, body_id)
    if result is None:
        return JSONResponse(
            content={"error": SYSTEM_NOT_FOUND}, status_code=404
        )
    return result


_system_map_cache = RedisCache(
    endpoint=REDIS_HOST, port=REDIS_PORT, namespace="system_map",
    serializer=PickleSerializer(),
)


async def _fetch_system_map_cached(name_or_id: str, mode: Optional[str]):
    """Best-effort Redis cache for /system-map (same heavy query as /bodies)."""
    cache_key = f"{name_or_id}|{mode}"
    try:
        cached_value = await _system_map_cache.get(cache_key)
    except Exception:
        logger.warning("system_map cache read failed; using DB", exc_info=True)
        cached_value = None
    if cached_value is not None:
        return cached_value
    result = await _run_db_task(fetch_system_map_from_db, name_or_id, mode)
    if result is not None:
        try:
            await _system_map_cache.set(cache_key, result, ttl=BODIES_CACHE_TTL)
        except Exception:
            logger.warning("system_map cache write failed", exc_info=True)
    return result


@app.get("/system-map", include_in_schema=True)
async def system_map(
    name_or_id: str = Query(..., description="The name or id64 of the system"),
    mode: Optional[str] = Query(
        None, description="Optional response mode adjustments for bodies"
    ),
):
    """Return the system-map data without changing the established /bodies API."""
    result = await _fetch_system_map_cached(name_or_id, mode)
    if result is None:
        return JSONResponse(
            content={"error": SYSTEM_NOT_FOUND}, status_code=404
        )
    return result


# Not an HTTP route any more: the public "refresh system from Spansh" button was
# removed (it was useful early on, no longer). Kept as a plain helper for the
# admin-gated ADMIN_ACTIONS["bodies-spansh-refresh"] tool only.
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


@app.get("/admin/actions", include_in_schema=False)
def list_admin_actions(request: Request):
    _require_admin_token(request)
    return {"actions": list(ADMIN_ACTIONS.values())}


@app.post("/admin/actions/{action_id}", include_in_schema=False)
async def run_admin_action(
    action_id: str, payload: AdminActionRequest, request: Request
):
    _require_admin_token(request)
    handler = ADMIN_ACTION_HANDLERS.get(action_id)
    if handler is None:
        raise HTTPException(status_code=404, detail="Unknown admin action")

    return {
        "action": action_id,
        "result": await handler(payload.params),
    }


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
async def get_coords_predict(name_or_id: str = Query(..., alias="q")):
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

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(os.path.join(STATIC_DIR, "favicon.png"))


@app.get("/admin", include_in_schema=False)
def read_admin():
    return FileResponse(ADMIN_HTML_PATH)


# Route to serve index.html
@app.get("/", include_in_schema=False)
def read_index():
    return FileResponse(INDEX_HTML_PATH)


# Route plotter test UI (the JSON API is at GET /route).
@app.get("/plotter", include_in_schema=False)
def read_route_plotter():
    return FileResponse(os.path.join(STATIC_DIR, "route.html"))


# ---------------------------------------------------------------------------
# Route plotter (neutron-boosted). The KD-tree of ~5M neutrons lives resident
# in this worker; the solver and entitlement gate live in routing.py /
# entitlements.py.
# ---------------------------------------------------------------------------

_neutron_index: "routing.NeutronIndex | None" = None
_neutron_index_error: str | None = None
_neutron_index_building = False
_neutron_index_lock = Lock()
_route_solve_semaphore: asyncio.Semaphore | None = None
# In-flight route solves, keyed by cache_key: a second identical request awaits
# the first instead of launching a duplicate (a 90 s rim route must not run
# twice concurrently). Holds the cacheable result dict (no caller "access").
_route_inflight: dict[str, "asyncio.Future"] = {}
# Best-effort Redis cache for whole routes. Keyed by (from, to, range, width,
# index version) so an index rebuild (changed neutron count) invalidates
# naturally. Caller-specific fields (access) are set fresh on every hit.
ROUTE_CACHE_TTL = int(os.getenv("ROUTE_CACHE_TTL") or str(24 * 60 * 60))
# Max seconds for the FSD-injection (pass 2) solve, which only runs after a
# non-injected solve already failed. Bounds the worst-case double-pass.
_ROUTE_INJECT_TIME_CAP = float(os.getenv("ROUTE_INJECT_TIME_CAP") or "45")
_route_cache = RedisCache(
    endpoint=REDIS_HOST, port=REDIS_PORT, namespace="route",
    serializer=PickleSerializer(),
)


# Companion (secondary) neutrons are folded into the index up to this arrival
# distance; each request then opts in via companion_max_ls (0 = main-star only,
# the default, filtered back out in graph_route/solve_route). A build cap keeps
# the index lean and bars absurd supercruise distances. star_type_id 28 = 'N'.
COMPANION_NEUTRON_BUILD_MAX_LS = 2000.0


def _fetch_neutron_rows_sync():
    with _db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT ST_X(coords), ST_Y(coords), ST_Z(coords), id64, name "
                "FROM systems_big WHERE mainstar IN ('N', 'Neutron Star')"
            )
            rows = cursor.fetchall()  # main-star neutrons; arrival_ls defaults 0
            # One point per system with a companion neutron (the closest one),
            # arrival distance as the 6th column. Uses idx_bodies_companion_neutron.
            cursor.execute(
                "SELECT ST_X(s.coords), ST_Y(s.coords), ST_Z(s.coords), "
                "b.system_id64, s.name, min(b.distance_from_arrival_ls) "
                "FROM bodies b JOIN systems_big s ON s.id64 = b.system_id64 "
                "WHERE b.star_type_id = 28 AND b.distance_from_arrival_ls > 0 "
                "AND b.distance_from_arrival_ls <= %s "
                "GROUP BY b.system_id64, s.coords, s.name",
                (COMPANION_NEUTRON_BUILD_MAX_LS,),
            )
            rows.extend(cursor.fetchall())
            return rows
        finally:
            cursor.close()


def _build_neutron_index() -> None:
    global _neutron_index, _neutron_index_error, _neutron_index_building
    try:
        index = routing.NeutronIndex.build(_fetch_neutron_rows_sync)
        _neutron_index = index
        _neutron_index_error = None
        logger.info("Neutron route index built: %d stars", index.count)
    except Exception as exc:  # pragma: no cover - logged, non-fatal
        _neutron_index_error = str(exc)
        logger.exception("Neutron route index build failed")
    finally:
        with _neutron_index_lock:
            _neutron_index_building = False


def _kick_neutron_index_build() -> bool:
    """Start a background index build if one isn't already running/done."""

    global _neutron_index_building
    with _neutron_index_lock:
        if _neutron_index is not None or _neutron_index_building:
            return False
        _neutron_index_building = True
    Thread(
        target=_build_neutron_index, name="neutron-index-build", daemon=True
    ).start()
    return True


def _solve_route_sync(
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    jump_range: float,
    beam_width: int | None,
    boost_mult: float = 4.0,
    companion_max_ls: float = 0.0,
    inject_mult: float = 1.0,
):
    index = _neutron_index
    if index is None:  # pragma: no cover - guarded by caller
        raise routing.RouteError("not-ready", "route index not built")
    with _db_connection() as conn:

        def fetch_stars_within(pos, radius):
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT ST_X(coords), ST_Y(coords), ST_Z(coords), name, "
                    "mainstar, id64 FROM systems_big "
                    "WHERE ST_3DDWithin(coords, ST_MakePoint(%s, %s, %s), %s)",
                    (pos[0], pos[1], pos[2], radius),
                )
                return cursor.fetchall()
            finally:
                cursor.close()

        def fetch_stars_near_segment(p0, p1, width):
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT ST_X(coords), ST_Y(coords), ST_Z(coords), name, "
                    "mainstar, id64 FROM systems_big "
                    "WHERE ST_3DDWithin(coords, ST_MakeLine("
                    "ST_MakePoint(%s, %s, %s), ST_MakePoint(%s, %s, %s)), %s) "
                    "LIMIT 250000",
                    (p0[0], p0[1], p0[2], p1[0], p1[1], p1[2], width),
                )
                return cursor.fetchall()
            finally:
                cursor.close()

        def fetch_stars_toward(pos, radius, target, limit):
            # Sorting the full reach-ball is ~6 s/step in the bubble; a small
            # ball around the "ahead point" (toward the target, still fully
            # inside reach) is ~40 ms and contains the stars greedy wants.
            # Fall back to the full sorted ball only when it comes up empty
            # (sparse space, where that ball is small anyway).
            d = math.dist(pos, target)
            r = radius / 4.0
            f = (radius - r) / d if d > 0 else 0.0
            ahead = tuple(p + (t - p) * f for p, t in zip(pos, target))
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT ST_X(coords), ST_Y(coords), ST_Z(coords), name, "
                    "mainstar, id64 FROM systems_big "
                    "WHERE ST_3DDWithin(coords, ST_MakePoint(%s, %s, %s), %s) "
                    "ORDER BY ST_3DDistance(coords, ST_MakePoint(%s, %s, %s)) "
                    "LIMIT %s",
                    (ahead[0], ahead[1], ahead[2], r,
                     target[0], target[1], target[2], limit),
                )
                rows = cursor.fetchall()
                if rows:
                    return rows
                cursor.execute(
                    "SELECT ST_X(coords), ST_Y(coords), ST_Z(coords), name, "
                    "mainstar, id64 FROM systems_big "
                    "WHERE ST_3DDWithin(coords, ST_MakePoint(%s, %s, %s), %s) "
                    "ORDER BY ST_3DDistance(coords, ST_MakePoint(%s, %s, %s)) "
                    "LIMIT %s",
                    (pos[0], pos[1], pos[2], radius,
                     target[0], target[1], target[2], limit),
                )
                return cursor.fetchall()
            finally:
                cursor.close()

        # Corridor graph A* is the primary engine: min-jumps by construction,
        # measured to match or beat the beam on every benchmarked route. The
        # beam stays as the fallback when the graph can't realize a route
        # within its time cap (scaled with route length: a 65 kly rim route
        # legitimately needs several blacklist rounds).
        # FSD injection is "when required": first solve WITHOUT it (the best
        # all-regular route), and only if no such route exists retry with
        # injection. A per-edge last resort inside the realizer isn't enough --
        # injection can rescue a worse neutron sequence that graph_route's
        # blacklist retry would otherwise reject for a better all-regular one
        # (Sol->Beagle@50 went 585 -> 603 that way). A global two-pass avoids it.
        base_cap = 60.0 + math.dist(src, dst) / 500.0
        passes = (1.0, inject_mult) if inject_mult > 1.0 else (1.0,)
        for im in passes:
            # The injection pass (pass 2) only runs when pass 1 already failed, so
            # bound its time cap -- otherwise a void-stuck route with injection on
            # could burn two full caps (minutes) and tie up a solve slot. An
            # injection rescue is quick when it exists.
            time_cap = base_cap if im == 1.0 else min(base_cap, _ROUTE_INJECT_TIME_CAP)
            result = routing.graph_route(
                index, src, dst, jump_range, fetch_stars_within,
                fetch_stars_near_segment=fetch_stars_near_segment,
                fetch_stars_toward=fetch_stars_toward,
                time_cap=time_cap,
                is_locked=permit_zones.is_locked,
                boost_mult=boost_mult,
                companion_max_ls=companion_max_ls,
                inject_mult=im,
            )
            if result is not None:
                result["engine"] = "graph"
                return result
        # Beam fallback. Tagged so the trace can tell us whether beam is ever the
        # winning path in prod -- if it never is, the whole beam engine
        # (solve_route/_beam/_nearest_star_toward, ~330 lines) can be removed.
        result = routing.solve_route(
            index, src, dst, jump_range, fetch_stars_within, width=beam_width,
            boost_mult=boost_mult, companion_max_ls=companion_max_ls,
        )
        if result is not None:
            result["engine"] = "beam"
        return result


# In-system refuel distance: a neutron waypoint whose system carries a scoopable
# KGBFOAM companion within this many light-seconds lets the commander refuel
# WITHOUT a divert jump (supercruise to the companion, scoop, jump out, chain
# unbroken). ~100 Ls matches spansh's "secondary scoopable near arrival" rule.
INFLIGHT_SCOOP_MAX_LS = 100.0


def _annotate_inflight_scoops(legs: list, conn, max_ls: float = INFLIGHT_SCOOP_MAX_LS) -> int:
    """Flag neutron-landing legs whose system has a scoopable companion within
    ``max_ls`` (read from the bodies table), so the fuel layer treats them as
    in-highway refuels instead of forcing a +2-jump divert. Mutates ``legs`` in
    place -- pass COPIES, never the cached geometry. Returns the count flagged."""
    names = sorted({
        leg.get("system") for leg in legs
        if leg.get("neutron_star") and leg.get("system")
    })
    if not names:
        return 0
    cursor = conn.cursor()
    try:
        # lower(name) = ANY hits idx_systems_big_name_lower_prefix (a plain
        # name= ANY seq-scans the 40M-row table, ~34s). The star-type regex
        # mirrors fsd.is_scoopable: the class letter must stand alone so a
        # black hole ("Black Hole") is not matched as a B-class star. Result
        # keeps the DB's original-case name, which matches the leg's `system`.
        cursor.execute(
            "SELECT s.name, min(b.distance_from_arrival_ls) "
            "FROM systems_big s "
            "JOIN bodies b ON b.system_id64 = s.id64 "
            "JOIN star_types st ON b.star_type_id = st.id "
            "WHERE lower(s.name) = ANY(%s) "
            "AND b.distance_from_arrival_ls > 0 "
            "AND b.distance_from_arrival_ls <= %s "
            "AND st.name ~ %s "
            "GROUP BY s.name",
            ([n.lower() for n in names], float(max_ls),
             r"^[KGBFOAM]($|[ _(0-9])"),
        )
        scoop_ls = {row[0]: float(row[1]) for row in cursor.fetchall()}
    finally:
        cursor.close()
    count = 0
    for leg in legs:
        if leg.get("neutron_star") and leg.get("system") in scoop_ls:
            leg["inflight_scoop"] = True
            leg["scoop_ls"] = scoop_ls[leg["system"]]
            count += 1
    return count


def _enforce_fuel_sync(base_result: dict, params, fuel_safe: bool,
                       boost_mult: float = 4.0,
                       scoop_max_ls: float = INFLIGHT_SCOOP_MAX_LS) -> dict:
    """Run the fuel layer on an already-solved (ship-agnostic) route: annotate
    neutron waypoints that allow an in-system refuel, then (when ``fuel_safe``)
    insert scoop-stop detours so it never strands, and attach a fuel report. Runs
    in a DB task because scoop detours and the companion lookup need queries. The
    base geometry stays cached and untouched; this works on copies.

    ``scoop_max_ls`` is how far in-system the commander will supercruise to a
    companion scoopable to refuel without a divert jump (the request-tunable
    counterpart to ``companion_max_ls`` for boosting)."""
    result = dict(base_result)
    legs = [dict(leg) for leg in (base_result.get("jumps") or [])]
    src = base_result.get("source_coords")
    do_enforce = bool(fuel_safe and src and legs)
    has_neutron = any(leg.get("neutron_star") for leg in legs)
    new_legs = None
    info = None
    if do_enforce or has_neutron:
        with _db_connection() as conn:
            if has_neutron:
                _annotate_inflight_scoops(legs, conn, scoop_max_ls)
            if do_enforce:
                dst = (legs[-1]["x"], legs[-1]["y"], legs[-1]["z"])
                src_t = (src["x"], src["y"], src["z"])

                def fetch_scoopable_near(pos, radius):
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            "SELECT ST_X(coords), ST_Y(coords), ST_Z(coords), name, "
                            "mainstar, id64 FROM systems_big "
                            "WHERE mainstar ~ %s AND ST_3DDWithin("
                            "coords, ST_MakePoint(%s, %s, %s), %s) "
                            "ORDER BY ST_3DDistance(coords, ST_MakePoint(%s, %s, %s)) "
                            "LIMIT 48",
                            # Class letter must stand alone so "Black Hole" (B+l)
                            # is not matched as a B-class star. Mirrors
                            # fsd.is_scoopable; _nearest_scoopable re-checks too.
                            (r"^[KGBFOAM]($|[ _(0-9])", pos[0], pos[1], pos[2],
                             radius, pos[0], pos[1], pos[2]),
                        )
                        return cursor.fetchall()
                    finally:
                        cursor.close()

                new_legs, info = fsd.enforce_fuel_safety(
                    legs, src_t, dst, params, fetch_scoopable_near,
                    src_name=base_result.get("source"),
                    is_locked=permit_zones.is_locked,
                    boost_mult=boost_mult,
                )
    if do_enforce:
        flown = sum(leg["distance_jumped"] for leg in new_legs)
        straight = base_result.get("straight_line_ly") or 0.0
        result["jumps"] = new_legs
        result["total_jumps"] = len(new_legs)
        result["neutron_jumps"] = sum(1 for leg in new_legs if leg["type"] == "boost")
        result["bridge_jumps"] = sum(1 for leg in new_legs if leg["type"] == "regular")
        result["total_distance_ly"] = flown
        result["efficiency"] = (straight / flown) if flown else 0.0
        fuel = fsd.fuel_report(new_legs, params, boost_mult=boost_mult)
        fuel["scoop_stops"] = info["scoop_stops"]
        fuel["enforced"] = True
        # Post-enforcement the route keeps an exact one-jump reserve, so the
        # authoritative verdict is "didn't actually strand, no jump is
        # over-range, AND every needed scoop was insertable" -- not
        # fuel_report's conservative max_dry<cap proxy (which a fully-used tank
        # can trip even when safe).
        fuel["safe"] = ((not fuel["stranded"]) and (not fuel["over_range"])
                        and info["safe"])
        result["fuel"] = fuel
        fsd.annotate_leg_fuel(new_legs, params, boost_mult=boost_mult)
    else:
        # Report-only (fuel-safe off, or no source coords to anchor detours).
        # Use the annotated copies so in-system refuels show in the report + UI.
        result["jumps"] = legs
        result["fuel"] = fsd.fuel_report(legs, params, boost_mult=boost_mult)
        result["fuel"]["enforced"] = False
        fsd.annotate_leg_fuel(legs, params, boost_mult=boost_mult)
    return result


async def _resolve_route_endpoint(
    value: str,
) -> tuple[tuple[float, float, float], str, Optional[str], Optional[int]]:
    """Resolve a system name/id64 to coordinates, predicting via EDTS for
    procedural systems that aren't scanned into the DB yet. Returns
    ``(coords, name, mainstar, id64)`` (id64 may be None for a prediction)."""

    record = await fetch_system_from_db(value)
    if record and record.get("coords"):
        coords = record["coords"]
        return (
            (coords["x"], coords["y"], coords["z"]),
            record["name"],
            record.get("mainstar"),
            record.get("id64"),
        )
    loop = asyncio.get_event_loop()
    try:
        sys_obj = await loop.run_in_executor(
            None, system.from_name, value, False, False
        )
    except Exception:
        sys_obj = None
    # Use `is not None`, not truthiness: EDTS predicts a Vector3 whose __bool__
    # is buggy (returns a float, raising TypeError), so `and <position>` would
    # 500 on any procedural system resolved via EDTS rather than the DB.
    position = getattr(sys_obj, "position", None) if sys_obj is not None else None
    if position is not None:
        return (
            (float(position[0]), float(position[1]), float(position[2])),
            getattr(sys_obj, "name", value),
            getattr(sys_obj, "mainstar", None),
            getattr(sys_obj, "id64", None),
        )
    raise HTTPException(status_code=404, detail=f"System not found: {value}")


class RouteJump(BaseModel):
    system: Optional[str] = None
    id64: Optional[int] = None
    spectral: Optional[str] = None
    x: float
    y: float
    z: float
    type: str
    neutron_star: bool
    distance_jumped: float
    distance_left: float
    # In-system refuel: set on a neutron waypoint that has a scoopable companion
    # within supercruise range (scoop_ls), so you refuel without a divert jump.
    inflight_scoop: Optional[bool] = None
    scoop_ls: Optional[float] = None
    # Set on a companion (secondary) neutron waypoint: its distance from arrival
    # (Ls). The UI shows "neutron at X Ls" (you supercruise to it to supercharge).
    arrival_ls: Optional[float] = None
    # Set when this jump required an FSD synthesis injection: the grade used
    # (1.25 basic / 1.5 standard / 2.0 premium), the least that bridged the gap.
    inject_grade: Optional[float] = None
    # Per-leg fuel (POST /route with a loadout): for the spansh-exact export.
    fuel_in_tank: Optional[float] = None
    fuel_used: Optional[float] = None
    must_refuel: Optional[bool] = None


class RouteResponse(BaseModel):
    source: str
    destination: str
    jump_range: float
    boost_range: float
    straight_line_ly: float
    total_jumps: int
    neutron_jumps: int
    bridge_jumps: int
    total_distance_ly: float
    efficiency: float
    beam_width: int
    solve_ms: float
    access: Optional[str] = None
    # Which solver produced the route: "graph" (primary corridor A*) or "beam"
    # (fallback). Diagnostic, for confirming the beam path is unused before removal.
    engine: Optional[str] = None
    # Source coordinates (the jumps carry their own; the start otherwise has
    # none). Lets the UI emit a faithful spansh-format export (source first).
    source_coords: Optional[dict] = None
    # id64 of the source system (the source leg otherwise carries none); for the
    # spansh-exact export's leading entry + parameters.source_system.
    source_id64: Optional[int] = None
    # Present only on POST /route when a ship loadout was supplied: the fuel
    # simulation verdict (safe/stranded, refuels, tank capacity in jumps).
    fuel: Optional[dict] = None
    # The spansh Galaxy-Plotter parameters block (POST /route with a loadout),
    # so the client can emit the exact-format export.
    spansh_params: Optional[dict] = None
    jumps: list[RouteJump]


@app.get("/route/status", include_in_schema=True)
def route_status():
    status = entitlements.entitlement_status("route")
    status["index_ready"] = _neutron_index is not None
    status["index_stars"] = (
        _neutron_index.count if _neutron_index is not None else 0
    )
    if _neutron_index_error:
        status["index_error"] = _neutron_index_error
    return status


@app.get("/auth/me", include_in_schema=True)
async def auth_me(request: Request):
    """Who is the caller — used by the UI to show patron/login state."""

    patron = await patreon_auth.resolve_patron(request)
    if not patron or not patron.get("name"):
        return {
            "authenticated": False,
            "patron": False,
            "login_url": entitlements.PATREON_URL,
            "identity_configured": patreon_auth.is_configured(),
        }
    return {
        "authenticated": True,
        "patron": bool(patron.get("entitled")),
        "name": patron.get("name"),
        "tier": patron.get("tier"),
    }


_route_trace_logger = logging.getLogger("edgis.route_trace")
if not _route_trace_logger.handlers:
    _route_trace_logger.propagate = False
    _route_trace_logger.setLevel(logging.INFO)
    _rt_path = os.getenv("ROUTE_TRACE_FILE") or os.path.join(
        os.path.dirname(__file__), "route_trace.jsonl"
    )
    try:
        from logging.handlers import RotatingFileHandler

        _rt_handler = RotatingFileHandler(
            _rt_path, maxBytes=5_000_000, backupCount=3
        )
        _rt_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        _route_trace_logger.addHandler(_rt_handler)
    except Exception:  # pragma: no cover - tracing must never break startup
        pass


def _log_route_trace(kind: str, **fields: Any) -> None:
    """One structured JSON line per plot/compare, for side-by-side comparison
    and failure triage. Written to ROUTE_TRACE_FILE (default
    api/route_trace.jsonl, rotated at 5 MB x3)."""

    try:
        _route_trace_logger.info(json.dumps({"kind": kind, **fields}))
    except Exception:  # pragma: no cover - logging must never break a request
        pass


async def _solve_route_cached(
    from_: str,
    to: str,
    jump_range: float,
    beam_width: Optional[int],
    supercharge: float,
    access_via: Optional[str],
    companion_max_ls: float = 0.0,
    inject_mult: float = 1.0,
) -> dict:
    """Resolve + solve a route with the Redis cache and in-flight coalescing.
    Shared by GET and POST /route; the fuel layer (POST only) runs on top of
    the dict this returns, so the geometry cache stays ship-agnostic (a route
    is the same for every ship at a given range/supercharge)."""

    global _route_solve_semaphore
    if _neutron_index is None:
        _kick_neutron_index_build()
        _log_route_trace(
            "route", frm=from_, to=to, range=jump_range, status="warming"
        )
        raise HTTPException(
            status_code=503,
            detail="Route index is warming up, please retry shortly.",
            headers={"Retry-After": "15"},
        )

    cache_key = (
        f"{from_.strip().lower()}|{to.strip().lower()}"
        f"|{jump_range:g}|{beam_width}|sc{supercharge:g}|v{_neutron_index.count}"
        # Only append when opted in, so the default path keeps its existing key
        # (no cache fragmentation / one-time miss on the common route).
        + (f"|cml{companion_max_ls:g}" if companion_max_ls > 0 else "")
        + (f"|inj{inject_mult:g}" if inject_mult > 1.0 else "")
    )
    try:
        cached = await _route_cache.get(cache_key)
    except Exception:
        logger.warning("route cache read failed; solving", exc_info=True)
        cached = None
    if cached is not None:
        cached["access"] = access_via
        _log_route_trace(
            "route", frm=cached.get("source"), to=cached.get("destination"),
            range=jump_range, status="ok", total_jumps=cached.get("total_jumps"),
            cached=True, access=access_via, engine=cached.get("engine"),
        )
        return cached

    # In-flight coalescing: if an identical solve is already running, await its
    # result instead of launching a duplicate. No `await` between the lookup
    # and the registration below, so only one coroutine becomes the "leader".
    existing = _route_inflight.get(cache_key)
    if existing is not None:
        # Mirror whatever the leader produced (result or the same failure).
        shared = await existing
        out = dict(shared)
        out["access"] = access_via
        _log_route_trace(
            "route", frm=out.get("source"), to=out.get("destination"),
            range=jump_range, status="ok", total_jumps=out.get("total_jumps"),
            coalesced=True, access=access_via, engine=out.get("engine"),
        )
        return out

    loop = asyncio.get_event_loop()
    inflight: asyncio.Future = loop.create_future()
    # If no follower awaits a failed solve, retrieve the exception ourselves so
    # asyncio doesn't log "exception was never retrieved".
    inflight.add_done_callback(
        lambda f: f.cancelled() or f.exception()
    )
    _route_inflight[cache_key] = inflight
    try:
        try:
            (src, src_name, _src_star, src_id64), (dst, dst_name, dst_star, dst_id64) = (
                await asyncio.gather(
                    _resolve_route_endpoint(from_),
                    _resolve_route_endpoint(to),
                )
            )
        except HTTPException as exc:
            _log_route_trace(
                "route", frm=from_, to=to, range=jump_range,
                status="not-found", detail=str(exc.detail),
            )
            raise

        if _route_solve_semaphore is None:
            _route_solve_semaphore = asyncio.Semaphore(
                max(1, int(os.getenv("ROUTE_CONCURRENCY") or "2"))
            )
        async with _route_solve_semaphore:
            try:
                result = await _run_db_task(
                    _solve_route_sync, src, dst, jump_range, beam_width,
                    supercharge, companion_max_ls, inject_mult,
                )
            except routing.RouteError as exc:
                _log_route_trace(
                    "route", frm=src_name, to=dst_name, range=jump_range,
                    status=exc.status, detail=exc.detail,
                    access=access_via,
                )
                raise HTTPException(
                    status_code=422,
                    detail={"error": exc.status, "message": exc.detail},
                )

        # The final leg is the arrival at the destination: label it with the
        # destination's name, spectral type and id64 (for the spansh export).
        if result["jumps"]:
            last = result["jumps"][-1]
            if last.get("system") is None:
                last["system"] = dst_name
            if not last.get("spectral"):
                last["spectral"] = dst_star
            if last.get("id64") is None:
                last["id64"] = dst_id64

        result["source"] = src_name
        result["destination"] = dst_name
        result["source_coords"] = {"x": src[0], "y": src[1], "z": src[2]}
        result["source_id64"] = src_id64
        result["access"] = access_via
        _log_route_trace(
            "route", frm=src_name, to=dst_name, range=jump_range, status="ok",
            total_jumps=result["total_jumps"], neutron=result["neutron_jumps"],
            bridge=result["bridge_jumps"], eff=round(result["efficiency"], 4),
            solve_ms=round(result["solve_ms"], 1), access=access_via,
            engine=result.get("engine"),
        )
        to_cache = dict(result)
        to_cache.pop("access", None)
        if not inflight.done():
            inflight.set_result(to_cache)  # hand coalesced followers the result
        try:
            await _route_cache.set(cache_key, to_cache, ttl=ROUTE_CACHE_TTL)
        except Exception:
            logger.warning("route cache write failed", exc_info=True)
        return result
    except BaseException as exc:
        if not inflight.done():
            inflight.set_exception(exc)
        raise
    finally:
        _route_inflight.pop(cache_key, None)


@app.get("/route", response_model=RouteResponse)
async def plot_route(
    from_: str = Query(..., alias="from", min_length=1),
    to: str = Query(..., min_length=1),
    jump_range: float = Query(50.0, alias="range", gt=0, le=200),
    beam_width: Optional[int] = Query(None, ge=1, le=100),
    supercharge: float = Query(
        4.0, ge=1.0, le=6.0,
        description="Neutron supercharge multiplier: 4 normal, 6 for a "
        "Caspian Explorer's Mk II SCO drive.",
    ),
    companion_max_ls: float = Query(
        0.0, ge=0.0, le=COMPANION_NEUTRON_BUILD_MAX_LS,
        description="Also boost from companion (secondary) neutron stars within "
        "this many light-seconds of arrival. 0 = main-star neutrons only.",
    ),
    inject_mult: float = Query(
        1.0, ge=1.0, le=2.0,
        description="FSD synthesis injection multiplier used 'when required' to "
        "cross gaps: 1 = off, 1.25 basic, 1.5 standard, 2 premium.",
    ),
    access: dict = Depends(entitlements.require_entitlement("route")),
):
    """Plot a neutron-boosted route (ship-agnostic). For fuel-aware plotting,
    POST the same parameters plus an EDSY/SLEF ``loadout``."""
    return await _solve_route_cached(
        from_, to, jump_range, beam_width, supercharge, access.get("via"),
        companion_max_ls=companion_max_ls, inject_mult=inject_mult,
    )


class ShipImportRequest(BaseModel):
    loadout: str
    cargo: float = Field(0.0, ge=0, le=2048)


class RouteRequest(BaseModel):
    from_: str = Field(..., alias="from", min_length=1)
    to: str = Field(..., min_length=1)
    jump_range: float = Field(50.0, alias="range", gt=0, le=200)
    beam_width: Optional[int] = Field(None, ge=1, le=100)
    supercharge: float = Field(4.0, ge=1.0, le=6.0)
    companion_max_ls: float = Field(0.0, ge=0.0, le=COMPANION_NEUTRON_BUILD_MAX_LS)
    inject_mult: float = Field(1.0, ge=1.0, le=2.0)
    loadout: Optional[str] = None
    cargo: float = Field(0.0, ge=0, le=2048)
    fuel_safe: bool = True
    # How far in-system to supercruise to a companion scoopable for an
    # in-highway refuel (no divert jump). Fuel-layer only; 0 = always divert.
    scoop_max_ls: float = Field(INFLIGHT_SCOOP_MAX_LS, ge=0.0, le=2000.0)
    model_config = {"populate_by_name": True}


@app.post("/route/ship", include_in_schema=False)
def import_ship(
    req: ShipImportRequest,
    access: dict = Depends(entitlements.require_entitlement("route")),
):
    """Parse an EDSY/SLEF loadout into a display summary (no solve): the UI
    uses it to auto-fill the jump range and show tank/scoop on paste."""
    params = fsd.parse_loadout(req.loadout, req.cargo)
    if params is None:
        raise HTTPException(
            status_code=422,
            detail="Could not read a Frame Shift Drive from that loadout.",
        )
    return fsd.ship_summary(params)


@app.post("/route", response_model=RouteResponse)
async def plot_route_post(
    req: RouteRequest,
    access: dict = Depends(entitlements.require_entitlement("route")),
):
    """Fuel-aware route plot. Same geometry as GET /route (shared,
    ship-agnostic cache) plus a fuel report whenever a ``loadout`` is supplied.
    When ``fuel_safe`` is set, ``_enforce_fuel_sync`` inserts scoop-stop detours
    (or an in-system companion scoop at a neutron waypoint) so the route never
    strands; otherwise fuel risk is only reported."""
    result = await _solve_route_cached(
        req.from_, req.to, req.jump_range, req.beam_width, req.supercharge,
        access.get("via"), companion_max_ls=req.companion_max_ls,
        inject_mult=req.inject_mult,
    )
    params = fsd.parse_loadout(req.loadout, req.cargo) if req.loadout else None
    if params is not None:
        # Fuel layer (enforcement + report) runs in a DB task: scoop-stop
        # insertion needs a scoopable-star query. Copies the cached geometry.
        result = await _run_db_task(
            _enforce_fuel_sync, result, params, req.fuel_safe, req.supercharge,
            req.scoop_max_ls,
        )
        # Parameters block for the spansh-exact export (ship build + fuel model).
        result["spansh_params"] = fsd.spansh_params(
            params, req.supercharge, req.inject_mult)
    # Record the fuel mode on its own trace line: the shared geometry trace is
    # ship-agnostic, so without this the trace never shows whether a ship was
    # imported or whether fuel-safe enforcement ran.
    _fuel = result.get("fuel") or {}
    _log_route_trace(
        "route-fuel",
        frm=result.get("source"), to=result.get("destination"),
        range=req.jump_range,
        has_ship=params is not None,
        fuel_safe=req.fuel_safe if params is not None else None,
        cargo=(req.cargo or None),
        safe=_fuel.get("safe"),
        scoop_stops=_fuel.get("scoop_stops"),
        enforced=_fuel.get("enforced"),
        access=access.get("via"),
    )
    return result


@app.get("/spansh/route", include_in_schema=False)
async def spansh_route(
    from_: str = Query(..., alias="from", min_length=1),
    to: str = Query(..., min_length=1),
    jump_range: float = Query(50.0, alias="range", gt=0, le=200),
    efficiency: int = Query(60, ge=1, le=100),
    supercharge: float = Query(4.0, ge=1.0, le=6.0),
    access: dict = Depends(entitlements.require_entitlement("route")),
):
    """Proxy spansh's neutron plotter (POST job + poll) for side-by-side
    comparison. Returns the real physical jump count (sum of per-leg `jumps`),
    since spansh's `total_jumps` is a waypoint count."""

    payload = {
        "efficiency": str(efficiency),
        "range": f"{jump_range:g}",
        "from": from_,
        "to": to,
        "supercharge_multiplier": f"{supercharge:g}",
    }
    headers = {"User-Agent": EXTERNAL_USER_AGENT, "Accept": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=15, headers=headers) as client:
            submit = await client.post(
                "https://spansh.co.uk/api/route", data=payload
            )
            submit.raise_for_status()
            job = submit.json().get("job")
            if not job:
                raise HTTPException(502, "spansh did not return a job")
            for _ in range(40):
                await asyncio.sleep(1.0)
                poll = await client.get(
                    f"https://spansh.co.uk/api/results/{job}"
                )
                poll.raise_for_status()
                data = poll.json()
                if data.get("result"):
                    res = data["result"]
                    legs = res.get("system_jumps", [])
                    total = sum(int(x.get("jumps", 0)) for x in legs)
                    flown = sum(
                        float(x.get("distance_jumped", 0)) for x in legs
                    )
                    duration_s = None
                    try:
                        from datetime import datetime as _dt

                        ca, ua = data.get("created_at"), data.get("updated_at")
                        if ca and ua:
                            duration_s = (
                                _dt.fromisoformat(ua) - _dt.fromisoformat(ca)
                            ).total_seconds()
                    except Exception:
                        duration_s = None
                    _log_route_trace(
                        "spansh", tool="neutron-plotter", frm=from_, to=to,
                        range=jump_range, efficiency=efficiency, status="ok",
                        total_jumps=total, waypoints=res.get("total_jumps"),
                        duration_s=duration_s,
                    )
                    results_url = (
                        "https://spansh.co.uk/plotter/results/"
                        f"{job}?efficiency={efficiency}&range={jump_range:g}"
                        f"&from={quote(from_)}&to={quote(to)}"
                        f"&supercharge_multiplier={supercharge:g}"
                    )
                    return {
                        "source": res.get("source_system", from_),
                        "destination": res.get("destination_system", to),
                        "total_jumps": total,
                        "waypoints": res.get("total_jumps"),
                        "distance_ly": res.get("distance"),
                        "total_distance_ly": flown,
                        "efficiency": efficiency,
                        "jump_range": jump_range,
                        "duration_s": duration_s,
                        "job": job,
                        "results_url": results_url,
                    }
                if str(data.get("status")) in ("error", "failed"):
                    _log_route_trace(
                        "spansh", frm=from_, to=to, range=jump_range,
                        efficiency=efficiency, status="failed",
                    )
                    raise HTTPException(502, "spansh job failed")
            _log_route_trace(
                "spansh", frm=from_, to=to, range=jump_range,
                efficiency=efficiency, status="timeout",
            )
            raise HTTPException(504, "spansh plot timed out")
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        _log_route_trace(
            "spansh", frm=from_, to=to, range=jump_range,
            efficiency=efficiency, status="error", detail=str(exc),
        )
        raise HTTPException(502, f"spansh request failed: {exc}")


class GalaxyRouteRequest(BaseModel):
    from_: str = Field(..., alias="from", min_length=1)
    to: str = Field(..., min_length=1)
    loadout: str
    cargo: float = Field(0.0, ge=0, le=2048)
    inject_mult: float = Field(1.0, ge=1.0, le=2.0)
    model_config = {"populate_by_name": True}


@app.post("/spansh/galaxy-route", include_in_schema=False)
async def spansh_galaxy_route(
    req: GalaxyRouteRequest,
    access: dict = Depends(entitlements.require_entitlement("route")),
):
    """Proxy spansh's Galaxy (exact) Plotter for a fuel-aware comparison. It
    needs a ship build: id64 endpoints + the fuel params parsed from the SLEF
    loadout. In the result each entry of ``jumps`` is one physical jump (the
    first is the source), so total jumps = len(jumps) - 1. The Galaxy Plotter is
    slow (~1 min), hence the long poll."""

    params = fsd.parse_loadout(req.loadout, req.cargo)
    if params is None:
        raise HTTPException(
            status_code=422,
            detail="Could not read a ship build from that loadout.",
        )
    src = await fetch_system_from_db(req.from_)
    dst = await fetch_system_from_db(req.to)
    if not src or not dst or src.get("id64") is None or dst.get("id64") is None:
        raise HTTPException(
            status_code=422,
            detail="Galaxy Plotter needs both systems in the catalogue (id64).",
        )

    body = {
        "source": str(src["id64"]),
        "destination": str(dst["id64"]),
        "is_supercharged": "0",
        "use_supercharge": "1",
        "use_injections": "0",
        # Match our own injection setting so the comparison is fair: when we plot
        # with FSD injection, let spansh inject "when required" too.
        "use_injections_when_required": "1" if req.inject_mult > 1.0 else "0",
        "exclude_secondary": "0",
        # 0 = refuel only when needed (range grows as the tanks empty) -- the
        # optimal mode serious explorers actually use, so the comparison matches
        # what they'd get on spansh. 1 (refuel at every scoopable) is safer but
        # less efficient; expose as a toggle later.
        "refuel_every_scoopable": "0",
        "fuel_power": f"{params.fuel_power:g}",
        "fuel_multiplier": f"{params.fuel_multiplier:g}",
        "optimal_mass": f"{params.optimal_mass:g}",
        # spansh's own frontend folds the reserve tank into base_mass (its
        # convention differs from the game's jump-mass, which excludes it), so
        # add reserve_size back here to keep the comparison apples-to-apples.
        # + cargo so spansh plans laden, like the tester's real ship.
        "base_mass": f"{params.base_mass + params.reserve_size + params.cargo:g}",
        "tank_size": f"{params.tank_size:g}",
        "internal_tank_size": f"{params.reserve_size:g}",
        "reserve_size": "0",
        "max_fuel_per_jump": f"{params.max_fuel_per_jump:g}",
        "range_boost": f"{params.range_boost:g}",
        "ship_build": req.loadout,
    }
    headers = {"User-Agent": EXTERNAL_USER_AGENT, "Accept": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=20, headers=headers) as client:
            submit = await client.post(
                "https://spansh.co.uk/api/generic/route", data=body
            )
            submit.raise_for_status()
            job = submit.json().get("job")
            if not job:
                raise HTTPException(502, "spansh did not return a job")
            for _ in range(80):  # Galaxy Plotter is slow (~1 min)
                await asyncio.sleep(1.5)
                poll = await client.get(
                    f"https://spansh.co.uk/api/results/{job}"
                )
                poll.raise_for_status()
                data = poll.json()
                if data.get("result"):
                    res = data["result"]
                    legs = res.get("jumps", [])
                    total = max(0, len(legs) - 1)  # first entry is the source
                    flown = sum(float(x.get("distance", 0) or 0) for x in legs)
                    duration_s = None
                    try:
                        from datetime import datetime as _dt

                        ca, ua = data.get("created_at"), data.get("updated_at")
                        if ca and ua:
                            duration_s = (
                                _dt.fromisoformat(ua) - _dt.fromisoformat(ca)
                            ).total_seconds()
                    except Exception:
                        duration_s = None
                    _log_route_trace(
                        "spansh", tool="galaxy-plotter", frm=src["name"],
                        to=dst["name"], status="ok", total_jumps=total,
                        waypoints=len(legs), duration_s=duration_s,
                    )
                    return {
                        "source": src["name"],
                        "destination": dst["name"],
                        "total_jumps": total,
                        "waypoints": len(legs),
                        "total_distance_ly": flown,
                        "duration_s": duration_s,
                        "job": job,
                        "results_url": (
                            f"https://spansh.co.uk/exact-plotter/results/{job}"
                        ),
                    }
                if str(data.get("status")) in ("error", "failed"):
                    _log_route_trace(
                        "spansh", tool="galaxy-plotter", frm=src["name"],
                        to=dst["name"], status="failed",
                    )
                    raise HTTPException(502, "spansh job failed")
            _log_route_trace(
                "spansh", tool="galaxy-plotter", frm=src["name"],
                to=dst["name"], status="timeout",
            )
            raise HTTPException(504, "spansh galaxy plot timed out")
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        _log_route_trace(
            "spansh", tool="galaxy-plotter", frm=req.from_, to=req.to,
            status="error", detail=str(exc),
        )
        raise HTTPException(502, f"spansh request failed: {exc}")


@app.post("/admin/route/rebuild-index", include_in_schema=False)
def rebuild_route_index(request: Request):
    _require_admin_token(request)
    global _neutron_index
    with _neutron_index_lock:
        _neutron_index = None
    started = _kick_neutron_index_build()
    return {"rebuilding": started}


@app.on_event("startup")
def _prebuild_neutron_index() -> None:
    if (os.getenv("ROUTE_PREBUILD") or "1").strip() not in ("1", "true", "yes"):
        return
    _kick_neutron_index_build()


@app.on_event("shutdown")
def close_db_pool() -> None:
    _db_pool.closeall()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=UVICORN_PORT)
