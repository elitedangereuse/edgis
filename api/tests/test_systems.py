import os
import pathlib
import sys
import types
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
import pytest

# Ensure repository root is on sys.path so `api` package imports work when pytest runs from /api
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api import systems

client = TestClient(systems.app)


class _CursorStub:
    def __init__(self, *, rows=None, first=None, description=None):
        self.rows = rows or []
        self.first = first
        self.description = description
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.first

    def close(self):
        self.closed = True


class _ConnStub:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.closed = False
        self.rollback_calls = 0

    def cursor(self):
        return self.cursor_instance

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.closed = True


def _patch_db(monkeypatch, *, rows=None, first=None, description=None):
    cursor = _CursorStub(rows=rows, first=first, description=description)
    conn = _ConnStub(cursor)
    monkeypatch.setattr(systems.psycopg, "connect", lambda **kwargs: conn)
    return cursor


@pytest.fixture
def anyio_backend():
    """Force AnyIO-based tests to run under asyncio backend only."""
    return "asyncio"


@pytest.fixture(autouse=True)
def reset_db_pool():
    systems._reset_db_pool_for_testing()
    yield
    systems._reset_db_pool_for_testing()


def test_get_neighbors_negative_radius():
    response = client.get(
        "/neighbors",
        params={"x": 0, "y": 0, "z": 0, "radius": -1},
    )
    assert response.status_code == 400
    assert response.json() == {"error": "Radius must be positive"}


def test_get_neighbors_radius_too_large():
    response = client.get(
        "/neighbors",
        params={
            "x": 0,
            "y": 0,
            "z": 0,
            "radius": systems.NEIGHBORS_MAX_RADIUS + 1,
        },
    )
    assert response.status_code == 400
    assert "Radius too large" in response.json()["error"]


@pytest.mark.parametrize(
    "expected",
    [
        {
            "id64": 42,
            "name": "Test System",
            "mainstar": "G",
            "coords": {"x": 1.0, "y": 2.0, "z": 3.0},
            "distance": 0.5,
        }
    ],
)
def test_get_neighbors_success(monkeypatch, expected):
    async def fake_fetch_neighbors_from_db(x, y, z, radius, limit):
        return [expected]

    monkeypatch.setattr(
        systems, "fetch_neighbors_from_db", fake_fetch_neighbors_from_db
    )

    response = client.get(
        "/neighbors",
        params={"x": 10, "y": -2, "z": 5, "radius": 25},
    )
    assert response.status_code == 200
    assert response.json() == [expected]


def test_get_neighbors_cursor_requires_page_size():
    response = client.get(
        "/neighbors",
        params={
            "x": 10,
            "y": -2,
            "z": 5,
            "radius": 25,
            "cursor": "abc",
        },
    )
    assert response.status_code == 400
    assert response.json() == {"error": "cursor requires page_size"}


def test_get_neighbors_paged_success(monkeypatch):
    async def fake_fetch_neighbors_seeded_page_from_db(x, y, z, radius, page_size):
        assert page_size == 100
        return {
            "items": [
                {
                    "id64": 42,
                    "name": "Test System",
                    "mainstar": "G",
                    "coords": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "distance": 0.5,
                }
            ],
            "has_more": True,
            "next_cursor": "cursor-1",
        }

    monkeypatch.setattr(
        systems,
        "fetch_neighbors_seeded_page_from_db",
        fake_fetch_neighbors_seeded_page_from_db,
    )

    response = client.get(
        "/neighbors",
        params={"x": 10, "y": -2, "z": 5, "radius": 25, "page_size": 100},
    )
    assert response.status_code == 200
    assert response.json()["has_more"] is True
    assert response.json()["next_cursor"] == "cursor-1"


def test_neighbors_seeded_radii_for_request():
    assert systems._neighbors_seeded_radii_for_request(12.0) == [12.0]
    assert systems._neighbors_seeded_radii_for_request(50.0) == [20.0, 50.0]
    assert systems._neighbors_seeded_radii_for_request(200.0) == [
        20.0,
        50.0,
        100.0,
        200.0,
    ]


def test_neighbors_cursor_roundtrip():
    encoded = systems._encode_neighbors_cursor(
        {"distance": 12.5, "name": "Sol", "id64": 42}
    )
    decoded = systems._decode_neighbors_cursor(encoded)
    assert decoded == (12.5, "Sol", 42)


class TestSystem:
    id64 = 9469999523369
    name = "Swoilz GG-B b5-4"
    mainstar = "null"
    position = (225.0, -235.0, 325.0)


class TestSystem2:
    id64 = 98765
    name = "Odotls EG-Y f0"
    mainstar = "null"
    position = (-49825.0, -33145.0, -5705.0)


@pytest.fixture
def patch_prediction(monkeypatch):
    monkeypatch.setattr(
        systems.system,
        "from_name",
        lambda name, *args, **kwargs: TestSystem(),
    )
    monkeypatch.setattr(
        systems.system,
        "from_id64",
        lambda value, *args, **kwargs: TestSystem2(),
    )


def test_coords_predict_success(patch_prediction):
    response = client.get("/coords/predict", params={"q": "Swoilz GG-B b5-4"})
    assert response.status_code == 200
    body = response.json()
    assert body["prediction"] is True
    assert body["coords"] == {"x": 225.0, "y": -235.0, "z": 325.0}


def test_coords_predict_numeric_uses_id64(monkeypatch):
    calls = {}

    def fake_from_id64(value, *args, **kwargs):
        calls["value"] = value
        return TestSystem2()

    monkeypatch.setattr(
        systems.system,
        "from_id64",
        fake_from_id64,
    )
    monkeypatch.setattr(
        systems.system,
        "from_name",
        lambda *args, **kwargs: None,
    )

    response = client.get("/coords/predict", params={"q": "98765"})
    assert response.status_code == 200
    assert calls["value"] == 98765
    body = response.json()
    assert body["prediction"] is True
    assert body["name"] == "Odotls EG-Y f0"


def test_coords_lookup_not_found(monkeypatch):
    async def fake_fetch_system_from_db(name_or_id):
        return None

    monkeypatch.setattr(
        systems, "fetch_system_from_db", fake_fetch_system_from_db
    )

    response = client.get("/coords", params={"q": "Kubeo"})
    assert response.status_code == 404
    assert response.json() == {"error": "System not found"}


def test_coords_lookup_success(monkeypatch):
    async def fake_fetch_system_from_db(name_or_id):
        return {
            "id64": 1109989017963,
            "name": "Alioth",
            "mainstar": "A",
            "coords": {"x": -33.65625, "y": 72.46875, "z": -20.65625},
        }

    monkeypatch.setattr(
        systems, "fetch_system_from_db", fake_fetch_system_from_db
    )

    response = client.get("/coords", params={"q": "Alioth"})
    assert response.status_code == 200
    assert response.json()["name"] == "Alioth"


def test_apply_mode_scaling_no_mode():
    import math

    sample = [{"radius": 2000, "gravity": 9.807}]
    result = systems._apply_mode_scaling(
        [body.copy() for body in sample], None
    )
    assert result[0]["radius"] == 2000
    assert math.isclose(
        result[0]["gravity"], 9.807, rel_tol=1e-09, abs_tol=1e-09
    )


def test_format_neutron_result_handles_decimal():
    payload = systems._format_neutron_result(
        (
            22712681061,
            "PSR J1752-2806",
            "Neutron Star",
            "POINT Z (1 2 3)",
            Decimal("253.3051924217356"),
        )
    )
    assert payload == {
        "neutron_id64": 22712681061,
        "neutron_name": "PSR J1752-2806",
        "distance_ly": pytest.approx(253.3051924217356),
        "coords": {"x": 1.0, "y": 2.0, "z": 3.0},
    }


@pytest.mark.anyio
async def test_fetch_nearest_neutron_star_hits_db(monkeypatch):
    cursor = _patch_db(
        monkeypatch,
        rows=[
            (
                22712681061,
                "PSR J1752-2806",
                "Neutron Star",
                "POINT Z (1 2 3)",
                Decimal("253.3051924217356"),
            )
        ],
    )

    result = await systems.fetch_nearest_neutron_star.__wrapped__("HIP 87621")

    assert result[0]["neutron_name"] == "PSR J1752-2806"
    assert cursor.executed
    query, params = cursor.executed[0]
    assert "nearest_neutron_star" in query
    assert params == ("HIP 87621",)


@pytest.mark.anyio
async def test_fetch_nearest_neutron_star_at_coords_hits_db(monkeypatch):
    cursor = _patch_db(
        monkeypatch,
        rows=[
            (
                123456,
                "Test Pulsar",
                "Neutron Star",
                "POINT Z (10 11 12)",
                Decimal("10.5"),
            )
        ],
    )

    result = await systems.fetch_nearest_neutron_star_at_coords.__wrapped__(
        -10.0, 5.0, 42.0
    )

    assert result[0]["neutron_id64"] == 123456
    query, params = cursor.executed[0]
    assert "nearest_neutron_star_at_coords" in query
    assert params == (-10.0, 5.0, 42.0)


@pytest.mark.anyio
async def test_fetch_total_systems_from_db_hits_db(monkeypatch):
    cursor = _patch_db(monkeypatch, first=(123456789,))

    total = await systems.fetch_total_systems_from_db.__wrapped__()

    assert total == 123456789
    query, params = cursor.executed[0]
    assert "reltuples" in query.lower()
    assert params is None


def test_manual_name_suggestions(monkeypatch):
    monkeypatch.setattr(
        systems,
        "_load_manual_system_names",
        lambda: (
            ["sol", "solitude", "wolf 359"],
            ["Sol", "Solitude", "Wolf 359"],
        ),
    )

    assert systems._manual_name_suggestions("so", 5) == ["Sol", "Solitude"]
    assert systems._manual_name_suggestions("wolf", 5) == ["Wolf 359"]


def test_load_manual_system_names_uses_cache(monkeypatch):
    pkg = types.ModuleType("edtslib")
    id_module = types.ModuleType("edtslib.id64data")
    id_module.known_systems = {"sol": 1, "wolf 359": 2}

    pg_module = types.ModuleType("edtslib.pgnames")
    pg_module.get_canonical_name = lambda name: name.title()
    pkg.id64data = id_module
    pkg.pgnames = pg_module

    monkeypatch.setitem(sys.modules, "edtslib", pkg)
    monkeypatch.setitem(sys.modules, "edtslib.id64data", id_module)
    monkeypatch.setitem(sys.modules, "edtslib.pgnames", pg_module)
    monkeypatch.setattr(
        systems, "_MANUAL_AUTOCOMPLETE_CACHE", None, raising=False
    )

    lowers, canonicals = systems._load_manual_system_names()

    assert lowers == ["sol", "wolf 359"]
    assert canonicals == ["Sol", "Wolf 359"]

    # Modify the source data and ensure cached copy is reused
    id_module.known_systems["new place"] = 3
    cached = systems._load_manual_system_names()
    assert cached == (lowers, canonicals)


def test_nearest_neutron_star_endpoint_success(monkeypatch):
    async def fake_fetch(system_name):
        assert system_name == "HIP 87621"
        return [{
            "neutron_id64": 222,
            "neutron_name": "PSR",
            "distance_ly": 12.3,
            "coords": {"x": 1.0, "y": 2.0, "z": 3.0},
        }]

    monkeypatch.setattr(systems, "fetch_nearest_neutron_star", fake_fetch)

    response = client.get(
        "/nearest-neutron-star",
        params={"system_name": "HIP 87621"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert payload[0]["neutron_name"] == "PSR"
    assert payload[0]["coords"] == {"x": 1.0, "y": 2.0, "z": 3.0}


def test_nearest_neutron_star_endpoint_not_found(monkeypatch):
    async def fake_fetch(system_name):
        return None

    monkeypatch.setattr(systems, "fetch_nearest_neutron_star", fake_fetch)

    response = client.get(
        "/nearest-neutron-star",
        params={"system_name": "Unknown"},
    )

    assert response.status_code == 404
    assert response.json()["error"] == "No neutron star found"


def test_nearest_neutron_star_coords_endpoint_success(monkeypatch):
    async def fake_fetch(x, y, z):
        assert (x, y, z) == (1.0, 2.0, 3.0)
        return [{
            "neutron_id64": 333,
            "neutron_name": "Coords Pulsar",
            "distance_ly": 88.0,
            "coords": {"x": 9.0, "y": 8.0, "z": 7.0},
        }]

    monkeypatch.setattr(
        systems, "fetch_nearest_neutron_star_at_coords", fake_fetch
    )

    response = client.get(
        "/nearest-neutron-star/coords",
        params={"x": 1.0, "y": 2.0, "z": 3.0},
    )

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert payload[0]["distance_ly"] == 88.0
    assert payload[0]["coords"] == {"x": 9.0, "y": 8.0, "z": 7.0}


def test_nearest_neutron_star_coords_endpoint_not_found(monkeypatch):
    async def fake_fetch(x, y, z):
        return None

    monkeypatch.setattr(
        systems, "fetch_nearest_neutron_star_at_coords", fake_fetch
    )

    response = client.get(
        "/nearest-neutron-star/coords",
        params={"x": 1.0, "y": 2.0, "z": 3.0},
    )

    assert response.status_code == 404
    assert response.json()["error"] == "No neutron star found"


def test_total_systems_endpoint_success(monkeypatch):
    async def fake_fetch():
        return 250000000

    monkeypatch.setattr(systems, "fetch_total_systems_from_db", fake_fetch)

    response = client.get("/stats/total-systems")

    assert response.status_code == 200
    assert response.json() == {"total_systems": 250000000}


def test_autocomplete_endpoint_short_query():
    response = client.get("/systems/autocomplete", params={"q": "s"})
    assert response.status_code == 200
    assert response.json() == {"suggestions": []}


def test_autocomplete_endpoint_success(monkeypatch):
    async def fake_fetch(term):
        assert term == "Sol"
        return ["Sol", "Solitude"]

    monkeypatch.setattr(systems, "fetch_system_name_suggestions", fake_fetch)

    response = client.get("/systems/autocomplete", params={"q": " Sol"})

    assert response.status_code == 200
    assert response.json()["suggestions"] == ["Sol", "Solitude"]


def test_fetch_system_names_from_db(monkeypatch):
    cursor = _patch_db(
        monkeypatch,
        rows=[("Sol",), ("Solace",)],
    )

    suggestions = systems._fetch_system_names_from_db(" sol ", 5)

    assert suggestions == ["Sol", "Solace"]
    assert cursor.executed == [
        (
            f"SET LOCAL statement_timeout = {systems.AUTOCOMPLETE_STATEMENT_TIMEOUT_MS}",
            None,
        ),
        (
            systems.AUTOCOMPLETE_QUERY,
            ("sol%", 5),
        ),
    ]


def test_fetch_system_names_from_db_retry_succeeds(monkeypatch):
    class _RetryCursor:
        def __init__(self):
            self.executed: list[tuple[str, Optional[tuple[str, int]]]] = []
            self.closed = False
            self.select_attempts = 0

        def execute(self, query, params=None):
            self.executed.append((query, params))
            if query.strip().lower().startswith("select"):
                self.select_attempts += 1
                if self.select_attempts == 1:
                    raise systems.psycopg.errors.QueryCanceled("timeout")

        def fetchall(self):
            return [("Stu",), ("Stux",)]

        def close(self):
            self.closed = True

    class _RetryConn:
        def __init__(self, cursor):
            self.cursor_instance = cursor
            self.closed = False
            self.rollback_calls = 0

        def cursor(self):
            return self.cursor_instance

        def rollback(self):
            self.rollback_calls += 1

        def close(self):
            self.closed = True

    cursor = _RetryCursor()
    conn = _RetryConn(cursor)
    monkeypatch.setattr(systems.psycopg, "connect", lambda **_: conn)
    monkeypatch.setattr(systems, "AUTOCOMPLETE_STATEMENT_TIMEOUT_MS", 50)
    monkeypatch.setattr(systems, "AUTOCOMPLETE_TIMEOUT_RETRY_MS", 200)

    suggestions = systems._fetch_system_names_from_db("Stu", 2)

    assert suggestions == ["Stu", "Stux"]
    assert cursor.select_attempts == 2
    assert cursor.closed
    assert conn.closed is False
    assert conn.rollback_calls == 2


def test_fetch_system_names_from_db_handles_timeout(monkeypatch):
    class _TimeoutCursor:
        def __init__(self):
            self.executed: list[tuple[str, Optional[tuple[str, int]]]] = []
            self.closed = False
            self.select_attempts = 0

        def execute(self, query, params=None):
            self.executed.append((query, params))
            if query.strip().lower().startswith("select"):
                self.select_attempts += 1
                raise systems.psycopg.errors.QueryCanceled("timeout")

        def fetchall(self):
            raise AssertionError("fetchall should not be called after timeout")

        def close(self):
            self.closed = True

    class _TimeoutConn:
        def __init__(self, cursor):
            self.cursor_instance = cursor
            self.closed = False
            self.rollback_calls = 0

        def cursor(self):
            return self.cursor_instance

        def rollback(self):
            self.rollback_calls += 1

        def close(self):
            self.closed = True

    cursor = _TimeoutCursor()
    conn = _TimeoutConn(cursor)
    monkeypatch.setattr(systems.psycopg, "connect", lambda **_: conn)
    monkeypatch.setattr(systems, "AUTOCOMPLETE_STATEMENT_TIMEOUT_MS", 20)
    monkeypatch.setattr(systems, "AUTOCOMPLETE_TIMEOUT_RETRY_MS", 40)

    suggestions = systems._fetch_system_names_from_db("Stuemae", 15)

    assert suggestions == []
    assert cursor.closed
    assert conn.closed is False
    assert cursor.select_attempts == 2
    assert conn.rollback_calls == 3


@pytest.mark.anyio
async def test_fetch_system_name_suggestions_uses_edsm_after_spansh(monkeypatch):
    async def fake_edsm(term):
        return ["HIP 1", "HIP 2"]

    async def fake_spansh(term):
        return []

    def fake_local(term):
        raise AssertionError(
            "Local fallback should not run when EDSM succeeds"
        )

    monkeypatch.setattr(systems, "_fetch_edsm_autocomplete", fake_edsm)
    monkeypatch.setattr(systems, "_fetch_spansh_autocomplete", fake_spansh)
    monkeypatch.setattr(systems, "_local_system_name_suggestions", fake_local)

    result = await systems.fetch_system_name_suggestions.__wrapped__("Hip")  # type: ignore[attr-defined]

    assert result == ["HIP 1", "HIP 2"]


@pytest.mark.anyio
async def test_fetch_system_name_suggestions_uses_spansh(monkeypatch):
    async def fake_edsm(term):
        return []

    async def fake_spansh(term):
        return ["Spansh One", "Spansh Two"]

    def fake_local(term):
        raise AssertionError(
            "Local fallback should not run when Spansh succeeds"
        )

    monkeypatch.setattr(systems, "_fetch_edsm_autocomplete", fake_edsm)
    monkeypatch.setattr(systems, "_fetch_spansh_autocomplete", fake_spansh)
    monkeypatch.setattr(systems, "_local_system_name_suggestions", fake_local)

    result = await systems.fetch_system_name_suggestions.__wrapped__("Span")  # type: ignore[attr-defined]

    assert result == ["Spansh One", "Spansh Two"]


@pytest.mark.anyio
async def test_fetch_system_name_suggestions_falls_back_to_db(monkeypatch):
    async def fake_edsm(term):
        return []

    async def fake_spansh(term):
        return []

    def fake_manual(term, limit):
        return ["Edts"]

    fetched = []

    def fake_db(term, limit):
        fetched.append((term, limit))
        return ["sol", "Solace", "Solitude"]

    monkeypatch.setattr(systems, "_fetch_edsm_autocomplete", fake_edsm)
    monkeypatch.setattr(systems, "_fetch_spansh_autocomplete", fake_spansh)
    monkeypatch.setattr(systems, "_manual_name_suggestions", fake_manual)
    monkeypatch.setattr(systems, "_fetch_system_names_from_db", fake_db)

    result = systems._local_system_name_suggestions("Sol")

    assert result == ["sol", "Solace", "Solitude"]
    assert fetched == [("Sol", systems.AUTOCOMPLETE_LIMIT)]


@pytest.mark.anyio
async def test_fetch_system_name_suggestions_falls_back_to_manual(monkeypatch):
    async def fake_edsm(term):
        return []

    async def fake_spansh(term):
        return []

    def fake_manual(term, limit):
        assert limit == systems.AUTOCOMPLETE_LIMIT
        return ["Sol", "sol"]

    def fake_db(term, limit):
        return []

    monkeypatch.setattr(systems, "_fetch_edsm_autocomplete", fake_edsm)
    monkeypatch.setattr(systems, "_fetch_spansh_autocomplete", fake_spansh)
    monkeypatch.setattr(systems, "_manual_name_suggestions", fake_manual)
    monkeypatch.setattr(systems, "_fetch_system_names_from_db", fake_db)

    result = await systems.fetch_system_name_suggestions.__wrapped__("Sol")  # type: ignore[attr-defined]

    assert result == ["Sol"]


@pytest.mark.anyio
async def test_fetch_system_name_suggestions_dedupes_case_and_whitespace(
    monkeypatch,
):
    async def fake_edsm(term):
        return []

    async def fake_spansh(term):
        return []

    def fake_manual(term, limit):
        return []

    def fake_db(term, limit):
        return [" Hip 87621", "HIP 87621", "HIP 87622"]

    monkeypatch.setattr(systems, "_fetch_edsm_autocomplete", fake_edsm)
    monkeypatch.setattr(systems, "_fetch_spansh_autocomplete", fake_spansh)
    monkeypatch.setattr(systems, "_manual_name_suggestions", fake_manual)
    monkeypatch.setattr(systems, "_fetch_system_names_from_db", fake_db)

    result = await systems.fetch_system_name_suggestions.__wrapped__("HIP 876")  # type: ignore[attr-defined]

    assert result == ["Hip 87621", "HIP 87622"]


@pytest.mark.anyio
async def test__fetch_edsm_autocomplete_handles_payload(monkeypatch):
    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return ["Alpha", {"value": "Beta"}, {"name": "Gamma"}]

    class _Client:
        def __init__(self, *_, **__):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, params=None):
            return _Resp()

    monkeypatch.setattr(systems.httpx, "AsyncClient", _Client)

    result = await systems._fetch_edsm_autocomplete("Al")
    assert result == ["Alpha", "Beta", "Gamma"]


@pytest.mark.anyio
async def test__fetch_spansh_autocomplete_parses_dict(monkeypatch):
    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"results": ["One", {"name": "Two"}, {"value": "Three"}]}

    class _Client:
        def __init__(self, *_, **__):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, params=None):
            return _Resp()

    monkeypatch.setattr(systems.httpx, "AsyncClient", _Client)

    result = await systems._fetch_spansh_autocomplete("Sp")
    assert result == ["One", "Two", "Three"]


def test__extract_spansh_results_various_formats():
    payload = {"results": ["One", {"name": "Two"}, {"value": "Three"}, 123, None]}
    assert systems._extract_spansh_results(payload) == ["One", "Two", "Three"]

    payload = ["Alpha", {"value": "Beta"}]
    assert systems._extract_spansh_results(payload) == ["Alpha", "Beta"]

    assert systems._extract_spansh_results("invalid") == []


def test_apply_mode_scaling_edsm_handles_units():
    source = [
        {
            "radius": 695500000,
            "type": "Star",
            "star_type": "G (Yellow) Star",
            "gravity": Decimal("19.614"),
            "surface_gravity": 19.614,
            "semiMajorAxis": 149597870700,
            "semi_major_axis": 299195741400,
            "surfacePressure": 101325,
            "surface_pressure": 202650,
        },
        {
            "radius": 2000,
            "type": "Planet",
            "planet_class": "Earthlike body",
            "gravity": 9.807,
            "surface_gravity": 19.614,
        },
    ]

    scaled = systems._apply_mode_scaling(
        [body.copy() for body in source], "edsm"
    )
    star, planet = scaled
    assert star["radius"] == pytest.approx(1.0)
    assert star["gravity"] == pytest.approx(2.0)
    assert star["surface_gravity"] == pytest.approx(2.0)
    assert star["semiMajorAxis"] == pytest.approx(1.0)
    assert star["semi_major_axis"] == pytest.approx(2.0)
    assert star["surfacePressure"] == pytest.approx(1.0)
    assert star["surface_pressure"] == pytest.approx(2.0)

    assert planet["radius"] == pytest.approx(2.0)
    assert planet["gravity"] == pytest.approx(1.0)
    assert planet["surface_gravity"] == pytest.approx(2.0)


def test_bodies_mode_query_passthrough(monkeypatch):
    def fake_fetch_bodies_from_db(name_or_id, mode=None):
        return [{"name_or_id": name_or_id, "mode": mode}]

    monkeypatch.setattr(
        systems, "fetch_bodies_from_db", fake_fetch_bodies_from_db
    )

    response = client.get(
        "/bodies", params={"name_or_id": "Sol", "mode": "edsm"}
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["mode"] == "edsm"


def test_bodies_with_explicit_body_id(monkeypatch):
    captured = {}

    def fake_fetch_bodies_from_db(name_or_id, mode=None, body_id=None):
        captured["args"] = (name_or_id, mode, body_id)
        return [{"name_or_id": name_or_id, "body_id": body_id}]

    monkeypatch.setattr(
        systems, "fetch_bodies_from_db", fake_fetch_bodies_from_db
    )

    response = client.get(
        "/bodies", params={"name_or_id": "Sol", "body_id": 42}
    )

    assert response.status_code == 200
    assert response.json()[0]["body_id"] == 42
    assert captured["args"] == ("Sol", None, 42)


def test_load_cors_origins_respects_env(monkeypatch):
    monkeypatch.setenv("CORS_ORIGINS", " https://a.example ,http://b.local ")
    assert systems._load_cors_origins() == [
        "https://a.example",
        "http://b.local",
    ]


def test_wildcard_cors_disables_credentials(monkeypatch):
    monkeypatch.setenv("CORS_ORIGINS", "*")
    origins = systems._load_cors_origins()
    assert origins == ["*"]
    assert systems._allow_cors_credentials(origins) is False


def test_read_index_uses_configured_path(tmp_path, monkeypatch):
    custom_index = tmp_path / "custom.html"
    custom_index.write_text("<html>staging</html>", encoding="utf-8")

    monkeypatch.setattr(systems, "INDEX_HTML_PATH", str(custom_index))

    response = systems.read_index()
    assert isinstance(response, FileResponse)
    assert response.path == str(custom_index)


def test_get_request_logs_user_agent(caplog):
    caplog.set_level("INFO", logger="uvicorn.error")

    response = client.get(
        "/systems/autocomplete",
        params={"q": "s"},
        headers={
            "User-Agent": "pytest-agent/1.0",
            "Referer": "https://example.test/from",
        },
    )

    assert response.status_code == 200
    message = next(
        rec.getMessage()
        for rec in caplog.records
        if rec.name == "uvicorn.error"
        and 'request_details method=GET path="/systems/autocomplete"' in rec.getMessage()
    )
    assert 'query="q=s"' in message
    assert 'ua="pytest-agent/1.0"' in message
    assert 'referer="https://example.test/from"' in message
    assert "duration_ms=" in message


@pytest.mark.anyio
async def test_favicon_serves_static_png():
    response = await systems.favicon()
    assert isinstance(response, FileResponse)
    assert response.path.endswith(os.path.join("static", "favicon.png"))


def test_get_neighbors_returns_500_on_error(monkeypatch):
    async def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(systems, "fetch_neighbors_from_db", boom)

    response = client.get(
        "/neighbors",
        params={"x": 1, "y": 2, "z": 3, "radius": 10},
    )

    assert response.status_code == 500
    assert response.json() == {"error": "boom"}


@pytest.mark.anyio("asyncio")
async def test_fetch_coords_for_systems_empty_list_short_circuits(monkeypatch):
    called = False

    def boom(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("DB should not be hit")

    monkeypatch.setattr(systems.psycopg, "connect", boom)

    result = await systems.fetch_coords_for_systems.__wrapped__([])  # type: ignore[attr-defined]
    assert result == {}
    assert called is False


@pytest.mark.anyio("asyncio")
async def test_fetch_coords_for_systems_returns_map(monkeypatch):
    _patch_db(
        monkeypatch,
        rows=[(42, "POINT Z (1 2 3)")],
    )

    result = await systems.fetch_coords_for_systems.__wrapped__([42])  # type: ignore[attr-defined]
    assert result[42] == {"x": 1.0, "y": 2.0, "z": 3.0}


@pytest.mark.anyio("asyncio")
async def test_fetch_neighbors_from_db_parses_rows(monkeypatch):
    cursor = _patch_db(
        monkeypatch,
        rows=[(99, "Sol", "G", "POINT Z (1 2 3)", 4.2)],
    )

    rows = await systems.fetch_neighbors_from_db.__wrapped__(
        0, 0, 0, 10, 25
    )  # type: ignore[attr-defined]
    assert rows == [
        {
            "id64": 99,
            "name": "Sol",
            "mainstar": "G",
            "coords": {"x": 1.0, "y": 2.0, "z": 3.0},
            "distance": 4.2,
        }
    ]

    executed = cursor.executed
    if systems.NEIGHBORS_STATEMENT_TIMEOUT_MS > 0:
        assert executed[0][0].startswith("SET LOCAL statement_timeout")
        select_call = executed[1]
    else:
        select_call = executed[0]
    assert select_call[0].strip().startswith("WITH ref AS")
    assert select_call[1][-1] == 25


@pytest.mark.anyio("asyncio")
async def test_fetch_neighbors_page_from_db_returns_cursor(monkeypatch):
    cursor = _patch_db(
        monkeypatch,
        rows=[
            (99, "Sol", "G", "POINT Z (1 2 3)", 4.2),
            (100, "Achenar", "A", "POINT Z (4 5 6)", 5.2),
            (101, "Alioth", "F", "POINT Z (7 8 9)", 6.2),
        ],
    )

    payload = await systems.fetch_neighbors_page_from_db.__wrapped__(
        0, 0, 0, 10, 2, None, None, None
    )  # type: ignore[attr-defined]

    assert len(payload["items"]) == 2
    assert payload["has_more"] is True
    assert payload["next_cursor"]
    decoded = systems._decode_neighbors_cursor(payload["next_cursor"])
    assert decoded == (5.2, "Achenar", 100)
    select_query, select_params = cursor.executed[-1]
    assert "WHERE (\n                    distance > %s" not in select_query
    assert select_params == (0, 0, 0, 10, 3)


@pytest.mark.anyio("asyncio")
async def test_fetch_neighbors_page_from_db_with_cursor_uses_keyset(monkeypatch):
    cursor = _patch_db(
        monkeypatch,
        rows=[
            (101, "Alioth", "F", "POINT Z (7 8 9)", 6.2),
        ],
    )

    payload = await systems.fetch_neighbors_page_from_db.__wrapped__(
        0, 0, 0, 10, 2, 5.2, "Achenar", 100
    )  # type: ignore[attr-defined]

    assert payload["items"][0]["id64"] == 101
    select_query, select_params = cursor.executed[-1]
    assert "WHERE (\n                    distance > %s" in select_query
    assert select_params == (0, 0, 0, 10, 5.2, 5.2, "Achenar", 5.2, "Achenar", 100, 3)


@pytest.mark.anyio("asyncio")
async def test_fetch_system_from_db_numeric(monkeypatch):
    _patch_db(
        monkeypatch,
        first=(123, "Sol", "G", "POINT Z (1 2 3)"),
    )

    result = await systems.fetch_system_from_db.__wrapped__("123")  # type: ignore[attr-defined]
    assert result == {
        "id64": 123,
        "name": "Sol",
        "mainstar": "G",
        "coords": {"x": 1.0, "y": 2.0, "z": 3.0},
    }


@pytest.mark.anyio("asyncio")
async def test_fetch_system_from_db_by_name(monkeypatch):
    cursor = _patch_db(
        monkeypatch,
        first=(321, "Alioth", "A", "POINT Z (-1 0 1)"),
    )

    result = await systems.fetch_system_from_db.__wrapped__("Alioth")  # type: ignore[attr-defined]
    assert result["name"] == "Alioth"
    assert "LOWER(name)" in cursor.executed[0][0]


@pytest.mark.anyio("asyncio")
async def test_fetch_system_from_db_none(monkeypatch):
    _patch_db(monkeypatch, first=None)
    result = await systems.fetch_system_from_db.__wrapped__("Unknown")  # type: ignore[attr-defined]
    assert result is None


def test_fetch_bodies_from_db_returns_rows(monkeypatch):
    description = [
        ("system_id64", None),
        ("body_id", None),
        ("body_name", None),
        ("type", None),
    ]
    _patch_db(
        monkeypatch,
        rows=[(123, 7, "Sol A", "Star")],
        description=description,
    )

    bodies = systems.fetch_bodies_from_db("123", mode=None, body_id=7)
    assert bodies == [
        {
            "system_id64": 123,
            "body_id": 7,
            "body_name": "Sol A",
            "type": "Star",
        }
    ]


def test_fetch_bodies_from_db_returns_none_when_empty(monkeypatch):
    cursor = _patch_db(monkeypatch, rows=[], description=[])
    cursor.description = []

    assert systems.fetch_bodies_from_db("Sol") is None


def test_fetch_bodies_from_db_named_filter(monkeypatch):
    description = [
        ("system_id64", None),
        ("body_id", None),
    ]
    cursor = _patch_db(
        monkeypatch,
        rows=[(999, 1)],
        first=(999,),
        description=description,
    )

    systems.fetch_bodies_from_db("Sol", body_id=1)
    id_lookup_query = cursor.executed[0][0]
    assert "SELECT id64" in id_lookup_query
    assert "LOWER(name)" in id_lookup_query

    bodies_query = cursor.executed[1][0]
    assert "b.system_id64 = %s" in bodies_query
    assert "AND b.body_id = %s" in bodies_query


@pytest.mark.anyio("asyncio")
async def test_proxy_edsm_bodies_success(monkeypatch):
    class DummyResponse:
        status_code = 200

        def json(self):
            return {"status": "ok"}

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params):
            assert params == {"systemName": "Sol"}
            return DummyResponse()

    monkeypatch.setattr(systems.httpx, "AsyncClient", DummyClient)

    payload = await systems.proxy_edsm_bodies.__wrapped__(systemName="Sol")  # type: ignore[attr-defined]
    assert payload == {"status": "ok"}


@pytest.mark.anyio("asyncio")
async def test_proxy_edsm_bodies_non_200(monkeypatch):
    class DummyResponse:
        status_code = 500

        def json(self):
            return {}

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

    async def _aexit(self, exc_type, exc, tb):
        return False

    async def _get(self, url, params):
        return DummyResponse()

    DummyClient.__aexit__ = _aexit  # type: ignore[attr-defined]
    DummyClient.get = _get  # type: ignore[attr-defined]

    monkeypatch.setattr(
        systems.httpx, "AsyncClient", lambda *args, **kwargs: DummyClient()
    )

    with pytest.raises(HTTPException) as excinfo:
        await systems.proxy_edsm_bodies.__wrapped__(systemName="Sol")  # type: ignore[attr-defined]
    assert excinfo.value.status_code == 500


@pytest.mark.anyio("asyncio")
async def test_proxy_edsm_bodies_timeout(monkeypatch):
    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

    async def _aexit(self, exc_type, exc, tb):
        return False

    async def _get(self, url, params):
        raise systems.httpx.ReadTimeout("boom")

    DummyClient.__aexit__ = _aexit  # type: ignore[attr-defined]
    DummyClient.get = _get  # type: ignore[attr-defined]

    monkeypatch.setattr(
        systems.httpx, "AsyncClient", lambda *args, **kwargs: DummyClient()
    )

    with pytest.raises(HTTPException) as excinfo:
        await systems.proxy_edsm_bodies.__wrapped__(systemName="Sol")  # type: ignore[attr-defined]
    assert excinfo.value.status_code == 504


def test_bodies_endpoint_returns_404(monkeypatch):
    monkeypatch.setattr(systems, "fetch_bodies_from_db", lambda *_, **__: None)
    response = client.get("/bodies", params={"name_or_id": "Sol"})
    assert response.status_code == 404
    assert response.json()["error"] == systems.SYSTEM_NOT_FOUND


def test_coords_predict_not_found(monkeypatch):
    monkeypatch.setattr(systems.system, "from_name", lambda *_, **__: None)
    monkeypatch.setattr(systems.system, "from_id64", lambda *_, **__: None)

    response = client.get("/coords/predict", params={"q": "Unknown"})
    assert response.status_code == 404


def test_coords_predict_exception(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("fail")

    monkeypatch.setattr(systems.system, "from_name", boom)
    monkeypatch.setattr(systems.system, "from_id64", boom)

    response = client.get("/coords/predict", params={"q": "Alioth"})
    assert response.status_code == 500
    assert response.json()["error"] == "fail"


@pytest.mark.anyio("asyncio")
async def test_proxy_spansh_system_success(monkeypatch):
    class DummyResponse:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self._data

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

    async def _aexit(self, exc_type, exc, tb):
        return False

    async def _get(self, url):
        return DummyResponse({"record": {"bodies": ["a"]}})

    DummyClient.__aexit__ = _aexit  # type: ignore[attr-defined]
    DummyClient.get = _get  # type: ignore[attr-defined]

    monkeypatch.setattr(
        systems.httpx, "AsyncClient", lambda *args, **kwargs: DummyClient()
    )

    payload = await systems.proxy_spansh_system(system_id=1)
    assert payload == ["a"]


@pytest.mark.anyio("asyncio")
async def test_proxy_spansh_system_http_error(monkeypatch):
    class DummyResponse:
        def raise_for_status(self):
            raise systems.httpx.HTTPStatusError(
                "fail",
                request=None,
                response=type("R", (), {"status_code": 403})(),
            )

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

    async def _aexit(self, exc_type, exc, tb):
        return False

    async def _get(self, url):
        return DummyResponse()

    DummyClient.__aexit__ = _aexit  # type: ignore[attr-defined]
    DummyClient.get = _get  # type: ignore[attr-defined]

    monkeypatch.setattr(
        systems.httpx, "AsyncClient", lambda *args, **kwargs: DummyClient()
    )

    with pytest.raises(HTTPException) as excinfo:
        await systems.proxy_spansh_system(system_id=1)
    assert excinfo.value.status_code == 403


@pytest.mark.anyio("asyncio")
async def test_proxy_spansh_faction_presence(monkeypatch):
    class DummyResponse:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload

        def json(self):
            return self._payload

    class DummyClient:
        def __init__(self, *args, **kwargs):
            self.get_calls = 0

        async def __aenter__(self):
            return self

    async def _aexit(self, exc_type, exc, tb):
        return False

    async def _post(self, url, json):
        return DummyResponse(200, {"search_reference": "abc"})

    async def _get(self, url, params):
        self.get_calls += 1
        return DummyResponse(
            200,
            {
                "results": [
                    {
                        "id64": 500,
                        "name": "Foo",
                        "controlling_minor_faction": "Test Faction",
                    }
                ],
                "count": 1,
            },
        )

    DummyClient.__aexit__ = _aexit  # type: ignore[attr-defined]
    DummyClient.post = _post  # type: ignore[attr-defined]
    DummyClient.get = _get  # type: ignore[attr-defined]

    dummy_client = DummyClient()
    monkeypatch.setattr(
        systems.httpx, "AsyncClient", lambda *args, **kwargs: dummy_client
    )

    async def fake_fetch_coords(id64_list):
        return {500: {"x": 0, "y": 0, "z": 0}}

    monkeypatch.setattr(systems, "fetch_coords_for_systems", fake_fetch_coords)

    payload = await systems.proxy_spansh_faction_presence.__wrapped__(faction="Test Faction")  # type: ignore[attr-defined]
    assert payload["results"][0]["coords"] == {"x": 0, "y": 0, "z": 0}


@pytest.mark.anyio("asyncio")
async def test_proxy_spansh_autocomplete_controlling_minor_faction(
    monkeypatch,
):
    class DummyResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class DummyClient:
        async def __aenter__(self):
            return self

    async def _aexit(self, exc_type, exc, tb):
        return False

    async def _get(self, url, params):
        assert params["q"] == "med"
        return DummyResponse({"values": ["Med"]})

    DummyClient.__aexit__ = _aexit  # type: ignore[attr-defined]
    DummyClient.get = _get  # type: ignore[attr-defined]

    monkeypatch.setattr(
        systems.httpx,
        "AsyncClient",
        lambda *args, **kwargs: DummyClient(),
    )

    payload = await systems.proxy_spansh_autocomplete_controlling_minor_faction.__wrapped__(  # type: ignore[attr-defined]
        q=" med "
    )
    assert payload == {"values": ["Med"]}


@pytest.mark.anyio("asyncio")
async def test_proxy_spansh_autocomplete_controlling_minor_faction_empty_query():
    with pytest.raises(HTTPException) as excinfo:
        await systems.proxy_spansh_autocomplete_controlling_minor_faction.__wrapped__(  # type: ignore[attr-defined]
            q="  "
        )

    assert excinfo.value.status_code == 400


def test_spansh_refresh_success(monkeypatch):
    called: dict[str, object] = {}

    class DummyConn:
        def __enter__(self):
            called["entered"] = True
            return self

        def __exit__(self, exc_type, exc, tb):
            called["exited"] = True

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    def fake_connect(**kwargs):
        called["connect_kwargs"] = kwargs
        return DummyConn()

    def fake_ingest(system_id64, *, connection):
        called["system_id64"] = system_id64
        called["connection"] = connection

    monkeypatch.setattr(systems.psycopg, "connect", fake_connect)
    monkeypatch.setattr(systems.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(systems, "spansh_ingest_system", fake_ingest)
    monkeypatch.setattr(systems, "_SPANSH_IMPORT_ERROR", None)

    response = client.post("/bodies/42/spansh-refresh")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "system_id64": 42}
    assert called["system_id64"] == 42
    assert isinstance(called["connection"], DummyConn)
    assert called.get("entered") and called.get("exited")


def test_spansh_refresh_unavailable(monkeypatch):
    monkeypatch.setattr(systems, "spansh_ingest_system", None)
    monkeypatch.setattr(systems, "_SPANSH_IMPORT_ERROR", "missing module")

    response = client.post("/bodies/99/spansh-refresh")
    assert response.status_code == 501
    assert "missing module" in response.json()["detail"]


def test_spansh_refresh_invalid_id():
    response = client.post("/bodies/0/spansh-refresh")
    assert response.status_code == 400


def test_favicon_and_index_routes(tmp_path, monkeypatch):
    # ensure static dir accessible
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "favicon.png").write_bytes(b"png")
    (static_dir / "index.html").write_text("<!doctype html>")

    monkeypatch.chdir(tmp_path)

    local_app = FastAPI()
    local_app.mount("/static", StaticFiles(directory="static"), name="static")

    @local_app.get("/favicon.ico")
    async def favicon():
        return FileResponse("static/favicon.png")

    @local_app.get("/")
    def read_index():
        return FileResponse(os.path.join("static", "index.html"))

    local_client = TestClient(local_app)
    assert local_client.get("/favicon.ico").status_code == 200
    assert local_client.get("/").status_code == 200
