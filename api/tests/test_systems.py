import os
import pathlib
import sys
from decimal import Decimal

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

    def execute(self, query, params):
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

    def cursor(self):
        return self.cursor_instance

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


def test_get_neighbors_negative_radius():
    response = client.get(
        "/neighbors",
        params={"x": 0, "y": 0, "z": 0, "radius": -1},
    )
    assert response.status_code == 200
    assert response.json() == {"error": "Radius must be positive"}


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
    async def fake_fetch_neighbors_from_db(x, y, z, radius):
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
        lambda name, allow_known=False: TestSystem(),
    )
    monkeypatch.setattr(
        systems.system,
        "from_id64",
        lambda value, allow_known=False: TestSystem2(),
    )


def test_coords_predict_success(patch_prediction):
    response = client.get("/coords/predict", params={"q": "Swoilz GG-B b5-4"})
    assert response.status_code == 200
    body = response.json()
    assert body["prediction"] is True
    assert body["coords"] == {"x": 225.0, "y": -235.0, "z": 325.0}


def test_coords_predict_numeric_uses_id64(monkeypatch):
    calls = {}

    def fake_from_id64(value, allow_known=False):
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

    monkeypatch.setattr(systems, "fetch_system_from_db", fake_fetch_system_from_db)

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

    monkeypatch.setattr(systems, "fetch_system_from_db", fake_fetch_system_from_db)

    response = client.get("/coords", params={"q": "Alioth"})
    assert response.status_code == 200
    assert response.json()["name"] == "Alioth"


def test_apply_mode_scaling_no_mode():
    import math

    sample = [{"radius": 2000, "gravity": 9.807}]
    result = systems._apply_mode_scaling([body.copy() for body in sample], None)
    assert result[0]["radius"] == 2000
    assert math.isclose(result[0]["gravity"], 9.807, rel_tol=1e-09, abs_tol=1e-09)


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

    scaled = systems._apply_mode_scaling([body.copy() for body in source], "edsm")
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

    monkeypatch.setattr(systems, "fetch_bodies_from_db", fake_fetch_bodies_from_db)

    response = client.get("/bodies", params={"name_or_id": "Sol", "mode": "edsm"})
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["mode"] == "edsm"


def test_bodies_with_explicit_body_id(monkeypatch):
    captured = {}

    def fake_fetch_bodies_from_db(name_or_id, mode=None, body_id=None):
        captured["args"] = (name_or_id, mode, body_id)
        return [{"name_or_id": name_or_id, "body_id": body_id}]

    monkeypatch.setattr(systems, "fetch_bodies_from_db", fake_fetch_bodies_from_db)

    response = client.get(
        "/bodies", params={"name_or_id": "Sol", "body_id": 42}
    )

    assert response.status_code == 200
    assert response.json()[0]["body_id"] == 42
    assert captured["args"] == ("Sol", None, 42)


def test_load_cors_origins_respects_env(monkeypatch):
    monkeypatch.setenv("CORS_ORIGINS", " https://a.example ,http://b.local ")
    assert systems._load_cors_origins() == ["https://a.example", "http://b.local"]


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
    _patch_db(
        monkeypatch,
        rows=[(99, "Sol", "G", "POINT Z (1 2 3)", 4.2)],
    )

    rows = await systems.fetch_neighbors_from_db.__wrapped__(0, 0, 0, 10)  # type: ignore[attr-defined]
    assert rows == [
        {
            "id64": 99,
            "name": "Sol",
            "mainstar": "G",
            "coords": {"x": 1.0, "y": 2.0, "z": 3.0},
            "distance": 4.2,
        }
    ]


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
        description=description,
    )

    systems.fetch_bodies_from_db("Sol", body_id=1)
    executed_query = cursor.executed[0][0]
    assert "LOWER(s.name)" in executed_query
    assert "AND b.body_id = %s" in executed_query


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

    monkeypatch.setattr(systems.httpx, "AsyncClient", lambda *args, **kwargs: DummyClient())

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

    monkeypatch.setattr(systems.httpx, "AsyncClient", lambda *args, **kwargs: DummyClient())

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

    monkeypatch.setattr(systems.httpx, "AsyncClient", lambda *args, **kwargs: DummyClient())

    payload = await systems.proxy_spansh_system(system_id=1)
    assert payload == ["a"]


@pytest.mark.anyio("asyncio")
async def test_proxy_spansh_system_http_error(monkeypatch):
    class DummyResponse:
        def raise_for_status(self):
            raise systems.httpx.HTTPStatusError("fail", request=None, response=type("R", (), {"status_code": 403})())

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

    monkeypatch.setattr(systems.httpx, "AsyncClient", lambda *args, **kwargs: DummyClient())

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
    monkeypatch.setattr(systems.httpx, "AsyncClient", lambda *args, **kwargs: dummy_client)
    async def fake_fetch_coords(id64_list):
        return {500: {"x": 0, "y": 0, "z": 0}}

    monkeypatch.setattr(systems, "fetch_coords_for_systems", fake_fetch_coords)

    payload = await systems.proxy_spansh_faction_presence.__wrapped__(faction="Test Faction")  # type: ignore[attr-defined]
    assert payload["results"][0]["coords"] == {"x": 0, "y": 0, "z": 0}


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
