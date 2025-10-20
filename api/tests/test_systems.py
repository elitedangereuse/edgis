import pathlib
import sys
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest

# Ensure repository root is on sys.path so `api` package imports work when pytest runs from /api
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api import systems

client = TestClient(systems.app)


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
    async def fake_fetch_bodies_from_db(name_or_id, mode=None):
        return [{"name_or_id": name_or_id, "mode": mode}]

    monkeypatch.setattr(systems, "fetch_bodies_from_db", fake_fetch_bodies_from_db)

    response = client.get("/bodies", params={"name_or_id": "Sol", "mode": "edsm"})
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["mode"] == "edsm"
