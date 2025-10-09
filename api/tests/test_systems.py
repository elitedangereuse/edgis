import pathlib
import sys

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


class DummySystem:
    id64 = 123
    name = "Test System"
    mainstar = "G"
    position = (11.1, 22.2, 33.3)


@pytest.fixture
def patch_prediction(monkeypatch):
    monkeypatch.setattr(
        systems.system,
        "from_name",
        lambda name, allow_known=False: DummySystem(),
    )
    monkeypatch.setattr(
        systems.system,
        "from_id64",
        lambda value, allow_known=False: DummySystem(),
    )


def test_coords_predict_success(patch_prediction):
    response = client.get("/coords/predict", params={"q": "Swoilz GG-B b5-4"})
    assert response.status_code == 200
    body = response.json()
    assert body["prediction"] is True
    assert body["coords"] == {"x": 11.1, "y": 22.2, "z": 33.3}


def test_coords_predict_numeric_uses_id64(monkeypatch):
    calls = {}

    def fake_from_id64(value, allow_known=False):
        calls["value"] = value
        return DummySystem()

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


def test_coords_lookup_not_found(monkeypatch):
    async def fake_fetch_system_from_db(name_or_id):
        return None

    monkeypatch.setattr(
        systems, "fetch_system_from_db", fake_fetch_system_from_db
    )

    response = client.get("/coords", params={"q": "Missing"})
    assert response.status_code == 404
    assert response.json() == {"error": "System not found"}


def test_coords_lookup_success(monkeypatch):
    async def fake_fetch_system_from_db(name_or_id):
        return {
            "id64": 999,
            "name": "Found",
            "mainstar": "K",
            "coords": {"x": -1.0, "y": 0.5, "z": 4.2},
        }

    monkeypatch.setattr(
        systems, "fetch_system_from_db", fake_fetch_system_from_db
    )

    response = client.get("/coords", params={"q": "Found"})
    assert response.status_code == 200
    assert response.json()["name"] == "Found"
