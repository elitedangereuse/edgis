import os
import pathlib
import sys
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["ROUTE_PREBUILD"] = "0"  # never build the real index during tests

from api import entitlements, systems

client = TestClient(systems.app)


def _gate_future(monkeypatch):
    monkeypatch.setitem(
        entitlements.FEATURES,
        "route",
        entitlements.Feature(
            "route", datetime(2999, 1, 1, tzinfo=timezone.utc), "supporter"
        ),
    )


def test_route_blocked_without_key(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    resp = client.get("/route", params={"from": "Sol", "to": "Colonia"})
    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "patron_only"


def test_route_warming_returns_503(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    monkeypatch.setattr(systems, "_neutron_index", None)
    monkeypatch.setattr(systems, "_kick_neutron_index_build", lambda: False)
    resp = client.get(
        "/route",
        params={"from": "Sol", "to": "Colonia"},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 503
    assert resp.headers.get("Retry-After") == "15"


def test_route_success_path(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    monkeypatch.setattr(systems, "_neutron_index", object())  # truthy = "ready"

    async def fake_resolve(value):
        coords = {"Sol": (0.0, 0.0, 0.0), "Colonia": (0.0, 0.0, 1000.0)}
        return coords[value], value, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    def fake_solve(src, dst, jump_range, beam_width):
        return {
            "jump_range": jump_range,
            "boost_range": 4 * jump_range,
            "straight_line_ly": 1000.0,
            "total_jumps": 2,
            "neutron_jumps": 1,
            "bridge_jumps": 1,
            "total_distance_ly": 1010.0,
            "efficiency": 0.99,
            "beam_width": 3,
            "solve_ms": 1.2,
            "jumps": [
                {
                    "system": "N0",
                    "id64": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "z": 40.0,
                    "type": "regular",
                    "neutron_star": True,
                    "distance_jumped": 40.0,
                    "distance_left": 960.0,
                },
                {
                    "system": None,
                    "id64": None,
                    "x": 0.0,
                    "y": 0.0,
                    "z": 1000.0,
                    "type": "boost",
                    "neutron_star": False,
                    "distance_jumped": 960.0,
                    "distance_left": 0.0,
                },
            ],
        }

    monkeypatch.setattr(systems, "_solve_route_sync", fake_solve)

    resp = client.get(
        "/route",
        params={"from": "Sol", "to": "Colonia", "range": 50},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"] == "Sol"
    assert body["destination"] == "Colonia"
    assert body["access"] == "key"
    assert body["total_jumps"] == 2
    assert len(body["jumps"]) == 2


def test_route_patron_access(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.delenv("ENTITLEMENT_KEYS", raising=False)
    monkeypatch.delenv("ROUTE_ACCESS_KEYS", raising=False)
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.setattr(systems, "_neutron_index", object())

    async def fake_resolve(value):
        return {"Sol": (0.0, 0.0, 0.0), "X": (0.0, 0.0, 10.0)}[value], value, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)
    monkeypatch.setattr(
        systems,
        "_solve_route_sync",
        lambda *a: {
            "jump_range": 50.0,
            "boost_range": 200.0,
            "straight_line_ly": 10.0,
            "total_jumps": 1,
            "neutron_jumps": 1,
            "bridge_jumps": 0,
            "total_distance_ly": 10.0,
            "efficiency": 1.0,
            "beam_width": 3,
            "solve_ms": 0.1,
            "jumps": [
                {
                    "system": None,
                    "id64": None,
                    "x": 0.0,
                    "y": 0.0,
                    "z": 10.0,
                    "type": "boost",
                    "neutron_star": False,
                    "distance_jumped": 10.0,
                    "distance_left": 0.0,
                }
            ],
        },
    )

    async def patron(_request):
        return {"entitled": True, "name": "CMDR Jameson", "tier": "supporter"}

    monkeypatch.setattr(systems.entitlements, "_patron_resolver", patron)
    resp = client.get("/route", params={"from": "Sol", "to": "X"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["access"] == "patron"


def test_auth_me_guest_and_patron(monkeypatch):
    async def guest(_request):
        return None

    monkeypatch.setattr(systems.patreon_auth, "resolve_patron", guest)
    body = client.get("/auth/me").json()
    assert body["authenticated"] is False
    assert body["patron"] is False
    assert body["login_url"]

    async def patron(_request):
        return {"entitled": True, "name": "CMDR Jameson", "tier": "supporter"}

    monkeypatch.setattr(systems.patreon_auth, "resolve_patron", patron)
    body = client.get("/auth/me").json()
    assert body["authenticated"] is True
    assert body["patron"] is True
    assert body["name"] == "CMDR Jameson"


def test_route_status_is_public_and_ungated(monkeypatch):
    monkeypatch.setattr(systems, "_neutron_index", None)
    resp = client.get("/route/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["feature"] == "route"
    assert body["index_ready"] is False
    assert "public" in body


def test_spansh_route_is_gated(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    # No key, no patron -> gate blocks before any outbound spansh call.
    resp = client.get("/spansh/route", params={"from": "Sol", "to": "Colonia"})
    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "patron_only"
