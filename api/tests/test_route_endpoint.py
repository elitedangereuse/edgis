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
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())  # truthy = "ready"

    async def fake_resolve(value):
        coords = {"Sol": (0.0, 0.0, 0.0), "Colonia": (0.0, 0.0, 1000.0)}
        return coords[value], value, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0):
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


def test_route_coalesces_concurrent_duplicates(monkeypatch):
    import asyncio
    import time

    import httpx

    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

    async def fake_resolve(value):
        return {"CoalA": (0.0, 0.0, 0.0), "CoalB": (0.0, 0.0, 999.0)}[value], value, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    calls = {"n": 0}

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0):
        calls["n"] += 1
        time.sleep(0.3)  # hold the solve so the second request overlaps it
        return {
            "jump_range": jump_range, "boost_range": 4 * jump_range,
            "straight_line_ly": 999.0, "total_jumps": 1, "neutron_jumps": 0,
            "bridge_jumps": 1, "total_distance_ly": 999.0, "efficiency": 1.0,
            "beam_width": 3, "solve_ms": 1.0,
            "jumps": [{
                "system": None, "id64": None, "x": 0.0, "y": 0.0, "z": 999.0,
                "type": "regular", "neutron_star": False,
                "distance_jumped": 999.0, "distance_left": 0.0,
            }],
        }

    monkeypatch.setattr(systems, "_solve_route_sync", fake_solve)

    async def go():
        transport = httpx.ASGITransport(app=systems.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://t"
        ) as c:
            params = {"from": "CoalA", "to": "CoalB", "range": 50}
            h = {"x-route-key": "k"}
            return await asyncio.gather(
                c.get("/route", params=params, headers=h),
                c.get("/route", params=params, headers=h),
            )

    r1, r2 = asyncio.run(go())
    assert r1.status_code == 200 and r2.status_code == 200
    # Two identical concurrent requests, but the solver ran only once.
    assert calls["n"] == 1
    assert r1.json()["total_jumps"] == r2.json()["total_jumps"] == 1


def test_route_patron_access(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.delenv("ENTITLEMENT_KEYS", raising=False)
    monkeypatch.delenv("ROUTE_ACCESS_KEYS", raising=False)
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

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
