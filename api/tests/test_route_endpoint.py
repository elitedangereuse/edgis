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


def _clear_route_cache():
    # The dev Redis persists real routes between runs; clear so assertions on
    # freshly-solved fields (e.g. source_coords) aren't served a stale entry.
    import asyncio
    try:
        asyncio.run(systems._route_cache.clear())
    except Exception:
        pass


def test_route_success_path(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    _clear_route_cache()
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())  # truthy = "ready"

    async def fake_resolve(value):
        coords = {"Sol": (0.0, 0.0, 0.0), "Colonia": (0.0, 0.0, 1000.0)}
        return coords[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0, companion_max_ls=0.0, inject_mult=1.0, tank_jumps=0, scoop_max_ls=0.0, reach_by_r=None):
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
    # Source coords are attached (for the spansh-format export's leading entry).
    assert body["source_coords"] == {"x": 0.0, "y": 0.0, "z": 0.0}


def test_route_companion_max_ls_threaded_and_arrival_ls_passthrough(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    _clear_route_cache()
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

    async def fake_resolve(value):
        return {"A": (0.0, 0.0, 0.0), "B": (0.0, 0.0, 360.0)}[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    seen = {}

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0, companion_max_ls=0.0, inject_mult=1.0, tank_jumps=0, scoop_max_ls=0.0, reach_by_r=None):
        seen["cml"] = companion_max_ls
        return {
            "jump_range": jump_range, "boost_range": 4 * jump_range,
            "straight_line_ly": 360.0, "total_jumps": 1, "neutron_jumps": 1,
            "bridge_jumps": 0, "total_distance_ly": 360.0, "efficiency": 1.0,
            "beam_width": 3, "solve_ms": 1.0,
            "jumps": [{
                "system": "N1", "id64": None, "x": 0.0, "y": 0.0, "z": 200.0,
                "type": "boost", "neutron_star": True, "distance_jumped": 200.0,
                "distance_left": 160.0, "arrival_ls": 50.0,
            }],
        }

    monkeypatch.setattr(systems, "_solve_route_sync", fake_solve)

    resp = client.get("/route", params={"from": "A", "to": "B", "range": 50,
                                        "companion_max_ls": 100},
                      headers={"x-route-key": "k"})
    assert resp.status_code == 200, resp.text
    assert seen["cml"] == 100.0                       # threaded to the solver
    assert resp.json()["jumps"][0]["arrival_ls"] == 50.0  # annotation passed through


def test_route_coalesces_concurrent_duplicates(monkeypatch):
    import asyncio
    import time

    import httpx

    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

    async def fake_resolve(value):
        return {"CoalA": (0.0, 0.0, 0.0), "CoalB": (0.0, 0.0, 999.0)}[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    calls = {"n": 0}

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0, companion_max_ls=0.0, inject_mult=1.0, tank_jumps=0, scoop_max_ls=0.0, reach_by_r=None):
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


# Minimal Caspian-class loadout (shared with test_fsd); known max range 67.72 ly.
COBRA_SLEF = (
    '[{"header":{"appName":"EDSY"},"data":{"event":"Loadout","Ship":"cobramkv",'
    '"UnladenMass":194.85,"MaxJumpRange":67.716111,'
    '"FuelCapacity":{"Main":32,"Reserve":0.49},"Modules":['
    '{"Slot":"FrameShiftDrive","Item":"int_hyperdrive_overcharge_size4_class5",'
    '"Engineering":{"Modifiers":[{"Label":"FSDOptimalMass","Value":1034.28},'
    '{"Label":"MaxFuelPerJump","Value":3.2}]}},'
    '{"Slot":"Slot01_Size5","Item":"int_guardianfsdbooster_size5"},'
    '{"Slot":"Slot02_Size4","Item":"int_fuelscoop_size4_class5"}]}}]'
)


def test_route_ship_import_parses_loadout(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    resp = client.post(
        "/route/ship",
        json={"loadout": COBRA_SLEF},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    # Full-tank range (feasible by construction), not the lightest-tank max.
    # EDSY/game-accurate value with the reserve excluded from jump mass.
    assert abs(body["jump_range"] - 60.45) < 0.1
    assert body["tank_jumps"] == 10
    assert body["has_scoop"] is True


def test_route_ship_import_rejects_garbage(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    resp = client.post(
        "/route/ship",
        json={"loadout": '[{"data":{"event":"Loadout","Modules":[]}}]'},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 422


def test_route_post_attaches_fuel_report(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

    async def fake_resolve(value):
        coords = {"Sol": (0.0, 0.0, 0.0), "Near": (0.0, 0.0, 80.0)}
        return coords[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0, companion_max_ls=0.0, inject_mult=1.0, tank_jumps=0, scoop_max_ls=0.0, reach_by_r=None):
        return {
            "jump_range": jump_range, "boost_range": 4 * jump_range,
            "straight_line_ly": 80.0, "total_jumps": 2, "neutron_jumps": 1,
            "bridge_jumps": 1, "total_distance_ly": 80.0, "efficiency": 1.0,
            "beam_width": 3, "solve_ms": 1.0,
            "jumps": [
                {"system": "N0", "id64": 1, "x": 0.0, "y": 0.0, "z": 40.0,
                 "spectral": "N", "type": "boost", "neutron_star": True,
                 "distance_jumped": 40.0, "distance_left": 40.0},
                {"system": "Near", "id64": None, "x": 0.0, "y": 0.0, "z": 80.0,
                 "spectral": "K (Yellow-Orange) Star", "type": "regular",
                 "neutron_star": False, "distance_jumped": 40.0,
                 "distance_left": 0.0},
            ],
        }

    monkeypatch.setattr(systems, "_solve_route_sync", fake_solve)

    # The neutron waypoint still triggers the in-system scoop annotation, which
    # opens a DB connection even with fuel_safe off: stub it out.
    class _NoDb:
        def __enter__(self):
            return None

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(systems, "_db_connection", lambda: _NoDb())
    monkeypatch.setattr(systems, "_annotate_inflight_scoops", lambda *a, **k: None)

    # fuel_safe off -> report-only path (enforcement insertion is unit-tested in
    # test_fsd with a fake scoopable fetch).
    resp = client.post(
        "/route",
        json={"from": "Sol", "to": "Near", "range": 50, "loadout": COBRA_SLEF,
              "fuel_safe": False},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_jumps"] == 2
    assert body["fuel"] is not None
    assert body["fuel"]["safe"] is True
    assert body["fuel"]["tank_jumps"] == 10
    assert body["fuel"]["enforced"] is False


def _capture_solve(monkeypatch, captured):
    """Stub the geometry solve so a test can inspect the fuel-aware kwargs
    (tank_jumps / scoop_max_ls) the endpoint threaded into it."""

    async def fake_resolve(value):
        coords = {"Sol": (0.0, 0.0, 0.0), "Near": (0.0, 0.0, 80.0)}
        return coords[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0,
                   companion_max_ls=0.0, inject_mult=1.0, tank_jumps=0,
                   scoop_max_ls=0.0, reach_by_r=None):
        captured["tank_jumps"] = tank_jumps
        captured["scoop_max_ls"] = scoop_max_ls
        captured["reach_by_r"] = reach_by_r
        return {
            "jump_range": jump_range, "boost_range": 4 * jump_range,
            "straight_line_ly": 80.0, "total_jumps": 1, "neutron_jumps": 0,
            "bridge_jumps": 1, "total_distance_ly": 80.0, "efficiency": 1.0,
            "beam_width": 3, "solve_ms": 1.0,
            "jumps": [
                {"system": "Near", "id64": None, "x": 0.0, "y": 0.0, "z": 80.0,
                 "spectral": "K (Yellow-Orange) Star", "type": "regular",
                 "neutron_star": False, "distance_jumped": 80.0,
                 "distance_left": 0.0},
            ],
        }

    monkeypatch.setattr(systems, "_solve_route_sync", fake_solve)

    class _NoDb:
        def __enter__(self):
            return None

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(systems, "_db_connection", lambda: _NoDb())
    monkeypatch.setattr(systems, "_annotate_inflight_scoops", lambda *a, **k: None)


def test_route_post_with_loadout_plans_fuel_aware(monkeypatch):
    # A loadout makes the geometry solve ship-specific: the Cobra's 10-jump tank
    # reaches graph_route as tank_jumps, and the scoop reach as scoop_max_ls.
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    _clear_route_cache()
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())
    captured: dict = {}
    _capture_solve(monkeypatch, captured)

    resp = client.post(
        "/route",
        json={"from": "Sol", "to": "Near", "range": 50, "loadout": COBRA_SLEF,
              "scoop_max_ls": 100},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    assert captured["tank_jumps"] == 10
    assert captured["scoop_max_ls"] == 100.0
    # B2 range growth: the per-resource reach profile is threaded and grows as the
    # tank empties (reach_by_r[0] < reach_by_r[-1]).
    rbr = captured["reach_by_r"]
    assert rbr is not None and len(rbr) == 10
    assert rbr[-1] > rbr[0]


def test_route_without_loadout_stays_ship_agnostic(monkeypatch):
    # No loadout -> tank_jumps 0 -> the shared ship-agnostic geometry (byte-for-
    # byte GET /route), so the fuel-aware search never runs.
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    _clear_route_cache()
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())
    captured: dict = {}
    _capture_solve(monkeypatch, captured)

    resp = client.post(
        "/route",
        json={"from": "Sol", "to": "Near", "range": 50},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    assert captured["tank_jumps"] == 0


def test_route_post_inflight_scoop_avoids_divert_stops(monkeypatch):
    # Every neutron waypoint has a scoopable companion in-system (the annotation
    # query returns one), so enforcement inserts NO divert stops and the chain is
    # safe at its original length.
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    _clear_route_cache()
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

    async def fake_resolve(value):
        coords = {"A": (0.0, 0.0, 0.0), "B": (1200.0, 0.0, 0.0)}
        return coords[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0, companion_max_ls=0.0, inject_mult=1.0, tank_jumps=0, scoop_max_ls=0.0, reach_by_r=None):
        legs, x = [], 0.0
        for i in range(20):
            x += 60.0
            legs.append({"system": f"N{i}", "id64": None, "x": x, "y": 0.0,
                         "z": 0.0, "spectral": "N", "type": "boost",
                         "neutron_star": True, "distance_jumped": 60.0,
                         "distance_left": 1200.0 - x})
        return {
            "jump_range": jump_range, "boost_range": 4 * jump_range,
            "straight_line_ly": 1200.0, "total_jumps": 20, "neutron_jumps": 20,
            "bridge_jumps": 0, "total_distance_ly": 1200.0, "efficiency": 1.0,
            "beam_width": 3, "solve_ms": 1.0, "jumps": legs,
        }

    monkeypatch.setattr(systems, "_solve_route_sync", fake_solve)

    class _FakeCursor:
        def execute(self, sql, params):
            if "distance_from_arrival_ls" in sql:
                # Every queried neutron system has a scoopable companion at 25 Ls.
                # params[0] is lowercased (the query matches lower(name)); the real
                # DB returns the original-case name, so echo "N0" not "n0".
                names = params[0]
                self._rows = [(n[:1].upper() + n[1:], 25.0) for n in names]
            else:
                self._rows = []  # should never need a divert

        def fetchall(self):
            return self._rows

        def close(self):
            pass

    class _FakeCtx:
        def __enter__(self):
            return type("C", (), {"cursor": lambda self: _FakeCursor()})()

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(systems, "_db_connection", lambda: _FakeCtx())

    resp = client.post(
        "/route",
        json={"from": "A", "to": "B", "range": 50, "loadout": COBRA_SLEF,
              "fuel_safe": True},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["fuel"]["enforced"] is True
    assert body["fuel"]["scoop_stops"] == 0        # no diverts
    assert body["fuel"]["safe"] is True
    assert body["total_jumps"] == 20               # chain untouched
    assert all(j.get("inflight_scoop") for j in body["jumps"])
    assert body["jumps"][0]["scoop_ls"] == 25.0


def test_route_post_fuel_safe_inserts_scoop_stops(monkeypatch):
    # fuel_safe=True drives enforcement: a stranding neutron chain gets scoop
    # detours inserted. A fake DB supplies a scoopable star near each query.
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    _clear_route_cache()
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

    async def fake_resolve(value):
        coords = {"A": (0.0, 0.0, 0.0), "B": (1200.0, 0.0, 0.0)}
        return coords[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0, companion_max_ls=0.0, inject_mult=1.0, tank_jumps=0, scoop_max_ls=0.0, reach_by_r=None):
        legs, x = [], 0.0
        for i in range(20):  # 20 neutron boosts -> strands a ~10-jump tank
            x += 60.0
            legs.append({"system": f"N{i}", "id64": None, "x": x, "y": 0.0,
                         "z": 0.0, "spectral": "N", "type": "boost",
                         "neutron_star": True, "distance_jumped": 60.0,
                         "distance_left": 1200.0 - x})
        return {
            "jump_range": jump_range, "boost_range": 4 * jump_range,
            "straight_line_ly": 1200.0, "total_jumps": 20, "neutron_jumps": 20,
            "bridge_jumps": 0, "total_distance_ly": 1200.0, "efficiency": 1.0,
            "beam_width": 3, "solve_ms": 1.0, "jumps": legs,
        }

    monkeypatch.setattr(systems, "_solve_route_sync", fake_solve)

    class _FakeCursor:
        def execute(self, sql, params):
            if "distance_from_arrival_ls" in sql:
                # In-system refuel lookup: no neutron here has a companion, so
                # enforcement still has to insert divert scoop-stops.
                self._rows = []
            else:
                # Scoopable-near query: params = (pattern, x, y, z, radius, ...);
                # a scoopable star 20 ly off.
                self._rows = [(params[1], params[2] + 20.0, params[3], "ScoopK",
                               "K (Yellow-Orange) Star")]

        def fetchall(self):
            return self._rows

        def close(self):
            pass

    class _FakeCtx:
        def __enter__(self):
            return type("C", (), {"cursor": lambda self: _FakeCursor()})()

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(systems, "_db_connection", lambda: _FakeCtx())

    resp = client.post(
        "/route",
        json={"from": "A", "to": "B", "range": 50, "loadout": COBRA_SLEF,
              "fuel_safe": True},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["fuel"]["enforced"] is True
    assert body["fuel"]["scoop_stops"] >= 1
    assert body["fuel"]["safe"] is True            # no longer strands
    assert body["total_jumps"] > 20                # detours added
    assert any(j["system"] == "ScoopK" for j in body["jumps"])


def test_route_post_without_loadout_has_no_fuel(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

    async def fake_resolve(value):
        return {"A": (0.0, 0.0, 0.0), "B": (0.0, 0.0, 30.0)}[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)
    monkeypatch.setattr(
        systems, "_solve_route_sync",
        lambda *a: {
            "jump_range": 50.0, "boost_range": 200.0, "straight_line_ly": 30.0,
            "total_jumps": 1, "neutron_jumps": 0, "bridge_jumps": 1,
            "total_distance_ly": 30.0, "efficiency": 1.0, "beam_width": 3,
            "solve_ms": 0.1,
            "jumps": [{"system": None, "id64": None, "x": 0.0, "y": 0.0,
                       "z": 30.0, "type": "regular", "neutron_star": False,
                       "distance_jumped": 30.0, "distance_left": 0.0}],
        },
    )
    resp = client.post(
        "/route", json={"from": "A", "to": "B", "range": 50},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json().get("fuel") is None


def test_resolve_endpoint_handles_edts_vector_with_buggy_bool(monkeypatch):
    # EDTS predicts a Vector3 whose __bool__ returns a float (raises TypeError in
    # a truthiness test). Resolving a not-in-DB procedural system must not 500.
    import asyncio

    class _BuggyVec:
        def __init__(self, c):
            self._c = c

        def __bool__(self):
            raise TypeError("__bool__ should return bool, returned float")

        def __getitem__(self, i):
            return self._c[i]

    class _FakeSys:
        position = _BuggyVec([1975.0, -305.0, -225.0])
        name = "Col 173 Sector CK-Q d5-54"
        mainstar = "K"
        id64 = 7654321

    async def _no_db(_value):
        return None

    monkeypatch.setattr(systems, "fetch_system_from_db", _no_db)
    monkeypatch.setattr(systems.system, "from_name", lambda *a, **k: _FakeSys())

    coords, name, star, id64 = asyncio.run(
        systems._resolve_route_endpoint("Col 173 Sector CK-Q d5-54")
    )
    assert coords == (1975.0, -305.0, -225.0)
    assert name == "Col 173 Sector CK-Q d5-54"
    assert star == "K"
    assert id64 == 7654321


def test_route_patron_access(monkeypatch):
    _gate_future(monkeypatch)
    monkeypatch.delenv("ENTITLEMENT_KEYS", raising=False)
    monkeypatch.delenv("ROUTE_ACCESS_KEYS", raising=False)
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.setattr(systems, "_neutron_index", type("I", (), {"count": 1})())

    async def fake_resolve(value):
        return {"Sol": (0.0, 0.0, 0.0), "X": (0.0, 0.0, 10.0)}[value], value, None, None

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
