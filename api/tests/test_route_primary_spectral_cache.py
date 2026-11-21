"""Proof that primary_spectral annotation does not regress the cache-hit path.

The /route cache uses a PickleSerializer, so ``get()`` returns a freshly
unpickled dict. If the primary_spectral mainstar lookup runs AFTER the cached
solve returns (response-time annotation), every cache HIT re-runs that DB
lookup -- turning a pure-Redis hit into a DB round-trip. These tests pin:

  1. the route (jumps + primary_spectral) is byte-identical on miss vs hit, and
  2. a cache hit performs ZERO mainstar DB lookups (the annotation is baked into
     the cached payload at solve time, not recomputed on every hit).
"""

import os
import pathlib
import pickle
import sys
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["ROUTE_PREBUILD"] = "0"

# The vendored EDTS submodule parses sys.argv at import time; under pytest its
# argv trips argparse. Blank it for the duration of the import only.
_saved_argv = sys.argv[:]
sys.argv = sys.argv[:1]
try:
    from api import entitlements, systems
finally:
    sys.argv = _saved_argv

client = TestClient(systems.app)


class _PickleRoundTripCache:
    """Faithful stand-in for the real PickleSerializer RedisCache: set stores a
    pickled copy, get returns a FRESH object (so mutating a returned value never
    writes back into the stored copy -- the exact condition the regression hides
    behind)."""

    def __init__(self):
        self.store = {}

    async def get(self, key):
        blob = self.store.get(key)
        return pickle.loads(blob) if blob is not None else None

    async def set(self, key, value, ttl=None):
        self.store[key] = pickle.dumps(value)

    async def clear(self):
        self.store.clear()


def _gate_open(monkeypatch):
    monkeypatch.setitem(
        entitlements.FEATURES,
        "route",
        entitlements.Feature(
            "route", datetime(2999, 1, 1, tzinfo=timezone.utc), "supporter"
        ),
    )
    monkeypatch.setenv("ENTITLEMENT_KEYS", "k")


def _wire(monkeypatch):
    """Open the gate, fake a ready index, resolver, solver (one companion-neutron
    leg lacking primary_spectral), an in-memory pickle cache, and a COUNTED
    mainstar lookup. Returns the call counter list for the DB lookup."""
    _gate_open(monkeypatch)
    monkeypatch.setattr(
        systems, "_neutron_index",
        type("I", (), {"count": 1, "version": "v1"})(),
    )

    async def fake_resolve(value):
        coords = {"Sol": (0.0, 0.0, 0.0), "Colonia": (0.0, 0.0, 1000.0)}
        return coords[value], value, None, None

    monkeypatch.setattr(systems, "_resolve_route_endpoint", fake_resolve)

    def fake_solve(src, dst, jump_range, beam_width, boost_mult=4.0,
                   companion_max_ls=0.0, inject_mult=1.0, tank_jumps=0,
                   scoop_max_ls=0.0, reach_by_r=None, fparams=None, fuel_buckets=0):
        return {
            "jump_range": jump_range, "boost_range": 4 * jump_range,
            "straight_line_ly": 1000.0, "total_jumps": 1, "neutron_jumps": 1,
            "bridge_jumps": 0, "total_distance_ly": 1000.0, "efficiency": 1.0,
            "beam_width": 3, "solve_ms": 1.0,
            "jumps": [{
                "system": "Companion N", "id64": 4242, "x": 0.0, "y": 0.0,
                "z": 1000.0, "type": "boost", "neutron_star": True,
                "distance_jumped": 1000.0, "distance_left": 0.0,
                "arrival_ls": 120.0,
            }],
        }

    monkeypatch.setattr(systems, "_solve_route_sync", fake_solve)
    monkeypatch.setattr(systems, "_route_cache", _PickleRoundTripCache())

    calls = []

    def counted_fetch_mainstars(id64s):
        calls.append(tuple(id64s))
        return {4242: "F"}  # companion-neutron system's scoopable main star

    monkeypatch.setattr(systems, "_fetch_mainstars_sync", counted_fetch_mainstars)
    return calls


def _plot(range_=50):
    resp = client.get(
        "/route",
        params={"from": "Sol", "to": "Colonia", "range": range_},
        headers={"x-route-key": "k"},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_primary_spectral_present_and_identical_miss_vs_hit(monkeypatch):
    _wire(monkeypatch)
    miss = _plot()
    hit = _plot()
    # The scoopable main star is annotated onto the neutron leg...
    assert miss["jumps"][0]["primary_spectral"] == "F"
    # ...and the cache HIT returns a byte-identical route (jumps included).
    assert hit["jumps"] == miss["jumps"]


def test_cache_hit_does_no_mainstar_db_lookup(monkeypatch):
    calls = _wire(monkeypatch)
    _plot()               # miss: solve + (at most) one lookup to annotate
    miss_calls = len(calls)
    _plot()               # hit: must be served from the cached payload
    hit_calls = len(calls) - miss_calls
    assert hit_calls == 0, (
        f"cache hit performed {hit_calls} mainstar DB lookup(s); the "
        "annotation must be baked into the cached payload, not recomputed"
    )


def test_stale_pre_change_cache_entry_still_annotated(monkeypatch):
    """The route-cache version is a restart-stable content digest, so entries
    written BEFORE this change (no primary_spectral) survive a deploy. The
    response-time fallback must still annotate them, at the cost of one lookup."""
    calls = _wire(monkeypatch)
    cache = systems._route_cache

    # Simulate a pre-change entry: a solved route cached WITHOUT primary_spectral.
    # Re-derive the exact key the endpoint will look up.
    legacy = {
        "source": "Sol", "destination": "Colonia",
        "source_coords": {"x": 0.0, "y": 0.0, "z": 0.0}, "source_id64": None,
        "jump_range": 50.0, "boost_range": 200.0, "straight_line_ly": 1000.0,
        "total_jumps": 1, "neutron_jumps": 1, "bridge_jumps": 0,
        "total_distance_ly": 1000.0, "efficiency": 1.0, "beam_width": 3,
        "solve_ms": 1.0, "engine": None, "resolver": None,
        "jumps": [{
            "system": "Companion N", "id64": 4242, "x": 0.0, "y": 0.0,
            "z": 1000.0, "type": "boost", "neutron_star": True,
            "distance_jumped": 1000.0, "distance_left": 0.0, "arrival_ls": 120.0,
        }],
    }
    key = "sol|colonia|50|None|sc4|vv1|cml500"  # default companion_max_ls=500
    import asyncio
    asyncio.run(cache.set(key, legacy))

    body = _plot()  # hit on the legacy entry -> fallback must annotate it
    assert body["jumps"][0]["primary_spectral"] == "F"
    assert len(calls) == 1, "fallback should annotate a stale entry with one lookup"


def test_annotation_db_failure_never_fails_the_route(monkeypatch):
    """The lookup is cosmetic: a DB hiccup must not 500 a solved route (nor a
    cache hit), and the entry cached without the field self-heals on a later
    request once the DB recovers (the endpoint fallback retries)."""
    calls = _wire(monkeypatch)
    state = {"down": True}

    def flaky_fetch(id64s):
        calls.append(tuple(id64s))
        if state["down"]:
            raise RuntimeError("db unavailable")
        return {4242: "F"}

    monkeypatch.setattr(systems, "_fetch_mainstars_sync", flaky_fetch)

    body = _plot()  # fresh solve, annotation DB down -> still 200
    assert body["total_jumps"] == 1
    assert "primary_spectral" not in body["jumps"][0] or not body["jumps"][0]["primary_spectral"]

    state["down"] = False
    body = _plot()  # cache hit on the unannotated entry -> fallback self-heals
    assert body["jumps"][0]["primary_spectral"] == "F"
