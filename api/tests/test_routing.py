import pathlib
import sys
import time

import numpy as np
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api import routing


def make_index(points):
    coords = np.asarray(points, dtype=np.float64)
    return routing.NeutronIndex(
        coords,
        list(range(len(points))),
        [f"N{i}" for i in range(len(points))],
    )


def no_stars(pos, radius):
    return []


def grid_stars(step=45):
    pts = [(0.0, 0.0, float(z)) for z in range(0, 2001, step)]

    def fetch(pos, radius):
        px, py, pz = pos
        r2 = radius * radius
        return [
            (x, y, z, f"S{int(z)}")
            for (x, y, z) in pts
            if (x - px) ** 2 + (y - py) ** 2 + (z - pz) ** 2 <= r2
        ]

    return fetch


def assert_legal_boost(result, jump_range, boost_mult):
    boost = boost_mult * jump_range
    for leg in result["jumps"]:
        cap = jump_range if leg["type"] == "regular" else boost
        assert leg["distance_jumped"] <= cap + 1e-4, leg
    assert result["jumps"][-1]["distance_left"] == pytest.approx(0.0)


def assert_legal(result, jump_range):
    assert_legal_boost(result, jump_range, 4.0)


def test_bridge_greedy_injects_only_when_required():
    # Stars every 16 ly toward the target; with J=10 a normal jump can't reach
    # the next one (dead end), but premium injection (2x -> 20 ly) can.
    J = 10.0
    pts = [(0.0, 0.0, float(z)) for z in range(16, 101, 16)]

    def fetch_within(pos, radius):
        px, py, pz = pos
        r2 = radius * radius
        return [(x, y, z, f"S{int(z)}") for (x, y, z) in pts
                if (x - px) ** 2 + (y - py) ** 2 + (z - pz) ** 2 <= r2]

    target = np.array([0.0, 0.0, 100.0])
    dst = target
    off = routing._bridge_greedy(
        fetch_within, None, np.zeros(3), False, target, J, dst, set(), 50,
        inject_mult=1.0,
    )
    assert off is None  # no forward star within J -> dead end
    on = routing._bridge_greedy(
        fetch_within, None, np.zeros(3), False, target, J, dst, set(), 50,
        inject_mult=2.0,
    )
    assert on is not None
    legs = on[0]
    assert legs, "injection should have produced bridge legs"
    # Every hop needed injection, and 16 ly gaps (1.6*J) fit a BASIC (1.25*J=12.5)?
    # No: 16 > 12.5, so the least sufficient grade is standard (1.5*J=15)? 16 > 15
    # too, so premium (2*J=20). Confirms "least grade that works" picks premium.
    assert all(leg.get("inject_grade") == 2.0 for leg in legs)
    assert all(leg["distance_jumped"] <= 2.0 * J + 1e-6 for leg in legs)


def test_bridge_greedy_picks_least_sufficient_grade():
    # A single 11 ly gap (J=10): basic (12.5 ly) already bridges it, so the
    # least-grade logic must pick 1.25, not a higher grade, even when premium
    # is allowed.
    J = 10.0

    def fetch_within(pos, radius):
        px, py, pz = pos
        s = (0.0, 0.0, 11.0)
        return [(s[0], s[1], s[2], "S11")] if (s[2] - pz) ** 2 <= radius ** 2 else []

    # pos 0 -> star at 11 (needs basic injection) -> target at 20 (within J).
    tgt = np.array([0.0, 0.0, 20.0])
    r = routing._bridge_greedy(
        fetch_within, None, np.zeros(3), False, tgt, J, tgt, set(), 5,
        inject_mult=2.0,
    )
    assert r is not None and r[0]
    assert r[0][0].get("inject_grade") == 1.25  # least grade that reaches 11 ly


def test_graph_route_injection_crosses_otherwise_impassable_gap():
    # One unusable off-axis neutron; the real path is a src->dst bridge over a
    # star field spaced 1.6*J. Without injection graph_route cannot realize it;
    # premium injection bridges the gaps.
    J = 10.0
    idx = make_index([(9999.0, 9999.0, 9999.0)])
    pts = [(0.0, 0.0, float(z)) for z in range(16, 97, 16)]

    def fetch_within(pos, radius):
        px, py, pz = pos
        r2 = radius * radius
        return [(x, y, z, f"S{int(z)}") for (x, y, z) in pts
                if (x - px) ** 2 + (y - py) ** 2 + (z - pz) ** 2 <= r2]

    src, dst = (0.0, 0.0, 0.0), (0.0, 0.0, 104.0)
    off = routing.graph_route(idx, src, dst, J, fetch_within,
                              inject_mult=1.0, time_cap=10.0)
    on = routing.graph_route(idx, src, dst, J, fetch_within,
                             inject_mult=2.0, time_cap=10.0)
    assert off is None
    assert on is not None
    assert any(leg.get("inject_grade") for leg in on["jumps"])
    # Injected legs legitimately exceed J (up to inject_mult*J); the final leg
    # still lands on the destination.
    assert on["jumps"][-1]["distance_left"] == pytest.approx(0.0)


def test_straight_boost_route_no_bridging():
    # Neutrons every 180 LY (<= 200 boost); first within jump range of src.
    pts = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]  # 40..1840
    index = make_index(pts)
    result = routing.solve_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0), 50.0, no_stars
    )
    assert_legal(result, 50.0)
    assert result["neutron_jumps"] >= 1
    # The initial board lands ON a neutron, so it counts as a neutron jump, not a
    # bridge. A void-free path has zero bridges (regular hops that never reach a
    # neutron) -- matching what a pilot counts in-game.
    assert result["bridge_jumps"] == 0


def test_bridges_across_a_void_and_resumes_boost():
    # Neutron field with a 420 LY gap (> boost) between z=820 and z=1240.
    near = [(0.0, 0.0, float(40 + 180 * k)) for k in range(5)]  # 40..760
    far = [(0.0, 0.0, float(1240 + 180 * k)) for k in range(5)]  # 1240..1960
    index = make_index(near + far)
    result = routing.solve_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0), 50.0, grid_stars()
    )
    assert_legal(result, 50.0)
    # Must have bridged (regular jumps beyond the initial board) AND resumed
    # boosting afterwards (boost legs on the far side).
    assert result["bridge_jumps"] > 1
    far_boosts = [
        leg
        for leg in result["jumps"]
        if leg["type"] == "boost" and leg["z"] >= 1240
    ]
    assert far_boosts, "expected boosting to resume past the void"


def test_unreachable_destination_raises():
    # One neutron near the source; destination stranded with no stars to bridge.
    index = make_index([(0.0, 0.0, 40.0)])
    with pytest.raises(routing.RouteError):
        routing.solve_route(
            index, (0.0, 0.0, 0.0), (0.0, 0.0, 5000.0), 50.0, no_stars
        )


def test_graph_route_pure_regular_route():
    # Destination far off the lone neutron's axis: the direct regular route
    # through the star grid wins and every hop stays <= J.
    index = make_index([(900.0, 900.0, 40.0)])
    result = routing.graph_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 900.0), 50.0, grid_stars()
    )
    assert result is not None
    assert result["jumps"][-1]["distance_left"] == pytest.approx(0.0)
    for leg in result["jumps"]:
        assert leg["distance_jumped"] <= 50.0 + 1e-4


def test_graph_route_reaches_dest():
    pts = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]
    index = make_index(pts)
    result = routing.graph_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0), 50.0, grid_stars()
    )
    assert result is not None
    assert_legal(result, 50.0)


def test_flip_edge_cases():
    assert routing._flip_edge((5, 9)) == (9, 5)         # interior: just reversed
    assert routing._flip_edge((7, -2)) == (-1, 7)       # ->DST  becomes SRC->
    assert routing._flip_edge((-1, 7)) == (7, -2)       # SRC->  becomes ->DST
    assert routing._flip_edge((-1, -2)) == (-1, -2)     # SRC->DST is its own flip


def test_graph_route_direction_symmetric_under_density_asymmetry(monkeypatch):
    # A straight neutron chain with a dense cluster at the z=2000 end and a
    # sparse z=0 end. Routing toward the sparse end must FLIP the search (goal =
    # the dense end) and still realize a valid route in the requested direction,
    # with a jump count close to the un-flipped direction.
    chain = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]  # 40..1840
    cluster = [(float(a), float(b), 1980.0)
               for a in range(-40, 41, 20) for b in range(-40, 41, 20)]  # 25 pts
    index = make_index(chain + cluster)
    A, B = (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0)

    # This tiny field's sparse end is well under the real density floor; disable
    # the floor so the flip mechanics (not the floor gate) are exercised.
    monkeypatch.setattr(routing, "_ORIENT_DENSITY_FLOOR", 0)
    goals = []
    orig = routing._graph_search

    def spy(index_, cand, ssv, sdv, *a, **k):
        goals.append(tuple(np.asarray(sdv, dtype=float)))
        return orig(index_, cand, ssv, sdv, *a, **k)

    monkeypatch.setattr(routing, "_graph_search", spy)

    fwd = routing.graph_route(index, A, B, 50.0, grid_stars())  # dst dense: no flip
    rev = routing.graph_route(index, B, A, 50.0, grid_stars())  # dst sparse: flip
    assert fwd is not None and rev is not None
    assert_legal(fwd, 50.0)
    assert_legal(rev, 50.0)
    # Forward searches toward B (dense); reverse FLIPS and also searches toward B.
    assert goals[0][2] == pytest.approx(2000.0)
    assert goals[-1][2] == pytest.approx(2000.0), "reverse did not flip toward the dense end"
    # Jump counts stay close (the neutron graph is direction-symmetric); the
    # small gap is realization-heuristic noise through the off-axis cluster, not
    # a worse route (on the real Sol<->Colonia it is +2).
    assert abs(fwd["total_jumps"] - rev["total_jumps"]) <= 4


def test_graph_route_density_floor_blocks_flip_into_extreme_rim(monkeypatch):
    # Same asymmetric field, but with the real density floor the sparse end is
    # far too sparse to flip toward: the search must run in the requested
    # (native) direction even though the source is denser. Guards the rim
    # regression (Sol->Beagle flipped and failed before the floor was added).
    chain = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]
    cluster = [(float(a), float(b), 1980.0)
               for a in range(-40, 41, 20) for b in range(-40, 41, 20)]
    index = make_index(chain + cluster)
    A, B = (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0)

    goals = []
    orig = routing._graph_search

    def spy(index_, cand, ssv, sdv, *a, **k):
        goals.append(tuple(np.asarray(sdv, dtype=float)))
        return orig(index_, cand, ssv, sdv, *a, **k)

    monkeypatch.setattr(routing, "_graph_search", spy)
    rev = routing.graph_route(index, B, A, 50.0, grid_stars())  # dst A is rim-sparse
    assert rev is not None
    assert_legal(rev, 50.0)
    # No flip: the search targets the requested sparse destination A (z=0).
    assert goals[-1][2] == pytest.approx(0.0), "flipped toward a sub-floor destination"


def test_graph_route_distance_cap_blocks_flip(monkeypatch):
    # Density asymmetry alone isn't enough: on a galaxy-spanning route the
    # reversed realization into the sparse approach explodes/hangs, so the flip
    # is also gated by _ORIENT_MAX_LY. Drop the floor (so only the distance cap
    # is in play) and shrink the cap below this route's length: it must NOT flip.
    chain = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]
    cluster = [(float(a), float(b), 1980.0)
               for a in range(-40, 41, 20) for b in range(-40, 41, 20)]
    index = make_index(chain + cluster)
    A, B = (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0)  # 2000 ly apart
    monkeypatch.setattr(routing, "_ORIENT_DENSITY_FLOOR", 0)
    monkeypatch.setattr(routing, "_ORIENT_MAX_LY", 1000.0)  # < 2000 ly route

    goals = []
    orig = routing._graph_search

    def spy(index_, cand, ssv, sdv, *a, **k):
        goals.append(tuple(np.asarray(sdv, dtype=float)))
        return orig(index_, cand, ssv, sdv, *a, **k)

    monkeypatch.setattr(routing, "_graph_search", spy)
    rev = routing.graph_route(index, B, A, 50.0, grid_stars())  # would flip if not capped
    assert rev is not None
    assert_legal(rev, 50.0)
    assert goals[-1][2] == pytest.approx(0.0), "flipped a galaxy-spanning route past the cap"


def test_graph_route_excludes_permit_locked_neutrons():
    # A straight neutron chain where one neutron (by name) is permit-locked.
    # With exclusion on, that neutron must not be boarded.
    pts = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]
    index = make_index(pts)  # names are "N0".."N10"

    def is_locked(name):
        return name == "N5"

    result = routing.graph_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0), 50.0, grid_stars(),
        is_locked=is_locked,
    )
    assert result is not None
    assert_legal(result, 50.0)
    for leg in result["jumps"]:
        assert leg.get("system") != "N5", f"boarded a permit-locked neutron: {leg}"


def test_graph_route_caspian_boost_reaches_further():
    # Neutrons spaced 260 LY apart. A normal 4x boost (<=200 at J=50) can't hop
    # neutron-to-neutron directly, so it bridges across each gap with regular
    # jumps; a Caspian 6x boost (<=300) chains them directly. Same field, same
    # neutron count, but the Caspian route is far shorter.
    index = make_index([(0.0, 0.0, float(40 + 260 * k)) for k in range(6)])
    args = (index, (0.0, 0.0, 0.0), (0.0, 0.0, 1400.0), 50.0, grid_stars())

    caspian = routing.graph_route(*args, boost_mult=6.0)
    normal = routing.graph_route(*args, boost_mult=4.0)
    assert caspian is not None and normal is not None
    assert_legal_boost(caspian, 50.0, 6.0)
    assert_legal_boost(normal, 50.0, 4.0)
    # Caspian chains the 260 LY-spaced neutrons with almost no bridging.
    assert caspian["total_jumps"] < normal["total_jumps"]
    assert caspian["bridge_jumps"] < normal["bridge_jumps"]


def test_graph_route_none_when_unbridgeable():
    index = make_index([(0.0, 0.0, 40.0)])
    assert routing.graph_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 5000.0), 50.0, no_stars
    ) is None


def test_zero_distance_hops_not_counted():
    # from == to: no jump needed; the degenerate arrival must not be counted.
    index = make_index([(0.0, 0.0, 40.0)])
    result = routing.graph_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 0.0), 50.0, grid_stars()
    )
    assert result is not None
    assert result["total_jumps"] == 0


def test_adaptive_beam_width_by_distance():
    pts = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]
    index = make_index(pts)
    short = routing.solve_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0), 50.0, no_stars
    )
    assert short["beam_width"] == 3  # straight-line < 5000


def make_arrival_index(points, arrival):
    coords = np.asarray(points, dtype=np.float64)
    return routing.NeutronIndex(
        coords,
        list(range(len(points))),
        [f"N{i}" for i in range(len(points))],
        np.asarray(arrival, dtype=np.float64),
    )


def test_neutron_index_arrival_ls_defaults_zero():
    # Main-only indexes (no 4th build column) have all-zero arrival_ls, so the
    # companion filter is a no-op for them.
    idx = make_index([(0.0, 0.0, 40.0), (0.0, 0.0, 220.0)])
    assert idx.arrival_ls.tolist() == [0.0, 0.0]


def test_build_reads_optional_arrival_ls_column():
    rows = [
        (0.0, 0.0, 40.0, 10, "A"),            # main: no arrival column -> 0
        (0.0, 0.0, 200.0, 11, "B", 50.0),     # companion: arrival 50 Ls
    ]
    idx = routing.NeutronIndex.build(lambda: rows)
    assert idx.arrival_ls.tolist() == [0.0, 50.0]


def test_companion_neutron_gated_by_threshold():
    # src -> main(40) -> companion(200, 50 Ls) -> dst(360). The companion is the
    # only boost source past the first neutron, so the route exists only when
    # companion_max_ls admits it; the default (0) drops it and the route fails.
    J = 50.0
    idx = make_arrival_index(
        [(0.0, 0.0, 40.0), (0.0, 0.0, 200.0)], [0.0, 50.0]
    )
    src, dst = (0.0, 0.0, 0.0), (0.0, 0.0, 360.0)
    on = routing.graph_route(idx, src, dst, J, no_stars, companion_max_ls=100.0, time_cap=10.0)
    off = routing.graph_route(idx, src, dst, J, no_stars, companion_max_ls=0.0, time_cap=10.0)
    assert on is not None
    assert_legal(on, J)
    assert any(leg.get("system") == "N1" for leg in on["jumps"])  # companion used
    assert off is None  # default: companion excluded, no other boost source


def test_companion_below_threshold_excluded():
    # Same layout but the companion sits at 300 Ls; a 100 Ls cap must exclude it.
    J = 50.0
    idx = make_arrival_index(
        [(0.0, 0.0, 40.0), (0.0, 0.0, 200.0)], [0.0, 300.0]
    )
    src, dst = (0.0, 0.0, 0.0), (0.0, 0.0, 360.0)
    near = routing.graph_route(idx, src, dst, J, no_stars, companion_max_ls=100.0, time_cap=10.0)
    far = routing.graph_route(idx, src, dst, J, no_stars, companion_max_ls=500.0, time_cap=10.0)
    assert near is None       # 300 Ls > 100 cap -> excluded
    assert far is not None     # 300 Ls <= 500 cap -> usable


def test_companion_waypoint_leg_carries_arrival_ls():
    # The realized companion waypoint should expose its arrival distance so the UI
    # can label it "neutron at X Ls"; a main-star neutron leg must not.
    J = 50.0
    idx = make_arrival_index(
        [(0.0, 0.0, 40.0), (0.0, 0.0, 200.0)], [0.0, 50.0]
    )
    src, dst = (0.0, 0.0, 0.0), (0.0, 0.0, 360.0)
    r = routing.graph_route(idx, src, dst, J, no_stars, companion_max_ls=100.0, time_cap=10.0)
    assert r is not None
    main_leg = next(l for l in r["jumps"] if l.get("system") == "N0")
    comp_leg = next(l for l in r["jumps"] if l.get("system") == "N1")
    assert "arrival_ls" not in main_leg          # main star: no annotation
    assert comp_leg.get("arrival_ls") == 50.0     # companion: its Ls


def test_solve_route_beam_respects_companion_cap():
    # The beam fallback must honour companion_max_ls too, or a default (cml=0)
    # request could board a companion when graph_route isn't used. Same layout
    # as the gating test, solved via the beam directly.
    J = 50.0
    idx = make_arrival_index(
        [(0.0, 0.0, 40.0), (0.0, 0.0, 200.0)], [0.0, 50.0]
    )
    src, dst = (0.0, 0.0, 0.0), (0.0, 0.0, 360.0)
    on = routing.solve_route(idx, src, dst, J, no_stars, companion_max_ls=100.0)
    assert any(leg.get("system") == "N1" for leg in on["jumps"])  # companion used
    with pytest.raises(routing.RouteError):
        routing.solve_route(idx, src, dst, J, no_stars, companion_max_ls=0.0)


def make_scoop_index(points, scoop_ls):
    coords = np.asarray(points, dtype=np.float64)
    return routing.NeutronIndex(
        coords,
        list(range(len(points))),
        [f"N{i}" for i in range(len(points))],
        scoop_ls=np.asarray(scoop_ls, dtype=np.float64),
    )


# Two equal-length boosted chains share an entry neutron N0 and a launch neutron
# N3; the middle hop is either N1 (on-axis, NOT scoopable) or N2 (offset,
# scoopable within 50 Ls). With a roomy tank they tie on jumps and the shorter
# on-axis N1 wins; with a one-jump tank the N1 chain must detour to refuel
# (+2 jumps) while the N2 chain refuels for free, so the fuel-aware search
# threads the scoopable N2 instead. J=10 -> boost=40; every hop fits in 40 ly.
_SCOOP_PTS = [(0.0, 0.0, 5.0), (0.0, 0.0, 40.0), (6.0, 0.0, 40.0), (0.0, 0.0, 75.0)]
_SCOOP_LS = [np.inf, np.inf, 50.0, np.inf]  # only N2 has a scoopable companion
_SCOOP_SRC, _SCOOP_DST = (0.0, 0.0, 0.0), (0.0, 0.0, 110.0)


def _fuel_seq(tank_jumps, scoop_max_ls=100.0, index=None):
    idx = index if index is not None else make_scoop_index(_SCOOP_PTS, _SCOOP_LS)
    return routing._graph_search_fuel(
        idx, np.arange(len(idx.id64)), np.asarray(_SCOOP_SRC),
        np.asarray(_SCOOP_DST), 10.0, 300.0, 16, set(), [],
        time.perf_counter() + 10.0, tank_jumps, scoop_max_ls,
    )


def test_fuel_search_threads_scoopable_neutron_on_tight_tank():
    # One-jump tank: the non-scoopable N1 chain forces a refuel detour, so the
    # search routes through the scoopable N2 (index 2) instead of N1 (index 1).
    seq = _fuel_seq(tank_jumps=1)
    assert seq == [0, 2, 3]


def test_fuel_search_matches_plain_when_tank_is_roomy():
    # A roomy tank never binds, so the fuel-aware search reduces to plain
    # min-jumps: equal jump counts tie-broken to the shorter on-axis N1 chain,
    # exactly what the ship-agnostic search returns.
    roomy = _fuel_seq(tank_jumps=50)
    plain = routing._graph_search(
        make_scoop_index(_SCOOP_PTS, _SCOOP_LS), np.arange(4),
        np.asarray(_SCOOP_SRC), np.asarray(_SCOOP_DST), 10.0, 300.0, 16,
        set(), [], time.perf_counter() + 10.0,
    )
    assert roomy == plain == [0, 1, 3]


def test_fuel_search_ignores_companion_beyond_scoop_cap():
    # N2's scoopable companion sits at 50 Ls; a 20 Ls cap must not treat it as a
    # free refuel, so the search gets no benefit from threading N2 and never
    # routes through it.
    seq = _fuel_seq(tank_jumps=1, scoop_max_ls=20.0)
    assert 2 not in seq


def test_graph_route_fuel_aware_disabled_without_scoop_data():
    # graph_route must stay byte-for-byte when the index carries no scoop data
    # (all +inf): tank_jumps is ignored and the plain geometry is produced.
    J = 50.0
    pts = [(0.0, 0.0, float(z)) for z in (40, 200, 360)]
    idx = make_index(pts)  # scoop_ls defaults to +inf everywhere
    src, dst = (0.0, 0.0, 0.0), (0.0, 0.0, 400.0)
    base = routing.graph_route(idx, src, dst, J, no_stars, time_cap=10.0)
    fuel = routing.graph_route(
        idx, src, dst, J, no_stars, time_cap=10.0, tank_jumps=3,
        scoop_max_ls=100.0,
    )
    assert base is not None and fuel is not None
    assert fuel["total_jumps"] == base["total_jumps"]
    assert [l["system"] for l in fuel["jumps"]] == [l["system"] for l in base["jumps"]]


def test_graph_route_fuel_aware_realizes_scoopable_route():
    # End to end through graph_route: the tight-tank fuel-aware solve realizes a
    # legal route that boards the scoopable N2 waypoint.
    idx = make_scoop_index(_SCOOP_PTS, _SCOOP_LS)
    route = routing.graph_route(
        idx, _SCOOP_SRC, _SCOOP_DST, 10.0, no_stars, time_cap=10.0,
        tank_jumps=1, scoop_max_ls=100.0,
    )
    assert route is not None
    assert_legal(route, 10.0)
    assert any(leg.get("system") == "N2" for leg in route["jumps"])
