import pathlib
import sys

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
