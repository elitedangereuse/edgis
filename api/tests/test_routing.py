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


def assert_legal(result, jump_range):
    boost = 4.0 * jump_range
    for leg in result["jumps"]:
        cap = jump_range if leg["type"] == "regular" else boost
        assert leg["distance_jumped"] <= cap + 1e-4, leg
    assert result["jumps"][-1]["distance_left"] == pytest.approx(0.0)


def test_straight_boost_route_no_bridging():
    # Neutrons every 180 LY (<= 200 boost); first within jump range of src.
    pts = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]  # 40..1840
    index = make_index(pts)
    result = routing.solve_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0), 50.0, no_stars
    )
    assert_legal(result, 50.0)
    assert result["neutron_jumps"] >= 1
    # Only the initial board is a regular jump; the void-free path never bridges.
    assert result["bridge_jumps"] == 1


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


def test_greedy_march_reaches_dest():
    index = make_index([(0.0, 0.0, 40.0)])  # index is unused by the march
    result = routing.greedy_march(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 900.0), 50.0, grid_stars()
    )
    assert result is not None
    assert result["jumps"][-1]["distance_left"] == pytest.approx(0.0)
    # grid has no neutrons -> every hop is a plain (<= J) regular jump.
    for leg in result["jumps"]:
        assert leg["distance_jumped"] <= 50.0 + 1e-4


def test_greedy_march_returns_none_in_void():
    index = make_index([(0.0, 0.0, 40.0)])
    assert routing.greedy_march(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 5000.0), 50.0, no_stars
    ) is None


def test_adaptive_beam_width_by_distance():
    pts = [(0.0, 0.0, float(40 + 180 * k)) for k in range(11)]
    index = make_index(pts)
    short = routing.solve_route(
        index, (0.0, 0.0, 0.0), (0.0, 0.0, 2000.0), 50.0, no_stars
    )
    assert short["beam_width"] == 3  # straight-line < 5000
