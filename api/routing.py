"""Neutron-boosted route plotter.

A directed **beam search** over an in-memory KD-tree of neutron stars (boosted
jumps up to ``4 x jump_range``), with **regular-jump bridging** across
neutron-sparse gaps — notably the galaxy-spanning ``x~=0`` / ``z~=0`` neutron
voids (the "black cross"). Validated against spansh: matches or beats its
jump count on dense and bridged routes, ~20-1000x faster to solve.

The KD-tree (~5M neutrons, ~120 MB) lives in :class:`NeutronIndex`, built once
at startup. All database access is injected as callables so the solver is pure
and unit-testable:

* ``fetch_neutron_rows() -> Iterable[(x, y, z, id64, name)]`` (index build)
* ``fetch_stars_within(pos, radius) -> Iterable[(x, y, z, name)]`` (bridging)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

import numpy as np
from scipy.spatial import cKDTree

NeutronRow = tuple[float, float, float, int, str]
StarRow = tuple[float, float, float, str]
FetchNeutrons = Callable[[], Iterable[NeutronRow]]
FetchStarsWithin = Callable[[tuple[float, float, float], float], Iterable[StarRow]]

# Safety caps so a pathological request can't run unbounded on the worker.
_MAX_TOTAL_JUMPS = 6000
_MAX_BRIDGE_STARS = 1500  # regular-jump stars across all bridge segments
_BEAM_STALL_LAYERS = 6
_BEAM_MAX_LAYERS = 40000


class NeutronIndex:
    """In-memory KD-tree of neutron stars for boosted-jump queries."""

    def __init__(
        self,
        coords: np.ndarray,
        id64: list[int],
        names: list[str],
    ) -> None:
        self.coords = coords
        self.id64 = id64
        self.names = names
        self.tree = cKDTree(coords)
        self.count = len(id64)
        self.built_at = time.time()

    @classmethod
    def build(cls, fetch_neutron_rows: FetchNeutrons) -> "NeutronIndex":
        rows = list(fetch_neutron_rows())
        if not rows:
            raise ValueError("no neutron rows returned for index build")
        coords = np.asarray([(r[0], r[1], r[2]) for r in rows], dtype=np.float64)
        return cls(coords, [r[3] for r in rows], [r[4] for r in rows])


@dataclass
class RouteError(Exception):
    """Raised when no legal route can be produced."""

    status: str
    detail: str = ""


def _dist(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.linalg.norm(a - b))


def _reconstruct(parent: dict[int, Optional[int]], node: int) -> list[int]:
    path: list[int] = []
    cur: Optional[int] = node
    while cur is not None:
        path.append(cur)
        cur = parent[cur]
    path.reverse()
    return path


def _beam(
    index: NeutronIndex,
    starts: list[int],
    dst: np.ndarray,
    jump_range: float,
    width: int,
) -> tuple[str, list[int]]:
    """Layered beam search over neutrons; returns ("ok"|"stuck", path).

    Keeps the ``width`` frontier nodes closest to ``dst`` each layer. Succeeds
    when a frontier neutron is within boost range of ``dst`` (a final boosted
    jump finishes). Reports "stuck" with the closest-approach path when the
    frontier can't advance (void ahead) — the caller then bridges.
    """

    coords = index.coords
    tree = index.tree
    boost = 4.0 * jump_range

    def dto(node: int) -> float:
        return float(np.linalg.norm(coords[node] - dst))

    parent: dict[int, Optional[int]] = {n: None for n in starts}
    visited: set[int] = set(starts)
    frontier = list(starts)
    best = min(starts, key=dto)
    best_d = dto(best)
    stall = 0

    for _ in range(1, _BEAM_MAX_LAYERS):
        finishers = [n for n in frontier if dto(n) <= boost]
        if finishers:
            return "ok", _reconstruct(parent, min(finishers, key=dto))

        neighbor_lists = tree.query_ball_point(coords[frontier], boost)
        candidates: dict[int, int] = {}
        for parent_node, neighbors in zip(frontier, neighbor_lists):
            for m in neighbors:
                if m in visited or m in candidates:
                    continue
                candidates[m] = parent_node
        if not candidates:
            return "stuck", _reconstruct(parent, best)

        keys = list(candidates.keys())
        order = np.argsort(np.linalg.norm(coords[keys] - dst, axis=1))[:width]
        new_frontier = [keys[i] for i in order]
        for m in new_frontier:
            parent[m] = candidates[m]
            visited.add(m)
        frontier = new_frontier

        layer_best = new_frontier[0]
        if dto(layer_best) < best_d - 1e-6:
            best_d = dto(layer_best)
            best = layer_best
            stall = 0
        else:
            stall += 1
            if stall >= _BEAM_STALL_LAYERS:
                return "stuck", _reconstruct(parent, best)

    return "stuck", _reconstruct(parent, best)


def _nearest_star_toward(
    fetch_stars_within: FetchStarsWithin,
    pos: tuple[float, float, float],
    target: tuple[float, float, float],
    jump_range: float,
    visited: set[str],
) -> Optional[StarRow]:
    """Among stars within ``jump_range`` of ``pos`` (closed-list), the one
    closest to ``target``. Allows lateral steps to escape concavities."""

    tx, ty, tz = target
    best: Optional[StarRow] = None
    best_d: Optional[float] = None
    for x, y, z, name in fetch_stars_within(pos, jump_range):
        if name in visited:
            continue
        d = (x - tx) ** 2 + (y - ty) ** 2 + (z - tz) ** 2
        if best_d is None or d < best_d:
            best_d = d
            best = (x, y, z, name)
    return best


def _leg(
    leg_type: str,
    name: Optional[str],
    coord: tuple[float, float, float],
    dist: float,
    is_neutron: bool,
    left: float,
    id64: Optional[int] = None,
) -> dict:
    return {
        "type": leg_type,
        "system": name,
        "id64": id64,
        "x": float(coord[0]),
        "y": float(coord[1]),
        "z": float(coord[2]),
        "neutron_star": is_neutron,
        "distance_jumped": float(dist),
        "distance_left": float(left),
    }


def solve_route(
    index: NeutronIndex,
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    jump_range: float,
    fetch_stars_within: FetchStarsWithin,
    width: Optional[int] = None,
) -> dict:
    """Plot a neutron-boosted route from ``src`` to ``dst``.

    Returns a dict with ``jumps`` (one per hop) and totals. Raises
    :class:`RouteError` when no legal route exists (e.g. the destination sits
    in a void with no star within one jump).
    """

    coords = index.coords
    tree = index.tree
    names = index.names
    id64 = index.id64
    boost = 4.0 * jump_range
    src_v = np.asarray(src, dtype=np.float64)
    dst_v = np.asarray(dst, dtype=np.float64)
    straight = _dist(src_v, dst_v)
    if width is None:
        # Narrow beam is optimal on short dense corridors; wider pays off on
        # long routes where greedy drifts. Saturates by ~10.
        width = 3 if straight < 5000 else 10

    start_time = time.perf_counter()
    legs: list[dict] = []
    bridge_star_count = 0

    def _check_caps() -> None:
        nonlocal bridge_star_count
        if len(legs) > _MAX_TOTAL_JUMPS:
            raise RouteError("too-long", "route exceeds jump cap")
        if bridge_star_count > _MAX_BRIDGE_STARS:
            raise RouteError("too-long", "bridging exceeds star cap")

    # ---- Entry: board the first neutron (bridge through the field if needed).
    pos = list(src)
    entry_within = tree.query_ball_point(pos, jump_range)
    if entry_within:
        start = min(entry_within, key=lambda n: _dist(coords[n], dst_v))
        legs.append(
            _leg(
                "regular",
                names[start],
                tuple(coords[start]),
                _dist(src_v, coords[start]),
                True,
                _dist(coords[start], dst_v),
                id64[start],
            )
        )
        pos = list(coords[start])
    else:
        bridge_visited: set[str] = set()
        start = None
        while True:
            _check_caps()
            near = tree.query_ball_point(pos, jump_range)
            if near:
                start = min(near, key=lambda n: _dist(coords[n], dst_v))
                legs.append(
                    _leg(
                        "regular",
                        names[start],
                        tuple(coords[start]),
                        _dist(np.asarray(pos), coords[start]),
                        True,
                        _dist(coords[start], dst_v),
                        id64[start],
                    )
                )
                pos = list(coords[start])
                break
            nxt = _nearest_star_toward(
                fetch_stars_within, tuple(pos), dst, jump_range, bridge_visited
            )
            bridge_star_count += 1
            if nxt is None:
                raise RouteError("void-at-source", "no star within jump range")
            bridge_visited.add(nxt[3])
            legs.append(
                _leg(
                    "regular",
                    nxt[3],
                    (nxt[0], nxt[1], nxt[2]),
                    _dist(np.asarray(pos), np.asarray(nxt[:3])),
                    False,
                    _dist(np.asarray(nxt[:3]), dst_v),
                )
            )
            pos = [nxt[0], nxt[1], nxt[2]]

    starts = [start]

    # ---- Boost march, bridging across voids until we arrive.
    for _safety in range(_MAX_TOTAL_JUMPS):
        status, path = _beam(index, starts, dst_v, jump_range, width)

        prev = coords[path[0]]
        for node in path[1:]:
            c = coords[node]
            legs.append(
                _leg(
                    "boost",
                    names[node],
                    tuple(c),
                    _dist(prev, c),
                    True,
                    _dist(c, dst_v),
                    id64[node],
                )
            )
            prev = c
        last = path[-1]
        pos = list(coords[last])
        _check_caps()

        if status == "ok":
            legs.append(
                _leg("boost", None, dst, _dist(np.asarray(pos), dst_v), False, 0.0)
            )
            break

        # Stuck: bridge toward the nearest forward neutron (to re-board and
        # resume boosting), falling back toward the destination.
        bridge_visited = set()
        nearest = tree.query(pos, k=min(200, index.count))[1]
        forward = [
            int(n)
            for n in np.atleast_1d(nearest)
            if _dist(coords[int(n)], dst_v) < _dist(np.asarray(pos), dst_v) - 1e-6
        ]
        target = tuple(coords[forward[0]]) if forward else tuple(dst)

        resumed = False
        while True:
            _check_caps()
            if _dist(np.asarray(pos), dst_v) <= jump_range:
                legs.append(
                    _leg(
                        "regular",
                        None,
                        dst,
                        _dist(np.asarray(pos), dst_v),
                        False,
                        0.0,
                    )
                )
                return _finalize(
                    legs, src, dst, jump_range, boost, straight, width, start_time
                )
            onward = [
                n
                for n in tree.query_ball_point(pos, jump_range)
                if _dist(coords[n], dst_v) < _dist(np.asarray(pos), dst_v) - 1e-6
                and n != last
            ]
            if onward:
                board = min(onward, key=lambda n: _dist(coords[n], dst_v))
                legs.append(
                    _leg(
                        "regular",
                        names[board],
                        tuple(coords[board]),
                        _dist(np.asarray(pos), coords[board]),
                        True,
                        _dist(coords[board], dst_v),
                        id64[board],
                    )
                )
                pos = list(coords[board])
                starts = [board]
                resumed = True
                break
            nxt = _nearest_star_toward(
                fetch_stars_within, tuple(pos), target, jump_range, bridge_visited
            )
            bridge_star_count += 1
            if nxt is None:
                nxt = _nearest_star_toward(
                    fetch_stars_within,
                    tuple(pos),
                    dst,
                    jump_range,
                    bridge_visited,
                )
                bridge_star_count += 1
                if nxt is None:
                    raise RouteError(
                        "void-stuck", "no star within jump range mid-route"
                    )
            bridge_visited.add(nxt[3])
            legs.append(
                _leg(
                    "regular",
                    nxt[3],
                    (nxt[0], nxt[1], nxt[2]),
                    _dist(np.asarray(pos), np.asarray(nxt[:3])),
                    False,
                    _dist(np.asarray(nxt[:3]), dst_v),
                )
            )
            pos = [nxt[0], nxt[1], nxt[2]]
        if not resumed:  # pragma: no cover - loop always breaks or returns
            raise RouteError("bridge-overrun", "bridging did not converge")
    else:  # pragma: no cover - guarded by caps
        raise RouteError("too-long", "route did not converge")

    return _finalize(
        legs, src, dst, jump_range, boost, straight, width, start_time
    )


def _finalize(
    legs: list[dict],
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    jump_range: float,
    boost: float,
    straight: float,
    width: int,
    start_time: float,
) -> dict:
    total_flown = sum(leg["distance_jumped"] for leg in legs)
    neutron_jumps = sum(1 for leg in legs if leg["type"] == "boost")
    bridge_jumps = sum(1 for leg in legs if leg["type"] == "regular")
    return {
        "jump_range": jump_range,
        "boost_range": boost,
        "straight_line_ly": straight,
        "total_jumps": len(legs),
        "neutron_jumps": neutron_jumps,
        "bridge_jumps": bridge_jumps,
        "total_distance_ly": total_flown,
        "efficiency": (straight / total_flown) if total_flown else 0.0,
        "beam_width": width,
        "solve_ms": (time.perf_counter() - start_time) * 1000.0,
        "jumps": legs,
    }
