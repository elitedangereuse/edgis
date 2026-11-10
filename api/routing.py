"""Neutron-boosted route plotter.

Primary engine: :func:`graph_route`, a min-jumps **A\\* over a corridor of
neutron stars** around the src->dst axis (boost edges cost 1; analytic
regular-bridging edges where neutrons gap out, notably the galaxy-spanning
``x~=0`` / ``z~=0`` neutron voids, the "black cross"). The sequence is then
realized star-by-star: greedy walk first, capsule-BFS where greedy dead-ends,
and edges proven unbridgeable are blacklisted so the search reroutes.
Validated against spansh on the benchmark routes: fewer physical jumps on
every one (e.g. Sol->Colonia 143 vs 166, Sol->Beagle@50 589 vs 631).

Fallback engine: :func:`solve_route`, a directed beam search with greedy
bridging (fast, but weaker on sparse routes).

The KD-tree (~5M neutrons, ~120 MB) lives in :class:`NeutronIndex`, built once
at startup. All database access is injected as callables so the solver is pure
and unit-testable:

* ``fetch_neutron_rows() -> Iterable[(x, y, z, id64, name)]`` (index build)
* ``fetch_stars_within(pos, radius) -> Iterable[(x, y, z, name[, mainstar])]``
* ``fetch_stars_near_segment(p0, p1, width)`` (capsule bridging, optional)
* ``fetch_stars_toward(pos, radius, target, limit)`` (greedy KNN, optional)
"""

from __future__ import annotations

import heapq
import math
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

import numpy as np
from scipy.spatial import cKDTree

# Optional native (Rust/PyO3) range-growth A* kernel. When present it replaces the
# pure-Python _graph_search_range hot loop (same min-jumps result, ~3x faster on
# the full Colonia corridor); absent, the Python path runs unchanged. Disable with
# EDGIS_NATIVE_ROUTE=0.
import os as _os

_native_route = None
if _os.environ.get("EDGIS_NATIVE_ROUTE", "1") != "0":
    try:
        import edgis_route as _native_route  # type: ignore
    except Exception:  # pragma: no cover - absence is the normal fallback
        _native_route = None
_native_route_warned = False

NeutronRow = tuple[float, float, float, int, str]
StarRow = tuple  # (x, y, z, name[, mainstar])
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
        arrival_ls: Optional[np.ndarray] = None,
        scoop_ls: Optional[np.ndarray] = None,
    ) -> None:
        self.coords = coords
        self.id64 = id64
        self.names = names
        # Distance-from-arrival (Ls) of the neutron within its system: 0 for a
        # main (arrival) star, >0 for a companion neutron you must fly to before
        # supercharging. All-zero unless companion neutrons were folded in.
        self.arrival_ls = (
            np.asarray(arrival_ls, dtype=np.float64)
            if arrival_ls is not None
            else np.zeros(len(id64), dtype=np.float64)
        )
        # Distance-from-arrival (Ls) of the nearest SCOOPABLE companion star in
        # this neutron's system, or +inf if none: landing here lets the commander
        # refuel without leaving the highway (supercruise to the scoopable, scoop,
        # jump out). The fuel-aware planner treats a neutron with
        # scoop_ls <= scoop_max_ls as a free refuel. +inf unless folded in from
        # the scoopable_neutrons precompute. See [[scoopable-neutron-precompute]].
        self.scoop_ls = (
            np.asarray(scoop_ls, dtype=np.float64)
            if scoop_ls is not None
            else np.full(len(id64), np.inf, dtype=np.float64)
        )
        self.tree = cKDTree(coords)
        self.count = len(id64)
        self.built_at = time.time()

    @classmethod
    def build(cls, fetch_neutron_rows: FetchNeutrons) -> "NeutronIndex":
        rows = list(fetch_neutron_rows())
        if not rows:
            raise ValueError("no neutron rows returned for index build")
        coords = np.asarray([(r[0], r[1], r[2]) for r in rows], dtype=np.float64)
        # Optional 6th column = arrival_ls (companion neutrons); default 0.
        arrival = np.asarray(
            [float(r[5]) if len(r) > 5 and r[5] is not None else 0.0 for r in rows],
            dtype=np.float64,
        )
        # Optional 7th column = scoop_ls (nearest scoopable companion Ls); +inf
        # when absent (NULL) or the column isn't provided.
        scoop = np.asarray(
            [float(r[6]) if len(r) > 6 and r[6] is not None else np.inf
             for r in rows],
            dtype=np.float64,
        )
        return cls(coords, [r[3] for r in rows], [r[4] for r in rows],
                   arrival, scoop)


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
    boost_mult: float = 4.0,
    allowed: Optional[np.ndarray] = None,
) -> tuple[str, list[int]]:
    """Layered beam search over neutrons; returns ("ok"|"stuck", path).

    Keeps the ``width`` frontier nodes closest to ``dst`` each layer. Succeeds
    when a frontier neutron is within boost range of ``dst`` (a final boosted
    jump finishes). Reports "stuck" with the closest-approach path when the
    frontier can't advance (void ahead) — the caller then bridges.
    """

    coords = index.coords
    tree = index.tree
    boost = boost_mult * jump_range

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
                if allowed is not None and not allowed[m]:
                    continue  # companion neutron beyond the opted-in Ls cap
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
    for row in fetch_stars_within(pos, jump_range):
        x, y, z, name = row[0], row[1], row[2], row[3]
        mainstar = row[4] if len(row) > 4 else None
        id64 = row[5] if len(row) > 5 else None
        if name in visited:
            continue
        d = (x - tx) ** 2 + (y - ty) ** 2 + (z - tz) ** 2
        if best_d is None or d < best_d:
            best_d = d
            best = (x, y, z, name, mainstar, id64)
    return best


def _leg(
    leg_type: str,
    name: Optional[str],
    coord: tuple[float, float, float],
    dist: float,
    is_neutron: bool,
    left: float,
    id64: Optional[int] = None,
    spectral: Optional[str] = None,
    arrival_ls: Optional[float] = None,
    inject_grade: Optional[float] = None,
) -> dict:
    leg = {
        "type": leg_type,
        "system": name,
        "id64": id64,
        "spectral": spectral,
        "x": float(coord[0]),
        "y": float(coord[1]),
        "z": float(coord[2]),
        "neutron_star": is_neutron,
        "distance_jumped": float(dist),
        "distance_left": float(left),
    }
    # Set when this jump required an FSD synthesis injection: the grade used
    # (1.25 basic / 1.5 standard / 2.0 premium), the LEAST that bridged the gap.
    # The UI shows the matching icon so the commander knows what to synthesise.
    if inject_grade:
        leg["inject_grade"] = float(inject_grade)
    # Only set for a companion neutron (boost source you supercruise to in-system
    # before charging); the UI shows it as "neutron at X Ls". Main-star neutrons
    # and bridge stars leave it unset.
    if arrival_ls is not None and float(arrival_ls) > 0.0:
        leg["arrival_ls"] = float(arrival_ls)
    return leg


def solve_route(
    index: NeutronIndex,
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    jump_range: float,
    fetch_stars_within: FetchStarsWithin,
    width: Optional[int] = None,
    boost_mult: float = 4.0,
    companion_max_ls: float = 0.0,
) -> dict:
    """Plot a neutron-boosted route from ``src`` to ``dst``.

    Returns a dict with ``jumps`` (one per hop) and totals. Raises
    :class:`RouteError` when no legal route exists (e.g. the destination sits
    in a void with no star within one jump). ``boost_mult`` is the neutron
    supercharge multiplier (4.0 normal, 6.0 Caspian). ``companion_max_ls``
    mirrors :func:`graph_route`: companion neutrons (arrival_ls > 0) are usable
    only within that supercruise distance (0 = main-star neutrons only, the
    default and a no-op on a main-only index).
    """

    coords = index.coords
    tree = index.tree
    names = index.names
    id64 = index.id64
    # Which index nodes this request may board: all main-star neutrons, plus
    # companion neutrons within the opted-in Ls cap. None when nothing is
    # filtered out (keeps the hot path allocation-free and behaviour-identical).
    als = getattr(index, "arrival_ls", None)
    allowed = None
    if als is not None and len(als) and float(als.max()) > float(companion_max_ls):
        allowed = als <= float(companion_max_ls)
    boost = boost_mult * jump_range
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
    if allowed is not None:
        entry_within = [n for n in entry_within if allowed[n]]
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
        # Head toward the nearest neutron that makes progress toward the
        # destination, not straight at the (possibly very distant) target:
        # boarding a neutron ASAP is what minimises jumps out of a
        # neutron-sparse start (the bubble / the x~=0 / z~=0 voids).
        knn = np.atleast_1d(tree.query(pos, k=min(200, index.count))[1])
        forward = [
            int(n) for n in knn
            if _dist(coords[int(n)], dst_v) < straight - 1e-6
            and (allowed is None or allowed[int(n)])
        ]
        entry_target = tuple(coords[forward[0]]) if forward else tuple(coords[int(knn[0])])
        while True:
            _check_caps()
            near = tree.query_ball_point(pos, jump_range)
            if allowed is not None:
                near = [n for n in near if allowed[n]]
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
                fetch_stars_within, tuple(pos), entry_target, jump_range, bridge_visited
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
                    spectral=(nxt[4] if len(nxt) > 4 else None),
                    id64=(nxt[5] if len(nxt) > 5 else None),
                )
            )
            pos = [nxt[0], nxt[1], nxt[2]]

    starts = [start]

    # ---- Boost march, bridging across voids until we arrive.
    for _safety in range(_MAX_TOTAL_JUMPS):
        status, path = _beam(
            index, starts, dst_v, jump_range, width, boost_mult, allowed=allowed
        )

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
                    arrival_ls=float(index.arrival_ls[node]),
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
                    spectral=(nxt[4] if len(nxt) > 4 else None),
                    id64=(nxt[5] if len(nxt) > 5 else None),
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


FetchStarsNearSegment = Callable[
    [tuple[float, float, float], tuple[float, float, float], float],
    Iterable[StarRow],
]
FetchStarsToward = Callable[
    [tuple[float, float, float], float, tuple[float, float, float], int],
    Iterable[StarRow],
]

_GAP_RADII_J = (8.0, 16.0, 32.0)  # gap-edge search radii, in jump-range units
_ZONE_RADIUS_CAP = 1200.0  # never ban more than this around a failed bridge
# Search toward the DENSER endpoint when it is at least this many times denser
# (local neutron count): the A* heuristic ceil(d/boost) is wildly optimistic when
# the GOAL sits in a neutron-sparse void (the Sol bubble inside the x~=0/z~=0
# "black cross"), so it explodes the frontier. The min-jumps neutron graph is
# direction-symmetric, so we search the fast way and reverse the sequence --
# Colonia->Sol drops from ~68 s to ~5 s for ~identical jumps.
_ORIENT_FLIP_RATIO = 1.5
# Never flip toward a destination this sparse (local neutron count within the
# entry radius): realization is only approximately direction-symmetric, and into
# an extreme-sparse rim the reversed sequence can fail to realize where the
# native direction succeeds. The Sol bubble (~394 within 30J) reverses fine; the
# Beagle Point rim (~35) does not. A flip that fails anyway falls back to native.
_ORIENT_DENSITY_FLOOR = 150
# Never flip on a galaxy-spanning route. Validation (Colonia->deep-space
# at J50) showed that beyond ~30 kly the reversed realization into the sparse
# approach either explodes the jump count (dens~40 @ 33 kly: 812 vs native 322)
# or runs pathologically long (dens~250 @ 46 kly: 790 s). The density floor alone
# can't catch the far-but-locally-dense case; this does. Bubble/Colonia-ward and
# moderate deep routes (<=29 kly measured) flip safely and are 10-100x faster.
_ORIENT_MAX_LY = 30000.0
# FSD synthesis injection grades (range multiplier), ascending so the bridger
# picks the LEAST that crosses a gap: basic +25%, standard +50%, premium +100%.
_INJECT_GRADES = (1.25, 1.5, 2.0)


def _is_neutron_star(mainstar) -> bool:
    return mainstar in ("N", "Neutron Star")


def _boosted_jumps(d: float, jump_range: float, boost_mult: float = 4.0) -> int:
    """Jumps to cover ``d`` starting supercharged: one boosted jump
    (``boost_mult``*J, 4x normal, 6x on a Caspian), then J-hops."""
    boost = boost_mult * jump_range
    if d <= boost:
        return 1
    return 1 + int(math.ceil((d - boost) / jump_range))


def _flip_edge(edge: tuple[int, int]) -> tuple[int, int]:
    """Map a realization-failure edge ``(prev, cur)`` (global neutron ids, with
    -1 = SRC and -2 = DST sentinels) from the requested direction into the
    flipped search orientation: reverse the order and swap the src/dst sentinels
    (true-SRC -1 <-> search-DST -2). So the re-search blacklists the same edge."""
    def swap(x: int) -> int:
        return -2 if x == -1 else (-1 if x == -2 else x)
    p, q = edge
    return (swap(q), swap(p))


def _corridor_candidates(
    index: NeutronIndex,
    sv: np.ndarray,
    dv: np.ndarray,
    radius: float,
    entry_r: float,
) -> np.ndarray:
    """Neutrons within ``radius`` of the src->dst segment (sampled balls along
    the axis), plus entry/exit spheres. Bounded even on galaxy-spanning routes,
    unlike the old src-dst ellipsoid."""

    straight = float(np.linalg.norm(dv - sv))
    n = max(2, int(straight / radius) + 2)
    ts = np.linspace(0.0, 1.0, n)
    pts = sv[None, :] + ts[:, None] * (dv - sv)[None, :]
    found: set[int] = set()
    for hit in index.tree.query_ball_point(pts, radius * 1.12):
        found.update(hit)
    for p in (sv, dv):
        found.update(index.tree.query_ball_point(p, entry_r))
    return np.fromiter(found, dtype=np.int64, count=len(found))


def _graph_search(
    index: NeutronIndex,
    cand: np.ndarray,
    sv: np.ndarray,
    dv: np.ndarray,
    jump_range: float,
    entry_r: float,
    gap_k: int,
    blacklist: set[tuple[int, int]],
    badzones: list[tuple[np.ndarray, float]],
    deadline: float,
    boost_mult: float = 4.0,
) -> Optional[list[int]]:
    """Min-jumps A* over the corridor neutrons. Boost edges (<=``boost_mult``*J)
    cost 1; gap edges (fired only where no boost neighbor makes forward
    progress) and the tail/direct edges use the analytic supercharged cost.
    Returns the neutron sequence as GLOBAL index ids, or ``None``. ``blacklist``
    holds proven unbridgeable (from, to) edges (-1 = SRC, -2 = DST); ``badzones``
    are (center, radius) spheres around proven-starless segments."""

    J = jump_range
    boost = boost_mult * J
    straight = float(np.linalg.norm(dv - sv))
    n = len(cand)
    cc = index.coords[cand] if n else np.zeros((0, 3))
    lt = cKDTree(cc) if n else None
    dd_dst = np.linalg.norm(cc - dv, axis=1) if n else np.zeros(0)
    hv = np.ceil(dd_dst / boost).astype(np.int64) if n else np.zeros(0, np.int64)

    def zone_ok(pa, pb) -> bool:
        if not badzones:
            return True
        mid = (pa + pb) / 2.0
        return all(float(np.linalg.norm(mid - c)) >= r for c, r in badzones)

    SRC, DST = n, n + 1
    BIG = np.iinfo(np.int64).max // 4
    g = np.full(n + 2, BIG, dtype=np.int64)
    gd = np.full(n + 2, np.inf)
    par = np.full(n + 2, -9, dtype=np.int64)
    closed = np.zeros(n + 2, dtype=bool)
    g[SRC] = 0
    gd[SRC] = 0.0
    pq: list[tuple[int, int, float, int]] = [
        (int(math.ceil(straight / boost)), 0, 0.0, SRC)
    ]

    def relax(v: int, nj: int, nd: float, u: int) -> None:
        if nj < g[v]:
            g[v] = nj
            gd[v] = nd
            par[v] = u
            h = 0 if v == DST else int(hv[v])
            heapq.heappush(pq, (nj + h, -nj, nd, v))

    found = False
    while pq:
        _, njneg, _, u = heapq.heappop(pq)
        if closed[u]:
            continue
        closed[u] = True
        gg = int(g[u])
        if u == DST:
            found = True
            break
        if time.perf_counter() > deadline:
            return None
        if u == SRC:
            if (-1, -2) not in blacklist and zone_ok(sv, dv):
                relax(DST, gg + int(math.ceil(straight / J)), straight, u)
            if lt is not None:
                ms = np.asarray(lt.query_ball_point(sv, entry_r), dtype=np.int64)
                ms = np.asarray(
                    [
                        m for m in ms
                        if (-1, int(cand[m])) not in blacklist
                        and zone_ok(sv, cc[m])
                    ],
                    dtype=np.int64,
                )
                if len(ms):
                    dms = np.linalg.norm(cc[ms] - sv, axis=1)
                    costs = np.maximum(np.ceil(dms / J), 1).astype(np.int64)
                    for m, dm, c in zip(ms, dms, costs):
                        relax(int(m), gg + int(c), float(dm), u)
            continue
        cu = cc[u]
        if (int(cand[u]), -2) not in blacklist and zone_ok(cu, dv):
            relax(
                DST,
                gg + _boosted_jumps(float(dd_dst[u]), J, boost_mult),
                gd[u] + float(dd_dst[u]),
                u,
            )
        ms = np.asarray(lt.query_ball_point(cu, boost), dtype=np.int64)
        ms = ms[~closed[ms]]
        forward = False
        if len(ms):
            dms = np.linalg.norm(cc[ms] - cu, axis=1)
            forward = bool(np.any(dd_dst[ms] < dd_dst[u] - 1e-6))
            keep = (gg + 1) < g[ms]
            for m, dm in zip(ms[keep], dms[keep]):
                relax(int(m), gg + 1, gd[u] + float(dm), u)
        if not forward:
            # Local dead end toward the target: bridge the void analytically.
            for mult in _GAP_RADII_J:
                near = np.asarray(
                    lt.query_ball_point(cu, mult * J), dtype=np.int64
                )
                near = near[(dd_dst[near] < dd_dst[u] - 1e-6) & ~closed[near]]
                if not len(near):
                    continue
                dms = np.linalg.norm(cc[near] - cu, axis=1)
                order = np.argsort(dms)[:gap_k]
                relaxed = 0
                for i in order:
                    m = int(near[i])
                    if (int(cand[u]), int(cand[m])) in blacklist:
                        continue
                    if not zone_ok(cu, cc[m]):
                        continue
                    relax(m, gg + _boosted_jumps(float(dms[i]), J, boost_mult),
                          gd[u] + float(dms[i]), u)
                    relaxed += 1
                if relaxed:
                    break
    if not found:
        return None
    seq: list[int] = []
    node = int(par[DST])
    while node >= 0 and node != SRC:
        seq.append(int(cand[node]))
        node = int(par[node])
    seq.reverse()
    return seq


class _FuelLabel:
    """A heap entry for the fuel-aware search: the node, cumulative jumps ``g``,
    refuel resource ``r`` (boosted jumps since the last refuel), distance ``d``
    (heap tiebreak + reconstruction), and the parent label for the path."""

    __slots__ = ("node", "g", "r", "d", "par")

    def __init__(self, node: int, g: int, r: int, d: float,
                 par: "Optional[_FuelLabel]") -> None:
        self.node = node
        self.g = g
        self.r = r
        self.d = d
        self.par = par


def _graph_search_fuel(
    index: NeutronIndex,
    cand: np.ndarray,
    sv: np.ndarray,
    dv: np.ndarray,
    jump_range: float,
    entry_r: float,
    gap_k: int,
    blacklist: set[tuple[int, int]],
    badzones: list[tuple[np.ndarray, float]],
    deadline: float,
    tank_jumps: int,
    scoop_max_ls: float,
    boost_mult: float = 4.0,
) -> Optional[list[int]]:
    """Fuel-aware variant of :func:`_graph_search`. Min-**jumps** over the
    corridor neutrons where a boosted hop burns one jump of fuel and landing on a
    *scoopable* neutron (``scoop_ls <= scoop_max_ls``, from the ``scoopable_neutrons``
    precompute) is a FREE refuel, so the search threads scoopable neutrons rather
    than landing on a dry neutron that forces a scoop-stop detour. This reproduces
    spansh's inline refuels and kills the detour tax that makes our reverse
    (Colonia->Sol) jump count blow out. See ``docs/fuel-aware-planner-plan.md``.

    Models a refuel RESOURCE ``r`` = boosted jumps since the last refuel, in
    ``[0, T]`` with ``T = tank_jumps``. A boost hop costs 1 and bumps ``r`` (reset
    to 0 on a scoopable landing); a hop attempted on a dry tank (``r == T``) costs
    3 -- the forced +2 scoop-stop detour folded into the edge. Multi-jump analytic
    edges (entry / gap / tail / direct) are scoop-safe bridges the fuel enforcer
    keeps fuelled, so they reset ``r`` to 0 and never pay the dry-tank penalty.

    Dominance pruning keeps the (node x fuel) state space small: a candidate is
    kept only when no lower-or-equal resource already reaches the node in no more
    jumps. The front per node is an anti-chain of size ``<= T + 1``. The dominance
    test is a ``best_g[node, r]`` array (fewest jumps to reach a node at each fuel
    level) so a whole neighbour batch is relaxed with one vectorised prefix-min +
    compare -- a naive per-label Python scan was ~10x the ship-agnostic search.

    Returns the neutron sequence as GLOBAL index ids (same contract as
    :func:`_graph_search`), or ``None`` on timeout / no route -- the caller then
    falls back to the ship-agnostic search, so the fuel layer never regresses the
    geometry below the proven min-jumps route.
    """

    J = jump_range
    boost = boost_mult * J
    straight = float(np.linalg.norm(dv - sv))
    n = len(cand)
    cc = index.coords[cand] if n else np.zeros((0, 3))
    lt = cKDTree(cc) if n else None
    dd_dst = np.linalg.norm(cc - dv, axis=1) if n else np.zeros(0)
    hv = np.ceil(dd_dst / boost).astype(np.int64) if n else np.zeros(0, np.int64)
    # Scoopable flag per corridor candidate: landing here is a free refuel.
    sls = getattr(index, "scoop_ls", None)
    scoopable = (
        (np.asarray(sls)[cand] <= scoop_max_ls)
        if (sls is not None and n) else np.zeros(n, dtype=bool)
    )
    T = int(tank_jumps)

    def zone_ok(pa, pb) -> bool:
        if not badzones:
            return True
        mid = (pa + pb) / 2.0
        return all(float(np.linalg.norm(mid - c)) >= r for c, r in badzones)

    SRC, DST = n, n + 1
    BIG = np.iinfo(np.int64).max // 4
    # best_g[node, r] = fewest jumps known to reach ``node`` with resource ``r``.
    # This array IS the dominance structure: a candidate (node, g, r) is dominated
    # iff some r' <= r already reaches node in <= g jumps, i.e. the prefix-minimum
    # best_g[node, 0..r] <= g. Checking/relaxing a whole neighbour batch is then a
    # vectorised prefix-min + compare (no per-label Python front scan, which was
    # the hot spot). Columns are capped at r == T. SRC/DST use column 0.
    best_g = np.full((n + 2, T + 1), BIG, dtype=np.int64)

    # A corridor neutron's outgoing edges (boost neighbours, void-bridge gap edges,
    # the tail to DST) are label-independent -- only the fuel bookkeeping on a boost
    # hop varies -- and a node is popped once per non-dominated label, so memoise
    # the (expensive) kd-tree queries per node.
    edge_cache: dict[int, tuple] = {}

    def node_edges(u: int) -> tuple:
        hit = edge_cache.get(u)
        if hit is not None:
            return hit
        cu = cc[u]
        ms = np.asarray(lt.query_ball_point(cu, boost), dtype=np.int64)
        if len(ms):
            ms = ms[ms != u]
        if len(ms):
            dm = np.linalg.norm(cc[ms] - cu, axis=1)
            scoop = scoopable[ms]
            forward = bool(np.any(dd_dst[ms] < dd_dst[u] - 1e-6))
        else:
            dm = np.zeros(0)
            scoop = np.zeros(0, dtype=bool)
            forward = False
        gap_edges: list[tuple[int, int, float]] = []
        if not forward:
            # Local dead end toward the target: bridge the void analytically
            # (a scoop-safe bridge, so arrive refuelled).
            for mult in _GAP_RADII_J:
                near = np.asarray(
                    lt.query_ball_point(cu, mult * J), dtype=np.int64
                )
                near = near[dd_dst[near] < dd_dst[u] - 1e-6]
                if not len(near):
                    continue
                dms = np.linalg.norm(cc[near] - cu, axis=1)
                got = False
                for i in np.argsort(dms)[:gap_k]:
                    m = int(near[i])
                    if (int(cand[u]), int(cand[m])) in blacklist:
                        continue
                    if not zone_ok(cu, cc[m]):
                        continue
                    gap_edges.append(
                        (m, _boosted_jumps(float(dms[i]), J, boost_mult),
                         float(dms[i]))
                    )
                    got = True
                if got:
                    break
        tail: Optional[tuple[int, float]] = None
        if (int(cand[u]), -2) not in blacklist and zone_ok(cu, dv):
            tail = (_boosted_jumps(float(dd_dst[u]), J, boost_mult),
                    float(dd_dst[u]))
        hit = (ms, dm, scoop, gap_edges, tail)
        edge_cache[u] = hit
        return hit

    seq_counter = 0
    pq: list[tuple[int, int, float, int, _FuelLabel]] = []

    def push(node: int, g: int, r: int, d: float,
             par: Optional[_FuelLabel]) -> None:
        nonlocal seq_counter
        h = 0 if node >= n else int(hv[node])
        heapq.heappush(
            pq, (g + h, -g, d, seq_counter, _FuelLabel(node, g, r, d, par))
        )
        seq_counter += 1

    def relax(m: int, cg: int, cr: int, cd: float,
              par: Optional[_FuelLabel]) -> None:
        """Scalar relax for the analytic edges (entry / gap / tail / direct), all
        of which arrive refuelled (cr == 0) or terminal. Pushes iff ``cg`` beats
        every resource level up to ``cr`` at ``m``."""
        if int(best_g[m, : cr + 1].min()) <= cg:
            return  # dominated by an equal-or-better label already at m
        best_g[m, cr] = cg
        push(m, cg, cr, cd, par)

    best_g[SRC, 0] = 0
    push(SRC, 0, 0, 0.0, None)

    dst_lab: Optional[_FuelLabel] = None
    while pq:
        _, _, _, _, lab = heapq.heappop(pq)
        u = lab.node
        if u == DST:
            dst_lab = lab  # first DST pop is min-jumps (h(DST) == 0)
            break
        if u < n:
            # Skip a label the array has since superseded: a smaller g reached its
            # own (u, r) cell, or a lower-r label now dominates it.
            if best_g[u, lab.r] < lab.g:
                continue
            if lab.r and int(best_g[u, : lab.r].min()) <= lab.g:
                continue
        if time.perf_counter() > deadline:
            return None
        gg = lab.g
        if u == SRC:
            if (-1, -2) not in blacklist and zone_ok(sv, dv):
                relax(DST, gg + int(math.ceil(straight / J)), 0, straight, lab)
            if lt is not None:
                ms = np.asarray(lt.query_ball_point(sv, entry_r), dtype=np.int64)
                for m in ms:
                    m = int(m)
                    if (-1, int(cand[m])) in blacklist or not zone_ok(sv, cc[m]):
                        continue
                    dm = float(np.linalg.norm(cc[m] - sv))
                    # Entry is a scoop-safe regular bridge: arrive refuelled.
                    relax(m, gg + max(int(math.ceil(dm / J)), 1), 0, dm, lab)
            continue
        r = lab.r
        ms, dm, scoop, gap_edges, tail = node_edges(u)
        if tail is not None:
            # Tail: a supercharged bridge straight to the destination (terminal).
            tcost, tdist = tail
            relax(DST, gg + tcost, 0, lab.d + tdist, lab)
        if len(ms):
            # Vectorised boost-hop relax over all neighbours at once. A dry tank
            # (r == T) folds the forced +2 scoop detour into the edge (cost 3);
            # otherwise cost 1. Landing on a scoopable neutron refuels (nr = 0).
            if r >= T:
                cg = gg + 3
                nr = np.where(scoop, 0, 1)
            else:
                cg = gg + 1
                nr = np.where(scoop, 0, r + 1)
            pmin = np.minimum.accumulate(best_g[ms], axis=1)
            keep = pmin[np.arange(len(ms)), nr] > cg  # non-dominated candidates
            if keep.any():
                km = ms[keep]
                knr = nr[keep]
                kd = lab.d + dm[keep]
                best_g[km, knr] = cg
                for mm, rr, dd in zip(km, knr, kd):
                    push(int(mm), cg, int(rr), float(dd), lab)
        for m, cost, gdm in gap_edges:
            relax(m, gg + cost, 0, lab.d + gdm, lab)
    if dst_lab is None:
        return None
    seq: list[int] = []
    cur = dst_lab.par
    while cur is not None and cur.node != SRC:
        seq.append(int(cand[cur.node]))
        cur = cur.par
    seq.reverse()
    return seq


def _native_graph_search_range(
    index: NeutronIndex,
    cand: np.ndarray,
    cc: np.ndarray,
    sv: np.ndarray,
    dv: np.ndarray,
    jump_range: float,
    entry_r: float,
    gap_k: int,
    blacklist: set[tuple[int, int]],
    badzones: list[tuple[np.ndarray, float]],
    deadline: float,
    reach_by_r: np.ndarray,
    scoop_max_ls: float,
    boost_mult: float,
) -> Optional[list[int]]:
    """Marshal the B2 inputs and run the native range-growth A* kernel. Returns
    the GLOBAL-id neutron sequence, or ``None`` (timeout / no route) exactly like
    the Python implementation. ``cc`` is the already-sliced candidate coord block.
    Kept byte-compatible with _graph_search_range: same scoopable predicate, same
    blacklist sentinels, same per-resource reach, same gap/tail edge rules."""
    sls = getattr(index, "scoop_ls", None)
    scoopable = (
        (np.asarray(sls)[cand] <= scoop_max_ls)
        if (sls is not None and len(cand)) else np.zeros(len(cand), dtype=bool)
    )
    budget = max(0.0, float(deadline) - time.perf_counter())
    bz = [(float(c[0]), float(c[1]), float(c[2]), float(r)) for c, r in badzones]
    return _native_route.graph_search_range(
        np.ascontiguousarray(cc, dtype=np.float64),
        np.ascontiguousarray(scoopable).astype(np.uint8),
        np.ascontiguousarray(cand, dtype=np.int64),
        np.ascontiguousarray(sv, dtype=np.float64),
        np.ascontiguousarray(dv, dtype=np.float64),
        float(jump_range),
        float(entry_r),
        int(gap_k),
        [float(x) for x in reach_by_r],
        float(boost_mult),
        float(budget),
        [(int(a), int(b)) for (a, b) in blacklist],
        bz,
    )


def _graph_search_range(
    index: NeutronIndex,
    cand: np.ndarray,
    sv: np.ndarray,
    dv: np.ndarray,
    jump_range: float,
    entry_r: float,
    gap_k: int,
    blacklist: set[tuple[int, int]],
    badzones: list[tuple[np.ndarray, float]],
    deadline: float,
    reach_by_r: np.ndarray,
    scoop_max_ls: float,
    boost_mult: float = 4.0,
) -> Optional[list[int]]:
    """B2 = fuel-aware search (B1's scoopable-neutron threading) PLUS range growth:
    a ship's jump range grows as the tank empties, so a boost hop made at resource
    r (boosted jumps since the last refuel) reaches ``reach_by_r[r]`` -- longer when
    the tank is emptier -- letting the chain take fewer, longer hops late in a
    refuel cycle. ``reach_by_r[r] = boost_mult * jump_range(fuel after r jumps)``,
    computed by the caller from the loadout; reach_by_r[0] == the B1 full-tank reach
    and grows toward 4*max_range_light at r == T-1 (T = len(reach_by_r)).

    OVER-RANGE SAFE by construction: reach_by_r assumes the CONSERVATIVE max
    fuel-burn per jump, so the realized fuel is always >= assumed and the real reach
    >= reach_by_r[r]; the long hops only occur mid-chain where fuel is genuinely
    low, and every bridge / refuel resets r to 0 (full tank, shortest reach).

    DOMINANCE is SAME-r only: under range growth a lower r (fuller tank, more jumps
    left but SHORTER reach) no longer dominates a higher r (emptier, longer reach),
    so keep the min-jumps label per (node, r). best_g[node, r] holds it; relax iff
    cg < best_g[m, r]. Heuristic uses the MAX reach to stay admissible.

    Returns the neutron sequence as GLOBAL ids (the caller forward-simulates the
    per-node resource for the realizer, which is flip-safe), or ``None`` (timeout /
    no route); the caller then falls back to the ship-agnostic search.
    """

    J = jump_range
    reach_by_r = np.asarray(reach_by_r, dtype=np.float64)
    T = len(reach_by_r)
    reach_max = float(reach_by_r.max())
    straight = float(np.linalg.norm(dv - sv))
    n = len(cand)
    cc = index.coords[cand] if n else np.zeros((0, 3))

    # Native kernel: identical min-jumps result, ~3x faster. Pure (no shared state),
    # so on any marshalling error we warn once and fall through to the Python loop.
    if _native_route is not None:
        try:
            return _native_graph_search_range(
                index, cand, cc, sv, dv, J, entry_r, gap_k, blacklist, badzones,
                deadline, reach_by_r, scoop_max_ls, boost_mult,
            )
        except Exception:  # pragma: no cover
            global _native_route_warned
            if not _native_route_warned:
                import logging
                logging.getLogger(__name__).warning(
                    "native edgis_route kernel failed; using Python fallback",
                    exc_info=True,
                )
                _native_route_warned = True

    lt = cKDTree(cc) if n else None
    dd_dst = np.linalg.norm(cc - dv, axis=1) if n else np.zeros(0)
    hv = np.ceil(dd_dst / reach_max).astype(np.int64) if n else np.zeros(0, np.int64)
    sls = getattr(index, "scoop_ls", None)
    scoopable = (
        (np.asarray(sls)[cand] <= scoop_max_ls)
        if (sls is not None and n) else np.zeros(n, dtype=bool)
    )

    def zone_ok(pa, pb) -> bool:
        if not badzones:
            return True
        mid = (pa + pb) / 2.0
        return all(float(np.linalg.norm(mid - c)) >= r for c, r in badzones)

    SRC, DST = n, n + 1
    BIG = np.iinfo(np.int64).max // 4
    best_g = np.full((n + 2, T + 1), BIG, dtype=np.int64)
    par: dict[tuple[int, int], tuple[int, int]] = {}

    # Boost neighbours within the MAX reach, kept sorted by distance so a label at
    # resource r uses the prefix within reach_by_r[r]. Memoised per node.
    edge_cache: dict[int, tuple] = {}

    def node_edges(u: int) -> tuple:
        hit = edge_cache.get(u)
        if hit is not None:
            return hit
        cu = cc[u]
        ms = np.asarray(lt.query_ball_point(cu, reach_max), dtype=np.int64)
        if len(ms):
            ms = ms[ms != u]
        if len(ms):
            dm = np.linalg.norm(cc[ms] - cu, axis=1)
            o = np.argsort(dm)
            ms = ms[o]; dm = dm[o]
            scoop = scoopable[ms]
        else:
            dm = np.zeros(0); scoop = np.zeros(0, dtype=bool)
        gap_edges: list[tuple[int, int, float]] = []
        # Gap edges fire where no forward boost neighbour exists even at reach_max;
        # a scoop-safe analytic bridge at full-tank J (arrives refuelled).
        forward = bool(len(ms) and np.any(dd_dst[ms] < dd_dst[u] - 1e-6))
        if not forward:
            for mult in _GAP_RADII_J:
                near = np.asarray(
                    lt.query_ball_point(cu, mult * J), dtype=np.int64
                )
                near = near[dd_dst[near] < dd_dst[u] - 1e-6]
                if not len(near):
                    continue
                dms = np.linalg.norm(cc[near] - cu, axis=1)
                got = False
                for i in np.argsort(dms)[:gap_k]:
                    m = int(near[i])
                    if (int(cand[u]), int(cand[m])) in blacklist:
                        continue
                    if not zone_ok(cu, cc[m]):
                        continue
                    gap_edges.append(
                        (m, _boosted_jumps(float(dms[i]), J, boost_mult),
                         float(dms[i]))
                    )
                    got = True
                if got:
                    break
        tail: Optional[tuple[int, float]] = None
        if (int(cand[u]), -2) not in blacklist and zone_ok(cu, dv):
            tail = (_boosted_jumps(float(dd_dst[u]), J, boost_mult),
                    float(dd_dst[u]))
        hit = (ms, dm, scoop, gap_edges, tail)
        edge_cache[u] = hit
        return hit

    order = 0
    pq: list[tuple[int, int, float, int, int, int]] = []

    def push(node: int, g: int, r: int, d: float) -> None:
        nonlocal order
        h = 0 if node >= n else int(hv[node])
        heapq.heappush(pq, (g + h, g, d, order, node, r))
        order += 1

    def relax(m: int, cg: int, cr: int, cd: float, pnode: int, pr: int) -> None:
        if best_g[m, cr] <= cg:
            return  # same (node, r) already reached in no more jumps
        best_g[m, cr] = cg
        par[(m, cr)] = (pnode, pr)
        push(m, cg, cr, cd)

    best_g[SRC, 0] = 0
    push(SRC, 0, 0, 0.0)
    dst_r: Optional[int] = None
    while pq:
        _, g, cd, _, u, r = heapq.heappop(pq)
        if u == DST:
            dst_r = r
            break
        if u < n and best_g[u, r] < g:
            continue  # a smaller g reached this (node, r) after it was queued
        if time.perf_counter() > deadline:
            return None
        gg = g
        if u == SRC:
            if (-1, -2) not in blacklist and zone_ok(sv, dv):
                relax(DST, gg + int(math.ceil(straight / J)), 0, straight, SRC, 0)
            if lt is not None:
                ms = np.asarray(lt.query_ball_point(sv, entry_r), dtype=np.int64)
                for m in ms:
                    m = int(m)
                    if (-1, int(cand[m])) in blacklist or not zone_ok(sv, cc[m]):
                        continue
                    dm = float(np.linalg.norm(cc[m] - sv))
                    relax(m, gg + max(int(math.ceil(dm / J)), 1), 0, dm, SRC, 0)
            continue
        ms, dm, scoop, gap_edges, tail = node_edges(u)
        if tail is not None:
            tcost, tdist = tail
            relax(DST, gg + tcost, 0, cd + tdist, u, r)
        if len(ms):
            # Boost hops usable at this resource: reach grows with r. r == T is a
            # dry tank -> +2 detour (cost 3) then a full-tank hop (reach_by_r[0]).
            if r >= T:
                reach = reach_by_r[0]; step = 3
            else:
                reach = reach_by_r[r]; step = 1
            k = int(np.searchsorted(dm, reach, side="right"))
            if k:
                mm = ms[:k]; sc = scoop[:k]
                if r >= T:
                    nr = np.where(sc, 0, 1)
                else:
                    nr = np.where(sc, 0, r + 1)
                cg = gg + step
                keep = cg < best_g[mm, nr]
                for i in np.nonzero(keep)[0]:
                    m = int(mm[i]); rn = int(nr[i])
                    best_g[m, rn] = cg; par[(m, rn)] = (u, r)
                    push(m, cg, rn, cd + float(dm[i]))
        for m, cost, gdm in gap_edges:
            relax(m, gg + cost, 0, cd + gdm, u, r)
    if dst_r is None:
        return None
    seq: list[int] = []
    node = (DST, dst_r)
    while node in par:
        pnode, pr = par[node]
        if node[0] < n:
            seq.append(int(cand[node[0]]))
        node = (pnode, pr)
    seq.reverse()
    return seq


def _bridge_greedy(
    fetch_stars_within: FetchStarsWithin,
    fetch_stars_toward: Optional[FetchStarsToward],
    pos: np.ndarray,
    boosted: bool,
    target: np.ndarray,
    jump_range: float,
    dst_v: np.ndarray,
    visited: set[str],
    max_steps: int,
    is_locked: Optional[Callable[[str], bool]] = None,
    boost_mult: float = 4.0,
    inject_mult: float = 1.0,
) -> Optional[tuple[list[dict], np.ndarray, bool]]:
    """Greedy star-walk toward ``target`` (excluded); returns the intermediate
    legs + final position/boost state, or ``None`` on a dead end. Fast and
    almost always sufficient in dense space. Bridge stars inside a permit zone
    (``is_locked``) are skipped: you can never arrive at a locked system.

    ``inject_mult`` > 1 enables FSD synthesis injection "when required": if a
    normal jump finds no forward star, one jump is retried at ``inject_mult``*J
    (1.25/1.5/2.0 for basic/standard/premium) and tagged ``injected``. Injection
    only applies to regular jumps (a boosted jump already reaches ``boost``)."""

    boost = boost_mult * jump_range
    legs: list[dict] = []
    cur = np.array(pos, dtype=np.float64)
    on_boost = boosted

    def _pick(reach: float):
        """Best (closest-to-target) unvisited, unlocked star within ``reach``."""
        if fetch_stars_toward is not None:
            rows = fetch_stars_toward(tuple(cur), reach, tuple(target), 32)
        else:
            rows = fetch_stars_within(tuple(cur), reach)
        best = best_d = None
        for row in rows:
            if row[3] in visited:
                continue
            if is_locked is not None and is_locked(row[3]):
                continue
            d = (
                (row[0] - target[0]) ** 2
                + (row[1] - target[1]) ** 2
                + (row[2] - target[2]) ** 2
            )
            if best_d is None or d < best_d:
                best_d, best = d, row
        return best

    for _ in range(max_steps):
        reach = boost if on_boost else jump_range
        if float(np.linalg.norm(target - cur)) <= reach:
            return legs, cur, on_boost
        best = _pick(reach)
        inject_grade = None
        if best is None and inject_mult > 1.0 and not on_boost:
            # No forward star within a normal jump: FSD-inject, using the LEAST
            # grade that bridges the gap (basic 1.25 / standard 1.5 / premium 2),
            # capped at the allowed inject_mult, to minimise material cost.
            for g in _INJECT_GRADES:
                if g > inject_mult + 1e-9:
                    break
                best = _pick(g * jump_range)
                if best is not None:
                    inject_grade = g
                    break
        if best is None:
            return None
        nxt = np.asarray(best[:3], dtype=np.float64)
        ms = best[4] if len(best) > 4 else None
        legs.append(
            _leg(
                "boost" if on_boost else "regular",
                best[3], tuple(nxt), float(np.linalg.norm(nxt - cur)),
                _is_neutron_star(ms), float(np.linalg.norm(nxt - dst_v)),
                spectral=ms,
                id64=(best[5] if len(best) > 5 else None),
                inject_grade=inject_grade,
            )
        )
        visited.add(best[3])
        cur = nxt
        on_boost = _is_neutron_star(ms)
    return None


def _bridge_capsule(
    fetch_stars_near_segment: FetchStarsNearSegment,
    pos: np.ndarray,
    boosted: bool,
    target: np.ndarray,
    jump_range: float,
    dst_v: np.ndarray,
    is_locked: Optional[Callable[[str], bool]] = None,
    boost_mult: float = 4.0,
) -> Optional[tuple[list[dict], np.ndarray, bool]]:
    """Min-jump bridge via BFS over one capsule star fetch around the segment.
    Slower than the greedy walk but immune to its local dead ends; used only
    where the greedy fails, which is sparse space where the fetch is small.
    Stars inside a permit zone (``is_locked``) are dropped before the BFS."""

    J = jump_range
    boost = boost_mult * J
    for width in (max(3.0 * J, 200.0), max(9.0 * J, 600.0)):
        rows = list(fetch_stars_near_segment(tuple(pos), tuple(target), width))
        if is_locked is not None:
            rows = [r for r in rows if not is_locked(r[3])]
        if not rows:
            continue
        pts = np.asarray([(r[0], r[1], r[2]) for r in rows])
        neut = np.asarray([_is_neutron_star(r[4] if len(r) > 4 else None)
                           for r in rows])
        tree = cKDTree(pts)
        start_reach = boost if boosted else J
        if float(np.linalg.norm(target - pos)) <= start_reach:
            return [], np.array(pos, float), boosted
        n = len(pts)
        parent = np.full(n, -2, dtype=np.int64)
        frontier = np.asarray(
            tree.query_ball_point(pos, start_reach), dtype=np.int64
        )
        parent[frontier] = -1
        max_layers = int(math.ceil(float(np.linalg.norm(target - pos)) / J)) + 12
        hit = -1
        for _ in range(max_layers):
            if not len(frontier):
                break
            reach_fr = np.where(neut[frontier], boost, J)
            dt = np.linalg.norm(pts[frontier] - target, axis=1)
            ok = frontier[dt <= reach_fr]
            if len(ok):
                hit = int(ok[0])
                break
            new: list[int] = []
            for grp, rad in ((frontier[neut[frontier]], boost),
                             (frontier[~neut[frontier]], J)):
                if not len(grp):
                    continue
                for pi, lst in zip(grp, tree.query_ball_point(pts[grp], rad)):
                    for m in lst:
                        if parent[m] == -2:
                            parent[m] = int(pi)
                            new.append(m)
            if not new:
                break
            frontier = np.asarray(new, dtype=np.int64)
        if hit < 0:
            continue
        chain: list[int] = []
        node = hit
        while node != -1:
            chain.append(node)
            node = int(parent[node])
        chain.reverse()
        legs: list[dict] = []
        cur = np.array(pos, float)
        on_boost = boosted
        for m in chain:
            nxt_p = pts[m]
            legs.append(
                _leg(
                    "boost" if on_boost else "regular",
                    rows[m][3], tuple(nxt_p),
                    float(np.linalg.norm(nxt_p - cur)),
                    bool(neut[m]), float(np.linalg.norm(nxt_p - dst_v)),
                    spectral=rows[m][4] if len(rows[m]) > 4 else None,
                    id64=rows[m][5] if len(rows[m]) > 5 else None,
                )
            )
            cur = nxt_p
            on_boost = bool(neut[m])
        return legs, cur, on_boost
    return None


def _realize_graph(
    index: NeutronIndex,
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    jump_range: float,
    seq: list[int],
    fetch_stars_within: FetchStarsWithin,
    fetch_stars_near_segment: Optional[FetchStarsNearSegment],
    fetch_stars_toward: Optional[FetchStarsToward],
    is_locked: Optional[Callable[[str], bool]] = None,
    boost_mult: float = 4.0,
    inject_mult: float = 1.0,
    seq_r: Optional[list[int]] = None,
    reach_by_r: Optional[np.ndarray] = None,
):
    """Realize a neutron sequence into legs. Returns ``(legs, None)`` or
    ``(None, (edge, midpoint, seg_d))`` naming the unbridgeable edge.
    ``inject_mult`` > 1 lets the greedy bridger FSD-inject across gaps.

    ``seq_r`` + ``reach_by_r`` enable B2 range growth: the boosted hop OFF
    ``seq[i]`` is allowed up to ``reach_by_r[seq_r[i]]`` (range grows as the tank
    empties), not a fixed ``boost_mult*J``, so longer late-cycle hops realize as a
    single boost leg instead of a bridge. Over-range safe (``reach_by_r`` assumes
    the conservative max fuel-burn, so the real reach is never shorter)."""

    J = jump_range
    boost = boost_mult * J
    rbr = None if reach_by_r is None else np.asarray(reach_by_r, dtype=np.float64)
    coords = index.coords
    names = index.names
    id64 = index.id64
    dst_v = np.asarray(dst, dtype=np.float64)
    legs: list[dict] = []
    pos = np.asarray(src, dtype=np.float64)
    boosted = False
    prev_gid = -1
    targets = [
        (coords[n], names[n], id64[n], True, int(n)) for n in seq
    ] + [(dst_v, None, None, False, -2)]
    for j, (tgt, name, idv, is_neut, gid) in enumerate(targets):
        tgt = np.asarray(tgt, dtype=np.float64)
        d = float(np.linalg.norm(tgt - pos))
        # B2: the hop boosts off the previous neutron seq[j-1] at its resource,
        # so it may reach reach_by_r[seq_r[j-1]] (grows as the tank empties).
        if boosted and rbr is not None and seq_r is not None and j >= 1:
            reach = float(rbr[min(seq_r[j - 1], len(rbr) - 1)])
        else:
            reach = boost if boosted else J
        if d > reach + 1e-9:
            max_steps = int(math.ceil(d / J)) * 4 + 8
            # First try to bridge WITHOUT injection: FSD injection is "when
            # required" only, so it must never be used merely to shorten a
            # segment a normal/capsule bridge can already cross (that would
            # regress hard-but-solvable routes).
            br = _bridge_greedy(
                fetch_stars_within, fetch_stars_toward, pos, boosted, tgt,
                J, dst_v, set(), max_steps, is_locked=is_locked,
                boost_mult=boost_mult, inject_mult=1.0,
            )
            if fetch_stars_near_segment is not None:
                # Quality guard: greedy can SUCCEED badly, wandering around an
                # obstacle (e.g. the empty permit sphere at HEN 2-333) in many
                # small steps. When it clearly exceeds the analytic estimate,
                # ask the capsule min-jumps BFS too and keep the shorter.
                analytic = (
                    _boosted_jumps(d, J, boost_mult) if boosted
                    else max(1, int(math.ceil(d / J)))
                )
                if br is None or len(br[0]) + 1 > analytic + max(
                    2, int(0.25 * analytic)
                ):
                    alt = _bridge_capsule(
                        fetch_stars_near_segment, pos, boosted, tgt, J, dst_v,
                        is_locked=is_locked, boost_mult=boost_mult,
                    )
                    if alt is not None and (
                        br is None or len(alt[0]) < len(br[0])
                    ):
                        br = alt
            if br is None and inject_mult > 1.0:
                # Last resort: no non-injected path bridges this segment, so
                # FSD-inject (reach inject_mult*J) to cross it. This is the only
                # place injection fires, keeping it strictly "when required".
                br = _bridge_greedy(
                    fetch_stars_within, fetch_stars_toward, pos, boosted, tgt,
                    J, dst_v, set(), max_steps, is_locked=is_locked,
                    boost_mult=boost_mult, inject_mult=inject_mult,
                )
            if br is None:
                mid = (pos + tgt) / 2.0
                return None, ((prev_gid, gid), mid, d)
            seg_legs, pos, boosted = br
            legs.extend(seg_legs)
            if len(legs) > _MAX_TOTAL_JUMPS:
                return None, ((prev_gid, gid), (pos + tgt) / 2.0, d)
            d = float(np.linalg.norm(tgt - pos))
        legs.append(
            _leg(
                "boost" if boosted else "regular",
                name, tuple(tgt), d, is_neut,
                float(np.linalg.norm(tgt - dst_v)),
                id64=idv, spectral="N" if is_neut else None,
                arrival_ls=(float(index.arrival_ls[gid])
                            if is_neut and gid >= 0 else None),
            )
        )
        pos = tgt
        boosted = is_neut
        prev_gid = gid
    return legs, None


# Radius (ly) around the B1 coarse path for the B2 flown-direction thin corridor.
# Measured sweep on Colonia->Sol: 120 ly keeps most of the range-growth win (114 ->
# 110) while the flown search is only ~17s (vs 103s at 300 ly), so two-phase lands
# ~30s with wide margin under the time cap. Wider radii can realize a couple fewer
# jumps but are slower and -- more importantly -- were observed to sometimes yield
# an over-range route; the caller's fuel-safety guard falls back to B1 in that case.
_B2_THIN_CORRIDOR_LY = 120.0

# Max wall-clock the B2 flown search may consume before giving up and realizing the
# B1 coarse path instead. Keeps default-on B2 from spending a long/rim route's whole
# time budget on a flown search that won't finish.
_B2_TIME_BUDGET = 60.0

# Max straight-line distance (ly) for which B2 range growth engages. Beyond it B1 is
# slow enough that an unsafe-B2 guard re-solve would risk the time cap, and the win
# is marginal; such routes use pure B1. Includes the common long routes (Colonia,
# Sag A*) and excludes the deep rim (Beagle).
_B2_MAX_LY = 30000.0


def _thin_corridor(index: NeutronIndex, seq_ids: list[int],
                   radius: float) -> np.ndarray:
    """Global ids of every neutron within ``radius`` ly of any neutron in
    ``seq_ids`` (the B1 coarse path): the bounded candidate set the B2 flown
    search runs over, so the sparse-goal direction stays tractable."""
    coords = index.coords
    hit: set[int] = set()
    for gid in seq_ids:
        for m in index.tree.query_ball_point(coords[gid], radius):
            hit.add(int(m))
    return np.array(sorted(hit), dtype=np.int64)


def graph_route(
    index: NeutronIndex,
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    jump_range: float,
    fetch_stars_within: FetchStarsWithin,
    fetch_stars_near_segment: Optional[FetchStarsNearSegment] = None,
    fetch_stars_toward: Optional[FetchStarsToward] = None,
    corridor_ly: float = 700.0,
    entry_jumps: int = 30,
    gap_k: int = 16,
    rounds: int = 5,
    time_cap: float = 60.0,
    is_locked: Optional[Callable[[str], bool]] = None,
    boost_mult: float = 4.0,
    companion_max_ls: float = 0.0,
    inject_mult: float = 1.0,
    tank_jumps: int = 0,
    scoop_max_ls: float = 0.0,
    reach_by_r: Optional[list[float]] = None,
) -> Optional[dict]:
    """Min-**jumps** route via corridor-bounded neutron-graph A*, realized into
    legs with greedy + capsule-BFS bridging. Edges the realizer proves
    unbridgeable are blacklisted (plus a bad zone around them) and the search
    re-runs, so the graph routes around genuinely starless regions. Returns
    ``None`` when no realizable route is found (caller falls back to the beam).

    ``is_locked(system_name) -> bool`` excludes permit-locked systems by name:
    locked neutrons are dropped from the candidate set and locked bridge stars
    are skipped, so no waypoint ever lands in a permit-locked system.
    ``src``/``dst`` are never checked (the commander may hold that permit, and
    it is their explicit endpoint).

    ``boost_mult`` is the neutron supercharge multiplier (4.0 normal; 6.0 for a
    Caspian Explorer's Mk II SCO drive).
    """

    start_time = time.perf_counter()
    deadline = start_time + time_cap
    J = float(jump_range)
    boost = boost_mult * J
    sv = np.asarray(src, dtype=np.float64)
    dv = np.asarray(dst, dtype=np.float64)
    straight = _dist(sv, dv)
    entry_r = entry_jumps * J

    # Fuel-aware search is ship-SPECIFIC (tank size + scoopable-companion reach)
    # and only pays off when the index carries the scoopable-neutron precompute.
    # Without it, fall through to the proven ship-agnostic min-jumps search so
    # GET /route and loadout-less POST stay byte-for-byte identical.
    _sls = getattr(index, "scoop_ls", None)
    use_fuel = (
        int(tank_jumps) >= 1
        # No reachable scoopable companions -> nothing to thread, so don't
        # perturb the proven geometry (the user opted out of in-highway scoops).
        and float(scoop_max_ls) > 0.0
        and _sls is not None
        and bool(np.isfinite(np.asarray(_sls)).any())
    )
    # B2: range growth on top of the fuel-aware search. Enabled when the caller
    # supplies reach_by_r (per-resource boost reach from the loadout) and the reach
    # actually grows; otherwise fall back to B1's fixed full-tank reach.
    _rbr = (
        np.asarray(reach_by_r, dtype=np.float64)
        if (use_fuel and reach_by_r is not None and len(reach_by_r) >= 1)
        else None
    )
    # Gate B2 by distance: on long/rim routes B1 is already slow, the range-growth
    # win is modest, and an unsafe B2 route would trigger the caller's guard into a
    # costly second (B1) solve that could blow the time cap. Below the cap, B2's
    # two-phase fits comfortably. Colonia<->Sol (~22 kly) stays in; Beagle (~65 kly)
    # falls back to pure B1.
    use_range = (
        _rbr is not None
        and float(_rbr.max()) > float(_rbr[0]) + 1e-9
        and straight < _B2_MAX_LY
    )
    _sls_arr = np.asarray(_sls) if _sls is not None else None

    cand = _corridor_candidates(index, sv, dv, corridor_ly, entry_r)
    # Companion neutrons (arrival_ls > 0) are boost sources you must fly to
    # in-system first; keep only those within companion_max_ls. Main-star
    # neutrons have arrival_ls == 0 so are always kept; companion_max_ls == 0
    # (the default) drops every companion. Factored so the B2 thin corridor gets
    # the same companion + permit filtering.
    def _filter(c: np.ndarray) -> np.ndarray:
        a = getattr(index, "arrival_ls", None)
        if a is not None and len(c) and float(companion_max_ls) >= 0.0:
            c = c[a[c] <= float(companion_max_ls)]
        if is_locked is not None and len(c):
            nm = index.names
            c = c[np.array([not is_locked(nm[int(x)]) for x in c], dtype=bool)]
        return c
    cand = _filter(cand)

    def _solve(flip: bool, cand_set: Optional[np.ndarray] = None,
               range_search: bool = False,
               dl: Optional[float] = None) -> Optional[dict]:
        """Run the A* + realization rounds in one orientation over ``cand_set``
        (default the full corridor). When ``flip`` is set the search runs dst->src
        (toward the denser endpoint, so the A* heuristic is well-informed) and the
        neutron sequence is reversed to realize src->dst. ``range_search`` selects
        the B2 range-growth search + a reach-aware realize; it must run UNFLIPPED
        (fuel burns in the flown direction). ``dl`` bounds the search below the
        overall ``deadline`` (B2 gets a sub-budget so a slow flown search can't
        starve the B1 fallback)."""
        cset = cand if cand_set is None else cand_set
        d = deadline if dl is None else dl
        ssv, sdv = (dv, sv) if flip else (sv, dv)
        blacklist: set[tuple[int, int]] = set()
        badzones: list[tuple[np.ndarray, float]] = []
        for _ in range(rounds):
            if range_search:
                seq = _graph_search_range(
                    index, cset, ssv, sdv, J, entry_r, gap_k, blacklist,
                    badzones, d, _rbr, float(scoop_max_ls),
                    boost_mult=boost_mult,
                )
            elif use_fuel:
                # B1: fuel-aware threading at the fixed full-tank reach; fall back
                # to the ship-agnostic min-jumps search so we never regress.
                seq = _graph_search_fuel(
                    index, cset, ssv, sdv, J, entry_r, gap_k, blacklist,
                    badzones, d, int(tank_jumps), float(scoop_max_ls),
                    boost_mult=boost_mult,
                )
                if seq is None:
                    seq = _graph_search(
                        index, cset, ssv, sdv, J, entry_r, gap_k, blacklist,
                        badzones, d, boost_mult=boost_mult,
                    )
            else:
                seq = _graph_search(
                    index, cset, ssv, sdv, J, entry_r, gap_k, blacklist,
                    badzones, d, boost_mult=boost_mult,
                )
            if seq is None:
                return None
            rseq = list(reversed(seq)) if flip else seq
            # B2: forward-simulate the per-neutron resource in realization order so
            # the realizer caps each boost leg at reach_by_r[r] (range growth).
            seq_r = None
            if range_search and _sls_arr is not None and rseq:
                T = len(_rbr)
                seq_r = []
                for i, gid in enumerate(rseq):
                    if i == 0:
                        r = 0
                    else:
                        r = seq_r[i - 1] + 1
                        if _sls_arr[gid] <= scoop_max_ls or r >= T:
                            r = 0
                    seq_r.append(r)
            legs, fail = _realize_graph(
                index, src, dst, J, rseq, fetch_stars_within,
                fetch_stars_near_segment, fetch_stars_toward, is_locked=is_locked,
                boost_mult=boost_mult, inject_mult=inject_mult,
                seq_r=seq_r, reach_by_r=(_rbr if range_search else None),
            )
            if legs is not None:
                return _finalize(
                    legs, src, dst, J, boost, straight, 0, start_time
                )
            edge, mid, seg_d = fail
            blacklist.add(_flip_edge(edge) if flip else edge)
            badzones.append(
                (mid, min(max(2.0 * J, 0.75 * seg_d), _ZONE_RADIUS_CAP))
            )
            if time.perf_counter() > d:
                return None
        return None

    # Orient the A* toward the denser endpoint when the destination is clearly
    # sparser (see _ORIENT_FLIP_RATIO): a sparse GOAL makes the ceil(d/boost)
    # heuristic wildly optimistic and explodes the frontier (Colonia->Sol was
    # ~14x slower than Sol->Colonia for the same route). Realization is only
    # APPROXIMATELY direction-symmetric, so the flip is gated three ways: a
    # DISTANCE cap (galaxy-spanning reversed realization explodes the jumps or
    # hangs -- _ORIENT_MAX_LY), a destination DENSITY floor (an extreme-sparse rim
    # like Beagle Point can't realize reversed -- _ORIENT_DENSITY_FLOOR), and the
    # ratio. If a flipped solve still fails, fall back to the native (proven)
    # orientation so we are never worse than before.
    dens_s = len(index.tree.query_ball_point(sv, entry_r))
    dens_d = len(index.tree.query_ball_point(dv, entry_r))
    flip = (
        straight < _ORIENT_MAX_LY
        and dens_d >= _ORIENT_DENSITY_FLOOR
        and dens_s > dens_d * _ORIENT_FLIP_RATIO
    )

    # B2 (range growth) is DIRECTION-DEPENDENT: fuel burns in the flown direction,
    # so it must search src->dst, which is the slow/sparse-goal orientation the
    # flip exists to avoid. Two-phase resolves the tension: phase 1 runs the fast
    # B1 search (oriented) just for a coarse path, phase 2 runs the range-growth
    # search FLOWN on a thin corridor around that path (a small candidate set, so
    # the sparse-goal direction stays tractable) with a reach-aware realize. On
    # any miss we fall through to the proven B1 solve, so B2 never regresses.
    if use_range:
        coarse = _graph_search_fuel(
            index, cand, (dv if flip else sv), (sv if flip else dv), J, entry_r,
            gap_k, set(), [], deadline, int(tank_jumps), float(scoop_max_ls),
            boost_mult=boost_mult,
        )
        if coarse:
            b1seq = list(reversed(coarse)) if flip else coarse
            tc = _filter(_thin_corridor(index, b1seq, _B2_THIN_CORRIDOR_LY))
            if len(tc):
                # Bound the flown search so a slow one (long/dense route) can't
                # eat the whole deadline and starve the fallback below.
                b2_dl = min(deadline, time.perf_counter() + _B2_TIME_BUDGET)
                res = _solve(False, cand_set=tc, range_search=True, dl=b2_dl)
                if res is not None:
                    return res
            # B2 missed (timeout / unrealizable): realize the coarse B1 path we
            # already have directly -- no second B1 search -- so default-on B2
            # never regresses below B1 and never wastes a full extra solve.
            legs, _f = _realize_graph(
                index, src, dst, J, b1seq, fetch_stars_within,
                fetch_stars_near_segment, fetch_stars_toward, is_locked=is_locked,
                boost_mult=boost_mult, inject_mult=inject_mult,
            )
            if legs is not None:
                return _finalize(
                    legs, src, dst, J, boost, straight, 0, start_time
                )

    result = _solve(flip)
    if result is None and flip:
        result = _solve(False)
    return result


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
    # Drop zero-distance hops — you don't "jump" to where you already are
    # (e.g. from == to, or coincident systems). spansh counts these as 0 jumps.
    legs = [leg for leg in legs if leg["distance_jumped"] > 1e-6]
    # Neutron hops carry no catalogued spectral type here; they are, by
    # construction, neutron stars.
    for leg in legs:
        if leg["neutron_star"] and not leg.get("spectral"):
            leg["spectral"] = "N"
    total_flown = sum(leg["distance_jumped"] for leg in legs)
    # A non-boosted jump that LANDS on a neutron is boarding (or re-boarding) the
    # neutron highway, not a void-crossing bridge. Count only the regular hops
    # that never reach a neutron as bridges, so the count matches what a pilot
    # sees in-game (the truly sparse-space crossings); everything else -- boosts
    # plus those entry hops -- is a neutron-chain jump. (Reported by a tester:
    # the jump onto the first neutron was wrongly tallied as a 3rd bridge.)
    bridge_jumps = sum(
        1 for leg in legs if leg["type"] == "regular" and not leg["neutron_star"]
    )
    neutron_jumps = len(legs) - bridge_jumps
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
