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
    boost_mult: float = 4.0,
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
        if name in visited:
            continue
        d = (x - tx) ** 2 + (y - ty) ** 2 + (z - tz) ** 2
        if best_d is None or d < best_d:
            best_d = d
            best = (x, y, z, name, mainstar)
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
) -> dict:
    return {
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


def solve_route(
    index: NeutronIndex,
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    jump_range: float,
    fetch_stars_within: FetchStarsWithin,
    width: Optional[int] = None,
    boost_mult: float = 4.0,
) -> dict:
    """Plot a neutron-boosted route from ``src`` to ``dst``.

    Returns a dict with ``jumps`` (one per hop) and totals. Raises
    :class:`RouteError` when no legal route exists (e.g. the destination sits
    in a void with no star within one jump). ``boost_mult`` is the neutron
    supercharge multiplier (4.0 normal, 6.0 Caspian).
    """

    coords = index.coords
    tree = index.tree
    names = index.names
    id64 = index.id64
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
            int(n) for n in knn if _dist(coords[int(n)], dst_v) < straight - 1e-6
        ]
        entry_target = tuple(coords[forward[0]]) if forward else tuple(coords[int(knn[0])])
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
                )
            )
            pos = [nxt[0], nxt[1], nxt[2]]

    starts = [start]

    # ---- Boost march, bridging across voids until we arrive.
    for _safety in range(_MAX_TOTAL_JUMPS):
        status, path = _beam(index, starts, dst_v, jump_range, width, boost_mult)

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
                    spectral=(nxt[4] if len(nxt) > 4 else None),
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


def _is_neutron_star(mainstar) -> bool:
    return mainstar in ("N", "Neutron Star")


def _boosted_jumps(d: float, jump_range: float, boost_mult: float = 4.0) -> int:
    """Jumps to cover ``d`` starting supercharged: one boosted jump
    (``boost_mult``*J, 4x normal, 6x on a Caspian), then J-hops."""
    boost = boost_mult * jump_range
    if d <= boost:
        return 1
    return 1 + int(math.ceil((d - boost) / jump_range))


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
) -> Optional[tuple[list[dict], np.ndarray, bool]]:
    """Greedy star-walk toward ``target`` (excluded); returns the intermediate
    legs + final position/boost state, or ``None`` on a dead end. Fast and
    almost always sufficient in dense space. Bridge stars inside a permit zone
    (``is_locked``) are skipped: you can never arrive at a locked system."""

    boost = boost_mult * jump_range
    legs: list[dict] = []
    cur = np.array(pos, dtype=np.float64)
    on_boost = boosted
    for _ in range(max_steps):
        reach = boost if on_boost else jump_range
        if float(np.linalg.norm(target - cur)) <= reach:
            return legs, cur, on_boost
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
):
    """Realize a neutron sequence into legs. Returns ``(legs, None)`` or
    ``(None, (edge, midpoint, seg_d))`` naming the unbridgeable edge."""

    J = jump_range
    boost = boost_mult * J
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
    for tgt, name, idv, is_neut, gid in targets:
        tgt = np.asarray(tgt, dtype=np.float64)
        d = float(np.linalg.norm(tgt - pos))
        reach = boost if boosted else J
        if d > reach + 1e-9:
            max_steps = int(math.ceil(d / J)) * 4 + 8
            br = _bridge_greedy(
                fetch_stars_within, fetch_stars_toward, pos, boosted, tgt,
                J, dst_v, set(), max_steps, is_locked=is_locked,
                boost_mult=boost_mult,
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
            )
        )
        pos = tgt
        boosted = is_neut
        prev_gid = gid
    return legs, None


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

    cand = _corridor_candidates(index, sv, dv, corridor_ly, entry_r)
    if is_locked is not None and len(cand):
        names = index.names
        cand = cand[np.array(
            [not is_locked(names[int(c)]) for c in cand], dtype=bool
        )]
    blacklist: set[tuple[int, int]] = set()
    badzones: list[tuple[np.ndarray, float]] = []
    for _ in range(rounds):
        seq = _graph_search(
            index, cand, sv, dv, J, entry_r, gap_k, blacklist, badzones,
            deadline, boost_mult=boost_mult,
        )
        if seq is None:
            return None
        legs, fail = _realize_graph(
            index, src, dst, J, seq, fetch_stars_within,
            fetch_stars_near_segment, fetch_stars_toward, is_locked=is_locked,
            boost_mult=boost_mult,
        )
        if legs is not None:
            return _finalize(
                legs, src, dst, J, boost, straight, 0, start_time
            )
        edge, mid, seg_d = fail
        blacklist.add(edge)
        badzones.append(
            (mid, min(max(2.0 * J, 0.75 * seg_d), _ZONE_RADIUS_CAP))
        )
        if time.perf_counter() > deadline:
            return None
    return None


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
