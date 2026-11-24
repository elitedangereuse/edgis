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

import hashlib
import math
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

import numpy as np
from scipy.spatial import cKDTree

# Native (Rust/PyO3) A* kernel. ONE search_core runs every mode -- plain
# (no_fuel), B1 fuel (fixed reach), B2 range growth, each flip-honest -- and is the
# ONLY search implementation: the Python hot loops were removed once the port was
# parity-verified and prod-proven, so there is no silent fallback to mask a native
# regression or confuse a benchmark (the port made native == Python exact via a
# deterministic (dist,idx) gap order). The kernel is mandatory; a search called
# without it raises (see _require_native). EDGIS_NATIVE_ROUTE=0 disables the import
# for an environment that no longer routes.
import os as _os

_native_route = None
if _os.environ.get("EDGIS_NATIVE_ROUTE", "1") != "0":
    try:
        import edgis_route as _native_route  # type: ignore
    except Exception:  # pragma: no cover
        _native_route = None


def _require_native() -> None:
    """Fail loudly rather than silently degrade: the native kernel is the only
    search path now that the Python fallback is gone."""
    if _native_route is None:
        raise RuntimeError(
            "native edgis_route kernel unavailable: it is required for routing "
            "(the Python search fallback was removed). Build/install the "
            "edgis_route wheel."
        )

NeutronRow = tuple[float, float, float, int, str]
StarRow = tuple  # (x, y, z, name[, mainstar])
FetchNeutrons = Callable[[], Iterable[NeutronRow]]
FetchStarsWithin = Callable[
    [tuple[float, float, float], float], Iterable[StarRow]
]

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
        # Sorted id64 view for O(log n) system lookups (scoop_ls_for). A dict
        # of the 5M ids would cost hundreds of MB; two int64 arrays are 80 MB.
        self._id64_arr = np.asarray(id64, dtype=np.int64)
        self._id64_sorted = np.sort(self._id64_arr)
        self._id64_order = np.argsort(self._id64_arr)
        # Content version for route-cache keys. The old key used only the
        # COUNT, so a rebuild landing on the same count served stale routes;
        # digesting the id64s plus the companion/scoop columns busts exactly
        # when the routing data changes (and cache stays valid across
        # restarts, which a build timestamp would throw away).
        h = hashlib.blake2b(digest_size=8)
        h.update(self._id64_arr.tobytes())
        h.update(self.arrival_ls.tobytes())
        h.update(self.scoop_ls.tobytes())
        self.version = h.hexdigest()

    def scoop_ls_for(self, id64: int) -> float:
        """``scoop_ls`` of the system ``id64`` -- the PLANNER'S scoopable model
        (the ``scoopable_neutrons`` precompute folded into this index). ``+inf``
        when the system is not in the index or has no scoopable companion. The
        fuel enforcer reads THIS, never a live re-derivation: planner and
        enforcer must agree on what counts as an inline refuel, or marginal
        routes flip unsafe and the B2 guard discards good range-growth routes.
        """
        pos = int(np.searchsorted(self._id64_sorted, id64))
        if pos >= len(self._id64_sorted) or self._id64_sorted[pos] != id64:
            return float("inf")
        return float(self.scoop_ls[self._id64_order[pos]])

    # Snapshot format version; bump if the on-disk layout changes so a stale
    # snapshot is rejected rather than mis-read.
    _SNAPSHOT_FORMAT = 1

    def save(self, path: str) -> None:
        """Serialize the index to ``path`` (a .npz) so it can be reloaded in
        seconds instead of the ~3 min DB build. Stores the raw inputs (coords,
        id64, names, companion/scoop arrays); :meth:`load` rebuilds the KD-tree
        and sorted views, so the loaded index is identical to a freshly built
        one. Names go in as one newline-joined blob (ED system names contain no
        newline) to avoid object-array pickling."""
        names_blob = "\n".join(self.names).encode("utf-8")
        tmp = f"{path}.tmp-{_os.getpid()}"
        np.savez(
            tmp,
            fmt=np.asarray(self._SNAPSHOT_FORMAT, dtype=np.int64),
            coords=np.ascontiguousarray(self.coords, dtype=np.float64),
            id64=self._id64_arr,
            arrival=self.arrival_ls,
            scoop=self.scoop_ls,
            names_blob=np.frombuffer(names_blob, dtype=np.uint8),
        )
        # np.savez appends .npz; rename into place atomically so a reader never
        # sees a half-written file.
        _os.replace(f"{tmp}.npz", path)

    @classmethod
    def load(cls, path: str) -> "NeutronIndex":
        """Reconstruct an index from a :meth:`save` snapshot. Raises on a
        missing file, a format mismatch, or a corrupt archive (caller falls
        back to building from the DB)."""
        with np.load(path) as z:
            if int(z["fmt"]) != cls._SNAPSHOT_FORMAT:
                raise ValueError(
                    f"snapshot format {int(z['fmt'])} != {cls._SNAPSHOT_FORMAT}"
                )
            coords = np.ascontiguousarray(z["coords"], dtype=np.float64)
            id64 = z["id64"]
            arrival = z["arrival"]
            scoop = z["scoop"]
            names = z["names_blob"].tobytes().decode("utf-8").split("\n")
        if len(names) != len(id64):
            raise ValueError("snapshot names/id64 length mismatch")
        return cls(coords, id64, names, arrival_ls=arrival, scoop_ls=scoop)

    @classmethod
    def build(cls, fetch_neutron_rows: FetchNeutrons) -> "NeutronIndex":
        rows = list(fetch_neutron_rows())
        if not rows:
            raise ValueError("no neutron rows returned for index build")
        coords = np.asarray(
            [(r[0], r[1], r[2]) for r in rows], dtype=np.float64
        )
        # Optional 6th column = arrival_ls (companion neutrons); default 0.
        arrival = np.asarray(
            [
                float(r[5]) if len(r) > 5 and r[5] is not None else 0.0
                for r in rows
            ],
            dtype=np.float64,
        )
        # Optional 7th column = scoop_ls (nearest scoopable companion Ls); +inf
        # when absent (NULL) or the column isn't provided.
        scoop = np.asarray(
            [
                float(r[6]) if len(r) > 6 and r[6] is not None else np.inf
                for r in rows
            ],
            dtype=np.float64,
        )
        return cls(
            coords, [r[3] for r in rows], [r[4] for r in rows], arrival, scoop
        )


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
    frontier can't advance (void ahead), the caller then bridges.
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
    if (
        als is not None
        and len(als)
        and float(als.max()) > float(companion_max_ls)
    ):
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
            int(n)
            for n in knn
            if _dist(coords[int(n)], dst_v) < straight - 1e-6
            and (allowed is None or allowed[int(n)])
        ]
        entry_target = (
            tuple(coords[forward[0]])
            if forward
            else tuple(coords[int(knn[0])])
        )
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
                fetch_stars_within,
                tuple(pos),
                entry_target,
                jump_range,
                bridge_visited,
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
            index,
            starts,
            dst_v,
            jump_range,
            width,
            boost_mult,
            allowed=allowed,
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
                _leg(
                    "boost",
                    None,
                    dst,
                    _dist(np.asarray(pos), dst_v),
                    False,
                    0.0,
                )
            )
            break

        # Stuck: bridge toward the nearest forward neutron (to re-board and
        # resume boosting), falling back toward the destination.
        bridge_visited = set()
        nearest = tree.query(pos, k=min(200, index.count))[1]
        forward = [
            int(n)
            for n in np.atleast_1d(nearest)
            if _dist(coords[int(n)], dst_v)
            < _dist(np.asarray(pos), dst_v) - 1e-6
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
                    legs,
                    src,
                    dst,
                    jump_range,
                    boost,
                    straight,
                    width,
                    start_time,
                )
            onward = [
                n
                for n in tree.query_ball_point(pos, jump_range)
                if _dist(coords[n], dst_v)
                < _dist(np.asarray(pos), dst_v) - 1e-6
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
                fetch_stars_within,
                tuple(pos),
                target,
                jump_range,
                bridge_visited,
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
# Only the FUEL search uses flown-honest edge costs (unboosted tail / boosted
# entry) on flipped routes BELOW this straight-line distance. On short flipped
# routes the honest tail boards the highway near a sparse source (Blaa Eork ->
# Lave, 14 -> 9 jumps) with a small frontier; past it the fuel-resource state
# space explodes and times out (Colonia -> Sag A* ~26 kly, 30 -> 48 via beam
# fallback), so long flipped fuel routes keep the tight old-cost (native)
# search. The ship-agnostic search is never gated (no resource dimension, so
# no explosion -- it only benefits).
_FLIP_HONEST_FUEL_MAX_LY = 8000.0
# FSD synthesis injection grades (range multiplier), ascending so the bridger
# picks the LEAST that crosses a gap: basic +25%, standard +50%, premium +100%.
_INJECT_GRADES = (1.25, 1.5, 2.0)


def _is_neutron_star(mainstar) -> bool:
    return mainstar in ("N", "Neutron Star")


def _boosted_jumps(
    d: float, jump_range: float, boost_mult: float = 4.0
) -> int:
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
    (true-SRC -1 <-> search-DST -2). So the re-search blacklists the same edge.
    """

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
    flip: bool = False,
    heur_weight: float = 1.0,
) -> Optional[list[int]]:
    """Min-jumps A* over the corridor neutrons. Boost edges (<=``boost_mult``*J)
    cost 1; gap edges (fired only where no boost neighbor makes forward
    progress) and the tail/direct edges use the analytic supercharged cost.
    Returns the neutron sequence as GLOBAL index ids, or ``None``. ``blacklist``
    holds proven unbridgeable (from, to) edges (-1 = SRC, -2 = DST); ``badzones``
    are (center, radius) spheres around proven-starless segments.

    ``flip`` = the caller runs this search REVERSED (dst->src) and will reverse
    the sequence to realize. Supercharge is NOT direction-symmetric: a boost is
    available when the FLOWN jump leaves a neutron. Flipped, the search's tail
    edge (candidate u -> goal) is flown goal -> u leaving the PLAIN flown-source
    system, so it must be costed UNBOOSTED (ceil(d/J)); pricing it as one
    supercharged hop made the A* park ~a boost-reach short of the flown source
    and dump a 6-7 jump regular crawl on the realizer (Blaa Eork -> Lave,
    2026-11-21: 14 jumps vs spansh's 9, first neutron 56 ly away ignored).
    Symmetrically the search's entry edges ARE flown boosted arrivals (leaving
    neutron m into the flown destination), so flipped they cost the analytic
    supercharged bridge instead of an unboosted one.

    Native-only: the no_fuel search_core mode. An empty corridor returns ``[]``
    (a direct, neutron-free bridge the realizer handles), not ``None``.
    ``heur_weight`` > 1 runs weighted A* (used for the coarse seed)."""
    _require_native()
    n = len(cand)
    if not n:
        return []
    cc = index.coords[cand]
    return _native_graph_search_plain(
        index, cand, cc, sv, dv, jump_range, entry_r, gap_k,
        blacklist, badzones, deadline, boost_mult, flip,
        heur_weight=heur_weight,
    )


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
    flip: bool = False,
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
    :func:`_graph_search`), or ``None`` on timeout / no route.
    """

    _require_native()
    n = len(cand)
    if not n:
        return []
    cc = index.coords[cand]
    return _native_graph_search_fuel(
        index, cand, cc, sv, dv, jump_range, entry_r, gap_k,
        blacklist, badzones, deadline, int(tank_jumps), scoop_max_ls,
        boost_mult, flip,
    )


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
        if (sls is not None and len(cand))
        else np.zeros(len(cand), dtype=bool)
    )
    budget = max(0.0, float(deadline) - time.perf_counter())
    bz = [
        (float(c[0]), float(c[1]), float(c[2]), float(r)) for c, r in badzones
    ]
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


def _native_graph_search_fuel(
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
    tank_jumps: int,
    scoop_max_ls: float,
    boost_mult: float,
    flip: bool = False,
) -> Optional[list[int]]:
    """Marshal the B1 inputs and run the native fuel A* kernel (fixed reach +
    prefix-min). Same contract as :func:`_graph_search_fuel`: the GLOBAL-id neutron
    sequence, or ``None`` (timeout / no route). ``cc`` is the sliced coord block.
    ``flip`` applies the flip-honest entry/tail edge costs (crate >= 0.3.0)."""
    sls = getattr(index, "scoop_ls", None)
    scoopable = (
        (np.asarray(sls)[cand] <= scoop_max_ls)
        if (sls is not None and len(cand))
        else np.zeros(len(cand), dtype=bool)
    )
    budget = max(0.0, float(deadline) - time.perf_counter())
    bz = [
        (float(c[0]), float(c[1]), float(c[2]), float(r)) for c, r in badzones
    ]
    return _native_route.graph_search_fuel(
        np.ascontiguousarray(cc, dtype=np.float64),
        np.ascontiguousarray(scoopable).astype(np.uint8),
        np.ascontiguousarray(cand, dtype=np.int64),
        np.ascontiguousarray(sv, dtype=np.float64),
        np.ascontiguousarray(dv, dtype=np.float64),
        float(jump_range),
        float(entry_r),
        int(gap_k),
        int(tank_jumps),
        float(boost_mult),
        float(budget),
        [(int(a), int(b)) for (a, b) in blacklist],
        bz,
        bool(flip),
    )


def _native_graph_search_plain(
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
    boost_mult: float,
    flip: bool = False,
    heur_weight: float = 1.0,
) -> Optional[list[int]]:
    """Native plain ship-agnostic min-jumps: the no_fuel search_core mode (crate
    >= 0.3.1), a single resource slot. Same contract as :func:`_graph_search`.
    ``scoopable`` is unused in no_fuel mode -> a zero dummy; tank_jumps=1 so
    reach_by_r = [boost_mult*J]. ``heur_weight`` > 1 runs weighted A* (coarse seed)."""
    budget = max(0.0, float(deadline) - time.perf_counter())
    bz = [
        (float(c[0]), float(c[1]), float(c[2]), float(r)) for c, r in badzones
    ]
    return _native_route.graph_search_fuel(
        np.ascontiguousarray(cc, dtype=np.float64),
        np.zeros(len(cand), dtype=np.uint8),
        np.ascontiguousarray(cand, dtype=np.int64),
        np.ascontiguousarray(sv, dtype=np.float64),
        np.ascontiguousarray(dv, dtype=np.float64),
        float(jump_range),
        float(entry_r),
        int(gap_k),
        1,
        float(boost_mult),
        float(budget),
        [(int(a), int(b)) for (a, b) in blacklist],
        bz,
        bool(flip),
        True,
        float(heur_weight),
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

    Returns the neutron sequence as GLOBAL ids (the caller forward-simulates
    the per-node resource for the realizer, which is flip-safe), or ``None``
    (timeout / no route).
    """

    _require_native()
    n = len(cand)
    if not n:
        return []
    cc = index.coords[cand]
    return _native_graph_search_range(
        index, cand, cc, sv, dv, jump_range, entry_r, gap_k,
        blacklist, badzones, deadline, np.asarray(reach_by_r, dtype=np.float64),
        scoop_max_ls, boost_mult,
    )


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
    only applies to regular jumps (a boosted jump already reaches ``boost``).
    """

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
            if is_locked is not None and is_locked(
                row[3],
                id64=(row[5] if len(row) > 5 else None),
                coords=(row[0], row[1], row[2]),
            ):
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
                best[3],
                tuple(nxt),
                float(np.linalg.norm(nxt - cur)),
                _is_neutron_star(ms),
                float(np.linalg.norm(nxt - dst_v)),
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
            rows = [
                r
                for r in rows
                if not is_locked(
                    r[3],
                    id64=(r[5] if len(r) > 5 else None),
                    coords=(r[0], r[1], r[2]),
                )
            ]
        if not rows:
            continue
        pts = np.asarray([(r[0], r[1], r[2]) for r in rows])
        neut = np.asarray(
            [_is_neutron_star(r[4] if len(r) > 4 else None) for r in rows]
        )
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
        max_layers = (
            int(math.ceil(float(np.linalg.norm(target - pos)) / J)) + 12
        )
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
            for grp, rad in (
                (frontier[neut[frontier]], boost),
                (frontier[~neut[frontier]], J),
            ):
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
                    rows[m][3],
                    tuple(nxt_p),
                    float(np.linalg.norm(nxt_p - cur)),
                    bool(neut[m]),
                    float(np.linalg.norm(nxt_p - dst_v)),
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
    rbr = (
        None
        if reach_by_r is None
        else np.asarray(reach_by_r, dtype=np.float64)
    )
    coords = index.coords
    names = index.names
    id64 = index.id64
    dst_v = np.asarray(dst, dtype=np.float64)
    legs: list[dict] = []
    pos = np.asarray(src, dtype=np.float64)
    boosted = False
    prev_gid = -1
    targets = [(coords[n], names[n], id64[n], True, int(n)) for n in seq] + [
        (dst_v, None, None, False, -2)
    ]
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
                fetch_stars_within,
                fetch_stars_toward,
                pos,
                boosted,
                tgt,
                J,
                dst_v,
                set(),
                max_steps,
                is_locked=is_locked,
                boost_mult=boost_mult,
                inject_mult=1.0,
            )
            if fetch_stars_near_segment is not None:
                # Quality guard: greedy can SUCCEED badly, wandering around an
                # obstacle (e.g. the empty permit sphere at HEN 2-333) in many
                # small steps. When it clearly exceeds the analytic estimate,
                # ask the capsule min-jumps BFS too and keep the shorter.
                analytic = (
                    _boosted_jumps(d, J, boost_mult)
                    if boosted
                    else max(1, int(math.ceil(d / J)))
                )
                if br is None or len(br[0]) + 1 > analytic + max(
                    2, int(0.25 * analytic)
                ):
                    alt = _bridge_capsule(
                        fetch_stars_near_segment,
                        pos,
                        boosted,
                        tgt,
                        J,
                        dst_v,
                        is_locked=is_locked,
                        boost_mult=boost_mult,
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
                    fetch_stars_within,
                    fetch_stars_toward,
                    pos,
                    boosted,
                    tgt,
                    J,
                    dst_v,
                    set(),
                    max_steps,
                    is_locked=is_locked,
                    boost_mult=boost_mult,
                    inject_mult=inject_mult,
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
                name,
                tuple(tgt),
                d,
                is_neut,
                float(np.linalg.norm(tgt - dst_v)),
                id64=idv,
                spectral="N" if is_neut else None,
                arrival_ls=(
                    float(index.arrival_ls[gid])
                    if is_neut and gid >= 0
                    else None
                ),
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
#
# Env-overridable (EDGIS_B2_THIN_CORRIDOR_LY) to sweep width on staging without a
# redeploy: the 2026-11-22 beam-probe diagnosis showed beam's winning waypoints ride
# an OFF-AXIS neutron band (~540 ly off the geodesic on Colonia->Sol, ~940 on
# Sol->Beagle) that this near-geodesic tube excludes -- 61-84% of beam's waypoints
# fall outside the 120 ly corridor. Widening is regression-safe behind the B2
# min-guard (a slower/over-range wide search just falls back to the B1 baseline), so
# the width can be swept empirically to find where the gap to beam closes vs where
# the candidate set drowns the flown search.
_B2_THIN_CORRIDOR_LY = float(_os.environ.get("EDGIS_B2_THIN_CORRIDOR_LY") or "120")

# SCOOPABLE-ONLY widening of the B2 thin corridor (ly). Scoopable-companion
# neutrons are rare (~4.6% of the index), so the 120 ly tube regularly has
# scoopless stretches far longer than a small tank's refuel cadence (measured
# cutter Sol->Colonia: longest scoopless stretch ~2050 ly vs the ~430 ly a
# 2-jump tank needs) -- the fuel search then CANNOT thread in-path refuels and
# the enforcer falls back to out-and-back scoop diverts (+2 jumps each; 23 of
# them = the whole +46 gap to spansh on that route, whose scoopable landings sit
# a median 222 ly off our tube). Widening the corridor for SCOOPABLE neutrons
# only restores threading coverage (500 ly: 98% of the route has one in reach,
# longest gap ~400 ly) while adding few candidates (the 4.6% density keeps the
# union tractable). 0 disables.
_SCOOP_CORRIDOR_LY = float(_os.environ.get("EDGIS_SCOOP_CORRIDOR_LY") or "500")
# Route-length band the scoopable widening engages in. Below the floor the route
# is too short to have scoopless stretches worth threading (widening only adds
# candidates + time: Riedquat->Asterope 22->23 at 82 s). Above the ceiling is the
# rim regime -- the B2 corridor already doubles there, and the extra off-axis
# scoopable candidates drown / misdirect the flown search (Sol->Beagle 226->255).
# Between the two is where in-path refuel on tank-limited ships pays off.
_SCOOP_MIN_ROUTE_LY = float(_os.environ.get("EDGIS_SCOOP_MIN_ROUTE_LY") or "5000")
_SCOOP_MAX_ROUTE_LY = float(_os.environ.get("EDGIS_SCOOP_MAX_ROUTE_LY") or "30000")

# Experimental fullgraph engine (cookie contraction=true): path to the
# precomputed full-galaxy sparse neutron graph built by
# feeders/build_sparse_neutron_graph.py. Unset = engine disabled (prod default).
# The graph MUST be built from the exact snapshot this service loads (graph row
# i == index row i); the loader enforces the row-count match and disables itself
# on any mismatch so a stale graph can never produce garbage routes.
_SPARSE_GRAPH_PATH = _os.environ.get("EDGIS_SPARSE_GRAPH") or ""
_sparse_graph_cache: object = None


def _load_sparse_graph(index) -> Optional[tuple]:
    """Lazy-load the sparse CSR graph (indptr, idx, dist_dly). None = disabled
    or failed (sticky: one warning, then permanently off for the process)."""
    global _sparse_graph_cache
    if not _SPARSE_GRAPH_PATH:
        return None
    if _sparse_graph_cache is False:
        return None
    if _sparse_graph_cache is not None:
        return _sparse_graph_cache
    try:
        g = np.load(_SPARSE_GRAPH_PATH)
        if int(g["fmt"]) != 3:
            raise ValueError(f"unsupported sparse graph fmt {int(g['fmt'])}")
        indptr = g["indptr"]
        if len(indptr) - 1 != len(index.coords):
            raise ValueError(
                f"sparse graph rows {len(indptr) - 1} != index rows "
                f"{len(index.coords)} (rebuild from this snapshot)"
            )
        # Hold the arrays for the process lifetime: the native kernel caches its
        # own copy keyed by the indptr data pointer, so a stable Python-side
        # reference means one copy, ever.
        _sparse_graph_cache = (indptr, g["idx"], g["dist_dly"])
    except Exception:
        import logging

        logging.getLogger(__name__).warning(
            "sparse graph unavailable; fullgraph engine disabled", exc_info=True
        )
        _sparse_graph_cache = False
        return None
    return _sparse_graph_cache


# Max wall-clock the B2 flown search may consume before giving up and realizing the
# B1 coarse path instead. Keeps default-on B2 from spending a long/rim route's whole
# time budget on a flown search that won't finish.
_B2_TIME_BUDGET = 60.0

# Max straight-line distance (ly) for which B2 range growth engages. The old
# 30 kly gate excluded the deep rim (Beagle) on the assumption the win was
# marginal there -- the 2026-11-21 beam probe showed the opposite: range growth
# is worth the MOST on the longest routes (spansh beam's whole Sol->Beagle edge
# is whole-route range growth; our pure-B1 Beagle trails it by ~20 jumps).
# Lifted 2026-11-22; env-overridable back down if a rim pathology appears.
# The guards that made default-on B2 safe are unchanged (per-route time budget
# below, coarse-path fallback, the caller's fuel-safety B1 re-solve).
_B2_MAX_LY = float(_os.environ.get("EDGIS_B2_MAX_LY") or "100000")

# Straight-line distance (ly) below which the B2 branch also realizes the B1
# coarse path and keeps the fewer-jumps of the two. Range growth's benefit scales
# with route length, so short routes are where B2 can LOSE to B1 (cobra 4x
# 76 Sigma Ceti->V2079: B2 18 vs B1 14); longer routes reliably favour B2 and
# realizing their long B1 path would roughly double realization time for nothing.
_B2_BASELINE_MAX_LY = float(_os.environ.get("EDGIS_B2_BASELINE_MAX_LY") or "8000")

# Weighted-A* multiplier for the B2 COARSE seed. The coarse only seeds the thin
# corridor, so a path within ~W x optimal is fine; inflating the heuristic collapses
# the frontier on dense full corridors (174k pops -> 34k, coarse 41s -> ~7s) for ~+1
# jump the fuel-aware final search over the thin corridor absorbs (fuel bench: net
# -1 jump, 0 regressions vs baselines). 1.0 = admissible/optimal (slow on dense).
# Tunable; a thorough per-ship/per-terrain sweep is the follow-up to pick the value.
_COARSE_WEIGHT = float(_os.environ.get("EDGIS_COARSE_WEIGHT") or "1.1")

# Local crawl repair: the thin B2 corridor (120 ly) blinds the search to off-axis
# neutrons, so a rim desert realizes as a long REGULAR crawl (a bridge across a
# wide gap between two sequence neutrons) where a slightly-off-geodesic neutron
# chain would boost across in a few hops (Beagle legs 87-95: 9 regular jumps, yet
# 48 neutrons sit within 500 ly of that segment, only 8 within 250). After a
# search realizes, re-search a WIDE local bubble across each wide gap, splice any
# strictly-shorter chain into the sequence, and re-realize -- keeping the result
# only when it has fewer jumps (min-guard, so it can never regress). The local
# bubble is a few hundred neutrons (vs the 100k-480k of a global wide corridor,
# which drowns in Python -- the A1 spike), so it stays tractable.
_CRAWL_REPAIR = _os.environ.get("EDGIS_CRAWL_REPAIR", "1") != "0"
# Minimum bridge jumps a gap must cost before it's worth a local re-search.
_CRAWL_MIN_BRIDGE = int(_os.environ.get("EDGIS_CRAWL_MIN_BRIDGE") or "3")
# Radius (ly) of the local bubble around a crawl segment -- wide enough to catch
# the off-axis highway the thin corridor missed.
_CRAWL_BUBBLE_LY = float(_os.environ.get("EDGIS_CRAWL_BUBBLE_LY") or "500")
# Most gaps to repair per route (widest first) and the repair time sub-budget.
_CRAWL_MAX_GAPS = int(_os.environ.get("EDGIS_CRAWL_MAX_GAPS") or "3")
_CRAWL_TIME_BUDGET = float(_os.environ.get("EDGIS_CRAWL_TIME_BUDGET") or "25")
# Only run repair on long B2 routes: rim crawls appear past ~this distance, and
# gating out short routes keeps the local searches + one re-realize from stealing
# the solve budget on routes that have no deserts (bench: a short route spent 99 s
# and a baseline realize timed out of its budget -> a +1 jump regression).
_CRAWL_MIN_ROUTE_LY = float(_os.environ.get("EDGIS_CRAWL_MIN_ROUTE_LY") or "12000")


def _thin_corridor(
    index: NeutronIndex, seq_ids: list[int], radius: float
) -> np.ndarray:
    """Global ids of every neutron within ``radius`` ly of any neutron in
    ``seq_ids`` (the B1 coarse path): the bounded candidate set the B2 flown
    search runs over, so the sparse-goal direction stays tractable."""
    coords = index.coords
    hit: set[int] = set()
    for gid in seq_ids:
        for m in index.tree.query_ball_point(coords[gid], radius):
            hit.add(int(m))
    return np.array(sorted(hit), dtype=np.int64)


def _repair_gaps(
    index: NeutronIndex,
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    jump_range: float,
    rseq: list[int],
    seq_r: Optional[list[int]],
    base_legs: list[dict],
    gap_k: int,
    boost_mult: float,
    scoop_max_ls: float,
    reach_by_r: Optional[np.ndarray],
    sls_arr: Optional[np.ndarray],
    range_search: bool,
    filt: Callable[[np.ndarray], np.ndarray],
    fetch_stars_within: FetchStarsWithin,
    fetch_stars_near_segment: Optional[FetchStarsNearSegment],
    fetch_stars_toward: Optional[FetchStarsToward],
    is_locked: Optional[Callable[[str], bool]],
    inject_mult: float,
    deadline: float,
) -> list[dict]:
    """Shorten rim-desert crawls. For each wide gap between consecutive sequence
    neutrons (one that realizes as >= ``_CRAWL_MIN_BRIDGE`` regular bridge jumps),
    re-search a wide LOCAL bubble for the off-axis neutron chain the thin B2
    corridor missed; splice a chain that crosses the gap in fewer hops and
    re-realize the whole route. Returns the shortest valid realization found,
    never longer than ``base_legs`` (min-guard). Detection is O(len(seq)) and the
    local search fires only on genuine gaps, so no-crawl routes pay ~nothing.

    Scoped to the B2 range search on long routes: rim deserts only appear at that
    scale, and the wins are there (Beagle, bishan-praae). Running it on every
    orientation/baseline solve instead stole the budget and regressed a short
    route, so short + non-range solves skip it entirely."""
    if not _CRAWL_REPAIR or not range_search or len(rseq) < 2:
        return base_legs
    straight_route = float(
        np.linalg.norm(np.asarray(dst, dtype=np.float64)
                       - np.asarray(src, dtype=np.float64))
    )
    if straight_route < _CRAWL_MIN_ROUTE_LY:
        return base_legs
    J = jump_range
    boost = boost_mult * J
    coords = index.coords

    def _reach_at(k: int) -> float:
        # Boost reach off sequence neutron k: the range-grown reach when B2 is
        # flying (matches the realizer), else the fixed full-tank boost.
        if range_search and reach_by_r is not None and seq_r is not None:
            return float(reach_by_r[min(seq_r[k], len(reach_by_r) - 1)])
        return boost

    gaps: list[tuple[int, int]] = []  # (estimated bridge jumps, seq index k)
    for k in range(len(rseq) - 1):
        d = float(np.linalg.norm(coords[rseq[k + 1]] - coords[rseq[k]]))
        reach = _reach_at(k)
        if d <= reach + 1e-9:
            continue
        est = 1 + int(math.ceil((d - reach) / J))
        if est >= _CRAWL_MIN_BRIDGE:
            gaps.append((est, k))
    if not gaps:
        return base_legs
    gaps.sort(reverse=True)  # widest gaps first

    def _resim(seq: list[int]) -> Optional[list[int]]:
        # Rebuild the B2 per-neutron resource (jumps since last refuel) for a
        # spliced sequence, mirroring the forward-sim in _solve.
        if not (range_search and reach_by_r is not None and sls_arr is not None):
            return None
        T = len(reach_by_r)
        out: list[int] = []
        for i, gid in enumerate(seq):
            if i == 0:
                r = 0
            else:
                r = out[i - 1] + 1
                if sls_arr[gid] <= scoop_max_ls or r >= T:
                    r = 0
            out.append(r)
        return out

    # The local search models boost edges at the MAXIMUM range-grown reach (not
    # the full-tank boost): a desert is crossed with a draining tank, so the
    # realizable off-axis hops are the long late-cycle ones. Edges the actual
    # resource state can't afford get bridged by the authoritative re-realize and
    # the min-guard drops them, so an optimistic reach here only widens the search.
    reach_cap = (
        float(reach_by_r.max())
        if (range_search and reach_by_r is not None)
        else boost
    )
    local_boost_mult = max(boost_mult, reach_cap / J)

    def _local_interior(a: int, b: int) -> list[int]:
        """Neutron chain strictly between neutrons a and b from a wide bubble."""
        av = np.asarray(coords[a], dtype=np.float64)
        bv = np.asarray(coords[b], dtype=np.float64)
        cand = filt(
            _corridor_candidates(index, av, bv, _CRAWL_BUBBLE_LY, reach_cap)
        )
        if not len(cand):
            return []
        seq = _graph_search(
            index, cand, av, bv, J, reach_cap, gap_k, set(), [], deadline,
            boost_mult=local_boost_mult,
        )
        if not seq:
            return []
        s = list(seq)
        while s and s[0] == a:
            s.pop(0)
        while s and s[-1] == b:
            s.pop()
        return [g for g in s if g != a and g != b]

    # Splice the widest gaps one at a time, re-realizing and KEEPING each only if
    # it actually shortens the route (min-guard). Per-gap validation matters:
    # splicing neutrons shifts the downstream refuel cadence (seq_r), so a combined
    # all-at-once splice can admit a chain that doesn't realize as planned and
    # cancels a good one (bench: all-at-once lost the Beagle win a cumulative pass
    # kept). The B2-long-haul + one-pass gating above bounds this to <= _CRAWL_MAX_
    # GAPS re-realizes within the repair sub-budget, so it can't starve the solve.
    cur_rseq = list(rseq)
    cur_legs = base_legs
    repair_dl = min(deadline, time.perf_counter() + _CRAWL_TIME_BUDGET)
    for est, k0 in gaps[:_CRAWL_MAX_GAPS]:
        if time.perf_counter() > repair_dl:
            break
        a, b = rseq[k0], rseq[k0 + 1]
        try:
            ka = cur_rseq.index(a)  # may have shifted after an earlier splice
        except ValueError:
            continue
        if ka + 1 >= len(cur_rseq) or cur_rseq[ka + 1] != b:
            continue
        interior = _local_interior(a, b)
        if not interior or len(interior) + 1 >= est:
            continue
        trial = cur_rseq[: ka + 1] + interior + cur_rseq[ka + 1 :]
        legs2, _f = _realize_graph(
            index,
            src,
            dst,
            J,
            trial,
            fetch_stars_within,
            fetch_stars_near_segment,
            fetch_stars_toward,
            is_locked=is_locked,
            boost_mult=boost_mult,
            inject_mult=inject_mult,
            seq_r=_resim(trial),
            reach_by_r=reach_by_r,
        )
        if legs2 is not None and len(legs2) < len(cur_legs):
            cur_rseq = trial
            cur_legs = legs2
    return cur_legs


def _fullgraph_solve(
    index: NeutronIndex,
    src: tuple[float, float, float],
    dst: tuple[float, float, float],
    J: float,
    boost_mult: float,
    deadline: float,
    start_time: float,
    straight: float,
    fetch_stars_within: FetchStarsWithin,
    fetch_stars_near_segment,
    fetch_stars_toward,
    is_locked,
    inject_mult: float,
    companion_max_ls: float,
    filter_fn,
) -> Optional[dict]:
    """Experimental full-galaxy engine (cookie contraction=true): native A* over
    the precomputed sparse neutron graph, NO corridor. The off-axis neutrons the
    corridor cannot reach (optimal low-range paths ride a median ~1150 ly off
    the geodesic on Colonia->Sol) are first-class here. Min-jumps only for now:
    the fuel layer still reports/enforces on the realized legs.

    Companion gating happens IN the search (the ``allowed`` node mask): the
    graph carries every neutron incl. ~1.1M companions, so a post-hoc check
    would kill nearly every route when the request's supercruise cap excludes
    them. Permits stay a post-check (rare, and a locked backbone just falls
    through to the production pipeline, which handles permits).

    Returns a finalized result dict, or None to fall through to the production
    pipeline: graph missing/mismatched, boarding candidates all filtered,
    permit violation on the backbone, unrealizable edge, or timeout. Falling
    through is always safe -- this engine can only ever ADD a better route,
    never replace the proven one with a failure. EDGIS_FGX_DBG=1 logs every
    fall-through reason (they are silent by design otherwise)."""

    def _miss(reason: str) -> None:
        if _os.environ.get("EDGIS_FGX_DBG") == "1":
            import logging

            logging.getLogger(__name__).warning("FGX miss: %s", reason)
        return None

    g = _load_sparse_graph(index)
    if g is None or _native_route is None:
        return _miss("graph or native kernel unavailable")
    indptr, idx, dly = g
    boost = boost_mult * J
    sv = np.asarray(src, dtype=np.float64)
    dv = np.asarray(dst, dtype=np.float64)
    k = min(8, len(index.coords))
    sd, si = index.tree.query(sv, k=k)
    dd, di = index.tree.query(dv, k=k)
    sd, si = np.atleast_1d(sd), np.atleast_1d(si).astype(np.int64)
    dd, di = np.atleast_1d(dd), np.atleast_1d(di).astype(np.int64)
    # Boarding obeys the same companion/permit filter as the corridor engine.
    # Entry is an unboosted chain from the source system; the exit hop departs a
    # neutron, so it prices boosted.
    sm = np.isin(si, filter_fn(si))
    dm = np.isin(di, filter_fn(di))
    srcs = [
        (int(i), 0.0 if d < 1e-6 else float(math.ceil(d / J)))
        for i, d in zip(si[sm], sd[sm])
    ]
    dsts = [
        (int(i), 0.0 if d < 1e-6 else float(_boosted_jumps(d, J, boost_mult)))
        for i, d in zip(di[dm], dd[dm])
    ]
    if not srcs or not dsts:
        return _miss("no boarding candidates after filtering")
    budget = min(deadline - time.perf_counter(), 30.0)
    if budget <= 1.0:
        return _miss("no time budget left")
    # Companion mask for the search itself (vectorized; permits are handled by
    # the post-check below). None when every node is usable.
    allowed = None
    als = getattr(index, "arrival_ls", None)
    if als is not None and len(als):
        allowed = np.ascontiguousarray(
            (np.asarray(als) <= float(companion_max_ls)).astype(np.uint8)
        )
    try:
        seq, _cost, _pops = _native_route.csr_search(
            indptr, idx, dly, index.coords, srcs, dsts,
            float(J), float(boost_mult), float(budget),
            allowed=allowed,
        )
    except Exception:
        import logging

        logging.getLogger(__name__).warning(
            "fullgraph csr_search failed; falling through", exc_info=True
        )
        return None
    if not seq:
        return _miss("csr_search found no route in budget")
    arr = np.asarray(seq, dtype=np.int64)
    if len(filter_fn(arr)) != len(arr):
        return _miss("backbone hits a permit-locked node")
    legs, fail = _realize_graph(
        index,
        src,
        dst,
        J,
        [int(x) for x in seq],
        fetch_stars_within,
        fetch_stars_near_segment,
        fetch_stars_toward,
        is_locked=is_locked,
        boost_mult=boost_mult,
        inject_mult=inject_mult,
    )
    if legs is None:
        return _miss(f"unrealizable edge {fail[0]} (seg {fail[2]:.0f} ly)")
    res = _finalize(legs, src, dst, J, boost, straight, 0, start_time)
    res["engine"] = "fullgraph-exp"
    return res


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
    fullgraph: bool = False,
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
            idl = index.id64
            cc = index.coords
            c = c[
                np.array(
                    [
                        not is_locked(
                            nm[int(x)], id64=idl[int(x)], coords=cc[int(x)]
                        )
                        for x in c
                    ],
                    dtype=bool,
                )
            ]
        return c

    cand = _filter(cand)

    # Experimental full-galaxy engine (cookie contraction=true): try the sparse
    # fullgraph A* first; ANY miss falls through to the production pipeline, so
    # the flag can only improve a route, never lose one.
    if fullgraph:
        res = _fullgraph_solve(
            index,
            src,
            dst,
            J,
            boost_mult,
            deadline,
            start_time,
            straight,
            fetch_stars_within,
            fetch_stars_near_segment,
            fetch_stars_toward,
            is_locked,
            inject_mult,
            companion_max_ls,
            _filter,
        )
        if res is not None:
            return res

    def _solve(
        flip: bool,
        cand_set: Optional[np.ndarray] = None,
        range_search: bool = False,
        dl: Optional[float] = None,
    ) -> Optional[dict]:
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
        # Flown-honest edge costs (unboosted tail / boosted entry) widen the A*
        # frontier near the goal -- the win that boards the highway close to a
        # sparse source (Blaa Eork -> Lave, 14 -> 9). In the FUEL search that
        # frontier is multiplied by the fuel-resource dimension, and on a long
        # dense flipped corridor (Colonia -> Sag A*, ~26 kly) it explodes past
        # the time budget and falls to the beam engine (30 -> 48). The plain
        # search has no resource dimension and stays fast (GET bench: Colonia ->
        # Sag A* 68 -> 65, no timeout), so only the fuel search is length-gated;
        # the native kernel (still old-cost) runs whenever honest costs are off.
        honest_fuel = flip and straight < _FLIP_HONEST_FUEL_MAX_LY
        blacklist: set[tuple[int, int]] = set()
        badzones: list[tuple[np.ndarray, float]] = []
        for _ in range(rounds):
            if range_search:
                seq = _graph_search_range(
                    index,
                    cset,
                    ssv,
                    sdv,
                    J,
                    entry_r,
                    gap_k,
                    blacklist,
                    badzones,
                    d,
                    _rbr,
                    float(scoop_max_ls),
                    boost_mult=boost_mult,
                )
            elif use_fuel:
                # B1: fuel-aware threading at the fixed full-tank reach; fall back
                # to the ship-agnostic min-jumps search so we never regress.
                seq = _graph_search_fuel(
                    index,
                    cset,
                    ssv,
                    sdv,
                    J,
                    entry_r,
                    gap_k,
                    blacklist,
                    badzones,
                    d,
                    int(tank_jumps),
                    float(scoop_max_ls),
                    boost_mult=boost_mult,
                    flip=honest_fuel,
                )
                if seq is None:
                    seq = _graph_search(
                        index,
                        cset,
                        ssv,
                        sdv,
                        J,
                        entry_r,
                        gap_k,
                        blacklist,
                        badzones,
                        d,
                        boost_mult=boost_mult,
                        flip=flip,
                    )
            else:
                seq = _graph_search(
                    index,
                    cset,
                    ssv,
                    sdv,
                    J,
                    entry_r,
                    gap_k,
                    blacklist,
                    badzones,
                    d,
                    boost_mult=boost_mult,
                    flip=flip,
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
                index,
                src,
                dst,
                J,
                rseq,
                fetch_stars_within,
                fetch_stars_near_segment,
                fetch_stars_toward,
                is_locked=is_locked,
                boost_mult=boost_mult,
                inject_mult=inject_mult,
                seq_r=seq_r,
                reach_by_r=(_rbr if range_search else None),
            )
            if legs is not None:
                # Local crawl repair: re-search wide bubbles across any rim-desert
                # gaps the thin corridor left as long regular crawls, keeping only
                # a strictly-shorter re-realization (min-guard).
                legs = _repair_gaps(
                    index,
                    src,
                    dst,
                    J,
                    rseq,
                    seq_r,
                    legs,
                    gap_k,
                    boost_mult,
                    float(scoop_max_ls),
                    _rbr,
                    _sls_arr,
                    range_search,
                    _filter,
                    fetch_stars_within,
                    fetch_stars_near_segment,
                    fetch_stars_toward,
                    is_locked,
                    inject_mult,
                    d,
                )
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
        # Kill-switch for A/B-ing the orientation flip's real benefit now that the
        # Rust B1 kernel independently speeds these searches. EDGIS_ROUTE_FLIP=0
        # disables it everywhere.
        _os.environ.get("EDGIS_ROUTE_FLIP", "1") != "0"
        and straight < _ORIENT_MAX_LY
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
        # The B2 coarse is a fuel search too, so its honest-cost toggle is
        # length-gated like the final one (see _solve). When honest costs are
        # on (short flipped route) cap its budget so a slow wide-frontier coarse
        # can't starve the B1 fallback chain.
        coarse_honest = flip and straight < _FLIP_HONEST_FUEL_MAX_LY
        coarse_dl = deadline
        if coarse_honest:
            coarse_dl = min(
                deadline,
                time.perf_counter()
                + 0.35 * max(0.0, deadline - time.perf_counter()),
            )
        # The coarse only SEEDS the thin corridor; the real fuel-aware search runs on
        # it afterward. So the coarse is a PLAIN, WEIGHTED min-jumps search, not a full
        # fuel-aware B1:
        #   - PLAIN (no fuel resource dimension) drops the (n x tank_jumps) best/par
        #     arrays to (n x 1) -- ~60x less memory on a dense full corridor (the
        #     ~200MB that tripped the native-only 429 shed) -- and skips the prefix-min
        #     dominance scan, for the same geometric spine.
        #   - WEIGHTED (_COARSE_WEIGHT) inflates the heuristic so the A* explores a
        #     narrow frontier: the admissible (W=1) search expands ~40% of a dense
        #     corridor (174k pops, 41s) for a seed; W=1.1 collapses it to ~34k pops /
        #     ~7s. The ~+1 jump this can cost the seed is absorbed by the fuel-aware
        #     final search over the thin corridor (fuel bench: net -1, 0 regressions).
        # Together these make the native coarse FASTER than the old scipy coarse with
        # no memory blowup -- the two-phase no longer needs a Python coarse.
        coarse = _graph_search(
            index,
            cand,
            (dv if flip else sv),
            (sv if flip else dv),
            J,
            entry_r,
            gap_k,
            set(),
            [],
            coarse_dl,
            boost_mult=boost_mult,
            flip=coarse_honest,
            heur_weight=_COARSE_WEIGHT,
        )
        if coarse:
            b1seq = list(reversed(coarse)) if flip else coarse

            def _realize_coarse():
                """Realize the B1 coarse path at fixed reach (no range growth)."""
                legs, _f = _realize_graph(
                    index,
                    src,
                    dst,
                    J,
                    b1seq,
                    fetch_stars_within,
                    fetch_stars_near_segment,
                    fetch_stars_toward,
                    is_locked=is_locked,
                    boost_mult=boost_mult,
                    inject_mult=inject_mult,
                )
                if legs is None:
                    return None
                return _finalize(legs, src, dst, J, boost, straight, 0, start_time)

            # Range growth (B2) chains FEWER, longer hops late in a tank -- a win
            # that GROWS with route length (more refuel cycles). On SHORT routes
            # with a small tank it can instead LOSE neutron boosts to the
            # thin-corridor approximation and ship MORE jumps than plain B1 (a
            # cobra mkV at 4x plotted 18 via B2 vs 14 via B1). So on short routes
            # realize the B1 coarse as a baseline and keep whichever plan has
            # fewer jumps. In the PROVEN band (8-30 kly) B2 reliably wins, so
            # trust it (realizing the long B1 path would double realization time
            # for nothing). Beyond 30 kly -- newly ungated -- B2 is NOT yet
            # proven (first Beagle test shipped 196 vs B1's 176), so realize the
            # baseline there too and ship the min: the gate lift can help but
            # never hurt.
            compare_b1 = (
                straight < _B2_BASELINE_MAX_LY or straight > 30000.0
            )
            b1_res = _realize_coarse() if compare_b1 else None

            b2_res = None
            # >30 kly: double the corridor. The 120 ly tube was tuned on
            # Colonia-scale routes; at rim scale the range-growth hop pattern
            # (long, straight, late-tank hops) wants candidates the thin tube
            # around a zigzagging fixed-reach coarse path doesn't contain
            # (first Beagle flown search LOST to its own coarse, 196 vs 176).
            # The min-guard above makes a wider (slower) search safe to try.
            b2_corridor = (
                _B2_THIN_CORRIDOR_LY if straight <= 30000.0
                else 2.0 * _B2_THIN_CORRIDOR_LY
            )
            tc_ids = _thin_corridor(index, b1seq, b2_corridor)
            # IN-PATH REFUEL candidates: union a WIDER, scoopable-only corridor so
            # the flown fuel search can thread refuel landings even where the thin
            # tube is scoopless (see _SCOOP_CORRIDOR_LY). The search's dry-hop cost
            # (+2 divert folded in) already prefers threading when it saves jumps;
            # it just never had the candidates before.
            n_wide = 0
            if (_SCOOP_CORRIDOR_LY > b2_corridor and _sls_arr is not None
                    and _SCOOP_MIN_ROUTE_LY < straight <= _SCOOP_MAX_ROUTE_LY):
                wide = _thin_corridor(index, b1seq, _SCOOP_CORRIDOR_LY)
                wide = wide[_sls_arr[wide] <= float(scoop_max_ls)]
                n_wide = len(wide)
                if len(wide):
                    tc_ids = np.union1d(tc_ids, wide)
            tc = _filter(tc_ids)
            if _os.environ.get("EDGIS_B2_DBG") == "1":
                import logging
                logging.getLogger(__name__).warning(
                    "B2DBG coarse=%d tc=%d (wide-scoopable +%d) t=%.1fs",
                    len(b1seq), len(tc), n_wide,
                    time.perf_counter() - start_time,
                )
            if len(tc) and time.perf_counter() < deadline:
                # Bound the flown search so a slow one (long/dense route) can't
                # eat the whole deadline and starve the fallback below.
                # Budget scales with route length now that B2 engages past
                # 30 kly: the fixed 60 s was sized for <=30 kly corridors; a
                # 65 kly Beagle corridor is ~2x the candidates. The overall
                # `deadline` (base_cap = 60 + dist/500) still bounds everything,
                # and a budget miss falls back to realizing the B1 coarse path.
                b2_budget = max(_B2_TIME_BUDGET, straight / 600.0)
                b2_dl = min(deadline, time.perf_counter() + b2_budget)
                b2_res = _solve(False, cand_set=tc, range_search=True, dl=b2_dl)

            if _os.environ.get("EDGIS_B2_DBG") == "1":
                import logging
                logging.getLogger(__name__).warning(
                    "B2DBG b2=%s b1=%s t=%.1fs",
                    None if b2_res is None else b2_res.get("total_jumps"),
                    None if b1_res is None else b1_res.get("total_jumps"),
                    time.perf_counter() - start_time,
                )
            # Keep the fewer-jumps realized route (B2 only when it actually wins).
            cands = [r for r in (b1_res, b2_res) if r is not None]
            if cands:
                return min(cands, key=lambda r: r["total_jumps"])
            # B2 missed and we skipped the baseline (long route): realize the
            # coarse B1 path directly rather than waste a second full B1 search.
            b1_res = _realize_coarse()
            if b1_res is not None:
                return b1_res

    if flip:
        # Flipped fuel searches run on Python (the native kernel lacks the
        # flip-aware edge costs) and the honest unboosted tail widens the
        # frontier near the goal; on dense-core corridors (e.g. Colonia ->
        # Sag A*) a flipped attempt can eat the whole budget and leave the
        # proven unflipped retry nothing (the route then falls to the beam
        # engine: 30 -> 48 jumps on the fuel bench). Cap the flipped attempt
        # at just over half the remaining budget so _solve(False) always has
        # room; sparse-corridor flips finish in well under a second anyway.
        rem = max(0.0, deadline - time.perf_counter())
        result = _solve(True, dl=time.perf_counter() + 0.55 * rem)
        if result is None:
            result = _solve(False)
    else:
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
    # Drop zero-distance hops: you don't "jump" to where you already are
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
        1
        for leg in legs
        if leg["type"] == "regular" and not leg["neutron_star"]
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
