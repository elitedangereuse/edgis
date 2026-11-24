//! Native port of `routing._graph_search_range` (the B2 range-growth fuel-aware
//! neutron A*). Faithful to the Python semantics: per-resource reach growth,
//! same-r dominance, boost-neighbour prefix within the resource's reach, entry
//! neutrons, tail-to-dst, gap edges, blacklist + bad zones, and parent
//! reconstruction returning the GLOBAL-id neutron sequence.
//!
//! The optimal JUMP COUNT is identical to Python; the exact tie-broken path may
//! differ because the neighbour-query order (kiddo vs scipy cKDTree) differs,
//! which never changes the min-jumps value. The caller realizes the returned
//! sequence, so an equivalent optimal path is all that matters.
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::time::Instant;

use kiddo::{KdTree, SquaredEuclidean};
use numpy::ndarray::{Array2, ArrayView2};
use numpy::{PyReadonlyArray1, PyReadonlyArray2};
use ordered_float::OrderedFloat;
use pyo3::prelude::*;

const GAP_RADII_J: [f64; 3] = [8.0, 16.0, 32.0];

#[inline]
fn boosted_jumps(d: f64, j: f64, boost: f64) -> i64 {
    if d <= boost {
        1
    } else {
        1 + ((d - boost) / j).ceil() as i64
    }
}

struct Edges {
    ms: Vec<u32>,
    dm: Vec<f64>,
    scoop: Vec<bool>,
    // gap edges: (node, cost, dist)
    gap: Vec<(u32, i64, f64)>,
    // tail to DST: (cost, dist)
    tail: Option<(i64, f64)>,
}

/// Core search. Returns (sequence, dst_g jump count, pops). sequence is None on
/// timeout / no route (dst_g == -1, pops still reported).
#[allow(clippy::too_many_arguments)]
fn search_core(
    cc: ArrayView2<f64>,
    scoop_u8: &[u8],
    cand: &[i64],
    sv: &[f64],
    dv: &[f64],
    jump_range: f64,
    entry_r: f64,
    gap_k: usize,
    reach_by_r: &[f64],
    boost_mult: f64,
    time_budget_s: f64,
    bl: &HashSet<(i64, i64)>,
    badzones: &[(f64, f64, f64, f64)],
    // B1 fuel search (fixed reach) uses PREFIX-MIN dominance: a label is dropped
    // when any lower-or-equal resource reaches the node in no more jumps. B2 range
    // growth uses SAME-r only (a fuller tank has shorter reach, so it cannot
    // dominate an emptier one). Both find the same min-jumps; prefix-min keeps the
    // (node x fuel) front a small anti-chain, so B1 stays fast on the full corridor.
    prefix_min: bool,
    // Flip-honest edge costs for a REVERSED (dst->src) search the caller realizes
    // src->dst. Supercharge is not direction-symmetric: a boost is available when
    // the FLOWN jump LEAVES a neutron. So flipped, the ENTRY edge (src->neutron m)
    // is a flown boosted arrival (boosted_jumps) and the TAIL edge (u->dst) is a
    // flown plain departure from the source (ceil(d/J)) -- the swap of the two
    // analytic edge costs vs the unflipped search. Boost/gap edges are unchanged.
    // Mirrors routing._graph_search / _graph_search_fuel's flip note (Blaa Eork ->
    // Lave 14 -> 9). B2 range never flips (always passes false).
    flip: bool,
    // Plain ship-agnostic min-jumps (routing._graph_search): no fuel dimension at
    // all. Verified identical jump count to the fuel search with a non-binding tank,
    // but with a SINGLE resource slot (r pinned at 0, every boost costs 1, no dry
    // penalty, no scoop reset) so memory is O(n), not O(n*tank). reach_by_r[0] is
    // the fixed boost reach (boost_mult * J).
    no_fuel: bool,
    // Weighted A* heuristic multiplier. 1.0 = admissible (optimal). > 1.0 inflates
    // the heuristic so the A* explores a NARROWER frontier -- used only for the
    // coarse seed (it just seeds the thin corridor, so a path within ~W x optimal is
    // fine, and on a dense full corridor the frontier collapse is a huge speedup:
    // W=1.1 cut 174k pops -> 34k, 41s -> 6.7s, for ~+1 jump the B2 final absorbs).
    // The fuel-aware final search passes 1.0 to stay optimal.
    heur_weight: f64,
) -> (Option<Vec<i64>>, i64, u64) {
    let n = cand.len();
    let j = jump_range;
    let boost = boost_mult * j;
    let t: usize = reach_by_r.len();
    let reach_max = reach_by_r.iter().cloned().fold(0.0_f64, f64::max);
    // Does the boost reach grow with the fuel resource (B2), or is it fixed (B1 /
    // plain)? When fixed, EVERY neighbour within reach_max is a valid cost-1 boost,
    // so the per-node boost edges need no distance order -- skipping the sort is the
    // difference between the dense full-corridor coarse taking ~85 s and ~seconds
    // (it was sorting tens of thousands of neighbours per node for nothing). Only
    // B2 needs the sorted prefix, to cut neighbours at the resource-dependent reach.
    let reach_varies = reach_by_r
        .iter()
        .any(|&x| (x - reach_by_r[0]).abs() > 1e-9);
    let sv3 = [sv[0], sv[1], sv[2]];
    let dv3 = [dv[0], dv[1], dv[2]];
    let straight = {
        let dx = sv3[0] - dv3[0];
        let dy = sv3[1] - dv3[1];
        let dz = sv3[2] - dv3[2];
        (dx * dx + dy * dy + dz * dz).sqrt()
    };

    let has_zones = !badzones.is_empty();
    let zone_ok = |pa: &[f64; 3], pb: &[f64; 3]| -> bool {
        if !has_zones {
            return true;
        }
        let mid = [
            (pa[0] + pb[0]) * 0.5,
            (pa[1] + pb[1]) * 0.5,
            (pa[2] + pb[2]) * 0.5,
        ];
        badzones.iter().all(|&(cx, cy, cz, r)| {
            let dx = mid[0] - cx;
            let dy = mid[1] - cy;
            let dz = mid[2] - cz;
            (dx * dx + dy * dy + dz * dz).sqrt() >= r
        })
    };

    // Per-node coords, dst distances, heuristic. Scoop as bool.
    let coord: Vec<[f64; 3]> = (0..n).map(|i| [cc[[i, 0]], cc[[i, 1]], cc[[i, 2]]]).collect();
    let scoop: Vec<bool> = scoop_u8.iter().map(|&b| b != 0).collect();
    let dd: Vec<f64> = coord
        .iter()
        .map(|c| {
            let dx = c[0] - dv3[0];
            let dy = c[1] - dv3[1];
            let dz = c[2] - dv3[2];
            (dx * dx + dy * dy + dz * dz).sqrt()
        })
        .collect();
    let hv: Vec<i64> = if reach_max > 0.0 {
        dd.iter()
            .map(|&x| (x / reach_max * heur_weight).ceil() as i64)
            .collect()
    } else {
        vec![0; n]
    };

    // kd-tree over corridor neutrons.
    let mut tree: KdTree<f64, 3> = KdTree::new();
    for (i, c) in coord.iter().enumerate() {
        tree.add(c, i as u64);
    }

    let src = n;
    let dst = n + 1;
    let big: i64 = i64::MAX / 4;
    let slots = if no_fuel { 1 } else { t + 1 };
    let mut best = vec![big; (n + 2) * slots];
    // parent per (node, r); (-1,-1) == unset.
    let mut par: Vec<(i32, i32)> = vec![(-1, -1); (n + 2) * slots];
    let idx = |node: usize, r: usize| node * slots + r;

    let mut edges: Vec<Option<Edges>> = Vec::with_capacity(n);
    for _ in 0..n {
        edges.push(None);
    }

    let mut heap: BinaryHeap<Reverse<(i64, i64, OrderedFloat<f64>, u64, usize, usize)>> =
        BinaryHeap::new();
    let mut order: u64 = 0;

    macro_rules! relax {
        ($m:expr, $cg:expr, $cr:expr, $cd:expr, $pnode:expr, $pr:expr) => {{
            let m: usize = $m;
            let cg: i64 = $cg;
            let cr: usize = $cr;
            // Dominated iff an equal-or-better label already reaches m: same-r for
            // B2, prefix-min over resources 0..=cr for B1.
            let dominated = if prefix_min {
                let base = m * slots;
                let mut pm = i64::MAX;
                for rr in 0..=cr {
                    if best[base + rr] < pm {
                        pm = best[base + rr];
                    }
                }
                pm <= cg
            } else {
                best[idx(m, cr)] <= cg
            };
            if !dominated {
                best[idx(m, cr)] = cg;
                par[idx(m, cr)] = ($pnode as i32, $pr as i32);
                let h = if m >= n { 0 } else { hv[m] };
                heap.push(Reverse((cg + h, cg, OrderedFloat($cd), order, m, cr)));
                order += 1;
            }
        }};
    }

    best[idx(src, 0)] = 0;
    heap.push(Reverse((0, 0, OrderedFloat(0.0), order, src, 0)));
    order += 1;

    let start = Instant::now();
    let mut dst_r: Option<usize> = None;
    let mut dst_g: i64 = -1;
    let mut pops: u64 = 0;

    while let Some(Reverse((_f, g, cd, _o, u, r))) = heap.pop() {
        if u == dst {
            dst_r = Some(r);
            dst_g = g;
            break;
        }
        if u < n {
            if best[idx(u, r)] < g {
                continue;
            }
            // Prefix-min (B1): a lower resource now reaches u in no more jumps.
            if prefix_min && r > 0 {
                let base = u * slots;
                let mut pm = i64::MAX;
                for rr in 0..r {
                    if best[base + rr] < pm {
                        pm = best[base + rr];
                    }
                }
                if pm <= g {
                    continue;
                }
            }
        }
        pops += 1;
        // Deadline check every 4096 real pops.
        if pops & 0xFFF == 0 && start.elapsed().as_secs_f64() > time_budget_s {
            return (None, -1, pops);
        }
        let cd = cd.0;

        if u == src {
            if !bl.contains(&(-1, -2)) && zone_ok(&sv3, &dv3) {
                relax!(dst, g + (straight / j).ceil() as i64, 0, straight, src, 0usize);
            }
            if n > 0 {
                for e in tree.within_unsorted::<SquaredEuclidean>(&sv3, entry_r * entry_r) {
                    let m = e.item as usize;
                    if bl.contains(&(-1, cand[m])) || !zone_ok(&sv3, &coord[m]) {
                        continue;
                    }
                    let dm = e.distance.sqrt();
                    // flipped: flown boosted arrival leaving neutron m; else plain src departure
                    let cost = if flip {
                        boosted_jumps(dm, j, boost)
                    } else {
                        ((dm / j).ceil() as i64).max(1)
                    };
                    relax!(m, g + cost, 0, dm, src, 0usize);
                }
            }
            continue;
        }

        // Build / fetch memoized edges for u.
        if edges[u].is_none() {
            let cu = coord[u];
            let mut ms = Vec::new();
            let mut dm = Vec::new();
            let mut sc = Vec::new();
            if reach_varies {
                // B2: keep neighbours distance-sorted so relax can take the prefix
                // within reach_by_r[r] (range growth).
                let mut nn = tree.within::<SquaredEuclidean>(&cu, reach_max * reach_max);
                nn.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
                for e in nn {
                    let m = e.item as usize;
                    if m != u {
                        ms.push(m as u32);
                        dm.push(e.distance.sqrt());
                        sc.push(scoop[m]);
                    }
                }
            } else {
                // B1 / plain: fixed reach, every neighbour is a valid cost-1 boost;
                // no order needed, so skip the sort (within_unsorted is O(k)).
                for e in tree.within_unsorted::<SquaredEuclidean>(&cu, reach_max * reach_max) {
                    let m = e.item as usize;
                    if m != u {
                        ms.push(m as u32);
                        dm.push(e.distance.sqrt());
                        sc.push(scoop[m]);
                    }
                }
            }
            // gap edges: only when no forward boost neighbour exists.
            let forward = ms.iter().any(|&m| dd[m as usize] < dd[u] - 1e-6);
            let mut gap: Vec<(u32, i64, f64)> = Vec::new();
            if !forward {
                for &mult in GAP_RADII_J.iter() {
                    let near: Vec<u64> = tree
                        .within_unsorted::<SquaredEuclidean>(&cu, (mult * j) * (mult * j))
                        .into_iter()
                        .map(|e| e.item)
                        .filter(|&mi| dd[mi as usize] < dd[u] - 1e-6)
                        .collect();
                    if near.is_empty() {
                        continue;
                    }
                    let mut nd: Vec<(u64, f64)> = near
                        .iter()
                        .map(|&mi| {
                            let c = coord[mi as usize];
                            let dx = c[0] - cu[0];
                            let dy = c[1] - cu[1];
                            let dz = c[2] - cu[2];
                            (mi, (dx * dx + dy * dy + dz * dz).sqrt())
                        })
                        .collect();
                    // Deterministic (distance, then neighbour index) so the gap_k
                    // truncation picks the SAME subset as python's lexsort -> the
                    // native gap graph is identical, not just same-set-different-order
                    // (closes the +8 native-vs-python drift on bridge-heavy routes).
                    nd.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
                    let mut got = false;
                    for &(mi, dist) in nd.iter().take(gap_k) {
                        let m = mi as usize;
                        if bl.contains(&(cand[u], cand[m])) {
                            continue;
                        }
                        if !zone_ok(&cu, &coord[m]) {
                            continue;
                        }
                        gap.push((mi as u32, boosted_jumps(dist, j, boost), dist));
                        got = true;
                    }
                    if got {
                        break;
                    }
                }
            }
            let tail = if !bl.contains(&(cand[u], -2)) && zone_ok(&cu, &dv3) {
                // flipped: flown plain departure from the source; else boosted arrival
                let tc = if flip {
                    ((dd[u] / j).ceil() as i64).max(1)
                } else {
                    boosted_jumps(dd[u], j, boost)
                };
                Some((tc, dd[u]))
            } else {
                None
            };
            edges[u] = Some(Edges { ms, dm, scoop: sc, gap, tail });
        }
        let e = edges[u].as_ref().unwrap();

        if let Some((tcost, tdist)) = e.tail {
            relax!(dst, g + tcost, 0, cd + tdist, u, r);
        }
        if !e.ms.is_empty() {
            let (reach, step, dry) = if no_fuel {
                (reach_by_r[0], 1i64, false) // plain: fixed boost reach, cost 1, never dry
            } else if r >= t {
                (reach_by_r[0], 3i64, true)
            } else {
                (reach_by_r[r], 1i64, false)
            };
            // B2: sorted dm -> prefix within the (grown) reach. B1/plain: dm is
            // unsorted but reach is fixed at reach_max, so every neighbour qualifies.
            let k = if reach_varies {
                e.dm.partition_point(|&x| x <= reach)
            } else {
                e.ms.len()
            };
            let cg = g + step;
            for i in 0..k {
                let m = e.ms[i] as usize;
                let nr = if no_fuel {
                    0
                } else if e.scoop[i] {
                    0
                } else if dry {
                    1
                } else {
                    r + 1
                };
                relax!(m, cg, nr, cd + e.dm[i], u, r);
            }
        }
        // gap edges copied out to avoid borrow conflict with relax! (mutates par/best only)
        let gaps: Vec<(u32, i64, f64)> = e.gap.clone();
        for (m, cost, gdm) in gaps {
            relax!(m as usize, g + cost, 0, cd + gdm, u, r);
        }
    }

    let dst_r = match dst_r {
        Some(r) => r,
        None => return (None, -1, pops),
    };

    // Reconstruct: walk parents from (DST, dst_r) to SRC, collecting global ids.
    let mut seq: Vec<i64> = Vec::new();
    let mut node = dst;
    let mut r = dst_r;
    loop {
        let (pnode, pr) = par[idx(node, r)];
        if pnode < 0 {
            break;
        }
        if node < n {
            seq.push(cand[node]);
        }
        node = pnode as usize;
        r = pr as usize;
    }
    seq.reverse();
    (Some(seq), dst_g, pops)
}

/// Marshal numpy inputs then call the shared core. Shared by both wrappers.
///
/// Copies the buffers under the GIL, then releases it for the search: the
/// kernel runs for seconds and the API is one single-process event loop, so
/// holding the GIL here would stall every other request for the whole solve.
/// The copies are a few MB at most, trivial next to the search itself.
#[allow(clippy::too_many_arguments)]
fn run<'py>(
    py: Python<'py>,
    cc: PyReadonlyArray2<'py, f64>,
    scoopable: PyReadonlyArray1<'py, u8>,
    cand: PyReadonlyArray1<'py, i64>,
    sv: PyReadonlyArray1<'py, f64>,
    dv: PyReadonlyArray1<'py, f64>,
    jump_range: f64,
    entry_r: f64,
    gap_k: usize,
    reach_by_r: Vec<f64>,
    boost_mult: f64,
    time_budget_s: f64,
    blacklist: Vec<(i64, i64)>,
    badzones: Vec<(f64, f64, f64, f64)>,
    prefix_min: bool,
    flip: bool,
    no_fuel: bool,
    heur_weight: f64,
) -> PyResult<(Option<Vec<i64>>, i64, u64)> {
    let cc_o: Array2<f64> = cc.as_array().to_owned();
    let scoop_o: Vec<u8> = scoopable.as_slice()?.to_vec();
    let cand_o: Vec<i64> = cand.as_slice()?.to_vec();
    let sv_o: Vec<f64> = sv.as_slice()?.to_vec();
    let dv_o: Vec<f64> = dv.as_slice()?.to_vec();
    let bl: HashSet<(i64, i64)> = blacklist.into_iter().collect();
    Ok(py.allow_threads(move || {
        search_core(
            cc_o.view(),
            &scoop_o,
            &cand_o,
            &sv_o,
            &dv_o,
            jump_range,
            entry_r,
            gap_k,
            &reach_by_r,
            boost_mult,
            time_budget_s,
            &bl,
            &badzones,
            prefix_min,
            flip,
            no_fuel,
            heur_weight,
        )
    }))
}

/// Production entry point: the GLOBAL-id neutron sequence, or None (timeout / no
/// route). Releases the GIL for the duration of the search.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
fn graph_search_range<'py>(
    py: Python<'py>,
    cc: PyReadonlyArray2<'py, f64>,
    scoopable: PyReadonlyArray1<'py, u8>,
    cand: PyReadonlyArray1<'py, i64>,
    sv: PyReadonlyArray1<'py, f64>,
    dv: PyReadonlyArray1<'py, f64>,
    jump_range: f64,
    entry_r: f64,
    gap_k: usize,
    reach_by_r: Vec<f64>,
    boost_mult: f64,
    time_budget_s: f64,
    blacklist: Vec<(i64, i64)>,
    badzones: Vec<(f64, f64, f64, f64)>,
) -> PyResult<Option<Vec<i64>>> {
    let (seq, _g, _p) = run(
        py, cc, scoopable, cand, sv, dv, jump_range, entry_r, gap_k, reach_by_r,
        boost_mult, time_budget_s, blacklist, badzones, false, false, false, 1.0,
    )?;
    Ok(seq)
}

/// Debug/parity entry point: (sequence, dst_g jump count, pops).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
fn graph_search_range_debug<'py>(
    py: Python<'py>,
    cc: PyReadonlyArray2<'py, f64>,
    scoopable: PyReadonlyArray1<'py, u8>,
    cand: PyReadonlyArray1<'py, i64>,
    sv: PyReadonlyArray1<'py, f64>,
    dv: PyReadonlyArray1<'py, f64>,
    jump_range: f64,
    entry_r: f64,
    gap_k: usize,
    reach_by_r: Vec<f64>,
    boost_mult: f64,
    time_budget_s: f64,
    blacklist: Vec<(i64, i64)>,
    badzones: Vec<(f64, f64, f64, f64)>,
) -> PyResult<(Option<Vec<i64>>, i64, u64)> {
    run(
        py, cc, scoopable, cand, sv, dv, jump_range, entry_r, gap_k, reach_by_r,
        boost_mult, time_budget_s, blacklist, badzones, false, false, false, 1.0,
    )
}

/// B1 fuel entry point. Same contract as graph_search_range but a FIXED reach
/// (boost_mult * jump_range at every fuel level) over `tank_jumps` resource
/// levels, with prefix-min dominance. The GLOBAL-id neutron sequence, or None.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (cc, scoopable, cand, sv, dv, jump_range, entry_r, gap_k, tank_jumps, boost_mult, time_budget_s, blacklist, badzones, flip=false, no_fuel=false, heur_weight=1.0))]
fn graph_search_fuel<'py>(
    py: Python<'py>,
    cc: PyReadonlyArray2<'py, f64>,
    scoopable: PyReadonlyArray1<'py, u8>,
    cand: PyReadonlyArray1<'py, i64>,
    sv: PyReadonlyArray1<'py, f64>,
    dv: PyReadonlyArray1<'py, f64>,
    jump_range: f64,
    entry_r: f64,
    gap_k: usize,
    tank_jumps: usize,
    boost_mult: f64,
    time_budget_s: f64,
    blacklist: Vec<(i64, i64)>,
    badzones: Vec<(f64, f64, f64, f64)>,
    flip: bool,
    no_fuel: bool,
    heur_weight: f64,
) -> PyResult<Option<Vec<i64>>> {
    let reach_by_r = vec![boost_mult * jump_range; tank_jumps];
    let (seq, _g, _p) = run(
        py, cc, scoopable, cand, sv, dv, jump_range, entry_r, gap_k, reach_by_r,
        boost_mult, time_budget_s, blacklist, badzones, true, flip, no_fuel, heur_weight,
    )?;
    Ok(seq)
}

/// Debug/parity entry point for B1: (sequence, dst_g jump count, pops).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (cc, scoopable, cand, sv, dv, jump_range, entry_r, gap_k, tank_jumps, boost_mult, time_budget_s, blacklist, badzones, flip=false, no_fuel=false, heur_weight=1.0))]
fn graph_search_fuel_debug<'py>(
    py: Python<'py>,
    cc: PyReadonlyArray2<'py, f64>,
    scoopable: PyReadonlyArray1<'py, u8>,
    cand: PyReadonlyArray1<'py, i64>,
    sv: PyReadonlyArray1<'py, f64>,
    dv: PyReadonlyArray1<'py, f64>,
    jump_range: f64,
    entry_r: f64,
    gap_k: usize,
    tank_jumps: usize,
    boost_mult: f64,
    time_budget_s: f64,
    blacklist: Vec<(i64, i64)>,
    badzones: Vec<(f64, f64, f64, f64)>,
    flip: bool,
    no_fuel: bool,
    heur_weight: f64,
) -> PyResult<(Option<Vec<i64>>, i64, u64)> {
    let reach_by_r = vec![boost_mult * jump_range; tank_jumps];
    run(
        py, cc, scoopable, cand, sv, dv, jump_range, entry_r, gap_k, reach_by_r,
        boost_mult, time_budget_s, blacklist, badzones, true, flip, no_fuel, heur_weight,
    )
}

/// Full-galaxy sparse CSR graph for the experimental fullgraph engine: the
/// distance-stratified sparsifier built by feeders/build_sparse_neutron_graph.py
/// (symmetrized; node i == row i of the neutron index). Cached per process after
/// the first call: the arrays must be COPIED out of numpy to release the GIL for
/// the search (same reason `run` copies), and at ~1.4 GB that copy must happen
/// once, not per query. Keyed by the indptr data pointer + length so a reloaded
/// (new) graph replaces the cache.
struct CsrGraph {
    indptr: Vec<i64>,
    idx: Vec<i32>,
    dist_dly: Vec<u16>,
    coords: Vec<[f64; 3]>,
}

static CSR_CACHE: std::sync::OnceLock<std::sync::Mutex<Option<(usize, usize, std::sync::Arc<CsrGraph>)>>> =
    std::sync::OnceLock::new();

/// Multi-source / multi-target A* over the sparse CSR graph. Unit cost per
/// boosted hop; edges longer than the boosted reach cost an optimistic regular
/// chain (same `boosted_jumps` model as the corridor engine's gap edges).
/// `srcs`/`dsts` carry per-candidate entry/exit jump costs (source attachment is
/// an unboosted chain; exit leaves a neutron, so the caller prices it boosted).
/// Returns (backbone node sequence, total jump cost incl. entry+exit, pops), or
/// (None, -1, pops) on timeout / no route.
#[pyfunction]
fn csr_search<'py>(
    py: Python<'py>,
    indptr: PyReadonlyArray1<'py, i64>,
    idx: PyReadonlyArray1<'py, i32>,
    dist_dly: PyReadonlyArray1<'py, u16>,
    coords: PyReadonlyArray2<'py, f64>,
    srcs: Vec<(usize, f64)>,
    dsts: Vec<(usize, f64)>,
    jump_range: f64,
    boost_mult: f64,
    time_budget_s: f64,
) -> PyResult<(Option<Vec<usize>>, f64, u64)> {
    let key = (indptr.as_slice()?.as_ptr() as usize, indptr.len()?);
    let cache = CSR_CACHE.get_or_init(|| std::sync::Mutex::new(None));
    let graph = {
        let mut guard = cache.lock().unwrap();
        match guard.as_ref() {
            Some((p, l, g)) if (*p, *l) == key => g.clone(),
            _ => {
                let cc = coords.as_array();
                let n = cc.shape()[0];
                let mut co = Vec::with_capacity(n);
                for i in 0..n {
                    co.push([cc[[i, 0]], cc[[i, 1]], cc[[i, 2]]]);
                }
                let g = std::sync::Arc::new(CsrGraph {
                    indptr: indptr.as_slice()?.to_vec(),
                    idx: idx.as_slice()?.to_vec(),
                    dist_dly: dist_dly.as_slice()?.to_vec(),
                    coords: co,
                });
                *guard = Some((key.0, key.1, g.clone()));
                g
            }
        }
    };
    Ok(py.allow_threads(move || {
        csr_search_core(&graph, &srcs, &dsts, jump_range, boost_mult, time_budget_s)
    }))
}

fn csr_search_core(
    g: &CsrGraph,
    srcs: &[(usize, f64)],
    dsts: &[(usize, f64)],
    jump_range: f64,
    boost_mult: f64,
    time_budget_s: f64,
) -> (Option<Vec<usize>>, f64, u64) {
    let n = g.indptr.len() - 1;
    let boost = boost_mult * jump_range;
    let start = Instant::now();
    let mut best_g: Vec<f32> = vec![f32::INFINITY; n];
    let mut parent: Vec<i32> = vec![-1; n];
    // Admissible heuristic: straight-line to the nearest exit candidate over the
    // boosted reach (no hop progresses more than `boost`; chain edges cost at
    // least ceil(progress / boost)). Exit cost >= 0 is ignored (stays admissible).
    let h = |u: usize| -> f64 {
        let p = g.coords[u];
        dsts.iter()
            .map(|&(d, _)| {
                let q = g.coords[d];
                let dx = p[0] - q[0];
                let dy = p[1] - q[1];
                let dz = p[2] - q[2];
                OrderedFloat((dx * dx + dy * dy + dz * dz).sqrt() / boost)
            })
            .min()
            .map(|x| x.0)
            .unwrap_or(0.0)
    };
    let mut heap: BinaryHeap<Reverse<(OrderedFloat<f64>, u32)>> = BinaryHeap::new();
    for &(s, c) in srcs {
        if (c as f32) < best_g[s] {
            best_g[s] = c as f32;
            heap.push(Reverse((OrderedFloat(c + h(s)), s as u32)));
        }
    }
    let exit_of: std::collections::HashMap<usize, f64> = dsts.iter().cloned().collect();
    let mut best_goal = f64::INFINITY;
    let mut best_dst: i64 = -1;
    let mut pops: u64 = 0;
    while let Some(Reverse((OrderedFloat(f), u32u))) = heap.pop() {
        let u = u32u as usize;
        pops += 1;
        if f >= best_goal {
            break; // every remaining label is at least the settled goal
        }
        if pops % 65536 == 0 && start.elapsed().as_secs_f64() > time_budget_s {
            return (None, -1.0, pops);
        }
        let gu = best_g[u] as f64;
        if f - h(u) > gu + 1e-6 {
            continue; // stale heap entry
        }
        if let Some(&ex) = exit_of.get(&u) {
            let total = gu + ex;
            if total < best_goal {
                best_goal = total;
                best_dst = u as i64;
            }
        }
        let (lo, hi) = (g.indptr[u] as usize, g.indptr[u + 1] as usize);
        for e in lo..hi {
            let v = g.idx[e] as usize;
            let d = g.dist_dly[e] as f64 / 10.0;
            let w = boosted_jumps(d, jump_range, boost) as f64;
            let ng = gu + w;
            if (ng as f32) < best_g[v] {
                best_g[v] = ng as f32;
                parent[v] = u as i32;
                heap.push(Reverse((OrderedFloat(ng + h(v)), v as u32)));
            }
        }
    }
    if best_dst < 0 {
        return (None, -1.0, pops);
    }
    let mut seq = vec![best_dst as usize];
    let mut cur = best_dst as usize;
    while parent[cur] >= 0 {
        cur = parent[cur] as usize;
        seq.push(cur);
    }
    seq.reverse();
    (Some(seq), best_goal, pops)
}

/// Debug: kiddo's radius query (item ids + distances) for a point, to compare the
/// neighbour SET/order against scipy cKDTree and diagnose the native-vs-python gap.
#[pyfunction]
fn debug_within<'py>(
    cc: PyReadonlyArray2<'py, f64>,
    query: Vec<f64>,
    radius: f64,
) -> PyResult<Vec<(u64, f64)>> {
    let cc_o: Array2<f64> = cc.as_array().to_owned();
    let n = cc_o.shape()[0];
    let mut tree: KdTree<f64, 3> = KdTree::new();
    for i in 0..n {
        tree.add(&[cc_o[[i, 0]], cc_o[[i, 1]], cc_o[[i, 2]]], i as u64);
    }
    let q = [query[0], query[1], query[2]];
    Ok(tree
        .within_unsorted::<SquaredEuclidean>(&q, radius * radius)
        .into_iter()
        .map(|e| (e.item, e.distance.sqrt()))
        .collect())
}

#[pymodule]
fn edgis_route(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_search_range, m)?)?;
    m.add_function(wrap_pyfunction!(graph_search_range_debug, m)?)?;
    m.add_function(wrap_pyfunction!(graph_search_fuel, m)?)?;
    m.add_function(wrap_pyfunction!(graph_search_fuel_debug, m)?)?;
    m.add_function(wrap_pyfunction!(csr_search, m)?)?;
    m.add_function(wrap_pyfunction!(debug_within, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
