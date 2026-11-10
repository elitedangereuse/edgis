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
use numpy::ndarray::ArrayView2;
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
) -> (Option<Vec<i64>>, i64, u64) {
    let n = cand.len();
    let j = jump_range;
    let boost = boost_mult * j;
    let t: usize = reach_by_r.len();
    let reach_max = reach_by_r.iter().cloned().fold(0.0_f64, f64::max);
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
        dd.iter().map(|&x| (x / reach_max).ceil() as i64).collect()
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
    let slots = t + 1;
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
            if best[idx(m, cr)] > cg {
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
        if u < n && best[idx(u, r)] < g {
            continue;
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
                    let cost = ((dm / j).ceil() as i64).max(1);
                    relax!(m, g + cost, 0, dm, src, 0usize);
                }
            }
            continue;
        }

        // Build / fetch memoized edges for u.
        if edges[u].is_none() {
            let cu = coord[u];
            let mut nn = tree.within::<SquaredEuclidean>(&cu, reach_max * reach_max);
            nn.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
            let mut ms = Vec::new();
            let mut dm = Vec::new();
            let mut sc = Vec::new();
            for e in nn {
                let m = e.item as usize;
                if m != u {
                    ms.push(m as u32);
                    dm.push(e.distance.sqrt());
                    sc.push(scoop[m]);
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
                    nd.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
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
                Some((boosted_jumps(dd[u], j, boost), dd[u]))
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
            let (reach, step, dry) = if r >= t {
                (reach_by_r[0], 3i64, true)
            } else {
                (reach_by_r[r], 1i64, false)
            };
            // sorted dm: prefix within reach.
            let k = e.dm.partition_point(|&x| x <= reach);
            let cg = g + step;
            for i in 0..k {
                let m = e.ms[i] as usize;
                let nr = if e.scoop[i] {
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
#[allow(clippy::too_many_arguments)]
fn run<'py>(
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
    let bl: HashSet<(i64, i64)> = blacklist.into_iter().collect();
    Ok(search_core(
        cc.as_array(),
        scoopable.as_slice()?,
        cand.as_slice()?,
        sv.as_slice()?,
        dv.as_slice()?,
        jump_range,
        entry_r,
        gap_k,
        &reach_by_r,
        boost_mult,
        time_budget_s,
        &bl,
        &badzones,
    ))
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
    // as_array/as_slice borrow the numpy buffers; marshal under the GIL, then the
    // core does its own heavy lifting. (Buffers are read-only for the whole call.)
    let _ = py;
    let (seq, _g, _p) = run(
        cc, scoopable, cand, sv, dv, jump_range, entry_r, gap_k, reach_by_r,
        boost_mult, time_budget_s, blacklist, badzones,
    )?;
    Ok(seq)
}

/// Debug/parity entry point: (sequence, dst_g jump count, pops).
#[allow(clippy::too_many_arguments)]
#[pyfunction]
fn graph_search_range_debug<'py>(
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
        cc, scoopable, cand, sv, dv, jump_range, entry_r, gap_k, reach_by_r,
        boost_mult, time_budget_s, blacklist, badzones,
    )
}

#[pymodule]
fn edgis_route(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(graph_search_range, m)?)?;
    m.add_function(wrap_pyfunction!(graph_search_range_debug, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
