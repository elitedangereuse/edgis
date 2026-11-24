#!/usr/bin/env python3
"""Build the full-galaxy sparse neutron graph for the experimental fullgraph engine.

Distance-stratified Yao-style sparsifier over the neutron index: per node, per
distance band, keep the FARTHEST K_FAR neighbours (max progress per jump: min-jumps
routing lives on the max-reach ring) plus the NEAREST K_NEAR (connectivity).
One stored topology serves every ship reach class: per-ship feasibility is a mask
by (boosted) reach over the stored edge distances, and edges beyond reach cost an
optimistic regular-jump chain.

Why farthest-per-band: the nearest-K variant measurably loses routes (cutter
Colonia->Sol full-graph optimal 161 vs 126 with farthest-first selection on the
SAME edge budget; spansh 131, corridor engine 154-172). Selection beats budget.

Why the extra SCOOPABLE picks per band: distance-only selection leaves the
fuel-aware search starved of refuel landings (scoopables are ~4.7% of the index;
distance-selected edges land on one just 4.5% of the time and 18.5% of nodes
had NO scoopable out-edge), forcing dry hops on small tanks. Keeping the nearest
and farthest scoopable neighbours per band gives the fuel labels the candidates
they exist to choose. Scoopability is baked at SCOOP_BAKE_LS (the common request
default): baking only ENRICHES candidates -- the per-request scoop mask still
decides what counts as a refuel at query time.

The output is the SYMMETRIZED CSR (kNN selection is asymmetric, jump physics is
not): fmt=3, indptr int64, idx int32, dist_dly uint16 (ly*10, max 6553.5 ly).

Known caveat: KQ caps candidates per node, which truncates the long bands in the
ultra-dense core (max degree ~50k within 500 ly). Acceptable there (alternatives
abound) but re-check if core routes underperform.

Usage:
    build_sparse_neutron_graph.py --index <neutron_index.npz> --out <graph.npz>
"""
import argparse
import os
import time

import numpy as np
from scipy.sparse import csr_matrix
from scipy.spatial import cKDTree

BANDS = [(0.0, 60.0), (60.0, 120.0), (120.0, 216.0), (216.0, 350.0), (350.0, 500.0)]

# Scoopability bake threshold (Ls): the common request default for the
# in-highway companion-scoop reach. Only used to ENRICH edge selection.
SCOOP_BAKE_LS = 1000.0


def build(index_path, out_path, k_far=6, k_near=2, k_scoop=2, kq=2048,
          chunk=50000, workers=4):
    d = np.load(index_path, allow_pickle=True)
    xyz = np.ascontiguousarray(d["coords"], dtype=np.float64)
    n = len(xyz)
    scoop_ls = d["scoop"] if "scoop" in d.files else None
    sc_ok = (
        np.isfinite(scoop_ls) & (scoop_ls <= SCOOP_BAKE_LS)
        if scoop_ls is not None
        else np.zeros(n, dtype=bool)
    )
    print(f"nodes {n:,}  scoopable {100 * sc_ok.mean():.1f}%", flush=True)
    tree = cKDTree(xyz)
    idx_parts, dist_parts = [], []
    counts = np.zeros(n, dtype=np.int64)
    t0 = time.time()
    for s in range(0, n, chunk):
        e = min(s + chunk, n)
        dd, ii = tree.query(xyz[s:e], k=kq, distance_upper_bound=BANDS[-1][1],
                            workers=workers)
        for r in range(e - s):
            drow, irow = dd[r], ii[r]
            m = np.isfinite(drow) & (irow != (s + r)) & (irow < n)
            drow, irow = drow[m], irow[m]
            keep_i, keep_d = [], []
            for lo, hi in BANDS:
                bm = (drow >= lo) & (drow < hi)
                if not bm.any():
                    continue
                bi, bd = irow[bm], drow[bm]  # tree.query returns sorted by distance
                sel = [bi[:k_near], bi[-k_far:]]
                seld = [bd[:k_near], bd[-k_far:]]
                # Refuel candidates: the nearest + farthest SCOOPABLE neighbours
                # in the band, so the fuel-aware search always has a landing to
                # thread (distance-only selection starved it, see header).
                if k_scoop > 0:
                    sm = sc_ok[bi]
                    if sm.any():
                        sbi, sbd = bi[sm], bd[sm]
                        sel += [sbi[:k_scoop], sbi[-k_scoop:]]
                        seld += [sbd[:k_scoop], sbd[-k_scoop:]]
                sel = np.concatenate(sel)
                seld = np.concatenate(seld)
                u, ui = np.unique(sel, return_index=True)
                keep_i.append(u)
                keep_d.append(seld[ui])
            if keep_i:
                ki = np.concatenate(keep_i)
                kd = np.concatenate(keep_d)
                idx_parts.append(ki.astype(np.int32))
                dist_parts.append(np.round(kd * 10.0).astype(np.uint16))
                counts[s + r] = len(ki)
        if (s // chunk) % 20 == 0:
            done = e / n
            el = time.time() - t0
            print(f"  {100 * done:.0f}%  {el / 60:.1f}m  "
                  f"eta {el / done / 60 * (1 - done):.0f}m", flush=True)
    idx = np.concatenate(idx_parts)
    dist = np.concatenate(dist_parts)
    indptr = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(counts, out=indptr[1:])

    # Symmetrize: distances are symmetric, so maximum() unions the structure while
    # keeping the true distance where only one direction kept the edge.
    g = csr_matrix((dist.astype(np.float32), idx, indptr), shape=(n, n))
    g = g.maximum(g.T).tocsr()
    print(f"edges {len(idx):,} raw -> {g.nnz:,} symmetric  "
          f"mean deg {g.nnz / n:.1f}", flush=True)
    np.savez(out_path, fmt=np.int64(3), indptr=g.indptr.astype(np.int64),
             idx=g.indices.astype(np.int32),
             dist_dly=np.round(g.data).astype(np.uint16))
    print(f"wrote {out_path}  {os.path.getsize(out_path) / 1e9:.2f} GB", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--index", required=True, help="neutron_index.npz path")
    ap.add_argument("--out", required=True, help="output graph npz path")
    ap.add_argument("--k-far", type=int, default=6)
    ap.add_argument("--k-near", type=int, default=2)
    ap.add_argument("--k-scoop", type=int, default=2)
    ap.add_argument("--kq", type=int, default=2048)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args()
    build(a.index, a.out, a.k_far, a.k_near, a.k_scoop, a.kq,
          workers=a.workers)
