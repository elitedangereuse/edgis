#!/usr/bin/env python3
"""Native-kernel parity / regression gate for the full-rust port.

Runs the Python search and the native `*_debug` kernel on a fixed set of SEEDED
synthetic corridors (no DB, no braben), and compares:
  - native `dst_g` (the authoritative min-jump count) against a golden baseline,
    so a Rust change that regresses the NON-flip path is caught instantly;
  - native vs Python returned-sequence length, as a cross-check that both solve
    the same optimum (exact chains may differ by tie-break -- kiddo vs cKDTree).

Usage:
  python kernel_parity.py            # compare against golden (fails on drift)
  python kernel_parity.py --bless    # write the current native dst_g as golden

Flip/honest cases are added here as that logic is ported into the Rust core; the
authoritative flip check is end-to-end (graph_route total_jumps, python vs native)
on braben, since the flipped edge costs are what the port changes.
"""
import argparse
import json
import os
import pathlib
import sys
import time

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
os.environ.setdefault("ROUTE_PREBUILD", "0")
import routing  # noqa: E402
import edgis_route  # noqa: E402

GOLDEN = pathlib.Path(__file__).parent / "kernel_parity_golden.json"

# (seed, n, scoop_fraction, J, span) -- a spread of densities / sizes / tanks.
CASES = [
    (1, 200, 0.30, 50.0, 600.0),
    (2, 500, 0.15, 55.0, 1200.0),
    (3, 1000, 0.40, 60.0, 2000.0),
    (4, 2000, 0.20, 48.0, 3000.0),
    (5, 300, 0.05, 70.0, 1500.0),   # sparse scoop
    (6, 800, 0.60, 52.0, 1800.0),   # dense scoop
]


def _corridor(seed, n, scoop_frac, span):
    rng = np.random.default_rng(seed)
    cc = np.ascontiguousarray(rng.uniform(0.0, span, size=(n, 3)))
    scoop = rng.random(n) < scoop_frac
    idx = routing.NeutronIndex(
        cc, list(range(n)), [f"N{i}" for i in range(n)],
        scoop_ls=np.where(scoop, 10.0, np.inf),
    )
    sv = np.zeros(3)
    dv = np.full(3, span)
    return idx, cc, scoop, sv, dv


def _reach_by_r(J, boost_mult, tank):
    # Synthetic growing reach: boost_mult*J at r=0 up to ~1.13x at the empty tank.
    base = boost_mult * J
    return [base * (1.0 + 0.13 * r / max(1, tank - 1)) for r in range(tank)]


def run():
    rows = []
    for seed, n, sf, J, span in CASES:
        idx, cc, scoop, sv, dv = _corridor(seed, n, sf, span)
        cand = np.arange(n)
        dl = time.perf_counter() + 30.0
        tank, boost, scoop_ls = 25, 4.0, 10.0
        ccf = np.ascontiguousarray(cc)
        scf = np.ascontiguousarray(scoop.astype(np.uint8))
        cnf = np.ascontiguousarray(cand, dtype=np.int64)
        svf = np.ascontiguousarray(sv)
        dvf = np.ascontiguousarray(dv)

        # B1 fuel
        py1 = routing._graph_search_fuel(
            idx, cand, sv, dv, J, 300.0, 16, set(), [], dl, tank, 100.0)
        n1 = edgis_route.graph_search_fuel_debug(
            ccf, scf, cnf, svf, dvf, J, 300.0, 16, tank, boost, 30.0, [], [])
        # B2 range
        rbr = _reach_by_r(J, boost, tank)
        py2 = routing._graph_search_range(
            idx, cand, sv, dv, J, 300.0, 16, set(), [], dl,
            np.asarray(rbr, dtype=np.float64), 100.0, boost_mult=boost)
        n2 = edgis_route.graph_search_range_debug(
            ccf, scf, cnf, svf, dvf, J, 300.0, 16, rbr, boost, 30.0, [], [])

        rows.append({
            "case": f"s{seed}n{n}",
            "fuel_g": n1[1], "fuel_len_nat": len(n1[0]) if n1[0] else None,
            "fuel_len_py": len(py1) if py1 else None,
            "range_g": n2[1], "range_len_nat": len(n2[0]) if n2[0] else None,
            "range_len_py": len(py2) if py2 else None,
        })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bless", action="store_true")
    a = ap.parse_args()
    rows = run()
    print(f"{'case':10} {'fuel_g':>6} {'f_nat/py':>9} {'range_g':>7} {'r_nat/py':>9}")
    for r in rows:
        print(f"{r['case']:10} {str(r['fuel_g']):>6} "
              f"{str(r['fuel_len_nat'])+'/'+str(r['fuel_len_py']):>9} "
              f"{str(r['range_g']):>7} "
              f"{str(r['range_len_nat'])+'/'+str(r['range_len_py']):>9}")
    if a.bless:
        GOLDEN.write_text(json.dumps(rows, indent=0))
        print(f"\nblessed golden -> {GOLDEN.name}")
        return 0
    if not GOLDEN.exists():
        print("\nNO GOLDEN yet -- run with --bless to create it.")
        return 1
    gold = json.loads(GOLDEN.read_text())
    bad = []
    for g, r in zip(gold, rows):
        # regression gate: native jump counts must not drift on the non-flip path;
        # len mismatch native-vs-python flags a real divergence (not a tie-break).
        if g["fuel_g"] != r["fuel_g"] or g["range_g"] != r["range_g"]:
            bad.append(f"{r['case']}: g drift fuel {g['fuel_g']}->{r['fuel_g']} "
                       f"range {g['range_g']}->{r['range_g']}")
        if r["fuel_len_nat"] != r["fuel_len_py"]:
            bad.append(f"{r['case']}: fuel len native {r['fuel_len_nat']} != "
                       f"python {r['fuel_len_py']}")
        if r["range_len_nat"] != r["range_len_py"]:
            bad.append(f"{r['case']}: range len native {r['range_len_nat']} != "
                       f"python {r['range_len_py']}")
    if bad:
        print("\nPARITY FAIL:")
        for b in bad:
            print("  " + b)
        return 1
    print("\nPARITY OK (native jump counts match golden; native len == python len)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
