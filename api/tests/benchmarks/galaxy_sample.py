#!/usr/bin/env python3
"""Galaxy-wide perf/quality sampler for the EDGIS route plotter.

Two phases:
  density-map  : a near-plane grid of points -> /neighbors -> density (capped
                 count) + nearest system per cell -> a density raster, class
                 breakdown, and an ASCII map. Cheap (fast spatial queries, no key).
  route-sample : from the density map, plot stratified route pairs -- including
                 the SAME pair in BOTH directions -- cache-busted, recording
                 solve_ms / jumps / engine. Separates the region-density effect
                 (plotting into sparse space) from the engine's directional
                 asymmetry (A->B vs B->A, same endpoints). Expensive (each is a
                 real solve); bounded by --n.

Usage:
  python galaxy_sample.py density-map  --base-url https://edgis.elitedangereuse.fr
  python galaxy_sample.py route-sample --base-url <URL> --key <KEY> --n 40 --bidir 12
"""
import argparse
import json
import math
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).parent
MAP_FILE = HERE / "galaxy_density_map.json"

# Near-plane galaxy extents (ly). Sol at the origin; z runs from behind-Sol to
# the Beagle rim; x spans the disc; y is the thin-disc height.
X_MIN, X_MAX = -41000, 41000
Z_MIN, Z_MAX = -17000, 67000


def _get(url, timeout=40):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def neighbors(base, x, y, z, radius, limit):
    q = urllib.parse.urlencode({"x": x, "y": y, "z": z, "radius": radius, "limit": limit})
    try:
        d = _get(f"{base}/neighbors?{q}")
    except Exception:
        return None
    arr = d if isinstance(d, list) else (d.get("neighbors") or d.get("results") or [])
    if not arr:
        return {"count": 0, "near": None, "name": None, "coords": [x, y, z]}

    def dist(it):
        c = it.get("coords") or it
        return math.dist((c["x"], c["y"], c["z"]), (x, y, z))

    best = min(arr, key=dist)
    bc = best.get("coords") or best
    return {"count": len(arr), "near": round(dist(best), 1),
            "name": best.get("name"), "coords": [bc["x"], bc["y"], bc["z"]]}


def classify(cell, limit):
    if not cell or cell["count"] == 0:
        return "empty"
    if cell["count"] >= limit:
        return "dense"
    if cell["count"] >= 20:
        return "medium"
    return "sparse"


_GLYPH = {"empty": " ", "sparse": ".", "medium": "+", "dense": "#", None: "?"}


def cmd_density_map(a):
    xs = list(range(X_MIN, X_MAX + 1, a.step))
    zs = list(range(Z_MIN, Z_MAX + 1, a.step))
    total = len(xs) * len(zs)
    print(f"Density map: {len(xs)}x{len(zs)} = {total} points at y={a.y}, "
          f"radius={a.radius}, limit={a.limit} -> {a.base_url}", file=sys.stderr)
    cells = []
    for i, z in enumerate(zs):
        for x in xs:
            c = neighbors(a.base_url, x, a.y, z, a.radius, a.limit)
            if c:
                c.update(x=x, y=a.y, z=z, cls=classify(c, a.limit))
            cells.append(c or {"x": x, "y": a.y, "z": z, "count": 0, "cls": "empty"})
        print(f"  row {i+1}/{len(zs)} (z={z})", file=sys.stderr)
    MAP_FILE.write_text(json.dumps(
        {"params": {"step": a.step, "radius": a.radius, "limit": a.limit, "y": a.y},
         "cells": cells}, indent=0) + "\n")

    # Class breakdown.
    from collections import Counter
    counts = Counter(c["cls"] for c in cells)
    print(f"\nClasses ({total} cells): " +
          ", ".join(f"{k}={counts.get(k,0)}" for k in ("dense", "medium", "sparse", "empty")))

    # ASCII map: z as rows (core/rim toward the top), x as columns.
    grid = {(c["x"], c["z"]): c["cls"] for c in cells}
    print(f"\nDensity map (rows=z {Z_MAX}..{Z_MIN} top-down, cols=x {X_MIN}..{X_MAX}; "
          f"#=dense +=medium .=sparse ' '=empty):\n")
    for z in reversed(zs):
        line = "".join(_GLYPH.get(grid.get((x, z)), "?") for x in xs)
        print(f"  z{z:>+7} |{line}|")
    print(f"           {' ' * 1}x axis: {X_MIN} .. {X_MAX}")
    print(f"\nSaved {total} cells to {MAP_FILE.name}")
    return 0


def plot_route(base, key, frm, to, timeout, nonce):
    params = {"from": frm, "to": to, "range": 50, "beam_width": nonce}
    url = f"{base}/route?{urllib.parse.urlencode(params)}"
    headers = {"x-route-key": key} if key else {}
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers),
                                    timeout=timeout) as r:
            d = json.load(r)
        return {"status": "ok", "jumps": d.get("total_jumps"),
                "solve_ms": d.get("solve_ms"), "engine": d.get("engine"),
                "wall": time.monotonic() - t0}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        try:
            det = json.loads(body).get("detail")
        except Exception:
            det = None
        st = det.get("error") if isinstance(det, dict) else (
            "not-found" if e.code == 404 else f"http{e.code}")
        return {"status": st, "jumps": None, "wall": time.monotonic() - t0}
    except Exception as e:
        return {"status": f"err:{type(e).__name__}", "jumps": None,
                "wall": time.monotonic() - t0}


def _pick_pairs(cells, n, bidir):
    """Build (from, to, tag) route specs from populated cells.

    `bidir` pairs cross a density boundary (dense<->sparse) and are emitted in
    BOTH directions (tag 'bidir:AB'/'bidir:BA') to isolate the engine direction.
    The rest vary endpoint classes + distance to map region effects.
    """
    import random
    rng = random.Random(42)  # deterministic selection
    named = [c for c in cells if c.get("name") and c["count"] > 0]
    by = {k: [c for c in named if c["cls"] == k] for k in ("dense", "medium", "sparse")}

    def far(a, b, lo=1500, hi=35000):
        d = math.dist((a["x"], a["y"], a["z"]), (b["x"], b["y"], b["z"]))
        return lo <= d <= hi

    specs, seen = [], set()
    # Bidirectional dense<->sparse pairs.
    tries = 0
    while sum(1 for s in specs if s[2].startswith("bidir")) < bidir * 2 and tries < 4000:
        tries += 1
        d = rng.choice(by["dense"] or named)
        s = rng.choice(by["sparse"] or named)
        if not far(d, s) or (d["name"], s["name"]) in seen:
            continue
        seen.add((d["name"], s["name"]))
        dist = round(math.dist((d["x"], d["y"], d["z"]), (s["x"], s["y"], s["z"])))
        specs.append((d["name"], s["name"], "bidir:dense->sparse", dist, d["cls"], s["cls"]))
        specs.append((s["name"], d["name"], "bidir:sparse->dense", dist, s["cls"], d["cls"]))
    # Fill the rest with varied class pairs.
    tries = 0
    while len(specs) < n and tries < 6000:
        tries += 1
        a, b = rng.choice(named), rng.choice(named)
        if a is b or not far(a, b) or (a["name"], b["name"]) in seen:
            continue
        seen.add((a["name"], b["name"]))
        dist = round(math.dist((a["x"], a["y"], a["z"]), (b["x"], b["y"], b["z"])))
        specs.append((a["name"], b["name"], f"{a['cls']}->{b['cls']}", dist, a["cls"], b["cls"]))
    return specs[:max(n, bidir * 2)]


def cmd_route_sample(a):
    if not MAP_FILE.exists():
        print(f"No {MAP_FILE.name}; run `density-map` first.", file=sys.stderr)
        return 2
    cells = json.loads(MAP_FILE.read_text())["cells"]
    specs = _pick_pairs(cells, a.n, a.bidir)
    print(f"Route sample: {len(specs)} solves -> {a.base_url} (cache-busted)\n")
    print(f"  {'tag':22}{'from -> to':44}{'dist':>7}{'jumps':>7}{'solve':>8}{'engine':>7}  status")
    print("  " + "-" * 104)
    results = []
    for i, (frm, to, tag, dist, fc, tc) in enumerate(specs):
        # beam_width is capped at 100 and only affects the beam fallback (graph
        # ignores it); a per-route value in 1..100 busts the cache since each
        # (from,to) is distinct.
        r = plot_route(a.base_url, a.key, frm, to, a.timeout, 1 + (i % 100))
        results.append({"from": frm, "to": to, "tag": tag, "dist": dist,
                        "from_cls": fc, "to_cls": tc, **r})
        ms = f"{r.get('solve_ms') or r['wall']*1000:.0f}ms" if r["status"] == "ok" else "-"
        pair = f"{frm[:20]} -> {to[:20]}"
        print(f"  {tag:22}{pair:44}{dist:>7}{str(r['jumps'] or '-'):>7}{ms:>8}"
              f"{str(r.get('engine') or '-'):>7}  {r['status']}")
    (HERE / "galaxy_route_sample.json").write_text(json.dumps(results, indent=1) + "\n")

    # Analysis.
    ok = [r for r in results if r["status"] == "ok" and r.get("solve_ms")]
    print("\n--- directional asymmetry (same pair, both ways) ---")
    ab = {(r["from"], r["to"]): r for r in ok if r["tag"].startswith("bidir")}
    for (f, t), r in ab.items():
        rev = ab.get((t, f))
        if rev and r["tag"].endswith("dense->sparse"):
            print(f"  {f[:18]:18} <-> {t[:18]:18}  into-sparse {r['solve_ms']:>7.0f}ms "
                  f"({r['jumps']}j)  into-dense {rev['solve_ms']:>7.0f}ms ({rev['jumps']}j)")
    print("\n--- median solve_ms by destination density ---")
    from statistics import median
    for cls in ("dense", "medium", "sparse"):
        g = [r["solve_ms"] for r in ok if r["to_cls"] == cls]
        if g:
            print(f"  -> {cls:7}: n={len(g):2}  median {median(g):>7.0f}ms  max {max(g):>7.0f}ms")
    fails = [r for r in results if r["status"] != "ok"]
    print(f"\n{len(ok)} solved, {len(fails)} failed/void-stuck. Saved galaxy_route_sample.json")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    dm = sub.add_parser("density-map")
    dm.add_argument("--base-url", default="https://edgis.elitedangereuse.fr")
    dm.add_argument("--step", type=int, default=4000)
    dm.add_argument("--radius", type=float, default=40.0)
    dm.add_argument("--limit", type=int, default=150)
    dm.add_argument("--y", type=int, default=0)
    dm.set_defaults(func=cmd_density_map)
    rs = sub.add_parser("route-sample")
    rs.add_argument("--base-url", default="https://edgis.elitedangereuse.fr")
    rs.add_argument("--key", default="")
    rs.add_argument("--n", type=int, default=40)
    rs.add_argument("--bidir", type=int, default=12)
    rs.add_argument("--timeout", type=float, default=120.0)
    rs.set_defaults(func=cmd_route_sample)
    a = ap.parse_args()
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
