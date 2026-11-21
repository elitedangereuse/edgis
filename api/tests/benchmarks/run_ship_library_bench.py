#!/usr/bin/env python3
"""Benchmark the fuel-aware planner across the exploration-ship library.

Ship-centric companion to run_plotter_bench.py (which is route-centric). For a
given route it plots EVERY loadout in loadouts/ at that ship's own range, with
range growth (B2) on and off, and prints jumps / safe / solve time plus the
B2-vs-B1 delta. That surfaces where B2 helps, ties, or regresses as a function
of ship profile (tank size, reach growth, supercharge) -- the axis the cobra
mkV regression lived on.

Run:
  python run_ship_library_bench.py --key <KEY> \
      --base-url http://127.0.0.1:8384 --from "76 Sigma Ceti" --to "V2079 Orionis"
Default route is the short bubble hop where B2 regressed the cobra.
"""
import argparse
import json
import pathlib
import time
import urllib.request

HERE = pathlib.Path(__file__).parent
LOADOUTS = HERE / "loadouts"


def solve(base_url, key, frm, to, rng, loadout, rg, nonce):
    body = {"from": frm, "to": to, "range": rng, "cargo": 0,
            "range_growth": rg, "loadout": loadout,
            "beam_width": nonce}
    req = urllib.request.Request(
        f"{base_url}/route", data=json.dumps(body).encode(),
        headers={"x-route-key": key, "Content-Type": "application/json"})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=400) as r:
            d = json.load(r)
        f = d.get("fuel") or {}
        return d.get("total_jumps"), f.get("safe"), round(time.monotonic() - t0, 1)
    except Exception as exc:
        return None, f"err:{type(exc).__name__}", round(time.monotonic() - t0, 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8384")
    ap.add_argument("--key", default="")
    ap.add_argument("--from", dest="frm", default="76 Sigma Ceti")
    ap.add_argument("--to", default="V2079 Orionis")
    ap.add_argument("--ships", default="", help="comma-filter loadout filenames")
    args = ap.parse_args()

    files = sorted(p for p in LOADOUTS.glob("*.json"))
    if args.ships:
        want = tuple(s.strip() for s in args.ships.split(","))
        files = [p for p in files if p.stem in want]

    print(f"Ship-library bench: {args.frm} -> {args.to}  ({len(files)} ships)\n")
    print(f"  {'ship':22}{'range':>6}{'B1 j':>6}{'B1 s':>6}{'B2 j':>6}{'B2 s':>6}"
          f"{'delta':>7}{'B1 t':>7}{'B2 t':>7}")
    print("  " + "-" * 73)
    for i, p in enumerate(files):
        loadout = p.read_text()
        try:
            d = json.loads(loadout)
            rng = round(float((d.get("data") or d).get("MaxJumpRange") or 50), 2)
        except Exception:
            rng = 50.0
        b1j, b1s, b1t = solve(args.base_url, args.key, args.frm, args.to, rng,
                              loadout, False, 1 + (2 * i) % 100)
        b2j, b2s, b2t = solve(args.base_url, args.key, args.frm, args.to, rng,
                              loadout, True, 2 + (2 * i) % 100)
        delta = (b2j - b1j) if (isinstance(b1j, int) and isinstance(b2j, int)) else "?"
        ds = f"{delta:+d}" if isinstance(delta, int) else str(delta)
        flag = "  <-- B2 WORSE" if (isinstance(delta, int) and delta > 0) else (
               "  (B2 wins)" if (isinstance(delta, int) and delta < 0) else "")
        print(f"  {p.stem:22}{rng:6.0f}{str(b1j):>6}{str(b1s)[:5]:>6}"
              f"{str(b2j):>6}{str(b2s)[:5]:>6}{ds:>7}{b1t:7.1f}{b2t:7.1f}{flag}")


if __name__ == "__main__":
    main()
