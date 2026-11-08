#!/usr/bin/env python3
"""Route-plotter QUALITY benchmark.

Re-plots the curated routes in ``route_cases.json`` against a running EDGIS
instance and compares each result to our stored baseline (jump count) and to
spansh's number from the same trace. It does NOT call spansh (the spansh numbers
are static, taken from production route_trace.jsonl), so it never hammers their
API -- see the go-easy-on-spansh rule.

This measures ROUTE QUALITY (jump counts + whether a route solves). For raw
throughput/latency under load, use the k6 scripts in ../k6 instead.

Usage:
  python run_plotter_bench.py --base-url http://127.0.0.1:8383 --key <KEY>
  python run_plotter_bench.py --only win,loss        # category-prefix filter
  python run_plotter_bench.py --no-slow              # skip rim/void-stuck (slow)
  python run_plotter_bench.py --fresh                # bust the route cache (do this
                                                     # after an engine change, or the
                                                     # cache returns old jump counts)
  python run_plotter_bench.py --update               # rewrite 'our' baselines in place

Exit code is non-zero if any regression or unexpected failure is seen (CI-friendly).
"""
import argparse
import json
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).parent
CASES_FILE = HERE / "route_cases.json"
# Categories whose routes re-solve slowly (no cache, or genuinely heavy).
SLOW_CATS = {"win-rim-slow", "void-stuck", "void-stuck-highrange"}


def plot(base_url, key, case, timeout, nonce):
    params = {"from": case["from"], "to": case["to"], "range": case["range"]}
    if nonce is not None:
        # A throwaway beam_width changes the Redis cache key (graph_route ignores
        # it), forcing a fresh solve. The route cache otherwise busts only on
        # neutron-index rebuild, so a code change would serve stale jump counts.
        params["beam_width"] = nonce
    url = f"{base_url}/route?{urllib.parse.urlencode(params)}"
    headers = {"x-route-key": key} if key else {}
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers),
                                    timeout=timeout) as resp:
            data = json.load(resp)
        return {"status": "ok", "jumps": data.get("total_jumps"),
                "wall": time.monotonic() - t0, "solve_ms": data.get("solve_ms")}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "ignore")
        try:
            detail = json.loads(body).get("detail")
        except Exception:
            detail = None
        if isinstance(detail, dict):
            status = detail.get("error", f"http{exc.code}")
        elif exc.code == 404:
            status = "not-found"  # /route 404 detail is a plain string
        elif exc.code == 503:
            status = "warming"
        else:
            status = f"http{exc.code}"
        return {"status": status, "jumps": None, "wall": time.monotonic() - t0}
    except Exception as exc:  # timeout, conn refused, ...
        return {"status": f"err:{type(exc).__name__}", "jumps": None,
                "wall": time.monotonic() - t0}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8383")
    ap.add_argument("--key", default="")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--only", default="", help="comma-separated category prefixes")
    ap.add_argument("--no-slow", action="store_true")
    ap.add_argument("--fresh", action="store_true", help="bust the route cache")
    ap.add_argument("--update", action="store_true", help="rewrite baselines")
    args = ap.parse_args()

    data = json.loads(CASES_FILE.read_text())
    cases = data["cases"]
    if args.only:
        prefixes = tuple(p.strip() for p in args.only.split(","))
        cases = [c for c in cases if c["cat"].startswith(prefixes)]
    if args.no_slow:
        cases = [c for c in cases if c["cat"] not in SLOW_CATS]

    print(f"Route-plotter benchmark: {len(cases)} cases -> {args.base_url}"
          f"{'  (fresh/cache-busted)' if args.fresh else ''}\n")
    header = f"  {'category':22}{'route':26}{'result':>8}  {'verdict':24}{'vs spansh':16}{'time':>7}"
    print(header + "\n  " + "-" * (len(header) - 2))

    regress = improve = fail = 0
    for i, c in enumerate(cases):
        # beam_width is capped at 100; 1..100 is enough to bust the cache since
        # each case is a distinct route.
        nonce = (1 + (i % 100)) if args.fresh else None
        r = plot(args.base_url, args.key, c, args.timeout, nonce)
        expected = c.get("status", "ok")
        our = c.get("our")

        if expected != "ok":
            # Expected to fail (void-stuck / not-found): PASS if it still fails the
            # same way; flag if it now solves (could be an improvement) or differs.
            if r["status"] == expected:
                verdict, result = "match (expected fail)", r["status"]
            elif r["status"] == "ok":
                verdict, result = f"NOW SOLVES ({r['jumps']})", r["jumps"]; improve += 1
            else:
                verdict, result = f"CHANGED -> {r['status']}", r["status"]; fail += 1
        elif r["status"] != "ok":
            verdict, result = f"REGRESSION (now {r['status']})", r["status"]; fail += 1
        elif our is None:
            verdict, result = f"NEW baseline {r['jumps']}", r["jumps"]
        elif r["jumps"] < our:
            verdict, result = f"IMPROVED {our}->{r['jumps']}", r["jumps"]; improve += 1
        elif r["jumps"] > our:
            verdict, result = f"REGRESSION {our}->{r['jumps']}", r["jumps"]; regress += 1
        else:
            verdict, result = "match", r["jumps"]

        vs = ""
        if r["status"] == "ok" and r["jumps"] is not None and c.get("spansh"):
            d = r["jumps"] - c["spansh"]
            vs = f"{c['spansh']} ({'+' if d >= 0 else ''}{d})"
        print(f"  {c['cat']:22}{c['name']:26}{str(result):>8}  {verdict:24}{vs:16}{r['wall']:6.1f}s")

        if args.update and r["status"] == "ok":
            c["our"] = r["jumps"]

    print(f"\nSummary: {regress} regressions, {fail} unexpected failures, "
          f"{improve} improvements, {len(cases)} total.")
    if args.update:
        CASES_FILE.write_text(json.dumps(data, indent=2) + "\n")
        print("Baselines updated in route_cases.json")
    return 1 if (regress or fail) else 0


if __name__ == "__main__":
    sys.exit(main())
