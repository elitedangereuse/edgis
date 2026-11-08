#!/usr/bin/env python3
"""Bidirectional quality-vs-spansh check: plot a small set of pairs BOTH ways via
our /route and spansh's /spansh/route, and report the margin per direction +
whether the win/loss FLIPS by direction.

DELIBERATELY SMALL (go-easy-on-spansh): a handful of marginal pairs, both ways,
cached by the server. Do not expand into a bulk loop without sign-off.

  python spansh_bidir.py --base-url https://edgis.elitedangereuse.fr --key <KEY>
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# Marginal / known-weak pairs where a directional flip is most plausible.
PAIRS = [
    ("Lave", "Maia", 20),
    ("Cubeo", "Sagittarius A*", 50),
    ("Col 173 Sector LJ-F c12-0", "HIP 26176", 50),
    ("Beagle Point", "Colonia", 80),
    ("Sol", "Colonia", 50),
]


def _jumps(base, path, frm, to, rng, key, timeout):
    q = urllib.parse.urlencode({"from": frm, "to": to, "range": rng,
                                **({"efficiency": 60} if "spansh" in path else {})})
    req = urllib.request.Request(f"{base}{path}?{q}",
                                 headers={"x-route-key": key} if key else {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r).get("total_jumps"), None
    except urllib.error.HTTPError as e:
        try:
            det = json.loads(e.read()).get("detail")
        except Exception:
            det = None
        return None, (det.get("error") if isinstance(det, dict) else f"http{e.code}")
    except Exception as e:
        return None, type(e).__name__


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="https://edgis.elitedangereuse.fr")
    ap.add_argument("--key", default="")
    ap.add_argument("--timeout", type=float, default=120.0)
    a = ap.parse_args()
    print(f"Bidirectional vs spansh ({len(PAIRS)} pairs, both ways) -> {a.base_url}\n")
    for frm, to, rng in PAIRS:
        res = {}
        for x, y in ((frm, to), (to, frm)):
            oj, oe = _jumps(a.base_url, "/route", x, y, rng, a.key, a.timeout)
            sj, se = _jumps(a.base_url, "/spansh/route", x, y, rng, a.key, a.timeout)
            res[(x, y)] = (oj, oe, sj, se)
            m = (f"{oj - sj:+d}" if oj and sj else "-")
            tag = ("WIN" if oj and sj and oj < sj else "LOSS" if oj and sj and oj > sj
                   else "TIE" if oj and sj else (oe or se or "-"))
            print(f"  {x[:26]+' -> '+y[:24]:54} ours={str(oj or oe):>8}  spansh={str(sj or se):>8}  {tag} {m}")
        # Flip detection.
        (o1, _, s1, _) = res[(frm, to)]
        (o2, _, s2, _) = res[(to, frm)]
        if all(v is not None for v in (o1, s1, o2, s2)):
            d1, d2 = o1 - s1, o2 - s2
            if (d1 < 0) != (d2 < 0) and d1 and d2:
                print(f"    *** FLIP: {frm}->{to} {'win' if d1<0 else 'loss'} "
                      f"but {to}->{frm} {'win' if d2<0 else 'loss'} ***")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
