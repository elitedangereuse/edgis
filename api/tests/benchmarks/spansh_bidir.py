#!/usr/bin/env python3
"""Bidirectional quality check vs spansh's EXACT (Galaxy) Plotter.

We measure against the exact plotter, NOT the neutron one -- it is the real,
fuel-aware competitor. Both sides therefore need a ship: our fuel-aware
POST /route vs POST /spansh/galaxy-route, same loadout. Reports the per-direction
margin and flags any win/loss FLIP by direction.

DELIBERATELY SMALL (go-easy-on-spansh): a handful of pairs, both ways. The exact
plotter is slow (~1 min/call); do not expand into a bulk loop without sign-off.

  python spansh_bidir.py --key <KEY> --loadout /path/to/ship.slef.json
"""
import argparse
import json
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).parent
# Mid-range pairs where a directional flip is plausible; kept short-ish since the
# exact plotter uses the SHIP's range (a low-range ship on a 20 kly route is huge).
PAIRS = [
    ("Lave", "Maia"),
    ("Sol", "Maia"),
    ("Cubeo", "Col 173 Sector LJ-F c12-0"),
    ("Sol", "Colonia"),
    ("Maia", "Merope"),
]


def _post(base, path, body, key, timeout):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{base}{path}", data=data,
        headers={"Content-Type": "application/json",
                 **({"x-route-key": key} if key else {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r).get("total_jumps"), None
    except urllib.error.HTTPError as e:
        try:
            det = json.loads(e.read()).get("detail")
        except Exception:
            det = None
        return None, (det.get("error") if isinstance(det, dict)
                      else (det if isinstance(det, str) else f"http{e.code}"))
    except Exception as e:
        return None, type(e).__name__


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="https://edgis.elitedangereuse.fr")
    ap.add_argument("--key", default="")
    ap.add_argument("--loadout", default=str(HERE / "../../../live_loadout.json"),
                    help="SLEF/Journal loadout JSON for the reference ship")
    ap.add_argument("--timeout", type=float, default=150.0)
    a = ap.parse_args()

    lp = pathlib.Path(a.loadout)
    if not lp.exists():
        print(f"Reference loadout not found: {lp}\nPass --loadout <ship.json> "
              f"(the exact plotter is fuel-aware and needs a ship).", file=sys.stderr)
        return 2
    loadout = lp.read_text()
    print(f"Bidirectional vs spansh EXACT (Galaxy) Plotter, ship={lp.name}, "
          f"{len(PAIRS)} pairs both ways -> {a.base_url}\n")

    for frm, to in PAIRS:
        res = {}
        for x, y in ((frm, to), (to, frm)):
            oj, oe = _post(a.base_url, "/route",
                           {"from": x, "to": y, "loadout": loadout, "fuel_safe": True},
                           a.key, a.timeout)
            sj, se = _post(a.base_url, "/spansh/galaxy-route",
                           {"from": x, "to": y, "loadout": loadout}, a.key, a.timeout)
            res[(x, y)] = (oj, sj)
            m = f"{oj - sj:+d}" if oj and sj else "-"
            tag = ("WIN" if oj and sj and oj < sj else "LOSS" if oj and sj and oj > sj
                   else "TIE" if oj and sj else (oe or se or "-"))
            print(f"  {x[:26]+' -> '+y[:24]:54} ours={str(oj or oe):>8}  "
                  f"spansh={str(sj or se):>8}  {tag} {m}")
        (o1, s1), (o2, s2) = res[(frm, to)], res[(to, frm)]
        if all(v is not None for v in (o1, s1, o2, s2)):
            d1, d2 = o1 - s1, o2 - s2
            if (d1 < 0) != (d2 < 0) and d1 and d2:
                print(f"    *** FLIP: {frm}->{to} {'win' if d1<0 else 'loss'} "
                      f"but {to}->{frm} {'win' if d2<0 else 'loss'} ***")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
