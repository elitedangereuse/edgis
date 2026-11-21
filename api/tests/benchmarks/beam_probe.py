#!/usr/bin/env python3
"""Probe spansh's BEAM exact-plotter algorithm vs optimistic vs EDGIS.

spansh reworked its exact plotter 2026-11-20 into two algorithms: `optimistic`
(anytime A*, the historical comparison) and `beam` (reported stronger on the
longest routes, e.g. Sol -> Beagle Point). This probe runs a SMALL, fixed case
set across the regimes where a beam search is structurally strong or weak, and
-- the interesting part -- replays every spansh route through OUR fuel model
(fsd.simulate_fuel) to surface fuel weaknesses their jump counts hide:
stranding, over-range jumps (planned at a fuel level the tank can't fly),
tight margins.

DELIBERATELY SMALL (go-easy-on-spansh): 6 cases x 2 algorithms, sequential,
results cached on disk so a re-run never re-queries spansh. Do not expand
into a loop without sign-off.

  python beam_probe.py --key <KEY> [--cases sol-beagle,colonia-sol] [--fresh]

Regimes probed:
  sol-beagle / beagle-sol   longest route in the galaxy, sparse rim end, both
                            directions (direction asymmetry)
  colonia-sol               long DENSE corridor (optimistic's home turf)
  blaa-lyaiso               the 2026-11-21 user trace route (far-side f-mass)
  sgra-clookae              scoopable-SPARSE stretch (the fuel-reserve strand
                            case): does a jump-minimizing beam strand?
  dhathaarib-synairaae      ex-void-stuck sparse rim (neutron plotter failed
                            here entirely): frontier pruning vs detours
"""
import argparse
import json
import pathlib
import sys
import time
import urllib.error
import urllib.request

HERE = pathlib.Path(__file__).parent
REPO_ROOT = HERE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from api import fsd  # noqa: E402  (fuel model for replaying spansh routes)

CASES = [
    ("sol-beagle", "Sol", "Beagle Point"),
    ("beagle-sol", "Beagle Point", "Sol"),
    ("colonia-sol", "Colonia", "Sol"),
    ("blaa-lyaiso", "Blaa Eork DH-M d7-2", "Lyaiso IM-W f1-2"),
    ("sgra-clookae", "Sagittarius A*", "Clookae AA-A h69"),
    ("dhathaarib-synairaae", "Dhathaarib", "Synairaae FG-Y f0"),
]
ALGOS = ("optimistic", "beam")


def _post(base, path, body, key, timeout):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{base}{path}", data=data,
        headers={"Content-Type": "application/json",
                 **({"x-route-key": key} if key else {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r), None
    except urllib.error.HTTPError as e:
        try:
            det = json.loads(e.read()).get("detail")
        except Exception:
            det = None
        return None, (det if isinstance(det, str) else f"http{e.code}")
    except Exception as e:
        return None, type(e).__name__


def analyze_spansh(result, params, boost_mult):
    """Validate spansh's OWN per-row fuel ledger against OUR physics.

    Row 0 is the source; jump i goes row[i-1] -> row[i], supercharged when it
    LEAVES a neutron (row[i-1].has_neutron). Their fuel_in_tank is after the
    burn and any refuel, so fuel before jump i is fuel_in_tank[i-1]. spansh
    plans LAZY refuels (refuel_every_scoopable=0), so replaying with our eager
    simulate_fuel would brim the tank where they stayed deliberately light and
    mis-flag their long late-tank hops; instead we check, at THEIR fuel levels:
      - reachability: every jump within our jump_range(fuel) x boost;
      - ledger sanity: tank never negative, burns within max_fuel_per_jump;
      - model agreement: their fuel_used vs our fuel_cost at the same state.
    """
    rows = result.get("jumps") or []
    reach_viol = []       # (jump#, dist, our reach at their fuel)
    burn_delta = 0.0      # worst |their burn - our burn| (model agreement)
    min_fuel = None
    refuels = 0
    dry = 0
    max_dry = 0
    boosted = []
    for i in range(1, len(rows)):
        prev, cur = rows[i - 1], rows[i]
        d = float(cur.get("distance") or 0.0)
        fuel_before = float(prev.get("fuel_in_tank") or 0.0)
        is_boost = bool(prev.get("has_neutron"))
        if is_boost:
            boosted.append(d)
        reach = params.jump_range(fuel_before) * (boost_mult if is_boost else 1.0)
        if d > reach + 0.5:
            reach_viol.append((i, round(d, 1), round(reach, 1)))
        their_burn = float(cur.get("fuel_used") or 0.0)
        our_burn = min(params.fuel_cost(d, fuel_before), params.max_fuel_per_jump)
        burn_delta = max(burn_delta, abs(their_burn - our_burn))
        after = float(cur.get("fuel_in_tank") or 0.0)
        min_fuel = after if min_fuel is None else min(min_fuel, after)
        if cur.get("must_refuel"):
            refuels += 1
            dry = 0
        else:
            dry += 1
            max_dry = max(max_dry, dry)
    return {
        "jumps": max(0, len(rows) - 1),
        "reach_violations": reach_viol[:5],
        "n_reach_violations": len(reach_viol),
        "burn_model_delta_t": round(burn_delta, 3),
        "min_fuel": round(min_fuel, 2) if min_fuel is not None else None,
        "max_dry_jumps": max_dry,
        "refuels": refuels,
        "scoopable_passed": sum(1 for r in rows[1:] if r.get("is_scoopable")),
        "longest_boosted_ly": round(max(boosted), 1) if boosted else None,
        "duration_s": result.get("duration_s"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="https://edgis.elitedangereuse.fr")
    ap.add_argument("--key", default="")
    ap.add_argument("--loadout", default=str(HERE / "loadouts/caspian_th10e.json"))
    ap.add_argument("--cases", default="",
                    help="comma-separated case names (default: all)")
    ap.add_argument("--out", default=str(HERE / "beam_probe_results"))
    ap.add_argument("--timeout", type=float, default=220.0)
    ap.add_argument("--fresh", action="store_true",
                    help="re-query even when a cached result file exists")
    a = ap.parse_args()

    loadout = pathlib.Path(a.loadout).read_text()
    params = fsd.parse_loadout(loadout)
    if params is None:
        print("could not parse the loadout", file=sys.stderr)
        return 2
    boost_mult = fsd.detect_supercharge(loadout)
    # Our /route defaults range to 50 when unset; the exact plotter plans at the
    # SHIP's range, so pass it (full-tank range, same number the UI auto-fills).
    ship_range = round(params.max_range_full(), 2)
    outdir = pathlib.Path(a.out)
    outdir.mkdir(exist_ok=True)
    wanted = {c.strip() for c in a.cases.split(",") if c.strip()}
    cases = [c for c in CASES if not wanted or c[0] in wanted]
    print(f"beam probe: {len(cases)} cases, ship range {ship_range} ly, tank "
          f"{params.tank_size:g}t, supercharge {boost_mult:g}x -> {a.base_url}\n")

    rows = []
    for name, frm, to in cases:
        row = {"case": name}
        # ours (fuel-aware POST; enforcement always on)
        f = outdir / f"{name}_ours.json"
        if f.exists() and not a.fresh:
            ours = json.loads(f.read_text())
        else:
            ours, err = _post(a.base_url, "/route",
                              {"from": frm, "to": to, "loadout": loadout,
                               "range": ship_range},
                              a.key, a.timeout)
            if ours:
                f.write_text(json.dumps(ours))
            else:
                print(f"  {name}: ours FAILED ({err})")
        if ours:
            fu = ours.get("fuel") or {}
            row["ours"] = ours.get("total_jumps")
            row["ours_safe"] = fu.get("safe")
            row["ours_scoop_stops"] = fu.get("scoop_stops")
        # spansh, both algorithms, sequential + cached
        for algo in ALGOS:
            f = outdir / f"{name}_{algo}.json"
            if f.exists() and not a.fresh:
                sp = json.loads(f.read_text())
            else:
                sp, err = _post(a.base_url, "/spansh/galaxy-route",
                                {"from": frm, "to": to, "loadout": loadout,
                                 "algorithm": algo, "include_jumps": True},
                                a.key, a.timeout)
                if sp:
                    f.write_text(json.dumps(sp))
                else:
                    print(f"  {name}: spansh {algo} FAILED ({err})")
                time.sleep(5)  # polite spacing between spansh jobs
            if sp:
                row[algo] = analyze_spansh(sp, params, boost_mult)
        rows.append(row)
        o = row.get("ours")
        po = (row.get("optimistic") or {}).get("jumps")
        pb = (row.get("beam") or {}).get("jumps")
        print(f"  {name:22} ours={o} opt={po} beam={pb}")

    print("\n=== summary (spansh fuel ledgers validated against OUR physics) ===")
    for r in rows:
        print(f"\n{r['case']}: ours {r.get('ours')} "
              f"(safe={r.get('ours_safe')}, +{r.get('ours_scoop_stops') or 0} scoop stops)")
        for algo in ALGOS:
            d = r.get(algo)
            if not d:
                continue
            flags = []
            if d["n_reach_violations"]:
                flags.append(f"UNFLYABLE: {d['n_reach_violations']} jumps beyond "
                             f"reach at their fuel, first {d['reach_violations'][0]}")
            if d["min_fuel"] is not None and d["min_fuel"] < params.max_fuel_per_jump:
                flags.append(f"thin margin (min fuel {d['min_fuel']}t "
                             f"< one jump {params.max_fuel_per_jump:g}t)")
            if d["burn_model_delta_t"] > 0.3:
                flags.append(f"fuel model diverges (worst {d['burn_model_delta_t']}t/jump)")
            print(f"  {algo:10} {d['jumps']:4} jumps  refuels {d['refuels']:3}"
                  f"/{d['scoopable_passed']:3} scoopables  max_dry {d['max_dry_jumps']:2}  "
                  f"longest boost {d['longest_boosted_ly']} ly  "
                  f"solve {d['duration_s']}s  "
                  f"{'; '.join(flags) if flags else 'flyable per our physics'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
