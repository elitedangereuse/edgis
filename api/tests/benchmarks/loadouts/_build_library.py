#!/usr/bin/env python3
"""Generate a benchmark library of exploration-ship loadouts.

Produces Journal-`Loadout`-shaped JSON (what `fsd.parse_loadout` reads) for a
spread of exploration ships, spanning the planner-relevant axes: jump range,
tank capacity in boosted jumps (small -> large), and neutron supercharge (4x
vs the Caspian Mk II's 6x). The ship list + target ranges mirror the community
meta at siriuscorp.cc/guides/exploration.html and
pilotstradenetwork.com/ship-builds/exploration/.

These are REPRESENTATIVE profiles, not exact community loadouts: hull masses are
approximate and the engineered FSD optimal-mass is calibrated here so the
computed full-tank range matches the published number. That makes the three
planner inputs correct (range, tank_jumps, reach-growth) without needing each
site's exact module list. For an EXACT build, paste its EDSY URL into the
plotter UI (which exports SLEF) and drop the JSON in next to these.

`caspian.json` and the live cobra (not regenerated here) are the two exact,
game-sourced loadouts; everything else is generated.

Run:  python3 _build_library.py        # writes <ship>.json + prints a report
"""
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE.parents[3]))  # repo root for `api` package
from api import fsd

# ship, journal Ship name, FSD item (FSD_TABLE key), approx unladen mass (t),
# main tank (t), guardian booster size (0 = none), target full-tank range (ly).
# Ranges from the community pages (engineered columns).
SHIPS = [
    ("diamondback_explorer", "diamondbackxl",
     "int_hyperdrive_overcharge_size5_class5", 190.0, 16.0, 5, 71.0),
    ("asp_explorer", "asp",
     "int_hyperdrive_overcharge_size5_class5", 280.0, 32.0, 5, 66.0),
    ("krait_phantom", "krait_light",
     "int_hyperdrive_overcharge_size5_class5", 270.0, 32.0, 5, 65.0),
    ("mandalay", "mandalay",
     "int_hyperdrive_overcharge_size5_class5", 230.0, 32.0, 5, 79.0),
    ("anaconda", "anaconda",
     "int_hyperdrive_overcharge_size6_class5", 400.0, 32.0, 5, 67.0),
    ("beluga", "belugaliner",
     "int_hyperdrive_overcharge_size5_class5", 520.0, 128.0, 5, 51.0),
    # Cobra Mk V: a size-4 FSD / 32t tank => ~10-jump tank, 4x. Representative
    # stand-in for the common live cobra build (not a personal loadout).
    ("cobra_mkv", "cobramkv",
     "int_hyperdrive_overcharge_size4_class5", 180.0, 32.0, 0, 67.7),
]


def calibrate_optmass(item, unladen, tank, gboost, target):
    """Pick the engineered FSDOptimalMass so max_range_full == target.
    jump_range is linear in optimal_mass, so one evaluation + scale is exact."""
    base = dict(zip(("fm", "fp", "mf", "om"), fsd.FSD_TABLE[item]))
    # Build a probe FuelParams at the base optimal mass, read its full-tank range
    # (minus the flat guardian boost, which is additive), then scale optimal mass.
    probe = fsd.FuelParams(
        fuel_power=base["fp"], fuel_multiplier=base["fm"],
        optimal_mass=base["om"], base_mass=unladen,
        max_fuel_per_jump=base["mf"], range_boost=0.0,
        tank_size=tank, reserve_size=0.0, has_scoop=True, max_jump_range=0.0,
    )
    r_base = probe.max_range_full()
    gb = fsd.GUARDIAN_BOOSTER.get(gboost, 0.0)
    # target = (optmass_scaled/base_optmass)*r_base + gb  ->  solve the scale.
    scale = (target - gb) / r_base
    return base["om"] * scale, base["mf"], gb


def build(spec):
    key, ship, item, unladen, tank, gboost, target = spec
    optmass, maxfuel, gb = calibrate_optmass(item, unladen, tank, gboost, target)
    modules = [
        {"Slot": "FrameShiftDrive", "Item": item,
         "Engineering": {"Modifiers": [
             {"Label": "FSDOptimalMass", "Value": round(optmass, 2)}]}},
    ]
    if gboost:
        modules.append({"Slot": "GuardianFSDBooster",
                        "Item": f"int_guardianfsdbooster_size{gboost}"})
    modules.append({"Slot": "FuelScoop", "Item": "int_fuelscoop_size6_class5"})
    loadout = {
        "Ship": ship,
        "ShipName": f"bench {key}",
        "UnladenMass": unladen,
        "FuelCapacity": {"Main": tank, "Reserve": 0.5},
        "MaxJumpRange": 0.0,  # filled below from the parsed params
        "Modules": modules,
    }
    params = fsd.parse_loadout(json.dumps(loadout), 0.0)
    loadout["MaxJumpRange"] = round(params.max_range_full(), 5)
    return loadout, params


def main():
    print(f"{'ship':22}{'range':>7}{'tank_j':>8}{'reach0':>8}{'reachN':>8}"
          f"{'growth':>8}{'6x?':>5}")
    for spec in SHIPS:
        loadout, params = build(spec)
        reach = fsd.reach_profile(params, fsd.detect_supercharge(json.dumps(loadout)))
        tj = fsd.tank_capacity_jumps(params)
        sc6 = fsd.detect_supercharge(json.dumps(loadout)) == 6.0
        growth = (reach[-1] / reach[0]) if reach else 1.0
        (HERE / f"{spec[0]}.json").write_text(json.dumps(loadout, indent=2) + "\n")
        print(f"{spec[0]:22}{params.max_range_full():7.1f}{tj:8d}"
              f"{(reach[0] if reach else 0):8.0f}{(reach[-1] if reach else 0):8.0f}"
              f"{growth:8.2f}{('yes' if sc6 else 'no'):>5}")


if __name__ == "__main__":
    main()
