"""Elite Dangerous FSD fuel + jump-range model, for fuel-aware routing.

Jump range grows as the tank empties (lighter ship). Formula, validated against
EDSY/spansh for a Caspian-class build (max range 67.7 ly):

    range(fuel) = optimal_mass / (base_mass + fuel)
                  * (max_fuel_per_jump / fuel_multiplier) ** (1 / fuel_power)
                  + range_boost                       # Guardian FSD booster (flat)

    fuel_cost(d, fuel) = fuel_multiplier
                  * ((max(0, d - range_boost) * (base_mass + fuel)) / optimal_mass)
                    ** fuel_power                      # capped at max_fuel_per_jump

Base FSD constants (fuelmul, fuelpower, maxfuel, optmass) are the Coriolis module
table extracted from spansh's exact-plotter bundle; Guardian FSD booster boosts
are the jumpboost-by-size values. ``parse_loadout`` turns an EDSY/SLEF Loadout
(Journal shape) into a ``FuelParams`` the router can use.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

# symbol(lowercase) -> [fuel_multiplier, fuel_power, max_fuel_per_jump, optimal_mass]
FSD_TABLE = {
    'int_hyperdrive_overcharge_size2_class1': [0.008, 2.0, 0.6, 60.0],
    'int_hyperdrive_overcharge_size2_class2': [0.012, 2.0, 0.9, 90.0],
    'int_hyperdrive_overcharge_size2_class3': [0.012, 2.0, 0.9, 90.0],
    'int_hyperdrive_overcharge_size2_class4': [0.012, 2.0, 0.9, 90.0],
    'int_hyperdrive_overcharge_size2_class5': [0.013, 2.0, 1.0, 100.0],
    'int_hyperdrive_overcharge_size3_class1': [0.008, 2.15, 1.2, 100.0],
    'int_hyperdrive_overcharge_size3_class2': [0.012, 2.15, 1.8, 150.0],
    'int_hyperdrive_overcharge_size3_class3': [0.012, 2.15, 1.8, 150.0],
    'int_hyperdrive_overcharge_size3_class4': [0.012, 2.15, 1.8, 150.0],
    'int_hyperdrive_overcharge_size3_class5': [0.013, 2.15, 1.9, 167.0],
    'int_hyperdrive_overcharge_size4_class1': [0.008, 2.3, 2.0, 350.0],
    'int_hyperdrive_overcharge_size4_class2': [0.012, 2.3, 3.0, 525.0],
    'int_hyperdrive_overcharge_size4_class3': [0.012, 2.3, 3.0, 525.0],
    'int_hyperdrive_overcharge_size4_class4': [0.012, 2.3, 3.0, 525.0],
    'int_hyperdrive_overcharge_size4_class5': [0.013, 2.3, 3.2, 585.0],
    'int_hyperdrive_overcharge_size5_class1': [0.008, 2.45, 3.3, 700.0],
    'int_hyperdrive_overcharge_size5_class2': [0.012, 2.45, 5.0, 1050.0],
    'int_hyperdrive_overcharge_size5_class3': [0.012, 2.45, 5.0, 1050.0],
    'int_hyperdrive_overcharge_size5_class4': [0.012, 2.45, 5.0, 1050.0],
    'int_hyperdrive_overcharge_size5_class5': [0.013, 2.45, 5.2, 1175.0],
    'int_hyperdrive_overcharge_size6_class1': [0.008, 2.6, 5.3, 1200.0],
    'int_hyperdrive_overcharge_size6_class2': [0.012, 2.6, 8.0, 1800.0],
    'int_hyperdrive_overcharge_size6_class3': [0.012, 2.6, 8.0, 1800.0],
    'int_hyperdrive_overcharge_size6_class4': [0.012, 2.6, 8.0, 1800.0],
    'int_hyperdrive_overcharge_size6_class5': [0.013, 2.6, 8.3, 2.0],
    'int_hyperdrive_overcharge_size7_class1': [0.008, 2.75, 8.5, 1800.0],
    'int_hyperdrive_overcharge_size7_class2': [0.012, 2.75, 12.8, 2700.0],
    'int_hyperdrive_overcharge_size7_class3': [0.012, 2.75, 12.8, 2700.0],
    'int_hyperdrive_overcharge_size7_class4': [0.012, 2.75, 12.8, 2700.0],
    'int_hyperdrive_overcharge_size7_class5': [0.013, 2.75, 13.1, 3.0],
    'int_hyperdrive_overcharge_size8_class1': [0.008, 2.9, 13.6, 2800.0],
    'int_hyperdrive_overcharge_size8_class2': [0.012, 2.9, 20.4, 4200.0],
    'int_hyperdrive_overcharge_size8_class3': [0.012, 2.9, 20.4, 4200.0],
    'int_hyperdrive_overcharge_size8_class4': [0.012, 2.9, 20.4, 4200.0],
    'int_hyperdrive_overcharge_size8_class5': [0.013, 2.9, 20.7, 4670.0],
    'int_hyperdrive_overcharge_size8_class5_overchargebooster_mkii': [0.011, 2.5025, 6.8, 4670.0],
    'int_hyperdrive_size2_class1': [0.011, 2.0, 0.6, 48.0],
    'int_hyperdrive_size2_class2': [0.01, 2.0, 0.6, 54.0],
    'int_hyperdrive_size2_class3': [0.008, 2.0, 0.6, 60.0],
    'int_hyperdrive_size2_class4': [0.01, 2.0, 0.8, 75.0],
    'int_hyperdrive_size2_class5': [0.012, 2.0, 0.9, 90.0],
    'int_hyperdrive_size3_class1': [0.011, 2.15, 1.2, 80.0],
    'int_hyperdrive_size3_class2': [0.01, 2.15, 1.2, 90.0],
    'int_hyperdrive_size3_class3': [0.008, 2.15, 1.2, 100.0],
    'int_hyperdrive_size3_class4': [0.01, 2.15, 1.5, 125.0],
    'int_hyperdrive_size3_class5': [0.012, 2.15, 1.8, 150.0],
    'int_hyperdrive_size4_class1': [0.011, 2.3, 2.0, 280.0],
    'int_hyperdrive_size4_class2': [0.01, 2.3, 2.0, 315.0],
    'int_hyperdrive_size4_class3': [0.008, 2.3, 2.0, 350.0],
    'int_hyperdrive_size4_class4': [0.01, 2.3, 2.5, 437.5],
    'int_hyperdrive_size4_class5': [0.012, 2.3, 3.0, 525.0],
    'int_hyperdrive_size5_class1': [0.011, 2.45, 3.3, 560.0],
    'int_hyperdrive_size5_class2': [0.01, 2.45, 3.3, 630.0],
    'int_hyperdrive_size5_class3': [0.008, 2.45, 3.3, 700.0],
    'int_hyperdrive_size5_class4': [0.01, 2.45, 4.1, 875.0],
    'int_hyperdrive_size5_class5': [0.012, 2.45, 5.0, 1050.0],
    'int_hyperdrive_size6_class1': [0.011, 2.6, 5.3, 960.0],
    'int_hyperdrive_size6_class2': [0.01, 2.6, 5.3, 1080.0],
    'int_hyperdrive_size6_class3': [0.008, 2.6, 5.3, 1200.0],
    'int_hyperdrive_size6_class4': [0.01, 2.6, 6.6, 1500.0],
    'int_hyperdrive_size6_class5': [0.012, 2.6, 8.0, 1800.0],
    'int_hyperdrive_size7_class1': [0.011, 2.75, 8.5, 1440.0],
    'int_hyperdrive_size7_class2': [0.01, 2.75, 8.5, 1620.0],
    'int_hyperdrive_size7_class3': [0.008, 2.75, 8.5, 1800.0],
    'int_hyperdrive_size7_class4': [0.01, 2.75, 10.6, 2250.0],
    'int_hyperdrive_size7_class5': [0.012, 2.75, 12.8, 2700.0],
}

# Guardian FSD booster: module size -> flat jump-range boost (ly).
GUARDIAN_BOOSTER = {1: 4.0, 2: 6.0, 3: 7.75, 4: 9.25, 5: 10.5}


@dataclass(frozen=True)
class FuelParams:
    fuel_power: float
    fuel_multiplier: float
    optimal_mass: float
    base_mass: float            # unladen + reserve
    max_fuel_per_jump: float
    range_boost: float          # Guardian booster flat add
    tank_size: float
    reserve_size: float
    has_scoop: bool
    max_jump_range: float       # from the loadout, for display/validation

    def jump_range(self, fuel: float) -> float:
        """Max single-jump distance (ly) at this main-tank fuel level."""
        mass = self.base_mass + max(0.0, fuel)
        base = (self.optimal_mass / mass
                * (self.max_fuel_per_jump / self.fuel_multiplier)
                ** (1.0 / self.fuel_power))
        return base + self.range_boost

    def fuel_cost(self, dist: float, fuel: float) -> float:
        """Fuel (tons) to jump ``dist`` ly at this fuel level (uncapped)."""
        mass = self.base_mass + max(0.0, fuel)
        d = max(0.0, dist - self.range_boost)
        return self.fuel_multiplier * ((d * mass) / self.optimal_mass) ** self.fuel_power

    def max_range_full(self) -> float:
        return self.jump_range(self.tank_size)

    def max_range_light(self) -> float:
        return self.jump_range(self.max_fuel_per_jump)


def _modifier(module: dict, label: str) -> Optional[float]:
    eng = module.get("Engineering") or {}
    for mod in eng.get("Modifiers", []):
        if mod.get("Label") == label:
            return mod.get("Value")
    return None


def parse_loadout(slef) -> Optional[FuelParams]:
    """Parse an EDSY/SLEF export (list wrapper or bare Journal Loadout dict)
    into FuelParams, or None if no FSD is found."""
    if isinstance(slef, str):
        slef = json.loads(slef)
    if isinstance(slef, list):
        slef = slef[0]
    data = slef.get("data", slef) if isinstance(slef, dict) else {}
    modules = data.get("Modules", [])

    fsd_mod = next((m for m in modules if m.get("Slot") == "FrameShiftDrive"), None)
    if not fsd_mod:
        return None
    base = FSD_TABLE.get(str(fsd_mod.get("Item", "")).lower())
    if not base:
        return None
    fuel_multiplier, fuel_power, max_fuel, optmass = base
    optmass = _modifier(fsd_mod, "FSDOptimalMass") or optmass
    max_fuel = _modifier(fsd_mod, "MaxFuelPerJump") or max_fuel

    range_boost = 0.0
    for m in modules:
        item = str(m.get("Item", "")).lower()
        if "guardianfsdbooster" in item:
            try:
                size = int(item.rsplit("size", 1)[1][0])
            except (IndexError, ValueError):
                size = 0
            range_boost = max(range_boost, GUARDIAN_BOOSTER.get(size, 0.0))

    fuel_cap = data.get("FuelCapacity", {}) or {}
    tank = float(fuel_cap.get("Main", 0.0))
    reserve = float(fuel_cap.get("Reserve", 0.0))
    unladen = float(data.get("UnladenMass", 0.0))
    has_scoop = any("fuelscoop" in str(m.get("Item", "")).lower() for m in modules)

    return FuelParams(
        fuel_power=float(fuel_power),
        fuel_multiplier=float(fuel_multiplier),
        optimal_mass=float(optmass),
        base_mass=unladen + reserve,
        max_fuel_per_jump=float(max_fuel),
        range_boost=float(range_boost),
        tank_size=tank,
        reserve_size=reserve,
        has_scoop=has_scoop,
        max_jump_range=float(data.get("MaxJumpRange", 0.0)),
    )


def ship_summary(params: "FuelParams") -> dict:
    """A compact, display-ready view of a parsed loadout for the UI's ship
    import (fills the jump-range field, shows tank/scoop at a glance). The
    plotting range is the laden max (``max_jump_range`` from the loadout) when
    present, else the lightest-practical one-jump range."""
    plot_range = params.max_jump_range or params.max_range_light()
    return {
        "ok": True,
        "jump_range": round(plot_range, 2),
        "max_jump_range": round(params.max_jump_range, 2),
        "max_range_light": round(params.max_range_light(), 2),
        "max_range_full": round(params.max_range_full(), 2),
        "tank_size": params.tank_size,
        "tank_jumps": tank_capacity_jumps(params),
        "has_scoop": params.has_scoop,
        "range_boost": params.range_boost,
    }


def is_scoopable(spectral: Optional[str]) -> bool:
    """KGBFOAM main-sequence stars are fuel-scoopable (matches the UI rule)."""
    return bool(spectral) and str(spectral).strip()[:1].upper() in "KGBFOAM"


def simulate_fuel(jumps, params: "FuelParams", start_fuel: Optional[float] = None):
    """Simulate the tank along a realized route to find fuel stranding.

    Starts full (or ``start_fuel``), burns ``fuel_cost`` per jump, and refuels to
    full when a leg lands on a scoopable KGBFOAM star (neutrons/white dwarfs are
    never scoopable). Returns where the tank runs dry (``stranded_at`` leg index
    or None), the lowest fuel reached, the number of refuels, and the longest run
    of jumps between scoopable refuels. This is the basis for the default-on
    'fuel-safe' guarantee (keep margin to divert to a scoopable star).
    """
    fuel = params.tank_size if start_fuel is None else float(start_fuel)
    stranded_at = None
    fuel_after = []
    dry = 0
    max_dry = 0
    refuels = 0
    for i, j in enumerate(jumps):
        d = float(j.get("distance_jumped", 0.0) or 0.0)
        if d <= 1e-9:
            fuel_after.append(fuel)
            continue
        cost = min(params.fuel_cost(d, fuel), params.max_fuel_per_jump)
        if cost > fuel + 1e-9:
            stranded_at = i
            fuel_after.append(fuel - cost)
            break
        fuel -= cost
        fuel_after.append(fuel)
        if not j.get("neutron_star") and is_scoopable(j.get("spectral")):
            fuel = params.tank_size
            refuels += 1
            dry = 0
        else:
            dry += 1
            max_dry = max(max_dry, dry)
    return {
        "stranded_at": stranded_at,
        "min_fuel": min(fuel_after) if fuel_after else fuel,
        "refuels": refuels,
        "max_dry_jumps": max_dry,
        "final_fuel": fuel,
        "fuel_after": fuel_after,
    }


def tank_capacity_jumps(params: "FuelParams") -> int:
    """Conservative count of consecutive non-scoopable jumps a full tank
    sustains, assuming each burns the worst case (a long boosted jump costs
    ``max_fuel_per_jump``). Real dry runs may go one or two further on short
    jumps; this is the floor used for the 'keep a divert margin' guarantee."""
    if params.max_fuel_per_jump <= 0:
        return 0
    # +epsilon so 32 / 3.2 (= 9.999... in float) floors to 10, not 9.
    return int(params.tank_size / params.max_fuel_per_jump + 1e-9)


def fuel_report(jumps, params: "FuelParams", start_fuel: Optional[float] = None):
    """Summarize a realized route's fuel safety for a given ship.

    Builds on ``simulate_fuel`` (exact per-jump burn) and adds a verdict:
    ``safe`` is True only when the route never strands AND its longest run of
    non-scoopable landings stays within the tank's conservative capacity, i.e.
    the commander can always divert to a scoopable star (the default-on
    fuel-safe margin). Returns the simulation fields plus ``tank_jumps``,
    ``stranded``, and ``safe``; ``fuel_after`` is dropped (per-leg detail the
    UI does not need)."""
    sim = simulate_fuel(jumps, params, start_fuel)
    cap = tank_capacity_jumps(params)
    stranded = sim["stranded_at"] is not None
    safe = (not stranded) and (cap <= 0 or sim["max_dry_jumps"] < cap)
    return {
        "safe": safe,
        "stranded": stranded,
        "stranded_at": sim["stranded_at"],
        "refuels": sim["refuels"],
        "max_dry_jumps": sim["max_dry_jumps"],
        "tank_jumps": cap,
        "min_fuel": round(sim["min_fuel"], 2),
        "final_fuel": round(sim["final_fuel"], 2),
        "tank_size": params.tank_size,
        "has_scoop": params.has_scoop,
    }
