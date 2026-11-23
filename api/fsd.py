"""Elite Dangerous FSD fuel + jump-range model, for fuel-aware routing.

Jump range grows as the tank empties (lighter ship). Formula, validated to 6
decimals against the game's own MaxJumpRange and EDSY's getJumpDistance for a
Cobra Mk V build (max range 67.72 ly). ``base_mass`` is UnladenMass only; the
reserve tank's fuel is not part of the jump mass (the game excludes it):

    range(fuel) = optimal_mass / (base_mass + fuel)
                  * (max_fuel_per_jump / fuel_multiplier) ** (1 / fuel_power)
                  + range_boost                       # Guardian FSD booster (flat)

    fuel_cost(d, fuel, mult) = fuel_multiplier
                  * ((d_eff * (base_mass + fuel)) / optimal_mass) ** fuel_power
    d_eff = d / mult * R / (R + range_boost)           # capped at max_fuel_per_jump
    # mult = supercharge multiplier on boosted legs (1 otherwise); R = raw FSD
    # range at this mass WITHOUT the booster. The booster scales optimal mass,
    # so its share of any jump is fuel-free proportionally (EDSY/spansh-exact
    # convention, ledger-verified 2026-11-21).

Base FSD constants (fuelmul, fuelpower, maxfuel, optmass) are the Coriolis module
table extracted from spansh's exact-plotter bundle; Guardian FSD booster boosts
are the jumpboost-by-size values. ``parse_loadout`` turns an EDSY/SLEF Loadout
(Journal shape) into a ``FuelParams`` the router can use.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Callable, Optional

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
    'int_hyperdrive_overcharge_size6_class5': [0.013, 2.6, 8.3, 2000.0],
    'int_hyperdrive_overcharge_size7_class1': [0.008, 2.75, 8.5, 1800.0],
    'int_hyperdrive_overcharge_size7_class2': [0.012, 2.75, 12.8, 2700.0],
    'int_hyperdrive_overcharge_size7_class3': [0.012, 2.75, 12.8, 2700.0],
    'int_hyperdrive_overcharge_size7_class4': [0.012, 2.75, 12.8, 2700.0],
    'int_hyperdrive_overcharge_size7_class5': [0.013, 2.75, 13.1, 3000.0],
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
    base_mass: float            # UnladenMass only (no reserve -- see _parse_loadout)
    max_fuel_per_jump: float
    range_boost: float          # Guardian booster flat add
    tank_size: float
    reserve_size: float
    has_scoop: bool
    max_jump_range: float       # from the loadout, for display/validation
    cargo: float = 0.0          # cargo tonnage (dynamic; set per-request/plugin)

    def jump_range(self, fuel: float) -> float:
        """Max single-jump distance (ly) at this main-tank fuel level."""
        mass = self.base_mass + self.cargo + max(0.0, fuel)
        base = (self.optimal_mass / mass
                * (self.max_fuel_per_jump / self.fuel_multiplier)
                ** (1.0 / self.fuel_power))
        return base + self.range_boost

    def fuel_cost(self, dist: float, fuel: float,
                  boost_mult: float = 1.0) -> float:
        """Fuel (tons) to jump ``dist`` ly at this fuel level (uncapped).

        Game-accurate (matches EDSY and spansh's exact-plotter ledger, verified
        to ~0.01 t on live results 2026-11-21): a neutron/WD supercharge
        multiplies the whole throw, so the FSD is charged for ``dist /
        boost_mult``; the Guardian booster scales optimal mass, so its share of
        any jump is fuel-free PROPORTIONALLY (``d * R / (R + boost)`` with R
        the raw FSD range at this mass), not as a flat subtraction. The old
        flat ``d - range_boost`` model overcharged partial-throw boosted hops
        to the ``max_fuel_per_jump`` cap (a 144 ly boosted hop billed 6.8 t
        instead of 0.3 t), which made the fuel layer refuel far too early."""
        mass = self.base_mass + self.cargo + max(0.0, fuel)
        d = max(0.0, dist) / max(1.0, boost_mult)
        if self.range_boost > 0.0:
            base = (self.optimal_mass / mass
                    * (self.max_fuel_per_jump / self.fuel_multiplier)
                    ** (1.0 / self.fuel_power))
            if base > 0.0:
                d *= base / (base + self.range_boost)
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


def parse_loadout(slef, cargo: float = 0.0) -> Optional[FuelParams]:
    """Parse an EDSY/SLEF export (list wrapper or bare Journal Loadout dict)
    into FuelParams, or None if it can't be read (bad JSON, no FSD, etc.).
    ``cargo`` (tonnage) is dynamic -- not in the loadout -- so it's passed in
    (manual field or the EDMC plugin) and folded into mass. Never raises on
    malformed input -- callers treat None as 'unreadable'."""
    try:
        return _parse_loadout(slef, cargo)
    except (ValueError, TypeError, KeyError, IndexError, AttributeError):
        return None


def _parse_loadout(slef, cargo: float = 0.0) -> Optional[FuelParams]:
    if isinstance(slef, str):
        slef = json.loads(slef)
    if isinstance(slef, list):
        slef = slef[0]
    data = slef.get("data", slef) if isinstance(slef, dict) else {}
    modules = data.get("Modules", [])

    # EDDiscovery exports slot names lowercased ("frameshiftdrive"), the game
    # journal capitalises them ("FrameShiftDrive"); match either.
    fsd_mod = next(
        (m for m in modules if str(m.get("Slot", "")).lower() == "frameshiftdrive"),
        None,
    )
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

    # Jump mass is UnladenMass + main-tank fuel + cargo. The reserve tank's fuel
    # is NOT counted (verified against the game's own MaxJumpRange field and
    # EDSY's getJumpDistance, which both use mass + main_fuel only). spansh's
    # frontend folds the reserve into base_mass instead; that convention is
    # applied only where we build the spansh comparison request, not here.
    return FuelParams(
        fuel_power=float(fuel_power),
        fuel_multiplier=float(fuel_multiplier),
        optimal_mass=float(optmass),
        base_mass=unladen,
        max_fuel_per_jump=float(max_fuel),
        range_boost=float(range_boost),
        tank_size=tank,
        reserve_size=reserve,
        has_scoop=has_scoop,
        max_jump_range=float(data.get("MaxJumpRange", 0.0)),
        cargo=max(0.0, float(cargo or 0.0)),
    )


def detect_supercharge(slef) -> float:
    """Neutron supercharge multiplier for a loadout: 6x only on the Caspian
    Explorer (``explorer_nx``) with its Mk II overcharge-booster SCO drive, 4x
    otherwise. The drive is matched loosely on purpose: EDSY names it
    ``..._overchargebooster_mkii`` while other exports may not carry that
    suffix, and the Mk II is the ONLY size-8 FSD in the game, so on a Caspian
    a size-8 (or booster/mkii-tagged) drive IS the 6x drive. A Caspian fitted
    with a smaller ordinary drive correctly stays 4x. Never raises."""
    try:
        if isinstance(slef, str):
            slef = json.loads(slef)
        if isinstance(slef, list):
            slef = slef[0]
        data = slef.get("data", slef) if isinstance(slef, dict) else {}
        if str(data.get("Ship", "")).lower() != "explorer_nx":
            return 4.0
        fsd_mod = next(
            (m for m in data.get("Modules", [])
             if str(m.get("Slot", "")).lower() == "frameshiftdrive"),
            None,
        )
        item = str((fsd_mod or {}).get("Item", "")).lower()
        if ("overchargebooster" in item or "mkii" in item
                or "size8" in item):
            return 6.0
        return 4.0
    except (ValueError, TypeError, KeyError, IndexError, AttributeError):
        return 4.0


def to_slef(loadout: str) -> str:
    """Normalize a loadout to SLEF text for spansh's ``ship_build`` field.

    spansh's readLoadout accepts only a coriolis ``$schema`` build or a
    SLEF-wrapped object (``{header, data}``); a bare journal Loadout object has
    no ``header`` and is rejected with "Could not parse ship config" when the
    commander hits Calculate on the shared results page. Our own parser accepts
    all three shapes, so wrap the bare-journal case and pass everything else
    through unchanged (including unparseable text: spansh shows its own error
    there, and mangling the field can't improve on that)."""
    try:
        obj = json.loads(loadout)
    except (ValueError, TypeError):
        return loadout
    if isinstance(obj, dict) and "header" not in obj and "$schema" not in obj \
            and isinstance(obj.get("Modules"), list):
        return json.dumps([{"header": {"appName": "EDGIS"}, "data": obj}])
    return loadout


def ship_summary(params: "FuelParams") -> dict:
    """A compact, display-ready view of a parsed loadout for the UI's ship
    import (fills the jump-range field, shows tank/scoop at a glance).

    The plotting range is the FULL-TANK range (``max_range_full``), not the
    ship's lightest-tank maximum. A route is flown at every fuel level and the
    tank is heaviest -- hence shortest-ranged -- right after a scoop, so
    planning at the max (an only-when-near-empty range) yields jumps that are
    physically unreachable on a full tank. The full-tank range is feasible at
    any fuel level, so routes are always flyable by construction. (Recovering
    the jumps the range-growth would allow -- reach growing as fuel burns -- is
    a separate, optional, fuel-aware optimization.)"""
    plot_range = params.max_range_full()
    return {
        "ok": True,
        "jump_range": round(plot_range, 2),
        "max_jump_range": round(params.max_jump_range, 2),
        "max_range_light": round(params.max_range_light(), 2),
        "max_range_full": round(params.max_range_full(), 2),
        "tank_size": params.tank_size,
        "tank_jumps": tank_capacity_jumps(params),
        # Total ly on one full tank, plain (unboosted) jumps -- "how far can I
        # get before I must refuel". More than tank_jumps * full-tank range
        # because jumps lengthen as fuel burns.
        "tank_range": round(tank_range(params, 1.0), 1),
        "has_scoop": params.has_scoop,
        "range_boost": params.range_boost,
        "cargo": params.cargo,
    }


# The class letter must STAND ALONE (end of string, or followed by a space /
# '_' / '(' / digit), so a black hole ("Black Hole") is not mistaken for a
# B-class star by the bare first-letter rule. Mirrors the UI's
# /^[KGBFOAM]($| |\()/ check. Covers "G", "M (Red dwarf) Star", "A_BlueWhite...".
_SCOOPABLE_RE = re.compile(r"^[KGBFOAM]($|[ _(0-9])", re.IGNORECASE)


def is_scoopable(spectral: Optional[str]) -> bool:
    """KGBFOAM main-sequence stars are fuel-scoopable (matches the UI rule).
    A black hole ("Black Hole") starts with 'B' but is NOT scoopable, so the
    class letter must stand alone rather than begin a word."""
    return bool(spectral) and bool(_SCOOPABLE_RE.match(str(spectral).strip()))


def is_refuel_landing(leg: dict) -> bool:
    """True when the commander can brim the tank at this landing WITHOUT leaving
    the neutron highway. Either the star jumped to is itself scoopable, or it is a
    neutron whose system carries a scoopable companion within supercruise range --
    arrive at the neutron, supercharge, hop in-system to the companion to scoop,
    jump out, chain unbroken. The companion case is flagged ``inflight_scoop`` by
    the DB layer that reads the bodies table (plain-Python fuel functions have no
    DB access, so they trust the annotation)."""
    if leg.get("inflight_scoop"):
        return True
    return (not leg.get("neutron_star")) and is_scoopable(leg.get("spectral"))


def simulate_fuel(jumps, params: "FuelParams", start_fuel: Optional[float] = None,
                  boost_mult: float = 4.0, accurate: bool = False):
    """Simulate the tank along a realized route to find fuel stranding.

    Starts full (or ``start_fuel``), burns ``fuel_cost`` per jump, and refuels to
    full when a leg lands on a scoopable KGBFOAM star (neutrons/white dwarfs are
    never scoopable). Returns where the tank runs dry (``stranded_at`` leg index
    or None), the lowest fuel reached, the number of refuels, and the longest run
    of jumps between scoopable refuels. This is the basis for the default-on
    'fuel-safe' guarantee (keep margin to divert to a scoopable star).

    Also reports ``over_range_at``: the first leg whose distance exceeds the
    range achievable at the current fuel level (``jump_range(fuel)``, times
    ``boost_mult`` on a supercharged leg). A heavier tank jumps shorter, so a
    long bridge jump right after a brim refuel can be physically unmakeable even
    with fuel to spare -- a separate failure from running dry, and one a scoop
    detour makes worse (more fuel = more mass = less range)."""
    fuel = params.tank_size if start_fuel is None else float(start_fuel)
    # The over-range check ALWAYS walks the conservative max-drain tracker,
    # even in accurate mode: the B2 plan allows its long late hops because the
    # max-drained (light) ship reaches them; judging reach at the accurate
    # (fuller, heavier, shorter-ranged) tank false-flags exactly those hops
    # and the caller's fuel-safety guard then re-solves B1 (the 2026-11-21
    # revert, re-hit 2026-11-23: 7 bench regressions, sol-colonia 57->88).
    fuel_cons = fuel
    stranded_at = None
    over_range_at = None
    fuel_after = []
    dry = 0
    max_dry = 0
    refuels = 0
    for i, j in enumerate(jumps):
        d = float(j.get("distance_jumped", 0.0) or 0.0)
        if d <= 1e-9:
            fuel_after.append(fuel)
            continue
        reach = params.jump_range(fuel_cons) * (
            boost_mult if j.get("type") == "boost" else 1.0
        )
        if d > reach + 1e-6 and over_range_at is None:
            over_range_at = i  # unreachable at this fuel level (too heavy)
        # Default: CONSERVATIVE burn (boosted legs billed the full cap, like
        # tank_capacity_jumps/reach_profile assume): the B2 range-growth plan
        # relies on the tank DRAINING at max rate (drained tank = light ship =
        # long reach), so simulating the true partial-throw burns here makes
        # the tank read fuller/heavier than planned and flags B2's long late
        # hops over-range -- the guard then discards good routes (bench: 6
        # regressions + 2 timeouts). The B2-unsafe guard therefore stays on
        # the max-burn model. ``accurate=True`` switches to the game-accurate
        # partial-throw burn (the ledger/enforcer-divert model) for callers
        # that want to measure the REAL tank (tests, diagnostics).
        cost_cons = min(params.fuel_cost(d, fuel_cons), params.max_fuel_per_jump)
        if accurate:
            mult = boost_mult if j.get("type") == "boost" else 1.0
            cost = min(params.fuel_cost(d, fuel, mult), params.max_fuel_per_jump)
        else:
            cost = cost_cons
        if cost > fuel + 1e-9:
            stranded_at = i
            fuel_after.append(fuel - cost)
            break
        fuel -= cost
        fuel_cons -= cost_cons
        fuel_after.append(fuel)
        if is_refuel_landing(j):
            fuel = params.tank_size
            fuel_cons = params.tank_size
            refuels += 1
            dry = 0
        else:
            dry += 1
            max_dry = max(max_dry, dry)
    return {
        "stranded_at": stranded_at,
        "over_range_at": over_range_at,
        "min_fuel": min(fuel_after) if fuel_after else fuel,
        "refuels": refuels,
        "max_dry_jumps": max_dry,
        "final_fuel": fuel,
        "fuel_after": fuel_after,
    }


def _mk_leg(leg_type, name, coord, dist, is_neutron, left, id64=None, spectral=None):
    """Build a route leg dict matching routing._leg's schema (so the fuel pass
    can splice scoop detours straight into the realized leg list)."""
    return {
        "type": leg_type, "system": name, "id64": id64, "spectral": spectral,
        "x": float(coord[0]), "y": float(coord[1]), "z": float(coord[2]),
        "neutron_star": bool(is_neutron), "distance_jumped": float(dist),
        "distance_left": float(left),
    }


def _nearest_scoopable(fetch_scoopable_near, pos, radius, is_locked):
    """Nearest scoopable KGBFOAM star to ``pos`` within ``radius`` (ly), skipping
    permit-locked systems, or None. ``fetch_scoopable_near(pos, radius)`` yields
    ``(x, y, z, name, mainstar)`` rows."""
    best = None
    best_d = None
    for row in fetch_scoopable_near(tuple(pos), float(radius)) or ():
        name = row[3] if len(row) > 3 else None
        if is_locked is not None and name is not None and is_locked(name):
            continue
        if not is_scoopable(row[4] if len(row) > 4 else None):
            continue
        d = math.dist(pos, (row[0], row[1], row[2]))
        if d <= 1e-6:
            continue
        if best_d is None or d < best_d:
            best_d, best = d, row
    return best


# Fraction of max_fuel_per_jump kept as the HARD floor when a planned in-path
# refuel waives the full one-jump reserve (quarter-jump, the margin class
# spansh's own routes run). The full reserve still gates whenever no refuel
# landing lies ahead in the plan.
_RESERVE_FLOOR_FRAC = 0.25


def _reaches_refuel(legs, start_i, fuel, params, maxf, boost_mult, floor):
    """From AFTER leg ``start_i`` holding ``fuel``, True iff the route reaches its
    next refuel landing with the tank at or above ``floor`` after every jump
    (game-accurate burns). False when it would dip under the floor first, or when
    no refuel landing lies ahead at all."""
    f = fuel
    for k in range(start_i + 1, len(legs)):
        d = float(legs[k].get("distance_jumped", 0.0) or 0.0)
        if d <= 1e-9:
            continue
        mult = boost_mult if legs[k].get("type") == "boost" else 1.0
        f -= min(params.fuel_cost(d, f, mult), maxf)
        if f < floor - 1e-9:
            return False
        if is_refuel_landing(legs[k]):
            return True
    return False


def enforce_fuel_safety(
    legs,
    src,
    dst,
    params: "FuelParams",
    fetch_scoopable_near: Callable,
    src_name: Optional[str] = None,
    is_locked: Optional[Callable[[str], bool]] = None,
    boost_mult: float = 4.0,
):
    """Insert scoop-stop detours so a realized route never strands (the default-on
    'fuel-safe' guarantee). Walks the legs tracking the exact tank; before any
    jump that would leave less than one jump of reserve before a non-scoopable
    landing (or that the tank can't afford), it diverts to the nearest scoopable
    star and back, refuelling. The detour is two normal jumps (``cur -> S`` then
    ``S -> cur``), always valid because the nearest scoopable is within range at
    the current (low, hence long-ranged) fuel level, and it leaves the rest of
    the proven geometry untouched. Returns ``(new_legs, info)``; ``info`` carries
    ``scoop_stops`` and ``safe`` (False with ``unsafe_at`` when no scoopable star
    is reachable, i.e. genuinely sparse space).

    ``fetch_scoopable_near(pos, radius) -> rows`` supplies candidate scoopable
    KGBFOAM stars (``(x, y, z, name, mainstar)``). This never touches neutron
    chains that are already fuel-safe; routes under the tank's dry-run capacity
    pass through unchanged.
    """
    tank = params.tank_size
    maxf = params.max_fuel_per_jump
    reserve = maxf  # always keep one full jump to divert to a scoopable star
    dstp = (float(dst[0]), float(dst[1]), float(dst[2]))
    out: list[dict] = []
    # The system we are currently sitting at (start = source).
    cur = {
        "pos": (float(src[0]), float(src[1]), float(src[2])),
        "name": src_name, "id64": None, "spectral": None, "neutron": False,
    }
    # TWO fuel trackers, one per concern:
    # - ``fuel`` (conservative, max-burn capped, boosted legs billed the raw
    #   distance): drives the OVER-RANGE check only. The B2 range-growth plan
    #   relies on the tank draining at max rate (drained = light = long reach),
    #   so that check must walk the same drain assumption or it flags B2's long
    #   late hops over-range and the guard discards good routes (see
    #   simulate_fuel's note; an earlier all-accurate attempt was reverted).
    # - ``facc`` (game-accurate partial-throw burns, the same model as the
    #   displayed ledger in annotate_leg_fuel): drives the DIVERT decisions
    #   (affordability + reserve margin). The old conservative margin billed a
    #   partial boosted hop the full max burn and inserted scoop diverts the
    #   accurate ledger then proved pointless (a 4 t-tank Sidewinder got a
    #   2-jump out-and-back the display showed refuelling nothing, 2026-11-22).
    #   Divert only when the REAL tank needs it; the reserve stays a full
    #   conservative jump, so the anti-stranding guarantee is unchanged.
    fuel = tank
    facc = tank
    scoop_stops = 0
    unsafe_at = None

    if tank_capacity_jumps(params) < 2:
        # A one-jump tank can't carry a divert margin; nothing we can guarantee.
        return [dict(l) for l in legs], {
            "scoop_stops": 0, "safe": False, "unsafe_at": 0,
            "reason": "tank too small",
        }

    floor = _RESERVE_FLOOR_FRAC * maxf
    for li, leg in enumerate(legs):
        d = float(leg.get("distance_jumped", 0.0) or 0.0)
        # A refuel landing = scoopable primary OR a neutron with a scoopable
        # companion in supercruise range (in-highway refuel, no divert jump).
        landing_scoop = is_refuel_landing(leg)
        if d > 1e-9:
            mult = boost_mult if leg.get("type") == "boost" else 1.0
            reach_now = params.jump_range(fuel) * mult
            # Conservative max-burn (boosted legs billed the raw distance),
            # matching simulate_fuel: the over-range check below must agree
            # with the planner's drain assumption.
            cost = min(params.fuel_cost(d, fuel), maxf)
            # Game-accurate burn (same model as the displayed ledger): what
            # the tank will REALLY hold after this jump.
            cost_acc = min(params.fuel_cost(d, facc, mult), maxf)
            after = facc - cost_acc
            breaks_margin = (not landing_scoop) and after < reserve - 1e-9
            if breaks_margin and after >= floor - 1e-9 and _reaches_refuel(
                legs, li, after, params, maxf, boost_mult, floor
            ):
                # IN-PATH refuel waiver: the plan provably reaches its next
                # refuel landing holding at least the quarter-jump floor the
                # whole way, so a divert-and-return (2 jumps) buys nothing --
                # trust the planned in-path scoop instead of the full one-jump
                # reserve. The strict reserve still fires when no refuel
                # landing lies ahead (genuinely scoopless space).
                breaks_margin = False
            if d > reach_now + 1e-6 and unsafe_at is None:
                # Over-range: unmakeable at this (heavy) fuel level; a scoop
                # detour only adds mass and shortens range, so it can't fix it.
                unsafe_at = len(out)
            elif (cost_acc > facc + 1e-9 or breaks_margin) and unsafe_at is None:
                # The divert must be flyable at the REAL (accurate) fuel level:
                # facc >= fuel, so the heavier real tank jumps shorter -- using
                # the accurate range here can only pick closer (safer) stars.
                reach = params.jump_range(facc)
                s = _nearest_scoopable(
                    fetch_scoopable_near, cur["pos"], reach, is_locked
                )
                s_pos = None if s is None else (
                    float(s[0]), float(s[1]), float(s[2])
                )
                d1 = None if s_pos is None else math.dist(cur["pos"], s_pos)
                if s_pos is not None and min(
                    params.fuel_cost(d1, facc), maxf
                ) <= facc + 1e-9:
                    # cur -> scoopable S (refuel), then S -> cur (resume).
                    out.append(_mk_leg(
                        "regular", s[3], s_pos, d1, False,
                        math.dist(s_pos, dstp),
                        spectral=(s[4] if len(s) > 4 else None),
                        id64=(s[5] if len(s) > 5 else None),
                    ))
                    fuel = tank
                    facc = tank
                    scoop_stops += 1
                    d2 = math.dist(s_pos, cur["pos"])
                    out.append(_mk_leg(
                        "regular", cur["name"], cur["pos"], d2, cur["neutron"],
                        math.dist(cur["pos"], dstp),
                        id64=cur["id64"], spectral=cur["spectral"],
                    ))
                    fuel -= min(params.fuel_cost(d2, fuel), maxf)
                    facc -= min(params.fuel_cost(d2, facc), maxf)
                    cost = min(params.fuel_cost(d, fuel), maxf)
                    cost_acc = min(params.fuel_cost(d, facc, mult), maxf)
                else:
                    unsafe_at = len(out)  # no scoopable star in range
            fuel -= cost
            facc -= cost_acc
        out.append(dict(leg))
        cur = {
            "pos": (float(leg["x"]), float(leg["y"]), float(leg["z"])),
            "name": leg.get("system"), "id64": leg.get("id64"),
            "spectral": leg.get("spectral"), "neutron": bool(leg.get("neutron_star")),
        }
        if landing_scoop:
            fuel = tank
            facc = tank

    return out, {
        "scoop_stops": scoop_stops,
        "safe": unsafe_at is None,
        "unsafe_at": unsafe_at,
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


def reach_profile(params: "FuelParams", boost_mult: float = 4.0) -> list[float]:
    """Boosted jump reach at each resource level r (boosted jumps since the last
    refuel), for the B2 range-growth planner: ``reach_by_r[r] = boost_mult *
    jump_range(fuel after r jumps)``. A ship's range grows as the tank empties
    (lighter), so the reach grows from ``boost_mult * max_range_full`` at r=0
    (full tank, just refuelled) toward ``boost_mult * max_range_light`` at
    r = T-1. Uses the CONSERVATIVE max fuel-burn per jump (matching
    ``tank_capacity_jumps``) so the realized fuel is never below the assumption
    and the planned long hops are never over-range. Returns ``[]`` when the tank
    holds fewer than ~1 jump (no useful growth profile)."""
    T = tank_capacity_jumps(params)
    if T <= 0:
        return []
    maxf = params.max_fuel_per_jump
    # Keep at least one jump of fuel in the mass estimate (you cannot jump drier).
    return [
        boost_mult * params.jump_range(max(maxf, params.tank_size - r * maxf))
        for r in range(T)
    ]


def tank_range(params: "FuelParams", boost_mult: float = 1.0) -> float:
    """Total distance (ly) a FULL tank sustains before a refuel is needed,
    flying max-length jumps. Range grows as the tank empties (lighter ship), so
    this is MORE than ``tank_jumps * max_range_full``: it sums the per-jump reach
    from a full tank down to the last jump (the same conservative per-jump burn
    as ``tank_capacity_jumps``/``reach_profile``, so it is a floor, never an
    over-estimate). ``boost_mult=1`` = plain jumps (the 'how far until I must
    scoop' figure a pilot reads); pass 4/6 for neutron-boosted travel."""
    return sum(reach_profile(params, boost_mult))


def fuel_report(jumps, params: "FuelParams", start_fuel: Optional[float] = None,
                boost_mult: float = 4.0, accurate: bool = False):
    """Summarize a realized route's fuel safety for a given ship.

    ``accurate=True`` judges stranding/min-fuel on the game-accurate partial-throw
    burn -- the model enforce_fuel_safety plans its diverts against, so the final
    verdict for an ENFORCED route agrees with the enforcer (the default
    conservative max-burn sim false-flags routes the real tank completes: a cutter
    route the enforcer made safe read stranded at -7.3 t conservatively, +2.85 t
    accurately, 2026-11-23).

    Builds on ``simulate_fuel`` (exact per-jump burn) and adds a verdict:
    ``safe`` is True only when the route never strands, has no jump that is
    physically unreachable at the fuel it is attempted with (``over_range``),
    AND its longest run of non-scoopable landings stays within the tank's
    conservative capacity, i.e. the commander can always divert to a scoopable
    star (the default-on fuel-safe margin). Returns the simulation fields plus
    ``tank_jumps``, ``stranded``, ``over_range``, and ``safe``; ``fuel_after``
    is dropped (per-leg detail the UI does not need)."""
    sim = simulate_fuel(jumps, params, start_fuel, boost_mult, accurate=accurate)
    cap = tank_capacity_jumps(params)
    stranded = sim["stranded_at"] is not None
    over_range = sim["over_range_at"] is not None
    safe = ((not stranded) and (not over_range)
            and (cap <= 0 or sim["max_dry_jumps"] < cap))
    return {
        "safe": safe,
        "stranded": stranded,
        "stranded_at": sim["stranded_at"],
        "over_range": over_range,
        "over_range_at": sim["over_range_at"],
        "refuels": sim["refuels"],
        "max_dry_jumps": sim["max_dry_jumps"],
        "tank_jumps": cap,
        "min_fuel": round(sim["min_fuel"], 2),
        "final_fuel": round(sim["final_fuel"], 2),
        "tank_size": params.tank_size,
        "has_scoop": params.has_scoop,
    }


def _scoop_needed(legs, i: int, fuel: float, params: "FuelParams",
                  maxf: float, landing, boost_mult: float = 4.0,
                  reserve: float = 0.0) -> bool:
    """From a scoopable landing ``i`` holding ``fuel`` (after that leg's burn),
    decide whether the commander actually has to top up here. The answer is no
    when the current fuel reaches the NEXT scoopable landing (or the route's end)
    without the tank running dry -- in that case scooping here only wastes time
    and leaves a heavier, shorter-ranged ship. Returns True only when skipping the
    scoop would strand -- or, with ``reserve`` > 0, dip below that reserve before
    a non-scoopable landing -- before the next chance to refuel. The reserve is
    the SAME one-jump margin enforce_fuel_safety diverts to protect, so the
    displayed refuel plan and the enforcer agree on where a top-up is required
    (the old reserve-blind plan could show an enforcer-inserted divert as
    refuelling nothing)."""
    f = fuel
    for k in range(i + 1, len(legs)):
        d = float(legs[k].get("distance_jumped", 0.0) or 0.0)
        if d <= 1e-9:
            continue
        mult = boost_mult if legs[k].get("type") == "boost" else 1.0
        cost = min(params.fuel_cost(d, f, mult), maxf)
        if cost > f + 1e-9:
            return True              # would run dry before the next refuel
        if (not landing[k]) and (f - cost) < reserve - 1e-9:
            return True              # would break the divert reserve en route
        f -= cost
        if landing[k]:
            return False             # reached the next scoopable with fuel to spare
    return False                     # reached the destination without stranding


def annotate_leg_fuel(legs, params: "FuelParams", start_fuel: Optional[float] = None,
                      boost_mult: float = 4.0) -> None:
    """Attach per-leg fuel detail for the spansh-exact export: ``fuel_used`` (burn
    on that jump), ``fuel_in_tank`` (after the burn, and after a refuel when one
    happens), and ``must_refuel`` (True where the tank was topped up).

    Scooping is LAZY -- the tank is only topped up at a scoopable landing when it
    could not otherwise reach the next scoopable (or the destination) without
    running dry. This matches the ``refuel_every_scoopable: 0`` model we send
    spansh, and avoids flagging a refuel at every scoopable star (issue #165:
    a commander who scoops when it isn't needed loses time and, carrying a heavier
    tank, jumps shorter). Mutates ``legs`` in place."""
    fuel = params.tank_size if start_fuel is None else float(start_fuel)
    maxf = params.max_fuel_per_jump
    landing = [is_refuel_landing(leg) for leg in legs]
    for i, leg in enumerate(legs):
        d = float(leg.get("distance_jumped", 0.0) or 0.0)
        used = 0.0
        if d > 1e-9:
            mult = boost_mult if leg.get("type") == "boost" else 1.0
            used = min(params.fuel_cost(d, fuel, mult), maxf)
            fuel = max(0.0, fuel - used)
        refueled = False
        # Top up only when this scoopable landing is actually needed to carry on
        # (the source, d == 0, is already full).
        if d > 1e-9 and landing[i] and _scoop_needed(
            legs, i, fuel, params, maxf, landing, boost_mult,
            # The same quarter-jump floor the enforcer's in-path waiver
            # guarantees: top up here iff skipping would dip below it before
            # the next refuel chance, so the displayed plan and the enforced
            # margins agree.
            reserve=_RESERVE_FLOOR_FRAC * maxf,
        ):
            fuel = params.tank_size
            refueled = True
        leg["fuel_used"] = round(used, 6)
        leg["fuel_in_tank"] = round(fuel, 6)
        leg["must_refuel"] = bool(refueled)


def ledger_stats(legs) -> dict:
    """Dry-run / min-fuel stats of the DISPLAYED refuel plan annotate_leg_fuel
    wrote (lazy scooping, game-accurate burns), for the UI verdict line.

    fuel_report's numbers come from the eager simulate_fuel model (brim at every
    scoopable), so on a route whose displayed plan advises NO scooping it would
    still report a short "longest dry run" and a high min fuel -- describing a
    plan nobody is advised to fly (user-reported 2026-11-23: verdict said
    "dry run 13 jumps - min fuel 100.3 t" over a ledger showing one 31-jump dry
    run down to 21.3 t). These stats are measured on the annotated legs
    themselves, so the verdict always matches the plan on screen. ``min_fuel``
    is the lowest ARRIVAL fuel (before any refill at that landing)."""
    refuels = 0
    dry = 0
    max_dry = 0
    min_fuel = None
    prev_tank = None
    for leg in legs:
        d = float(leg.get("distance_jumped", 0.0) or 0.0)
        tank_after = leg.get("fuel_in_tank")
        if d <= 1e-9:
            prev_tank = tank_after if tank_after is not None else prev_tank
            continue
        used = float(leg.get("fuel_used", 0.0) or 0.0)
        if prev_tank is not None:
            arrival = prev_tank - used
            min_fuel = arrival if min_fuel is None else min(min_fuel, arrival)
        if leg.get("must_refuel"):
            refuels += 1
            dry = 0
        else:
            dry += 1
            max_dry = max(max_dry, dry)
        prev_tank = tank_after if tank_after is not None else prev_tank
    out = {"refuels": refuels, "max_dry_jumps": max_dry}
    if min_fuel is not None:
        out["min_fuel"] = round(min_fuel, 2)
    return out


def spansh_params(params: "FuelParams", supercharge: float = 4.0,
                  inject_mult: float = 1.0) -> dict:
    """The spansh Galaxy (exact) Plotter ``parameters`` block derived from a
    parsed loadout, for the exact-format export. Mirrors the mapping used by the
    /spansh/galaxy-route proxy (reserve folded into base_mass, reserve_size 0).
    ``inject_mult`` > 1 sets use_injections_when_required so the exported route
    matches ours (spansh only models premium injection, multiplier 2)."""
    return {
        "algorithm": "optimistic",
        "base_mass": round(params.base_mass + params.reserve_size + params.cargo, 4),
        "cargo": int(params.cargo),
        "exclude_secondary": False,
        "fuel_multiplier": params.fuel_multiplier,
        "fuel_power": params.fuel_power,
        "injection_multiplier": 2,
        "internal_tank_size": params.reserve_size,
        "is_supercharged": False,
        "max_fuel_per_jump": params.max_fuel_per_jump,
        "optimal_mass": params.optimal_mass,
        "range_boost": params.range_boost,
        "refuel_every_scoopable": 0,
        "reserve_size": 0,
        "supercharge_multiplier": int(supercharge),
        "tank_size": params.tank_size,
        "use_injections": False,
        "use_injections_when_required": inject_mult > 1.0,
        "use_supercharge": True,
    }
