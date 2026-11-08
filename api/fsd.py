"""Elite Dangerous FSD fuel + jump-range model, for fuel-aware routing.

Jump range grows as the tank empties (lighter ship). Formula, validated to 6
decimals against the game's own MaxJumpRange and EDSY's getJumpDistance for a
Cobra Mk V build (max range 67.72 ly). ``base_mass`` is UnladenMass only; the
reserve tank's fuel is not part of the jump mass (the game excludes it):

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
import math
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

    def fuel_cost(self, dist: float, fuel: float) -> float:
        """Fuel (tons) to jump ``dist`` ly at this fuel level (uncapped)."""
        mass = self.base_mass + self.cargo + max(0.0, fuel)
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
        "has_scoop": params.has_scoop,
        "range_boost": params.range_boost,
        "cargo": params.cargo,
    }


def is_scoopable(spectral: Optional[str]) -> bool:
    """KGBFOAM main-sequence stars are fuel-scoopable (matches the UI rule)."""
    return bool(spectral) and str(spectral).strip()[:1].upper() in "KGBFOAM"


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
                  boost_mult: float = 4.0):
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
        reach = params.jump_range(fuel) * (
            boost_mult if j.get("type") == "boost" else 1.0
        )
        if d > reach + 1e-6 and over_range_at is None:
            over_range_at = i  # unreachable at this fuel level (too heavy)
        cost = min(params.fuel_cost(d, fuel), params.max_fuel_per_jump)
        if cost > fuel + 1e-9:
            stranded_at = i
            fuel_after.append(fuel - cost)
            break
        fuel -= cost
        fuel_after.append(fuel)
        if is_refuel_landing(j):
            fuel = params.tank_size
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
    fuel = tank
    scoop_stops = 0
    unsafe_at = None

    if tank_capacity_jumps(params) < 2:
        # A one-jump tank can't carry a divert margin; nothing we can guarantee.
        return [dict(l) for l in legs], {
            "scoop_stops": 0, "safe": False, "unsafe_at": 0,
            "reason": "tank too small",
        }

    for leg in legs:
        d = float(leg.get("distance_jumped", 0.0) or 0.0)
        # A refuel landing = scoopable primary OR a neutron with a scoopable
        # companion in supercruise range (in-highway refuel, no divert jump).
        landing_scoop = is_refuel_landing(leg)
        if d > 1e-9:
            reach_now = params.jump_range(fuel) * (
                boost_mult if leg.get("type") == "boost" else 1.0
            )
            cost = min(params.fuel_cost(d, fuel), maxf)
            breaks_margin = (not landing_scoop) and (fuel - cost) < reserve - 1e-9
            if d > reach_now + 1e-6 and unsafe_at is None:
                # Over-range: unmakeable at this (heavy) fuel level; a scoop
                # detour only adds mass and shortens range, so it can't fix it.
                unsafe_at = len(out)
            elif (cost > fuel + 1e-9 or breaks_margin) and unsafe_at is None:
                reach = params.jump_range(fuel)
                s = _nearest_scoopable(
                    fetch_scoopable_near, cur["pos"], reach, is_locked
                )
                s_pos = None if s is None else (
                    float(s[0]), float(s[1]), float(s[2])
                )
                d1 = None if s_pos is None else math.dist(cur["pos"], s_pos)
                if s_pos is not None and min(
                    params.fuel_cost(d1, fuel), maxf
                ) <= fuel + 1e-9:
                    # cur -> scoopable S (refuel), then S -> cur (resume).
                    out.append(_mk_leg(
                        "regular", s[3], s_pos, d1, False,
                        math.dist(s_pos, dstp),
                        spectral=(s[4] if len(s) > 4 else None),
                        id64=(s[5] if len(s) > 5 else None),
                    ))
                    fuel = tank
                    scoop_stops += 1
                    d2 = math.dist(s_pos, cur["pos"])
                    out.append(_mk_leg(
                        "regular", cur["name"], cur["pos"], d2, cur["neutron"],
                        math.dist(cur["pos"], dstp),
                        id64=cur["id64"], spectral=cur["spectral"],
                    ))
                    fuel -= min(params.fuel_cost(d2, fuel), maxf)
                    cost = min(params.fuel_cost(d, fuel), maxf)
                else:
                    unsafe_at = len(out)  # no scoopable star in range
            fuel -= cost
        out.append(dict(leg))
        cur = {
            "pos": (float(leg["x"]), float(leg["y"]), float(leg["z"])),
            "name": leg.get("system"), "id64": leg.get("id64"),
            "spectral": leg.get("spectral"), "neutron": bool(leg.get("neutron_star")),
        }
        if landing_scoop:
            fuel = tank

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


def fuel_report(jumps, params: "FuelParams", start_fuel: Optional[float] = None,
                boost_mult: float = 4.0):
    """Summarize a realized route's fuel safety for a given ship.

    Builds on ``simulate_fuel`` (exact per-jump burn) and adds a verdict:
    ``safe`` is True only when the route never strands, has no jump that is
    physically unreachable at the fuel it is attempted with (``over_range``),
    AND its longest run of non-scoopable landings stays within the tank's
    conservative capacity, i.e. the commander can always divert to a scoopable
    star (the default-on fuel-safe margin). Returns the simulation fields plus
    ``tank_jumps``, ``stranded``, ``over_range``, and ``safe``; ``fuel_after``
    is dropped (per-leg detail the UI does not need)."""
    sim = simulate_fuel(jumps, params, start_fuel, boost_mult)
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


def annotate_leg_fuel(legs, params: "FuelParams", start_fuel: Optional[float] = None,
                      boost_mult: float = 4.0) -> None:
    """Attach per-leg fuel detail for the spansh-exact export: ``fuel_used`` (burn
    on that jump), ``fuel_in_tank`` (after the burn, and after a refuel when one
    happens), and ``must_refuel`` (True where the tank was topped up). Mirrors
    simulate_fuel's burn + refuel-at-scoopable logic. Mutates ``legs`` in place."""
    fuel = params.tank_size if start_fuel is None else float(start_fuel)
    maxf = params.max_fuel_per_jump
    for leg in legs:
        d = float(leg.get("distance_jumped", 0.0) or 0.0)
        used = 0.0
        if d > 1e-9:
            used = min(params.fuel_cost(d, fuel), maxf)
            fuel = max(0.0, fuel - used)
        refueled = False
        # A refuel landing (scoopable, or a neutron with an in-system companion
        # scoop) tops the tank; the source (d == 0) is already full.
        if d > 1e-9 and is_refuel_landing(leg):
            fuel = params.tank_size
            refueled = True
        leg["fuel_used"] = round(used, 6)
        leg["fuel_in_tank"] = round(fuel, 6)
        leg["must_refuel"] = bool(refueled)


def spansh_params(params: "FuelParams", supercharge: float = 4.0) -> dict:
    """The spansh Galaxy (exact) Plotter ``parameters`` block derived from a
    parsed loadout, for the exact-format export. Mirrors the mapping used by the
    /spansh/galaxy-route proxy (reserve folded into base_mass, reserve_size 0)."""
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
        "use_injections_when_required": True,
        "use_supercharge": True,
    }
