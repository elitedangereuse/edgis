import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api import fsd

# Minimal Caspian-class build (tester's Cobra MkV); known EDSY max range 67.72 ly.
COBRA_SLEF = (
    '[{"header":{"appName":"EDSY"},"data":{"event":"Loadout","Ship":"cobramkv",'
    '"UnladenMass":194.85,"MaxJumpRange":67.716111,'
    '"FuelCapacity":{"Main":32,"Reserve":0.49},"Modules":['
    '{"Slot":"FrameShiftDrive","Item":"int_hyperdrive_overcharge_size4_class5",'
    '"Engineering":{"Modifiers":[{"Label":"FSDOptimalMass","Value":1034.28},'
    '{"Label":"MaxFuelPerJump","Value":3.2}]}},'
    '{"Slot":"Slot01_Size5","Item":"int_guardianfsdbooster_size5"},'
    '{"Slot":"Slot02_Size4","Item":"int_fuelscoop_size4_class5"}]}}]'
)


def test_parse_loadout_game_accurate_params():
    p = fsd.parse_loadout(COBRA_SLEF)
    assert p is not None
    assert p.fuel_power == 2.3
    assert p.fuel_multiplier == 0.013
    assert p.optimal_mass == 1034.28
    # base_mass is UnladenMass only; the reserve is tracked separately and is
    # NOT part of the jump mass (the game excludes it). spansh's base_mass
    # convention (unladen + reserve) is re-applied only in the spansh request.
    assert round(p.base_mass, 2) == 194.85
    assert round(p.reserve_size, 2) == 0.49
    assert p.max_fuel_per_jump == 3.2
    assert p.range_boost == 10.5             # Guardian booster size 5
    assert p.tank_size == 32.0
    assert p.has_scoop is True


def test_max_range_matches_edsy():
    p = fsd.parse_loadout(COBRA_SLEF)
    # Must reproduce the game/EDSY numbers exactly (reserve excluded from mass).
    # Lightest-practical (one-jump fuel) range = the ship's advertised max.
    assert abs(p.max_range_light() - 67.72) < 0.1
    # Full-tank (laden) range = what the plotter plans at; EDSY shows ~60.45.
    assert abs(p.max_range_full() - 60.45) < 0.1


def test_range_grows_as_fuel_burns():
    p = fsd.parse_loadout(COBRA_SLEF)
    assert p.jump_range(32) < p.jump_range(16) < p.jump_range(0.5)


def test_fuel_cost_capped_and_monotonic():
    p = fsd.parse_loadout(COBRA_SLEF)
    # A longer jump costs more fuel; heavier (more fuel aboard) costs more.
    assert p.fuel_cost(60, 16) < p.fuel_cost(67, 16)
    assert p.fuel_cost(60, 5) < p.fuel_cost(60, 30)


def test_no_fsd_returns_none():
    assert fsd.parse_loadout('[{"data":{"event":"Loadout","Modules":[]}}]') is None


def test_malformed_input_returns_none_not_raises():
    # Non-JSON / wrong-shape input must yield None, never an exception
    # (the /route/ship endpoint turns None into a clean 422).
    assert fsd.parse_loadout("not json at all") is None
    assert fsd.parse_loadout("") is None
    assert fsd.parse_loadout("[]") is None
    assert fsd.parse_loadout("{}") is None
    assert fsd.parse_loadout('{"data": 123}') is None


def _jump(dist, neutron=False, spectral=None):
    return {"distance_jumped": dist, "neutron_star": neutron, "spectral": spectral}


def test_simulate_fuel_strands_on_long_neutron_chain():
    p = fsd.parse_loadout(COBRA_SLEF)  # tank 32, ~3.2 fuel/jump -> ~10 dry jumps
    chain = [_jump(60, neutron=True, spectral="N") for _ in range(20)]
    sim = fsd.simulate_fuel(chain, p)
    assert sim["stranded_at"] is not None and sim["stranded_at"] < 20


def test_simulate_fuel_safe_with_periodic_scooping():
    p = fsd.parse_loadout(COBRA_SLEF)
    # Every 4th jump lands on a scoopable K star -> never strands.
    legs = []
    for i in range(40):
        legs.append(_jump(55, neutron=(i % 4 != 3),
                          spectral="N" if i % 4 != 3 else "K (Yellow-Orange) Star"))
    sim = fsd.simulate_fuel(legs, p)
    assert sim["stranded_at"] is None
    assert sim["refuels"] >= 9


def test_simulate_fuel_flags_over_range_jump():
    p = fsd.parse_loadout(COBRA_SLEF)  # full-tank range ~60.45, light ~67.72
    # A 63 ly regular jump is unreachable on a full tank (heavy ship -> short
    # range), even though the tank has fuel to spare -- flagged over_range, NOT
    # stranded (the two are different failures).
    sim = fsd.simulate_fuel([_jump(63)], p)
    assert sim["over_range_at"] == 0
    assert sim["stranded_at"] is None


def test_simulate_fuel_over_range_feasible_when_light():
    p = fsd.parse_loadout(COBRA_SLEF)
    # The same 63 ly jump IS makeable once the ship is light (longer range).
    sim = fsd.simulate_fuel([_jump(63)], p, start_fuel=10.0)
    assert sim["over_range_at"] is None
    assert sim["stranded_at"] is None


def test_simulate_fuel_boosted_jump_not_over_range():
    p = fsd.parse_loadout(COBRA_SLEF)
    # A 200 ly leap on a full tank is impossible unboosted but fine as a neutron
    # supercharge (4x range), so it must NOT be flagged over_range.
    boosted = {"distance_jumped": 200, "neutron_star": False,
               "spectral": "G", "type": "boost"}
    sim = fsd.simulate_fuel([boosted], p)
    assert sim["over_range_at"] is None


def test_fuel_report_over_range_route_is_unsafe():
    p = fsd.parse_loadout(COBRA_SLEF)
    rep = fsd.fuel_report([_jump(63)], p)
    assert rep["over_range"] is True
    assert rep["safe"] is False


def test_tank_capacity_jumps():
    p = fsd.parse_loadout(COBRA_SLEF)  # tank 32, max fuel/jump 3.2
    assert fsd.tank_capacity_jumps(p) == 10


def test_fuel_report_flags_long_neutron_chain_unsafe():
    p = fsd.parse_loadout(COBRA_SLEF)
    chain = [_jump(60, neutron=True, spectral="N") for _ in range(20)]
    rep = fsd.fuel_report(chain, p)
    assert rep["safe"] is False
    assert rep["stranded"] is True
    assert rep["stranded_at"] is not None
    assert "fuel_after" not in rep  # trimmed from the report


def test_fuel_report_safe_with_periodic_scooping():
    p = fsd.parse_loadout(COBRA_SLEF)
    legs = [_jump(55, neutron=(i % 4 != 3),
                  spectral="N" if i % 4 != 3 else "K (Yellow-Orange) Star")
            for i in range(40)]
    rep = fsd.fuel_report(legs, p)
    assert rep["safe"] is True
    assert rep["stranded"] is False
    assert rep["max_dry_jumps"] < rep["tank_jumps"]


def test_fuel_report_short_dry_run_within_margin_is_safe():
    p = fsd.parse_loadout(COBRA_SLEF)  # capacity 10 jumps
    # 8 neutron hops then a scoopable landing: under the 10-jump tank, safe.
    legs = [_jump(50, neutron=True, spectral="N") for _ in range(8)]
    legs.append(_jump(40, neutron=False, spectral="G (White-Yellow) Star"))
    rep = fsd.fuel_report(legs, p)
    assert rep["safe"] is True
    assert rep["max_dry_jumps"] == 8


def _neutron_chain(n, step=60.0):
    legs, x = [], 0.0
    for i in range(n):
        x += step
        legs.append({"system": f"N{i}", "id64": None, "x": x, "y": 0.0, "z": 0.0,
                     "spectral": "N", "type": "boost", "neutron_star": True,
                     "distance_jumped": step, "distance_left": 0.0})
    return legs, (x, 0.0, 0.0)


def _scoop_fetch(offset=20.0):
    # A scoopable K star 'offset' ly off-axis from any queried position.
    def fetch(pos, radius):
        s = (pos[0], pos[1] + offset, pos[2], "ScoopK", "K (Yellow-Orange) Star")
        return [s] if offset <= radius else []
    return fetch


def test_enforce_fuel_safety_breaks_a_stranding_neutron_chain():
    p = fsd.parse_loadout(COBRA_SLEF)  # ~10-jump tank
    legs, dst = _neutron_chain(20)
    # Baseline: the raw chain strands.
    assert fsd.simulate_fuel(legs, p)["stranded_at"] is not None
    new_legs, info = fsd.enforce_fuel_safety(
        legs, (0.0, 0.0, 0.0), dst, p, _scoop_fetch(), src_name="Start"
    )
    assert info["safe"] is True
    assert info["scoop_stops"] >= 1
    assert len(new_legs) > len(legs)  # detours inserted
    # The enforced route no longer strands, per an independent simulation.
    assert fsd.simulate_fuel(new_legs, p)["stranded_at"] is None
    # Detour stars are scoopable K stars inserted as normal jumps.
    assert any(j["system"] == "ScoopK" and j["type"] == "regular" for j in new_legs)


def test_enforce_fuel_safety_leaves_safe_routes_unchanged():
    p = fsd.parse_loadout(COBRA_SLEF)
    legs, dst = _neutron_chain(5)  # well under the 10-jump tank
    new_legs, info = fsd.enforce_fuel_safety(
        legs, (0.0, 0.0, 0.0), dst, p, _scoop_fetch(), src_name="Start"
    )
    assert info["scoop_stops"] == 0
    assert info["safe"] is True
    assert len(new_legs) == len(legs)


def test_enforce_fuel_safety_flags_unsafe_when_no_scoopable_in_range():
    p = fsd.parse_loadout(COBRA_SLEF)
    legs, dst = _neutron_chain(20)
    no_scoop = lambda pos, radius: []  # sparse space: nothing scoopable
    new_legs, info = fsd.enforce_fuel_safety(
        legs, (0.0, 0.0, 0.0), dst, p, no_scoop, src_name="Start"
    )
    assert info["safe"] is False
    assert info["unsafe_at"] is not None
    assert info["scoop_stops"] == 0


def test_enforce_fuel_safety_respects_permit_lock():
    p = fsd.parse_loadout(COBRA_SLEF)
    legs, dst = _neutron_chain(20)
    # Only scoopable candidate is locked -> can't use it -> unsafe.
    locked = lambda name: name == "ScoopK"
    new_legs, info = fsd.enforce_fuel_safety(
        legs, (0.0, 0.0, 0.0), dst, p, _scoop_fetch(), src_name="Start",
        is_locked=locked,
    )
    assert info["safe"] is False


def test_is_refuel_landing_covers_scoopable_and_inflight():
    # Plain scoopable primary.
    assert fsd.is_refuel_landing({"neutron_star": False, "spectral": "K"}) is True
    # Neutron with no companion -> not a refuel landing.
    assert fsd.is_refuel_landing({"neutron_star": True, "spectral": "N"}) is False
    # Neutron flagged with an in-system scoopable companion -> refuel landing.
    assert fsd.is_refuel_landing(
        {"neutron_star": True, "spectral": "N", "inflight_scoop": True}
    ) is True


def test_simulate_fuel_safe_when_neutrons_have_inflight_scoops():
    p = fsd.parse_loadout(COBRA_SLEF)  # ~10-jump tank
    # A 20-neutron chain strands on its own, but if every neutron carries a
    # scoopable companion you brim in-system each hop -> never strands.
    chain = []
    for i in range(20):
        chain.append({"distance_jumped": 60.0, "neutron_star": True,
                      "spectral": "N", "inflight_scoop": True})
    sim = fsd.simulate_fuel(chain, p)
    assert sim["stranded_at"] is None
    assert sim["refuels"] == 20
    assert sim["max_dry_jumps"] == 0


def test_enforce_fuel_safety_no_divert_when_neutrons_scoop_inflight():
    p = fsd.parse_loadout(COBRA_SLEF)
    legs, dst = _neutron_chain(20)  # would strand and force divert stops
    for leg in legs:
        leg["inflight_scoop"] = True  # each neutron refuels in-system
    new_legs, info = fsd.enforce_fuel_safety(
        legs, (0.0, 0.0, 0.0), dst, p, _scoop_fetch(), src_name="Start"
    )
    assert info["safe"] is True
    assert info["scoop_stops"] == 0          # no +2-jump diverts needed
    assert len(new_legs) == len(legs)        # chain untouched
    assert not any(j["system"] == "ScoopK" for j in new_legs)


def test_enforce_reserve_margin_refuels_sooner():
    p = fsd.parse_loadout(COBRA_SLEF)  # ~10-jump tank
    legs, dst = _neutron_chain(20)  # a chain that needs refuels
    _, info1 = fsd.enforce_fuel_safety(
        legs, (0.0, 0.0, 0.0), dst, p, _scoop_fetch(), src_name="Start",
        reserve_jumps=1.0,
    )
    _, info3 = fsd.enforce_fuel_safety(
        legs, (0.0, 0.0, 0.0), dst, p, _scoop_fetch(), src_name="Start",
        reserve_jumps=3.0,
    )
    # Both stay safe; a bigger reserve refuels sooner -> at least as many stops.
    assert info1["safe"] and info3["safe"]
    assert info3["scoop_stops"] >= info1["scoop_stops"] >= 1


def test_cargo_reduces_range_and_is_reported():
    p0 = fsd.parse_loadout(COBRA_SLEF)
    p6 = fsd.parse_loadout(COBRA_SLEF, cargo=6)
    assert p0.cargo == 0 and p6.cargo == 6
    # Cargo adds mass -> shorter range at the same fuel, costs more fuel per jump.
    assert p6.jump_range(32) < p0.jump_range(32)
    assert p6.fuel_cost(50, 32) > p0.fuel_cost(50, 32)
    s = fsd.ship_summary(p6)
    assert s["cargo"] == 6
    assert s["jump_range"] < fsd.ship_summary(p0)["jump_range"]


def test_ship_summary_fills_range_and_flags_scoop():
    p = fsd.parse_loadout(COBRA_SLEF)
    s = fsd.ship_summary(p)
    assert s["ok"] is True
    # Plots at the FULL-TANK range (always feasible), not the lightest max.
    assert abs(s["jump_range"] - 60.34) < 0.5
    assert abs(s["max_range_full"] - 60.34) < 0.5
    assert abs(s["max_range_light"] - 67.57) < 0.5  # lightest still shown
    assert s["tank_jumps"] == 10
    assert s["has_scoop"] is True
