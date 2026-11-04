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


def test_parse_loadout_matches_spansh_params():
    p = fsd.parse_loadout(COBRA_SLEF)
    assert p is not None
    assert p.fuel_power == 2.3
    assert p.fuel_multiplier == 0.013
    assert p.optimal_mass == 1034.28
    assert round(p.base_mass, 2) == 195.34  # unladen + reserve
    assert p.max_fuel_per_jump == 3.2
    assert p.range_boost == 10.5             # Guardian booster size 5
    assert p.tank_size == 32.0
    assert p.has_scoop is True


def test_max_range_matches_edsy():
    p = fsd.parse_loadout(COBRA_SLEF)
    # EDSY reports 67.72 ly; the lightest-practical (one-jump fuel) range.
    assert abs(p.max_range_light() - 67.72) < 0.5


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
