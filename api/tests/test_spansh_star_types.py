from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest
from pytest import MonkeyPatch


class _FakeCursor:
    def __init__(self) -> None:
        self.closed = False

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def execute(self, *args, **kwargs) -> None:  # pragma: no cover - no-op stub
        return None

    def fetchall(self) -> list[tuple]:
        return []

    def fetchone(self):
        return None

    def close(self) -> None:
        self.closed = True

    def executemany(self, *args, **kwargs) -> None:  # pragma: no cover - no-op stub
        return None


class _FakeConnection:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor()

    def close(self) -> None:  # pragma: no cover - no-op stub
        return None

    def commit(self) -> None:  # pragma: no cover - no-op stub
        return None


class _DummyTqdm:
    def __enter__(self) -> "_DummyTqdm":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def update(self, *_args, **_kwargs) -> None:  # pragma: no cover - no-op stub
        return None


@pytest.fixture(scope="module")
def spansh_module():
    monkeypatch = MonkeyPatch()
    project_root = str(Path(__file__).resolve().parents[2])
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    fake_psycopg = types.ModuleType("psycopg")
    fake_psycopg.Connection = _FakeConnection
    fake_psycopg.Cursor = _FakeCursor
    fake_psycopg.connect = lambda *args, **kwargs: _FakeConnection()
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)

    fake_ijson = types.ModuleType("ijson")
    fake_ijson.items = lambda *args, **kwargs: iter(())
    monkeypatch.setitem(sys.modules, "ijson", fake_ijson)

    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)

    fake_tqdm = types.ModuleType("tqdm")
    fake_tqdm.tqdm = lambda *args, **kwargs: _DummyTqdm()
    monkeypatch.setitem(sys.modules, "tqdm", fake_tqdm)

    module_name = "feeders.spansh_dump_bodies_ingestor"
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    yield module
    monkeypatch.undo()


@pytest.mark.parametrize(
    "body,expected",
    [
        ({"subType": "Neutron Star"}, "N"),
        ({"spectralClass": "K5 V"}, "K"),
        ({"subType": "White Dwarf (DAZ) Star"}, "DAZ"),
        ({"subType": "M (Red super giant) Star"}, "M_RedSuperGiant"),
        ({"subType": "Wolf-Rayet NC Star"}, "WNC"),
        ({"subType": "Supermassive Black Hole"}, "SupermassiveBlackHole"),
        ({"subType": "L (Brown dwarf) Star"}, "L"),
        ({"subType": "K (Yellow-Orange giant) Star"}, "K_OrangeGiant"),
    ],
)
def test_resolve_star_type_variants(spansh_module, body, expected):
    assert spansh_module.resolve_star_type(body) == expected


def test_resolve_star_type_prefers_secondary_fields(spansh_module):
    body = {"spectralClass": None, "starType": "N"}
    assert spansh_module.resolve_star_type(body) == "N"


def test_spansh_upsert_separates_parent_and_lock_assignments(spansh_module):
    assert "END,\n        tidally_locked" in spansh_module.UPSERT_BODY


def test_spansh_upsert_preserves_existing_atmosphere_on_no_atmosphere(
    spansh_module,
):
    assert "WHEN bodies.atmosphere_id IS NOT NULL" in spansh_module.UPSERT_BODY
    assert "AND name = 'no atmosphere'" in spansh_module.UPSERT_BODY
    assert "THEN bodies.atmosphere_id" in spansh_module.UPSERT_BODY


def test_resolve_star_type_handles_missing_data(spansh_module):
    assert spansh_module.resolve_star_type({}) is None


@pytest.mark.parametrize(
    "description,subtype,expected",
    [
        ("Hot thick methane atmosphere", "Earth-like world", "EarthLike"),
        ("Sulfur-dioxide-rich atmosphere", "Gas Giant", "Sulphur-dioxideRich"),
        ("Thin carbon dioxide atmosphere", None, "CarbonDioxide"),
    ],
)
def test_convert_atmosphere_type_variants(spansh_module, description, subtype, expected):
    assert (
        spansh_module.convert_atmosphere_type(description, subtype, "Demo") == expected
    )


@pytest.mark.parametrize(
    "value,multiplier,expected",
    [
        (2, 86400, 172800),
        (1.5, 1, 1.5),
        (None, 1, None),
        ("bad", 60, None),
    ],
)
def test_to_seconds(spansh_module, value, multiplier, expected):
    assert spansh_module.to_seconds(value, multiplier=multiplier) == expected


class _RecordingCursor:
    def __init__(self, fetch_results=None) -> None:
        self.statements: list[tuple[str, tuple | None]] = []
        self.fetch_results = list(fetch_results or [])
        self.closed = False

    def execute(self, query, params=None) -> None:
        self.statements.append((query, params))

    def fetchone(self):
        return self.fetch_results.pop(0) if self.fetch_results else None

    def fetchall(self) -> list[tuple]:
        return []

    def close(self) -> None:
        self.closed = True


class _RecordingConnection:
    def __init__(self) -> None:
        self.cursors: list[_RecordingCursor] = []

    def cursor(self) -> _RecordingCursor:
        cursor = _RecordingCursor()
        self.cursors.append(cursor)
        return cursor

    def commit(self) -> None:  # pragma: no cover - no-op stub
        return None

    def rollback(self) -> None:  # pragma: no cover - no-op stub
        return None

    def close(self) -> None:  # pragma: no cover - no-op stub
        return None


def _make_ring_session(spansh_module, existing_ring):
    connection = _RecordingConnection()
    session = spansh_module.SpanshBodyIngestSession(
        connection=connection, log_func=lambda _message: None
    )
    session.body_cursor.fetch_results.append(existing_ring)
    return session, connection


PLANET_BODY = {
    "bodyId": 26,
    "name": "Col 285 Sector KM-V d2-106 5",
    "type": "Planet",
    "subType": "Class III gas giant",
    "terraformingState": "Not terraformable",
    "rings": [
        {
            "name": "Col 285 Sector KM-V d2-106 5 A Ring",
            "type": "Metallic",
            "innerRadius": 138820000.0,
            "outerRadius": 181440000.0,
            "mass": 427150000000.0,
        }
    ],
}


def _seed_ring_lookup_cache(spansh_module):
    spansh_module.lookup_cache["body_types"]["Planet"] = 1
    spansh_module.lookup_cache["body_types"]["PlanetaryRing"] = 2
    spansh_module.lookup_cache["planet_classes"]["Class III gas giant"] = 3
    spansh_module.lookup_cache["terraform_states"]["Not terraformable"] = 4
    spansh_module.lookup_cache["ring_classes"]["eRingClass_Metallic"] = 5


def test_spansh_backfills_missing_ring_metadata_on_positive_ring(spansh_module):
    _seed_ring_lookup_cache(spansh_module)
    session, connection = _make_ring_session(
        spansh_module, (27, None, None, None, None)
    )

    session.process_body(PLANET_BODY, 3652643195227, None)

    body_inserts = [
        params
        for query, params in connection.cursors[0].statements
        if "INSERT INTO bodies" in query
    ]
    assert len(body_inserts) == 1  # no inferred ring row created
    ring_updates = [
        (query, params)
        for query, params in connection.cursors[0].statements
        if "UPDATE bodies SET" in query
    ]
    assert len(ring_updates) == 1
    query, params = ring_updates[0]
    assert "COALESCE(ring_class_id" in query
    assert "COALESCE(ring_mass_mt" in query
    assert params == [5, 138820000.0, 181440000.0, 427150000000.0, 3652643195227, 27]


def test_spansh_skips_backfill_for_complete_positive_ring(spansh_module):
    _seed_ring_lookup_cache(spansh_module)
    session, connection = _make_ring_session(
        spansh_module, (27, 5, 138820000.0, 181440000.0, 427150000000.0)
    )

    session.process_body(PLANET_BODY, 3652643195227, None)

    body_inserts = [
        params
        for query, params in connection.cursors[0].statements
        if "INSERT INTO bodies" in query
    ]
    assert len(body_inserts) == 1
    ring_updates = [
        params
        for query, params in connection.cursors[0].statements
        if "UPDATE bodies SET" in query
    ]
    assert ring_updates == []


def test_spansh_backfill_statement_only_fills_missing_ring_fields(spansh_module):
    assert "COALESCE(ring_class_id" in spansh_module.BACKFILL_RING_METADATA
    assert "COALESCE(ring_inner_rad" in spansh_module.BACKFILL_RING_METADATA
    assert "COALESCE(ring_outer_rad" in spansh_module.BACKFILL_RING_METADATA
    assert "COALESCE(ring_mass_mt" in spansh_module.BACKFILL_RING_METADATA
