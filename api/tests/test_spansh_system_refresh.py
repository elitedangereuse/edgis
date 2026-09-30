"""End-to-end coverage for the /bodies/{id64}/spansh-refresh ingestion path."""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest
from pytest import MonkeyPatch


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _RecordingCursor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple | None]] = []
        self.fetch_results: list[tuple] = []
        self.closed = False

    def __enter__(self) -> "_RecordingCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, query, params=None) -> None:
        self.statements.append((query, params))

    def fetchone(self):
        return self.fetch_results.pop(0) if self.fetch_results else None

    def fetchall(self) -> list[tuple]:
        return []

    def close(self) -> None:
        self.closed = True


class _RecordingConnection:
    def __init__(self, fetch_results: list[tuple] | None = None) -> None:
        self.cursors: list[_RecordingCursor] = []
        self.fetch_results = list(fetch_results or [])

    def cursor(self) -> _RecordingCursor:
        cursor = _RecordingCursor()
        if self.fetch_results:
            cursor.fetch_results = self.fetch_results
            self.fetch_results = []
        self.cursors.append(cursor)
        return cursor

    def commit(self) -> None:  # pragma: no cover - no-op stub
        return None

    def rollback(self) -> None:  # pragma: no cover - no-op stub
        return None

    def close(self) -> None:  # pragma: no cover - no-op stub
        return None


class _DummyTqdm:
    def __enter__(self) -> "_DummyTqdm":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def update(self, *_args, **_kwargs) -> None:  # pragma: no cover
        return None


@pytest.fixture(scope="module")
def ingestor_module():
    monkeypatch = MonkeyPatch()
    fake_psycopg = types.ModuleType("psycopg")
    fake_psycopg.Connection = _RecordingConnection
    fake_psycopg.Cursor = _RecordingCursor
    fake_psycopg.connect = lambda *args, **kwargs: _RecordingConnection()
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

    module_name = "feeders.spansh_system_ingestor"
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    yield module
    monkeypatch.undo()


STAR_ID64 = 936752375136256322
PLANET_5_ID64 = 936752375136258395
BARYCENTER_ID64 = 900723578117294427
SYSTEM_ID64 = 3652643195227

SYSTEM_PAYLOAD = {
    "record": {
        "id64": SYSTEM_ID64,
        "name": "Col 285 Sector KM-V d2-106",
        "updated_at": "2026-09-28T12:05:11Z",
        "bodies": [
            {
                "id64": STAR_ID64,
                "name": "Col 285 Sector KM-V d2-106",
                "type": "Star",
            },
            {
                "id64": PLANET_5_ID64,
                "name": "Col 285 Sector KM-V d2-106 5",
                "type": "Planet",
                "subtype": "Class III gas giant",
            },
        ],
    }
}

STAR_DETAIL = {
    "record": {
        "id64": STAR_ID64,
        "body_id": 0,
        "name": "Col 285 Sector KM-V d2-106",
        "type": "Star",
        "updated_at": "2026-09-28T12:05:11Z",
    }
}

BARYCENTER_DETAIL = {
    "record": {
        "id64": BARYCENTER_ID64,
        "body_id": 25,
        "name": "Col 285 Sector KM-V d2-106 Barycenter",
        "type": "Barycentre",
        "updated_at": "2026-09-28T12:05:11Z",
    }
}

# Payload captured from https://spansh.co.uk/api/body/936752375136258395
PLANET_5_DETAIL = {
    "record": {
        "id64": PLANET_5_ID64,
        "body_id": 26,
        "name": "Col 285 Sector KM-V d2-106 5",
        "type": "Planet",
        "subtype": "Class III gas giant",
        "terraforming_state": "Not terraformable",
        "parents": [{"type": "Null", "id64": BARYCENTER_ID64}],
        "rings": [
            {
                "inner_radius": 138820000.0,
                "mass": 427150000000.0,
                "name": "Col 285 Sector KM-V d2-106 5 A Ring",
                "outer_radius": 181440000.0,
                "type": "Metallic",
            },
            {
                "inner_radius": 181540000.0,
                "mass": 3654400000000.0,
                "name": "Col 285 Sector KM-V d2-106 5 B Ring",
                "outer_radius": 386910000.0,
                "type": "Metal Rich",
            },
        ],
        "updated_at": "2026-09-28T12:05:11Z",
    }
}


def _seed_lookup_cache(dump_module) -> None:
    dump_module.lookup_cache["body_types"]["Star"] = 1
    dump_module.lookup_cache["body_types"]["Planet"] = 2
    dump_module.lookup_cache["body_types"]["PlanetaryRing"] = 3
    dump_module.lookup_cache["body_types"]["Barycenter"] = 8
    dump_module.lookup_cache["planet_classes"]["Class III gas giant"] = 4
    dump_module.lookup_cache["terraform_states"]["Not terraformable"] = 5
    dump_module.lookup_cache["ring_classes"]["eRingClass_Metallic"] = 6
    dump_module.lookup_cache["ring_classes"]["eRingClass_MetalRich"] = 7


def test_ingest_system_backfills_incomplete_rings(
    ingestor_module, monkeypatch
):
    dump_module = importlib.import_module("feeders.spansh_dump_bodies_ingestor")
    _seed_lookup_cache(dump_module)

    payloads = {
        f"/system/{SYSTEM_ID64}": SYSTEM_PAYLOAD,
        f"/body/{STAR_ID64}": STAR_DETAIL,
        f"/body/{PLANET_5_ID64}": PLANET_5_DETAIL,
        f"/body/{BARYCENTER_ID64}": BARYCENTER_DETAIL,
    }
    monkeypatch.setattr(
        ingestor_module, "fetch_json", lambda path: payloads[path]
    )

    connection = _RecordingConnection(
        fetch_results=[(27, None, None, None, None), (28, None, None, None, None)]
    )

    ingestor_module.ingest_system(SYSTEM_ID64, connection=connection)

    backfills = [
        (query, params)
        for query, params in connection.cursors[0].statements
        if "UPDATE bodies SET" in query and "COALESCE(ring_class_id" in query
    ]
    assert len(backfills) == 2
    (first_query, first_params), (second_query, second_params) = backfills
    assert first_params == [
        6,
        138820000.0,
        181440000.0,
        427150000000.0,
        SYSTEM_ID64,
        27,
    ]
    assert second_params == [
        7,
        181540000.0,
        386910000.0,
        3654400000000.0,
        SYSTEM_ID64,
        28,
    ]


def test_ingest_system_keeps_inferred_rings_when_no_positive_row(
    ingestor_module, monkeypatch
):
    dump_module = importlib.import_module("feeders.spansh_dump_bodies_ingestor")
    _seed_lookup_cache(dump_module)

    payloads = {
        f"/system/{SYSTEM_ID64}": SYSTEM_PAYLOAD,
        f"/body/{STAR_ID64}": STAR_DETAIL,
        f"/body/{PLANET_5_ID64}": PLANET_5_DETAIL,
        f"/body/{BARYCENTER_ID64}": BARYCENTER_DETAIL,
    }
    monkeypatch.setattr(
        ingestor_module, "fetch_json", lambda path: payloads[path]
    )

    connection = _RecordingConnection()

    ingestor_module.ingest_system(SYSTEM_ID64, connection=connection)

    body_cursor = connection.cursors[0]
    ring_inserts = [
        params
        for query, params in body_cursor.statements
        if "INSERT INTO bodies" in query and params[1] < 0
    ]
    assert [params[1] for params in ring_inserts] == [
        dump_module.inferred_ring_body_id(26, 1),
        dump_module.inferred_ring_body_id(26, 2),
    ]
    assert not any(
        "COALESCE(ring_class_id" in query
        for query, _params in body_cursor.statements
        if "UPDATE bodies SET" in query
    )
