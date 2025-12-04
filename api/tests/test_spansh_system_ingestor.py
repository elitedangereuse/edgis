from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture()
def spansh_system_module(monkeypatch):
    class DummySession:
        created: list["DummySession"] = []

        def __init__(self, connection=None, verbose: bool = False):
            self.connection = connection
            self.verbose = verbose
            self.calls: list[tuple[str, dict, int, object]] = []
            DummySession.created.append(self)

        def process_body(self, body: dict, sys_id: int, updatetime):
            self.calls.append(("process_body", body, sys_id, updatetime))

        def flush_batches(self):
            self.calls.append(("flush", {}, 0, None))

        def close(self):
            self.calls.append(("close", {}, 0, None))

    fake_psycopg = types.ModuleType("psycopg")
    fake_psycopg.Connection = object
    fake_psycopg.Cursor = object
    fake_psycopg.connect = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)

    stub_module = types.SimpleNamespace(
        SpanshBodyIngestSession=DummySession,
        conn="dummy",
        parse_timestamp=lambda value: value,
    )

    module_name = "feeders.spansh_dump_bodies_ingestor"
    monkeypatch.setitem(sys.modules, module_name, stub_module)

    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    system_module_name = "feeders.spansh_system_ingestor"
    sys.modules.pop(system_module_name, None)
    module = importlib.import_module(system_module_name)
    yield module, DummySession
    DummySession.created.clear()


def test_map_name_share_sequence_supports_multiple_sources(spansh_system_module):
    module, _ = spansh_system_module
    assert module.map_name_share_sequence({"Iron": 12.5}) == {"Iron": 12.5}
    mix = module.map_name_share_sequence([
        {"name": "Hydrogen", "share": 73.4},
        {"name": "Helium", "share": 26.6},
    ])
    assert mix == {"Hydrogen": 73.4, "Helium": 26.6}


def test_remap_parent_ids(spansh_system_module):
    module, _ = spansh_system_module
    parents = module.map_parents(
        [{"type": "Null", "id64": 10}, {"type": "Star", "id64": 20}]
    )
    remapped = module.remap_parent_ids(parents, {10: 1})
    assert remapped == [{"Null": 1}]


def test_normalize_body_merges_summary_fields(spansh_system_module):
    module, _ = spansh_system_module
    detail = {
        "body_id": 1,
        "name": "Alpha",
        "type": "Star",
        "parents": [],
        "rotational_period": 1.0,
    }
    summary = {"distance_to_arrival": 12.5, "subtype": "G"}
    normalized = module.normalize_body(detail, summary)
    assert normalized["bodyId"] == 1
    assert normalized["distanceToArrival"] == 12.5
    assert normalized["rotationalPeriod"] == 1.0
    assert normalized["subType"] == "G"


def test_ingest_system_processes_and_remaps(monkeypatch, spansh_system_module):
    module, DummySession = spansh_system_module

    class DummyConn:
        def __init__(self):
            self.commit_count = 0

        def commit(self):
            self.commit_count += 1

    bodies_summary = [
        {"id64": 111, "body_id": 11, "type": "Star", "name": "Alpha"},
        {
            "id64": 222,
            "body_id": 22,
            "type": "Planet",
            "parents": [{"type": "Null", "id64": 111}],
        },
    ]

    body_details = {
        111: {
            "body_id": 11,
            "id64": 111,
            "name": "Alpha",
            "type": "Star",
            "subtype": "G",
            "parents": [],
            "updated_at": "2024-01-01",
        },
        222: {
            "body_id": 22,
            "id64": 222,
            "name": "Beta",
            "type": "Planet",
            "subtype": "Class II gas giant",
            "parents": [{"type": "Null", "id64": 111}],
            "updated_at": "2024-01-02",
        },
    }

    def fake_fetch_json(path: str):
        if path.startswith("/system/"):
            return {
                "record": {
                    "updated_at": "2024-01-01",
                    "bodies": bodies_summary,
                }
            }
        body_key = int(path.split("/")[-1])
        return {"record": body_details[body_key]}

    monkeypatch.setattr(module, "fetch_json", fake_fetch_json)

    fake_conn = DummyConn()
    module.ingest_system(999, connection=fake_conn)

    session = DummySession.created[-1]
    first_call = session.calls[0]
    assert first_call[0] == "process_body"
    assert first_call[2] == 999
    assert first_call[1]["bodyId"] == 11

    second_call = session.calls[1]
    assert second_call[1]["bodyId"] == 22
    assert second_call[1]["parents"] == [{"Null": 11}]

    assert fake_conn.commit_count == 1
