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


def test_resolve_star_type_handles_missing_data(spansh_module):
    assert spansh_module.resolve_star_type({}) is None
