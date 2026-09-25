import importlib
import sys
import types
from datetime import datetime, timezone
from importlib.machinery import ModuleSpec
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FEEDERS_PATH = REPO_ROOT / "feeders"


class DummyCursor:
    def __init__(self):
        self.statements: list[tuple[str, tuple]] = []
        self.closed = False

    def execute(self, query, params):
        self.statements.append((query, params))

    def fetchone(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed = True


class DummyConn:
    def __init__(self):
        self.cursors: list[DummyCursor] = []

    def cursor(self):
        cursor = DummyCursor()
        self.cursors.append(cursor)
        return cursor

    def close(self, *args, **kwargs):
        pass

    def commit(self):
        pass


class DummySocket:
    def __init__(self):
        self.messages: list[bytes] = []
        self.closed = False

    def connect(self, *_args, **_kwargs):
        pass

    def setsockopt_string(self, *_args, **_kwargs):
        pass

    def recv(self):
        if not self.messages:
            raise RuntimeError("No data queued")
        return self.messages.pop(0)

    def close(self, *_args, **_kwargs):
        self.closed = True


def _make_zmq_stub():
    class Context:
        def __init__(self):
            self._socket = DummySocket()

        def socket(self, *_args, **_kwargs):
            return self._socket

        def term(self):
            pass

    class Poller:
        emit_next = False

        def __init__(self):
            self._registered = None
            self._emit = self.__class__.emit_next
            self.__class__.emit_next = False

        def register(self, sock, _flag):
            self._registered = sock

        def poll(self, _timeout):
            if (
                self._emit
                and self._registered is not None
                and getattr(self._registered, "messages", None)
            ):
                return {self._registered: 1}
            return {}

    zmq_module = types.ModuleType("zmq")
    zmq_module.Context = Context
    zmq_module.Poller = Poller
    zmq_module.POLLIN = 1
    zmq_module.SUB = 2
    zmq_module.SUBSCRIBE = 3
    zmq_module.Socket = DummySocket
    return zmq_module


def _install_stub_modules(monkeypatch):
    conn = DummyConn()
    psycopg_module = types.ModuleType("psycopg")

    def _connect(**_kwargs):
        return conn

    psycopg_module.connect = _connect
    psycopg_module.Connection = DummyConn
    dotenv_module = types.ModuleType("dotenv")
    dotenv_module.load_dotenv = lambda: None
    zmq_module = _make_zmq_stub()
    feeders_pkg = types.ModuleType("feeders")
    feeders_pkg.__path__ = [str(FEEDERS_PATH)]
    feeders_pkg.__spec__ = ModuleSpec("feeders", loader=None, is_package=True)

    monkeypatch.setitem(sys.modules, "psycopg", psycopg_module)
    monkeypatch.setitem(sys.modules, "dotenv", dotenv_module)
    monkeypatch.setitem(sys.modules, "zmq", zmq_module)
    monkeypatch.setitem(sys.modules, "feeders", feeders_pkg)

    return conn, zmq_module


def _import_feeder(monkeypatch, module_name):
    _install_stub_modules(monkeypatch)
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


@pytest.fixture
def systems_module(monkeypatch):
    return _import_feeder(monkeypatch, "feeders.eddn_systems_feeder")


@pytest.fixture
def bodies_module(monkeypatch):
    return _import_feeder(monkeypatch, "feeders.eddn_bodies_feeder")


def test_systems_validators(systems_module):
    assert systems_module.is_valid_coordinates(10, 5, -10)
    assert not systems_module.is_valid_coordinates(80000, 0, 0)
    assert not systems_module.is_valid_coordinates(0.5, 0.0, 0.0)
    assert systems_module.is_valid_system_name("Sol")
    assert not systems_module.is_valid_system_name("Test")


def test_systems_record_metrics(monkeypatch, systems_module):
    cursor = DummyCursor()
    bucket_time = datetime(2024, 1, 1, 12, 34, 56, tzinfo=timezone.utc)

    class FakeDateTime:
        @classmethod
        def now(cls, tz=None):  # noqa: D401 - simple stub
            return bucket_time

    monkeypatch.setattr(systems_module, "datetime", FakeDateTime)

    systems_module.record_systems_processed(cursor, amount=2, is_new=False)
    systems_module.record_systems_processed(cursor, amount=3, is_new=True)

    minute_bucket = bucket_time.replace(second=0, microsecond=0)
    assert cursor.statements[0][1] == (minute_bucket, 2)
    assert cursor.statements[1][1] == (minute_bucket, 3)
    assert "systems_processed" in cursor.statements[0][0]
    assert "systems_new" in cursor.statements[1][0]


def test_systems_upsert_rejects_duplicate_mapped_name(systems_module):
    inserted = systems_module._upsert_system(
        12345,
        "Sol",
        None,
        datetime(2026, 9, 25, tzinfo=timezone.utc),
        (3.0, -1.0, 5.0),
    )

    assert not inserted
    query, params = systems_module.conn.cursors[0].statements[0]
    assert "LOWER(existing.name) = LOWER(%s)" in query
    assert "FROM bodies b" in query
    assert params[-2:] == (12345, "Sol")


def test_systems_recv_with_watchdog_timeout(systems_module):
    sock = systems_module.zmq.Socket()
    with pytest.raises(systems_module.StreamStalledError):
        systems_module.recv_with_watchdog(sock, timeout_seconds=1)


def test_systems_recv_with_watchdog_success(systems_module):
    sock = systems_module.zmq.Socket()
    sock.messages.append(b"data")
    systems_module.zmq.Poller.emit_next = True
    payload = systems_module.recv_with_watchdog(sock, timeout_seconds=5)
    assert payload == b"data"


def test_bodies_validators(bodies_module):
    assert bodies_module.is_valid_coordinates(-10, 0, 10)
    assert not bodies_module.is_valid_coordinates(0, 50000, 0)
    assert not bodies_module.is_valid_coordinates(0.3, 0, 0)
    assert bodies_module.is_valid_system_name("Colonia")
    assert not bodies_module.is_valid_system_name("NULL")


def test_bodies_record_metrics(monkeypatch, bodies_module):
    cursor = DummyCursor()
    bucket_time = datetime(2023, 7, 4, 15, 0, 59, tzinfo=timezone.utc)

    class FakeDateTime:
        @classmethod
        def now(cls, tz=None):
            return bucket_time

    monkeypatch.setattr(bodies_module, "datetime", FakeDateTime)

    bodies_module.record_bodies_processed(cursor, amount=1, is_new=False)
    bodies_module.record_bodies_processed(cursor, amount=4, is_new=True)

    minute_bucket = bucket_time.replace(second=0, microsecond=0)
    assert cursor.statements[0][1] == (minute_bucket, 1)
    assert cursor.statements[1][1] == (minute_bucket, 4)
    assert "bodies_processed" in cursor.statements[0][0]
    assert "bodies_new" in cursor.statements[1][0]


def test_bodies_recv_with_watchdog_timeout(bodies_module):
    sock = bodies_module.zmq.Socket()
    with pytest.raises(bodies_module.StreamStalledError):
        bodies_module.recv_with_watchdog(sock, timeout_seconds=1)


def test_bodies_recv_with_watchdog_success(bodies_module):
    sock = bodies_module.zmq.Socket()
    sock.messages.append(b"payload")
    bodies_module.zmq.Poller.emit_next = True
    payload = bodies_module.recv_with_watchdog(sock, timeout_seconds=5)
    assert payload == b"payload"


def peacock_ring_scan_message():
    return {
        "header": {"softwareName": "EDDiscovery"},
        "message": {
            "timestamp": "2026-09-21T01:26:03Z",
            "event": "Scan",
            "BodyName": "Peacock 5",
            "BodyID": 29,
            "StarSystem": "Peacock",
            "SystemAddress": 4459065949,
            "PlanetClass": "Rocky body",
            "Rings": [
                {
                    "Name": "Peacock 5 A Ring",
                    "RingClass": "eRingClass_Rocky",
                }
            ],
        },
    }


def test_bodies_assigns_negative_ids_to_inferred_rings(monkeypatch, bodies_module):
    monkeypatch.setattr(
        bodies_module, "get_lookup_id", lambda *_args, **_kwargs: 1
    )

    result = bodies_module.process_message(peacock_ring_scan_message(), verbose=False)

    assert result.status == "success"
    body_inserts = [
        params
        for cursor in bodies_module.conn.cursors
        for query, params in cursor.statements
        if "INSERT INTO bodies" in query
    ]
    assert [params[1] for params in body_inserts] == [-117, 29]
    assert bodies_module.inferred_ring_body_id(29, 2) == -118


def test_bodies_does_not_duplicate_existing_positive_ring(
    monkeypatch, bodies_module
):
    monkeypatch.setattr(
        bodies_module, "get_lookup_id", lambda *_args, **_kwargs: 1
    )
    monkeypatch.setattr(
        bodies_module, "has_positive_ring_body", lambda *_args, **_kwargs: True
    )

    result = bodies_module.process_message(peacock_ring_scan_message(), verbose=False)

    assert result.status == "success"
    body_inserts = [
        params
        for cursor in bodies_module.conn.cursors
        for query, params in cursor.statements
        if "INSERT INTO bodies" in query
    ]
    assert [params[1] for params in body_inserts] == [29]


def test_bodies_preserves_and_reads_direct_ring_metadata(
    monkeypatch, bodies_module
):
    monkeypatch.setattr(
        bodies_module, "get_lookup_id", lambda *_args, **_kwargs: 1
    )
    message = peacock_ring_scan_message()
    message["message"].update(
        {
            "BodyName": "Peacock 5 A Ring",
            "BodyID": 32,
            "RingClass": "eRingClass_Rocky",
            "InnerRad": 2725800000.0,
            "OuterRad": 6868500000.0,
            "MassMT": 221260000000000.0,
        }
    )

    result = bodies_module.process_message(message, verbose=False)

    assert result.status == "success"
    direct_ring_insert = next(
        params
        for cursor in bodies_module.conn.cursors
        for query, params in cursor.statements
        if "INSERT INTO bodies" in query and params[1] == 32
    )
    assert direct_ring_insert[37:] == [
        1,
        2725800000.0,
        6868500000.0,
        221260000000000.0,
    ]
    assert "ring_class_id           = COALESCE" in bodies_module.UPSERT_BODY
    assert "jsonb_array_length(EXCLUDED.parents) = 0" in bodies_module.UPSERT_BODY
