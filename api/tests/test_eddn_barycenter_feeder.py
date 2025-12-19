import importlib
import sys

import pytest


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@pytest.fixture
def feeder_module(monkeypatch):
    fake_conn = FakeConnection()
    fake_psycopg = type("FakePsycopg", (), {})()
    fake_psycopg.connect = lambda **kwargs: fake_conn
    fake_psycopg.Connection = object
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    fake_renamer = type(
        "FakeRenamer",
        (),
        {
            "insert_barycenter_first_pass": lambda *args, **kwargs: None,
            "rename_barycenters_second_pass": lambda *args, **kwargs: [],
        },
    )()
    monkeypatch.setitem(sys.modules, "barycenter_renamer", fake_renamer)
    # Minimal zmq stub to avoid network/socket setup during import
    class FakeSocket:
        def connect(self, *_args, **_kwargs):
            pass

        def setsockopt_string(self, *_args, **_kwargs):
            pass

        def close(self, *_args, **_kwargs):
            pass

        def recv(self, *_args, **_kwargs):
            return b""

    class FakeContext:
        def __init__(self):
            self._socket = FakeSocket()

        def socket(self, *_args, **_kwargs):
            return self._socket

        def term(self):
            pass

    class FakePoller:
        def register(self, *_args, **_kwargs):
            pass

        def poll(self, *_args, **_kwargs):
            return {}

    class FakeZmq:
        SUB = "SUB"
        POLLIN = "POLLIN"
        SUBSCRIBE = "SUBSCRIBE"
        Socket = FakeSocket

        def Context(self):
            return FakeContext()

        def Poller(self):
            return FakePoller()

    monkeypatch.setitem(sys.modules, "zmq", FakeZmq())
    # Ensure a clean import each time so conn is reinitialized with the fake
    if "feeders.eddn_barycenter_feeder" in sys.modules:
        importlib.reload(sys.modules["feeders.eddn_barycenter_feeder"])
    module = importlib.import_module("feeders.eddn_barycenter_feeder")
    module.conn = fake_conn
    return module, fake_conn


def test_run_barycenter_renamer_accepts_int_updates(feeder_module, monkeypatch, capsys):
    feeder, fake_conn = feeder_module
    calls = []

    monkeypatch.setattr(
        feeder, "insert_barycenter_first_pass", lambda conn, sid, debug=False: calls.append(("insert", sid))
    )
    monkeypatch.setattr(
        feeder, "rename_barycenters_second_pass", lambda conn, sid, debug=False: 3
    )

    feeder._run_barycenter_renamer(42, "Test System")

    assert ("insert", 42) in calls
    assert fake_conn.commits == 1
    out = capsys.readouterr().out
    assert "3 barycenter(s) renamed" in out


def test_run_barycenter_renamer_rolls_back_on_error(feeder_module, monkeypatch):
    feeder, fake_conn = feeder_module

    def boom(*_args, **_kwargs):
        raise RuntimeError("fail")

    monkeypatch.setattr(feeder, "insert_barycenter_first_pass", boom)
    # simulate closed connection so reconnect branch is hit
    fake_conn.closed = True
    feeder._run_barycenter_renamer(99, "Broken")

    assert fake_conn.rollbacks == 1
    assert fake_conn.commits == 0


def test_process_event_skips_untrusted_source(feeder_module, monkeypatch):
    feeder, _ = feeder_module
    handler_called = False

    def nonlocal_handler():
        nonlocal handler_called
        handler_called = True

    monkeypatch.setattr(feeder, "is_trusted_source", lambda _: False)
    monkeypatch.setattr(feeder, "_handle_fss_all_bodies_found", lambda *_args, **_kwargs: nonlocal_handler())

    message = {
        "header": {"softwareName": "Untrusted"},
        "message": {"event": feeder.TARGET_EVENT, "SystemAddress": 1, "timestamp": "2024-01-01T00:00:00Z"},
    }

    feeder._process_event(message)

    assert handler_called is False


def test_process_event_routes_valid_payload(feeder_module, monkeypatch):
    feeder, _ = feeder_module
    received = []

    monkeypatch.setattr(feeder, "is_trusted_source", lambda _: True)
    monkeypatch.setattr(
        feeder, "_run_barycenter_renamer", lambda system_id, system_name: received.append((system_id, system_name))
    )

    message = {
        "header": {"softwareName": "Trusted"},
        "message": {
            "event": feeder.TARGET_EVENT,
            "SystemAddress": 12345,
            "StarSystem": "Sol",
            "timestamp": "2024-01-01T00:00:00Z",
        },
    }

    feeder._process_event(message)

    assert received == [(12345, "Sol")]


def test_is_trusted_source(feeder_module):
    feeder, _ = feeder_module
    assert feeder.is_trusted_source("EDDI") is True
    assert feeder.is_trusted_source("UnknownClient") is False


def test_process_event_ignores_other_events(feeder_module, monkeypatch):
    feeder, _ = feeder_module
    called = False

    monkeypatch.setattr(feeder, "is_trusted_source", lambda _: True)
    monkeypatch.setattr(feeder, "_handle_fss_all_bodies_found", lambda *_args, **_kwargs: setattr(sys.modules[__name__], "called", True))

    message = {
        "header": {"softwareName": "Trusted"},
        "message": {"event": "OtherEvent"},
    }
    feeder._process_event(message)
    assert not globals().get("called")


def test_parse_timestamp_handles_valid_and_invalid(feeder_module):
    feeder, _ = feeder_module
    good = feeder._parse_timestamp("2024-01-01T00:00:00Z")
    bad = feeder._parse_timestamp("not-a-date")
    empty = feeder._parse_timestamp(None)
    assert good is not None
    assert bad is None
    assert empty is None


def test_handle_fss_all_bodies_found_validates_input(feeder_module, monkeypatch):
    feeder, _ = feeder_module
    called = []
    monkeypatch.setattr(feeder, "_run_barycenter_renamer", lambda sid, name: called.append((sid, name)))

    # invalid system id
    feeder._handle_fss_all_bodies_found({"SystemAddress": "abc", "timestamp": "2024-01-01T00:00:00Z"})
    # invalid timestamp
    feeder._handle_fss_all_bodies_found({"SystemAddress": 1, "timestamp": "bad-time"})
    # valid
    feeder._handle_fss_all_bodies_found({"SystemAddress": 77, "timestamp": "2024-01-01T00:00:00Z", "StarSystem": "Foo"})

    assert called == [(77, "Foo")]

def test_recv_with_watchdog_timeout_and_success(feeder_module, monkeypatch):
    feeder, _ = feeder_module

    class ReadyPoller:
        def __init__(self, sock):
            self.sock = sock

        def register(self, *_args, **_kwargs):
            return None

        def poll(self, *_args, **_kwargs):
            return {self.sock: feeder.zmq.POLLIN}

    class DeadPoller:
        def register(self, *_args, **_kwargs):
            return None

        def poll(self, *_args, **_kwargs):
            return {}

    class FakeSock:
        def __init__(self, payload=b"ok"):
            self.payload = payload
            self.recv_calls = 0

        def recv(self):
            self.recv_calls += 1
            return self.payload

    # success path
    sock = FakeSock()
    monkeypatch.setattr(feeder.zmq, "Poller", lambda: ReadyPoller(sock))
    out = feeder.recv_with_watchdog(sock, 1)
    assert out == b"ok"
    assert sock.recv_calls == 1

    # timeout path
    sock2 = FakeSock()
    monkeypatch.setattr(feeder.zmq, "Poller", DeadPoller)
    with pytest.raises(feeder.StreamStalledError):
        feeder.recv_with_watchdog(sock2, 1)


def test_recv_with_watchdog_zero_timeout(feeder_module):
    feeder, _ = feeder_module

    class FakeSock:
        def __init__(self):
            self.called = 0

        def recv(self):
            self.called += 1
            return b"instant"

    sock = FakeSock()
    out = feeder.recv_with_watchdog(sock, 0)
    assert out == b"instant"
    assert sock.called == 1


def test_run_barycenter_renamer_handles_none_and_len_error(feeder_module, monkeypatch, capsys):
    feeder, fake_conn = feeder_module

    class BadLen:
        def __len__(self):
            raise RuntimeError("nope")

    # None result
    monkeypatch.setattr(feeder, "insert_barycenter_first_pass", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(feeder, "rename_barycenters_second_pass", lambda *_args, **_kwargs: None)
    feeder._run_barycenter_renamer(11, None)
    assert fake_conn.commits >= 1

    # len error branch
    monkeypatch.setattr(feeder, "rename_barycenters_second_pass", lambda *_args, **_kwargs: BadLen())
    feeder._run_barycenter_renamer(12, "LenFail")
    out = capsys.readouterr().out
    assert "LenFail" in out


def test_run_barycenter_renamer_handles_rollback_failure(feeder_module, monkeypatch):
    feeder, fake_conn = feeder_module

    def boom(*_args, **_kwargs):
        raise RuntimeError("explode")

    monkeypatch.setattr(feeder, "insert_barycenter_first_pass", boom)

    class RudeConn(FakeConnection):
        def rollback(self):
            super().rollback()
            raise RuntimeError("rollback fail")

    rude_conn = RudeConn()
    feeder.conn = rude_conn
    rude_conn.closed = True
    feeder._run_barycenter_renamer(13, "RollbackFail")
    assert rude_conn.rollbacks == 1


def test_stream_events_handles_one_message_then_stall(feeder_module, monkeypatch):
    feeder, fake_conn = feeder_module
    processed = []

    # minimal socket/context with tracking
    class TrackSocket:
        def __init__(self):
            self.closed = False

        def close(self, *_args, **_kwargs):
            self.closed = True

    class TrackContext:
        def __init__(self):
            self.terminated = False

        def term(self):
            self.terminated = True

    feeder.socket = TrackSocket()
    feeder.context = TrackContext()
    feeder.conn = fake_conn

    msg = {
        "header": {"softwareName": "EDDI"},
        "message": {
            "event": feeder.TARGET_EVENT,
            "SystemAddress": 5,
            "timestamp": "2024-01-01T00:00:00Z",
        },
    }
    payload = feeder.zlib.compress(feeder.json.dumps(msg).encode("utf-8"))

    def fake_recv(sock, timeout):
        if fake_recv.calls == 0:
            fake_recv.calls += 1
            return payload
        raise feeder.StreamStalledError("stall")

    fake_recv.calls = 0

    monkeypatch.setattr(feeder, "recv_with_watchdog", fake_recv)
    monkeypatch.setattr(feeder, "_process_event", lambda m: processed.append(m))

    with pytest.raises(SystemExit):
        feeder.stream_events()

    assert processed == [msg]
    assert feeder.socket.closed is True
    assert feeder.context.terminated is True
