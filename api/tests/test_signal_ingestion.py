from __future__ import annotations

from datetime import datetime, timezone
import importlib
import json
import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feeders import signal_ingestion


def fss_message() -> dict:
    return {
        "timestamp": "2026-09-30T12:00:00Z",
        "event": "FSSSignalDiscovered",
        "SystemAddress": 1900262951243,
        "signals": [
            {
                "timestamp": "2026-09-30T12:00:01Z",
                "SignalName": "The Sentinel",
                "SignalType": "Installation",
            },
            {
                "timestamp": "2026-09-30T12:00:02Z",
                "SignalName": "EXPLORER-CLASS X2X-74M",
                "IsStation": True,
            },
            {
                "timestamp": "2026-09-30T12:00:03Z",
                "SignalName": "$USS_NonHumanSignalSource;",
                "USSType": "$USS_Type_NonHuman;",
            },
        ],
    }


def test_fss_signals_keep_durable_records_and_skip_uss():
    rows = signal_ingestion.signal_rows_from_eddn(fss_message())

    assert [row["signal_name"] for row in rows] == [
        "The Sentinel", "EXPLORER-CLASS X2X-74M",
    ]
    assert rows[0]["signal_type"] == "Installation"
    assert rows[0]["is_station"] is False
    assert rows[1]["is_station"] is True
    assert rows[0]["observed_at"] == datetime(
        2026, 9, 30, 12, 0, 1, tzinfo=timezone.utc
    )


def test_fss_signals_require_system_id_and_timestamp():
    message = fss_message()
    message.pop("SystemAddress")
    assert signal_ingestion.signal_rows_from_eddn(message) == []

    message = fss_message()
    message["signals"][0].pop("timestamp")
    message.pop("timestamp")
    assert [
        row["signal_name"]
        for row in signal_ingestion.signal_rows_from_eddn(message)
    ] == ["EXPLORER-CLASS X2X-74M"]


def test_signal_upsert_uses_system_and_name_identity():
    assert "ON CONFLICT (system_id64, signal_name)" in signal_ingestion.SIGNAL_UPSERT
    assert "first_seen_at = LEAST" in signal_ingestion.SIGNAL_UPSERT
    assert "signals.is_station OR EXCLUDED.is_station" in signal_ingestion.SIGNAL_UPSERT


def test_eddn_signals_feeder_persists_fss_signals(monkeypatch, capsys):
    class Cursor:
        def __init__(self):
            self.statements = []
            self.results = iter([(True,), (False,)])

        def execute(self, query, params):
            self.statements.append((query, params))

        def fetchone(self):
            return next(self.results)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()
            self.commits = 0

        def cursor(self):
            return self.cursor_instance

        def commit(self):
            self.commits += 1

    class Socket:
        def connect(self, *_args):
            pass

        def setsockopt_string(self, *_args):
            pass

    class Context:
        def socket(self, *_args):
            return Socket()

    fake_conn = Connection()
    psycopg = types.ModuleType("psycopg")
    psycopg.Connection = Connection
    psycopg.connect = lambda **_kwargs: fake_conn
    zmq = types.ModuleType("zmq")
    zmq.Context = Context
    zmq.SUB = 1
    zmq.SUBSCRIBE = 2
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None
    monkeypatch.setitem(sys.modules, "psycopg", psycopg)
    monkeypatch.setitem(sys.modules, "zmq", zmq)
    monkeypatch.setitem(sys.modules, "dotenv", dotenv)
    sys.modules.pop("feeders.eddn_signals_feeder", None)
    feeder = importlib.import_module("feeders.eddn_signals_feeder")

    result = feeder.process_message(
        {"header": {"softwareName": "EDDiscovery"}, "message": fss_message()},
        connection=fake_conn,
    )

    assert result == feeder.ProcessOutcome("success", None, 2, 1)
    assert fake_conn.commits == 1
    inserts = [
        params for query, params in fake_conn.cursor_instance.statements
        if "INSERT INTO signals" in query
    ]
    assert [row["signal_name"] for row in inserts] == [
        "The Sentinel", "EXPLORER-CLASS X2X-74M",
    ]
    metric_statements = [
        query
        for query, _ in fake_conn.cursor_instance.statements
        if "eddn_signals_metrics" in query
    ]
    assert len(metric_statements) == 2
    assert (
        "FSS: The Sentinel (Installation) in 1900262951243"
        in capsys.readouterr().out
    )


def test_eddn_signals_feeder_rejects_unsupported_or_untrusted(monkeypatch):
    class Socket:
        def connect(self, *_args):
            pass

        def setsockopt_string(self, *_args):
            pass

    class Context:
        def socket(self, *_args):
            return Socket()

    psycopg = types.ModuleType("psycopg")
    psycopg.Connection = object
    zmq = types.ModuleType("zmq")
    zmq.Context = Context
    zmq.SUB = 1
    zmq.SUBSCRIBE = 2
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None
    monkeypatch.setitem(sys.modules, "psycopg", psycopg)
    monkeypatch.setitem(sys.modules, "zmq", zmq)
    monkeypatch.setitem(sys.modules, "dotenv", dotenv)
    sys.modules.pop("feeders.eddn_signals_feeder", None)
    feeder = importlib.import_module("feeders.eddn_signals_feeder")

    assert (
        feeder.process_message({"message": {"event": "Scan"}}).reason
        == "unsupported_event"
    )
    assert feeder.process_message(
        {"header": {"softwareName": "Unknown"}, "message": fss_message()}
    ).reason == "untrusted_source"
