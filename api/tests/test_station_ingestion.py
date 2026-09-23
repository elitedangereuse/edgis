from __future__ import annotations

from datetime import datetime, timezone
import importlib
import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feeders import station_ingestion


def test_spansh_nested_station_keeps_game_body_id():
    row = station_ingestion.station_from_spansh(
        {
            "id": 3910909952,
            "name": "Egebe Command Facility",
            "type": "Settlement",
            "updateTime": "2025-09-06T21:49:08Z",
        },
        13862946481609,
        None,
        body_id=12,
        body_name="Upaniklis B 2 b",
    )

    assert row is not None
    assert row["market_id"] == 3910909952
    assert row["body_id"] == 12
    assert row["body_name"] == "Upaniklis B 2 b"
    assert row["attachment_source"] == "spansh_body_id"


def test_eddn_carrier_jump_preserves_market_and_body_ids():
    row = station_ingestion.station_from_eddn(
        {
            "event": "CarrierJump",
            "timestamp": "2026-09-24T12:00:00Z",
            "MarketID": 3700005632,
            "StationName": "FC L14-X1J",
            "StationType": "FleetCarrier",
            "SystemAddress": 5363877956440,
            "Body": "Hermitage",
            "BodyID": 0,
        }
    )

    assert row is not None
    assert row["market_id"] == 3700005632
    assert row["system_id64"] == 5363877956440
    assert row["body_id"] == 0
    assert row["is_carrier"] is True
    assert row["attachment_source"] == "eddn_body_id"


def test_journal_station_normalizes_economies_and_allegiance():
    row = station_ingestion.station_from_eddn(
        {
            "event": "Docked",
            "timestamp": "2026-09-24T12:00:00Z",
            "MarketID": 123,
            "StationName": "Example Orbital",
            "StationType": "Coriolis",
            "SystemAddress": 456,
            "StationAllegiance": "Federation",
            "StationEconomy": "$economy_High_Tech;",
            "StationEconomies": [
                {"Name": "$economy_High_Tech;", "Proportion": 0.7},
                {"Name": "$economy_Industrial;", "Proportion": 0.3},
            ],
        }
    )

    assert row is not None
    assert row["allegiance"] == "Federation"
    assert row["primary_economy"] == "High Tech"
    assert row["economies"] == '{"High Tech": 70.0, "Industrial": 30.0}'


def test_journal_station_prefers_localized_name_and_decodes_generated_name():
    message = {
        "event": "Location",
        "timestamp": "2026-09-24T12:00:00Z",
        "MarketID": 987654321,
        "StationName": "$Operations_Runner_Name:#index=1;",
        "StationType": "Coriolis",
        "SystemAddress": 7268024264097,
    }
    row = station_ingestion.station_from_eddn(message)
    assert row is not None
    assert row["name"] == "Operations Runner"

    message["StationName_Localised"] = "Operations Runner Localised"
    row = station_ingestion.station_from_eddn(message)
    assert row is not None
    assert row["name"] == "Operations Runner Localised"


def test_reserved_operations_runner_market_ids_are_not_stations():
    message = {
        "event": "Location",
        "timestamp": "2026-09-24T12:00:00Z",
        "StationName": "Operations Runner",
        "StationType": "Coriolis",
        "SystemAddress": 7268024264097,
    }
    for market_id in station_ingestion.EXCLUDED_MARKET_IDS:
        assert station_ingestion.station_from_eddn({**message, "MarketID": market_id}) is None


def test_spansh_skips_reserved_operations_runner_market_ids():
    assert station_ingestion.station_from_spansh(
        {
            "id": 127000000,
            "name": "Operations Runner",
            "type": "Coriolis",
            "updateTime": "2026-09-24T12:00:00Z",
        },
        7268024264097,
        None,
    ) is None


def test_system_allegiance_ignores_none_but_preserves_real_values():
    message = {
        "SystemAddress": 42,
        "timestamp": "2026-09-24T12:00:00Z",
        "SystemAllegiance": "Alliance",
    }
    assert station_ingestion.system_allegiance_from_eddn(message) == (
        42,
        "Alliance",
        datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc),
    )
    message["SystemAllegiance"] = ""
    assert station_ingestion.system_allegiance_from_eddn(message) is None


def test_eddn_station_feeder_handles_carrier_jump(monkeypatch):
    class Cursor:
        def __init__(self):
            self.statements = []

        def execute(self, query, params):
            self.statements.append((query, params))

        def fetchone(self):
            return (True,)

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
    sys.modules.pop("feeders.eddn_stations_feeder", None)
    feeder = importlib.import_module("feeders.eddn_stations_feeder")

    result = feeder.process_message(
        {
            "header": {"softwareName": "EDDiscovery"},
            "message": {
                "event": "CarrierJump", "timestamp": "2026-09-24T12:00:00Z",
                "MarketID": 3700005632, "StationName": "FC L14-X1J",
                "StationType": "FleetCarrier", "SystemAddress": 5363877956440,
                "Body": "Hermitage", "BodyID": 0, "SystemAllegiance": "Alliance",
            },
        },
        verbose=False,
    )

    assert result.status == "success"
    assert fake_conn.commits == 1
    statements = fake_conn.cursor_instance.statements
    assert any("system_allegiances" in query for query, _ in statements)
    assert any("INSERT INTO stations" in query for query, _ in statements)
    assert any("eddn_stations_metrics" in query for query, _ in statements)
