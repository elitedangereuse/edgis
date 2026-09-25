from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import importlib
import json
import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feeders import station_ingestion


def test_station_upsert_backfills_empty_parents_without_requiring_a_newer_location():
    assert "stations.parents = '[]'::jsonb" in station_ingestion.STATION_UPSERT
    assert "COALESCE(EXCLUDED.body_id, stations.body_id)" in station_ingestion.STATION_UPSERT


def test_spansh_nested_station_uses_its_host_as_first_parent():
    row = station_ingestion.station_from_spansh(
        {
            "id": 3910909952,
            "name": "Egebe Command Facility",
            "type": "Settlement",
            "updateTime": "2025-09-06T21:49:08Z",
        },
        13862946481609,
        None,
        body_name="Upaniklis B 2 b",
        parents=[{"Planet": 12}, {"Star": 2}, {"Null": 0}],
    )

    assert row is not None
    assert row["market_id"] == 3910909952
    assert row["body_id"] is None
    assert row["body_name"] == "Upaniklis B 2 b"
    assert json.loads(row["parents"]) == [
        {"Planet": 12}, {"Star": 2}, {"Null": 0}
    ]


def test_station_parent_chain_requires_a_typed_host_body():
    assert station_ingestion.station_parent_chain(
        12, "Planet", [{"Star": 2}, {"Null": 0}]
    ) == [{"Planet": 12}, {"Star": 2}, {"Null": 0}]
    assert station_ingestion.station_parent_chain(None, "Planet", []) == []


def test_spansh_station_serializes_ijson_decimal_economies():
    row = station_ingestion.station_from_spansh(
        {
            "id": 1,
            "name": "Example Station",
            "type": "Outpost",
            "updateTime": "2026-09-24T12:00:00Z",
            "economies": {"High Tech": Decimal("0.67")},
        },
        42,
        None,
    )

    assert row is not None
    assert json.loads(row["economies"]) == {"High Tech": 0.67}


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


def test_docked_station_reconstructs_parents_from_its_host_body():
    class Cursor:
        def __init__(self):
            self.statement = None

        def execute(self, query, params):
            self.statement = (query, params)

        def fetchone(self):
            return (14, "Planet", [{"Star": 1}, {"Null": 0}])

    station = station_ingestion.station_from_eddn(
        {
            "event": "Docked",
            "timestamp": "2026-09-24T12:00:00Z",
            "MarketID": 128666762,
            "StationName": "Jameson Memorial",
            "StationType": "Orbis",
            "SystemAddress": 3932277478106,
            "Body": "Founders World",
            "BodyID": 69,
        }
    )

    assert station is not None
    cursor = Cursor()
    assert station_ingestion.reconstruct_station_parents(
        cursor, station, "Founders World"
    )
    assert station["body_id"] == 69
    assert json.loads(station["parents"]) == [
        {"Planet": 14}, {"Star": 1}, {"Null": 0}
    ]
    assert cursor.statement[1] == (3932277478106, "Founders World")


def test_docked_station_leaves_parents_empty_when_host_is_unknown():
    class Cursor:
        def execute(self, *_args):
            pass

        def fetchone(self):
            return None

    station = {"system_id64": 42, "parents": json.dumps([])}

    assert not station_ingestion.reconstruct_station_parents(
        Cursor(), station, "Unknown World"
    )
    assert json.loads(station["parents"]) == []


def test_station_parents_fall_back_to_nearest_arrival_distance():
    class Cursor:
        def __init__(self):
            self.statement = None

        def execute(self, query, params):
            self.statement = (query, params)

        def fetchone(self):
            return (3, "Planet", [{"Star": 1}, {"Null": 0}], "Earth", 502.233897)

    station = {"system_id64": 10477373803, "parents": json.dumps([])}

    assert station_ingestion.reconstruct_station_parents(
        Cursor(), station, None, distance_from_arrival_ls=502.254085
    )
    assert json.loads(station["parents"]) == [
        {"Planet": 3}, {"Star": 1}, {"Null": 0}
    ]
    resolution = station["_parent_resolution"]
    assert resolution["source"] == "arrival_distance"
    assert resolution["body_name"] == "Earth"
    assert resolution["body_distance"] == 502.233897
    assert abs(resolution["distance_delta"] - 0.020188) < 1e-9


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


def test_eddn_station_feeder_reconstructs_location_parents(monkeypatch, capsys):
    class Cursor:
        def __init__(self):
            self.statements = []
            self.results = iter([
                (28, "Planet", [{"Planet": 22}, {"Null": 21}, {"Star": 0}]),
                (True,),
            ])

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
    sys.modules.pop("feeders.eddn_stations_feeder", None)
    feeder = importlib.import_module("feeders.eddn_stations_feeder")

    result = feeder.process_message(
        {
            "header": {"softwareName": "EDDiscovery"},
            "message": {
                "event": "Location", "Docked": True,
                "timestamp": "2026-09-24T12:00:00Z",
                "MarketID": 128102648, "StationName": "de Kamp Orbital",
                "StationType": "Orbis", "SystemAddress": 1178725255531,
                "StarSystem": "Delkar", "Body": "Delkar 28", "BodyID": 55,
                "SystemAllegiance": "Alliance",
            },
        },
    )

    assert result.status == "success"
    assert fake_conn.commits == 1
    statements = fake_conn.cursor_instance.statements
    assert any("system_allegiances" in query for query, _ in statements)
    station_params = next(
        params for query, params in statements if "INSERT INTO stations" in query
    )
    assert json.loads(station_params["parents"]) == [
        {"Planet": 28}, {"Planet": 22}, {"Null": 21}, {"Star": 0}
    ]
    assert any("eddn_stations_metrics" in query for query, _ in statements)
    output = capsys.readouterr().out
    station_log = "Location: de Kamp Orbital [128102648] in Delkar [1178725255531]"
    assert station_log in output
    assert output.index(station_log) < output.index("  parents: [{'Planet': 28}")

    fake_conn.cursor_instance.results = iter([None, (True,)])
    feeder.process_message(
        {
            "header": {"softwareName": "EDDiscovery"},
            "message": {
                "event": "Docked", "timestamp": "2026-09-24T12:00:00Z",
                "MarketID": 128102648, "StationName": "Wyeth City",
                "StationType": "Orbis", "SystemAddress": 233238947004,
                "StarSystem": "Theta Indi", "Body": "Wyeth City", "BodyID": 55,
                "DistFromStarLS": 502.0,
            },
        },
        connection=fake_conn,
    )

    output = capsys.readouterr().out
    assert "parents: can't find host '<none>'" in output
    assert "BodyName=<missing>" in output
    assert "Parents=<missing>" in output
    assert "BodyID=55" in output
