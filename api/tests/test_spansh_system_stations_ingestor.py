from __future__ import annotations

from dataclasses import dataclass

from feeders import spansh_system_stations_ingestor as ingestor


def test_station_market_ids_skips_invalid_values_and_deduplicates():
    record = {
        "stations": [
            {"market_id": 1},
            {"market_id": "2"},
            {"market_id": 1},
            {"market_id": -1},
            {"name": "No market ID"},
            "not a station",
        ]
    }

    assert ingestor.station_market_ids(record) == [1, 2]


def test_station_from_system_summary_uses_the_system_timestamp():
    row = ingestor.station_from_system_summary(
        {
            "market_id": 10,
            "name": "Summary Carrier",
            "type": "Drake-Class Carrier",
            "large_pads": 8,
            "services": ["Dock"],
        },
        42,
        "2026-10-10T12:00:00Z",
    )

    assert row is not None
    assert row["system_id64"] == 42
    assert row["last_source"] == "spansh_system_api"
    assert row["services"] == '["Dock"]'


@dataclass
class _Response:
    payload: dict
    status_code: int = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self):
        self.urls: list[str] = []

    def get(self, url, timeout):
        self.urls.append(url)
        if "/system/" in url:
            return _Response({"record": {"stations": [{"market_id": 10}, {"market_id": 20}]}})
        market_id = int(url.rsplit("/", 1)[-1])
        return _Response(
            {
                "record": {
                    "market_id": market_id,
                    "name": f"Station {market_id}",
                    "type": "Drake-Class Carrier",
                    "system_id64": 42 if market_id == 10 else 99,
                    "updated_at": "2026-10-09T12:00:00Z",
                }
            }
        )


class _Cursor:
    def __init__(self):
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, query, params):
        self.queries.append((query, params))

    def fetchone(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Connection:
    def __init__(self):
        self.cursor_instance = _Cursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_sync_system_stations_dry_run_discovers_missing_and_notes_moves():
    connection = _Connection()
    counts = ingestor.sync_system_stations(
        connection,
        42,
        apply=False,
        request_delay_seconds=0,
        session=_Session(),
    )

    assert counts == {
        "listed": 2,
        "fetched": 2,
        "created": 2,
        "updated": 0,
        "not_found": 0,
        "invalid": 0,
        "moved": 1,
        "existing": 0,
        "located_elsewhere": 0,
        "summary_fallback": 0,
    }
    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_sync_system_stations_applies_each_valid_record(monkeypatch):
    connection = _Connection()
    persisted: list[int] = []

    def fake_upsert(_cursor, station):
        persisted.append(station["market_id"])
        return station["market_id"] == 10

    monkeypatch.setattr(
        ingestor,
        "upsert_station",
        fake_upsert,
    )

    counts = ingestor.sync_system_stations(
        connection,
        42,
        apply=True,
        request_delay_seconds=0,
        session=_Session(),
    )

    assert persisted == [10, 20]
    assert counts["created"] == 1
    assert counts["updated"] == 1
    assert connection.commits == 1
