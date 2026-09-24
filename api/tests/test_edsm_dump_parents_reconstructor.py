from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feeders import edsm_dump_parents_reconstructor as reconstructor


def test_station_host_reference_uses_edsm_market_system_and_body_name():
    assert reconstructor.station_host_reference(
        {
            "marketId": 3227207936,
            "systemId64": 1762807253363,
            "body": {"id": 10159049, "name": "Wolf 827 A 3"},
        }
    ) == (3227207936, 1762807253363, "Wolf 827 A 3")


def test_station_host_reference_rejects_rows_without_an_edsm_body_name():
    assert reconstructor.station_host_reference(
        {"marketId": 1, "systemId64": 2, "body": {"id": 3}}
    ) is None


def test_lookup_parent_chain_and_backfill_update_only_parents():
    class Cursor:
        def __init__(self):
            self.statements = []
            self.rowcount = 1

        def execute(self, query, params):
            self.statements.append((query, params))

        def fetchone(self):
            return (14, "Planet", [{"Star": 1}, {"Null": 0}])

    cursor = Cursor()
    parents = reconstructor.lookup_parent_chain(
        cursor, 3932277478106, "Founders World"
    )

    assert parents == [{"Planet": 14}, {"Star": 1}, {"Null": 0}]
    assert reconstructor.update_empty_station_parents(
        cursor, 128666762, 3932277478106, parents
    )
    query, params = cursor.statements[-1]
    assert "UPDATE stations" in query
    assert "parents IS NULL OR parents = '[]'::jsonb" in query
    assert json.loads(params[0]) == parents
    assert params[1:] == (128666762, 3932277478106)
