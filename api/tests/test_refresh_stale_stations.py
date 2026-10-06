from __future__ import annotations

from datetime import datetime, timezone

from feeders.refresh_stale_stations import StaleStation, describe_update


def test_describe_update_distinguishes_a_move_from_an_in_place_refresh():
    station = StaleStation(
        market_id=3713969920,
        name="TNY-59G",
        system_id64=10461677819,
        system_name="HIP 58832",
        last_seen_at=datetime(2026, 1, 11, tzinfo=timezone.utc),
    )

    assert "move TNY-59G" in describe_update(
        station, {"system_id64": 633877140210}
    )
    assert "refresh TNY-59G" in describe_update(
        station, {"system_id64": 10461677819}
    )
