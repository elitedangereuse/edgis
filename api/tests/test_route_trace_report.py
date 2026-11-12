import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "tools"))

import route_trace_report as rtr  # noqa: E402


LINES = [
    '2026-11-07 18:27:01,123 {"kind": "route", "frm": "A", "to": "B", "status": "ok", "total_jumps": 10, "solve_ms": 200.0}',
    '{"ts": "2026-11-12T15:40:00.000+00:00", "kind": "route", "status": "ok", "total_jumps": 80, "solve_ms": 9000.0, '
    '"prof": {"wall_ms": 9000, "cpu_ms": 6000, "db_ms": 2500, "db_calls": 40, "db_rows": 90000, "wait_ms": 500}}',
    '{"ts": "2026-11-12T15:40:30.000+00:00", "kind": "route", "status": "ok", "cached": true, "total_jumps": 80, "solve_ms": 9000.0}',
    '{"ts": "2026-11-12T15:41:00.000+00:00", "kind": "route", "status": "warming"}',
    'not json at all',
]


def test_parse_lines_reads_both_formats_and_skips_garbage():
    rows = rtr.parse_lines(LINES)
    assert len(rows) == 4
    assert rows[0]["_t"] == "2026-11-07 18:27:01"
    assert rows[1]["_t"] == "2026-11-12 15:40:00"


def test_report_splits_fresh_solve_time_into_cpu_db_and_waiting():
    text = rtr.report(rtr.parse_lines(LINES))
    assert "cache hits: 1/3 (33%)" in text
    assert "1 profiled solves" in text
    assert re.search(r"CPU \(search \+ Python\)\s+67%", text)
    assert re.search(r"DB \(PostGIS bridge queries\)\s+28%", text)
    assert re.search(r"waiting \(GIL contention\)\s+6%", text)
    assert "warming 1" in text


def test_report_since_filters_older_rows():
    text = rtr.report(rtr.parse_lines(LINES), since="2026-11-12")
    assert "2026-11-07" not in text
