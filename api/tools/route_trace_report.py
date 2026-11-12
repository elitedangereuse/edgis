#!/usr/bin/env python3
"""Summarise route traces (route_trace*.jsonl) into a load/latency baseline.

Reads one or more trace files -- the current JSON-lines format and the older
"<asctime> {json}" lines -- and prints, for the period they cover:

  - volume: plots per day, peak requests per minute, cache hit rate, callers;
  - fresh (uncached) solve times: percentiles, overall and by route length;
  - with "prof" (per-solve breakdown, since 2026-11-12): where solve time goes,
    CPU vs DB vs waiting (GIL contention), and DB calls/rows per solve;
  - failures by status, and spansh compare calls.

The same report before and after each scaling step is the comparison. Usage:

  python3 api/tools/route_trace_report.py route_trace*.jsonl [--since 2026-11-12]
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from typing import Iterable, Optional

_LEGACY = re.compile(r"^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d),\d+ (\{.*\})$")


def parse_lines(lines: Iterable[str]) -> list[dict]:
    """Trace rows with a ``_t`` "YYYY-MM-DD HH:MM:SS" (UTC) key; bad lines skipped."""
    rows = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        legacy = _LEGACY.match(line)
        try:
            if legacy:
                row = json.loads(legacy.group(3))
                row["_t"] = f"{legacy.group(1)} {legacy.group(2)}"
            else:
                row = json.loads(line)
                row["_t"] = str(row.get("ts", ""))[:19].replace("T", " ")
        except ValueError:
            continue
        if isinstance(row, dict) and row.get("_t"):
            rows.append(row)
    return rows


def percentile(sorted_values: list[float], p: float) -> float:
    return sorted_values[min(len(sorted_values) - 1, int(p * len(sorted_values)))]


def _pcts(values: list[float]) -> str:
    v = sorted(values)
    return (f"n={len(v)} p50={percentile(v, .5):.0f} p90={percentile(v, .9):.0f} "
            f"p99={percentile(v, .99):.0f} max={v[-1]:.0f}")


def _length_band(jumps: int) -> str:
    if jumps < 20:
        return "<20 jumps"
    if jumps < 60:
        return "20-59 jumps"
    if jumps < 150:
        return "60-149 jumps"
    return "150+ jumps"


def report(rows: list[dict], since: Optional[str] = None) -> str:
    if since:
        rows = [r for r in rows if r["_t"] >= since]
    out: list[str] = []
    if not rows:
        return "no trace rows"
    out.append(f"period: {min(r['_t'] for r in rows)} .. {max(r['_t'] for r in rows)} UTC"
               f" ({len(rows)} lines)")

    route = [r for r in rows if r.get("kind") == "route"]
    ok = [r for r in route if r.get("status") == "ok"]
    fresh = [r for r in ok if not r.get("cached") and not r.get("coalesced")
             and r.get("solve_ms") is not None]
    cached = sum(1 for r in ok if r.get("cached"))
    out.append("\n== volume")
    days = collections.Counter(r["_t"][:10] for r in ok)
    out.append("ok plots/day: " + ", ".join(f"{d} {n}" for d, n in sorted(days.items())))
    per_min = collections.Counter(r["_t"][:16] for r in route)
    if per_min:
        minute, n = per_min.most_common(1)[0]
        out.append(f"peak route requests/minute: {n} ({minute})")
    if ok:
        out.append(f"cache hits: {cached}/{len(ok)} ({cached / len(ok):.0%}),"
                   f" coalesced: {sum(1 for r in ok if r.get('coalesced'))}")
    out.append(f"distinct callers: {len({r.get('caller') for r in ok})}")

    if fresh:
        out.append("\n== fresh solve time (ms)")
        out.append("all          " + _pcts([r["solve_ms"] for r in fresh]))
        bands = collections.defaultdict(list)
        for r in fresh:
            bands[_length_band(int(r.get("total_jumps") or 0))].append(r["solve_ms"])
        for band in ("<20 jumps", "20-59 jumps", "60-149 jumps", "150+ jumps"):
            if bands.get(band):
                out.append(f"{band:12s} " + _pcts(bands[band]))

    profiled = [r["prof"] for r in fresh if isinstance(r.get("prof"), dict)]
    if profiled:
        out.append(f"\n== where solve time goes ({len(profiled)} profiled solves)")
        wall = sum(p.get("wall_ms", 0) for p in profiled) or 1.0
        for key, label in (("cpu_ms", "CPU (search + Python)"),
                           ("db_ms", "DB (PostGIS bridge queries)"),
                           ("wait_ms", "waiting (GIL contention)")):
            total = sum(p.get(key, 0) for p in profiled)
            out.append(f"{label:30s} {total / wall:5.0%}  " + _pcts([p.get(key, 0) for p in profiled]))
        out.append("DB calls/solve               " + _pcts([p.get("db_calls", 0) for p in profiled]))
        out.append("DB rows/solve                " + _pcts([p.get("db_rows", 0) for p in profiled]))

    failures = collections.Counter(r.get("status") for r in route if r.get("status") != "ok")
    if failures:
        out.append("\n== non-ok route answers: "
                   + ", ".join(f"{s} {n}" for s, n in failures.most_common()))
    spansh = [r for r in rows if r.get("kind") == "spansh"]
    if spansh:
        by = collections.Counter(f"{r.get('tool') or '?'}:{r.get('status')}" for r in spansh)
        out.append("== spansh compares: " + ", ".join(f"{k} {n}" for k, n in by.most_common()))
    return "\n".join(out)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("files", nargs="+", help="route_trace*.jsonl files")
    parser.add_argument("--since", help="only rows at or after this UTC time (YYYY-MM-DD[ HH:MM])")
    args = parser.parse_args(argv)
    rows: list[dict] = []
    for path in args.files:
        with open(path, encoding="utf-8") as fh:
            rows.extend(parse_lines(fh))
    print(report(rows, args.since))
    return 0


if __name__ == "__main__":
    sys.exit(main())
