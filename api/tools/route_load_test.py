#!/usr/bin/env python3
"""Cold-solve load test for the route plotter, with a safety probe.

Steps through levels of concurrent "CMDRs" (default 1, 2, 4, 8). Each simulated
CMDR plots uncached routes back to back for ``--step-seconds`` (a per-request
``beam_width`` nonce busts the route cache, so every request is a real solve).
Between levels it waits for in-flight plots to finish, so levels don't overlap.

Meanwhile a probe times a cheap endpoint (``/coords?q=Sol``) every few seconds:
the rest of EDGIS shares the process, so the test ABORTS if that probe stays
slow (``--probe-max`` seconds, twice in a row) -- run it against production
without starving real users.

Per level it prints plots completed, throughput (solves/min), latency
percentiles, errors by status, and the probe latency. Usage:

  ROUTE_KEY=... python3 api/tools/route_load_test.py \\
      --base-url https://edgis.elitedangereuse.fr --levels 1,2,4,8

Note: the server keeps solving requests a client gave up on, so after an abort
or a client timeout the box stays busy until its queue drains.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

# Mixed lengths, from a few jumps to a long crossing.
ROUTES = [
    ("Sol", "Maia", 50),
    ("Lave", "Maia", 50),
    ("Sol", "Colonia", 50),
    ("Celaeno", "Colonia", 80),
    ("Colonia", "Sagittarius A*", 50),
    ("Cubeo", "Col 173 Sector LJ-F c12-0", 50),
    ("Sol", "Beagle Point", 70),
]


@dataclass
class Level:
    users: int
    latencies: list[float] = field(default_factory=list)
    statuses: dict[str, int] = field(default_factory=dict)
    started: float = 0.0
    ended: float = 0.0

    def record(self, status: str, seconds: Optional[float]) -> None:
        self.statuses[status] = self.statuses.get(status, 0) + 1
        if status == "200" and seconds is not None:
            self.latencies.append(seconds)


def _pct(values: list[float], p: float) -> float:
    v = sorted(values)
    return v[min(len(v) - 1, int(p * len(v)))]


class Probe:
    def __init__(self, client: httpx.AsyncClient, base: str, every: float, limit: float):
        self.client, self.base, self.every, self.limit = client, base, every, limit
        self.samples: list[tuple[float, float]] = []  # (time, seconds; inf = failed)
        self.tripped = asyncio.Event()

    async def run(self) -> None:
        slow_streak = 0
        while not self.tripped.is_set():
            t0 = time.perf_counter()
            try:
                r = await self.client.get(f"{self.base}/coords", params={"q": "Sol"}, timeout=15)
                took = time.perf_counter() - t0 if r.status_code == 200 else float("inf")
            except httpx.HTTPError:
                took = float("inf")
            self.samples.append((time.time(), took))
            slow_streak = slow_streak + 1 if took > self.limit else 0
            if slow_streak >= 2:
                print(f"\n!! probe slow twice in a row ({took:.1f}s > {self.limit}s): aborting", flush=True)
                self.tripped.set()
                return
            await asyncio.sleep(self.every)

    def between(self, t0: float, t1: float) -> list[float]:
        return [s for t, s in self.samples if t0 <= t <= t1]


async def cmdr(client, base, key, level: Level, deadline: float, stop: asyncio.Event,
               timeout: float, nonce_base: int) -> None:
    i = 0
    while time.time() < deadline and not stop.is_set():
        frm, to, rng = random.choice(ROUTES)
        nonce = (nonce_base + i) % 100 + 1  # beam_width 1..100: a cache key, not the graph engine
        i += 1
        t0 = time.perf_counter()
        try:
            r = await client.get(
                f"{base}/route",
                params={"from": frm, "to": to, "range": rng, "beam_width": nonce},
                headers={"x-route-key": key}, timeout=timeout,
            )
            level.record(str(r.status_code), time.perf_counter() - t0)
            if r.status_code in (401, 403):
                print("!! access refused: check ROUTE_KEY", flush=True)
                stop.set()
        except httpx.TimeoutException:
            level.record("timeout", None)
        except httpx.HTTPError as exc:
            level.record(type(exc).__name__, None)


async def main_async(args) -> int:
    key = os.environ.get("ROUTE_KEY", "")
    if not key:
        print("ROUTE_KEY is required (an early-access key)", file=sys.stderr)
        return 2
    levels = [Level(int(n)) for n in args.levels.split(",")]
    base = args.base_url.rstrip("/")
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=64)) as client:
        probe = Probe(client, base, args.probe_every, args.probe_max)
        probe_task = asyncio.create_task(probe.run())
        await asyncio.sleep(args.probe_every * 2)  # baseline probe samples first
        baseline = [s for _, s in probe.samples]
        print(f"probe baseline /coords: {', '.join(f'{s:.2f}s' for s in baseline)}", flush=True)
        for n, level in enumerate(levels):
            if probe.tripped.is_set():
                break
            print(f"\n== {level.users} concurrent CMDR(s), {args.step_seconds}s", flush=True)
            level.started = time.time()
            deadline = level.started + args.step_seconds
            workers = [asyncio.create_task(cmdr(client, base, key, level, deadline, probe.tripped,
                                                args.timeout, 13 * n + 7 * u))
                       for u in range(level.users)]
            await asyncio.gather(*workers)  # includes waiting for the last in-flight plots
            level.ended = time.time()
            report(level, probe)
        probe.tripped.set()
        await probe_task
    return 1 if any(s == float("inf") or s > args.probe_max for _, s in probe.samples[-2:]) else 0


def report(level: Level, probe: Probe) -> None:
    span = max(1.0, level.ended - level.started)
    ok = level.latencies
    line = f"plots ok {len(ok)} in {span:.0f}s -> {len(ok) / span * 60:.1f} solves/min"
    if ok:
        line += (f" | latency p50 {_pct(ok, .5):.1f}s p90 {_pct(ok, .9):.1f}s"
                 f" max {max(ok):.1f}s")
    print(line, flush=True)
    others = {k: v for k, v in level.statuses.items() if k != "200"}
    if others:
        print("other answers: " + ", ".join(f"{k} x{v}" for k, v in sorted(others.items())), flush=True)
    p = [s for s in probe.between(level.started, level.ended) if s != float("inf")]
    failed = sum(1 for s in probe.between(level.started, level.ended) if s == float("inf"))
    if p:
        print(f"probe /coords during level: median {statistics.median(p):.2f}s max {max(p):.2f}s"
              + (f", {failed} failed" if failed else ""), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", default="https://edgis.elitedangereuse.fr")
    parser.add_argument("--levels", default="1,2,4,8", help="concurrent CMDRs per step")
    parser.add_argument("--step-seconds", type=float, default=90.0)
    parser.add_argument("--timeout", type=float, default=150.0, help="per-plot client timeout (s)")
    parser.add_argument("--probe-every", type=float, default=5.0)
    parser.add_argument("--probe-max", type=float, default=3.0, help="abort above this probe latency (s)")
    return asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
