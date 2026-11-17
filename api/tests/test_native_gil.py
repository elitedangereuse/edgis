"""The native kernel must release the GIL for the whole search.

The API is one single-process event loop; a kernel that holds the GIL stalls
every other request (cache hits, /neighbors, health checks) for the full
solve, seconds at a time. Skipped when the .so is not built.
"""

import threading
import time

import numpy as np
import pytest

edgis_route = pytest.importorskip("edgis_route")


def test_graph_search_fuel_releases_gil():
    rng = np.random.default_rng(42)
    n = 30000
    cc = np.ascontiguousarray(rng.uniform(0.0, 3000.0, size=(n, 3)))
    scoop = np.ascontiguousarray((rng.random(n) < 0.3).astype(np.uint8))
    cand = np.ascontiguousarray(np.arange(n, dtype=np.int64))
    sv = np.ascontiguousarray(np.zeros(3))
    dv = np.ascontiguousarray(np.full(3, 3000.0))

    started = threading.Event()
    done = threading.Event()
    result = {}

    def solve():
        started.set()
        t0 = time.perf_counter()
        result["seq"] = edgis_route.graph_search_fuel(
            cc, scoop, cand, sv, dv, 8.0, 240.0, 24, 25, 4.0, 30.0, [], []
        )
        result["secs"] = time.perf_counter() - t0
        done.set()

    worker = threading.Thread(target=solve)
    worker.start()
    started.wait()
    # With the GIL released the main thread runs freely during the solve
    # (millions of iterations); with it held, it blocks until done and gets
    # at most a handful. The margin makes the check immune to machine speed.
    ticks = 0
    while not done.is_set():
        ticks += 1
    worker.join()
    assert result["seq"], "synthetic corridor should solve"
    assert ticks > 1000, (
        f"only {ticks} main-thread iterations during a "
        f"{result['secs']:.2f}s native solve: the kernel is holding the GIL"
    )
