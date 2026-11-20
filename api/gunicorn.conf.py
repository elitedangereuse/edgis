"""Gunicorn config for the multi-worker EDGIS launch.

Runs N uvicorn workers behind one port so concurrent COLD solves parallelise
across cores (the Python solver is GIL-bound, so one process caps at ~1 core;
measured ~1.6x on 2 processes, flat throughput / 90s p95 latency on 1). With
preload_app the neutron index is built once in the master (see the
EDGIS_PRELOAD_INDEX block in systems.py) and workers copy-on-write share it, so
RAM stays near one index instead of N.

Launch (via the service wrapper, which sets the env):
    EDGIS_PRELOAD_INDEX=1 UVICORN_PORT=8383 EDGIS_WORKERS=3 \
        gunicorn -c gunicorn.conf.py systems:app

Tune EDGIS_WORKERS to the box: each worker adds ~1 core of cold-solve capacity
but some private RAM; 3 fits braben alongside Postgres's page cache. Keep it
below (cores - Postgres headroom).
"""
import os

bind = f"127.0.0.1:{os.getenv('UVICORN_PORT', '8383')}"
workers = int(os.getenv("EDGIS_WORKERS", "3"))
worker_class = "uvicorn.workers.UvicornWorker"

# Build the index ONCE in the master, then fork -> COW-shared index.
preload_app = True

# Cold solves can take tens of seconds; don't let gunicorn kill a working one.
timeout = int(os.getenv("EDGIS_WORKER_TIMEOUT", "180"))
graceful_timeout = 180

# Recycle each worker after ~N requests so a big solve's retained private RSS is
# released, not accumulated (2026-11-20: memory stayed pinned near 0 for ~40 min
# AFTER a burst of galaxy-spanning solves finished, because the workers held the
# freed-but-not-returned allocations). A recycled worker re-forks from the
# master and COW-re-shares the index (NOT rebuilt), dropping only its private
# RSS. Jitter staggers the recycles. 0 = off.
_max_req = int(os.getenv("EDGIS_MAX_REQUESTS", "800"))
if _max_req > 0:
    max_requests = _max_req
    max_requests_jitter = max(1, _max_req // 10)
