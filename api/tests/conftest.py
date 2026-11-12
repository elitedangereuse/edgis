import os
import tempfile

import psycopg
import pytest

# Route traces from tests go to a temp file, not the dev's api/route_trace.jsonl
# (set before any test module imports systems, which opens the file).
os.environ["ROUTE_TRACE_FILE"] = os.path.join(
    tempfile.gettempdir(), "edgis-test-route_trace.jsonl"
)


@pytest.fixture(autouse=True)
def _no_real_database(monkeypatch):
    """Fail fast if a test reaches a real PostgreSQL server.

    Tests must stub the DB (``systems._db_connection`` or ``psycopg.connect``);
    without this guard an unstubbed path silently connects to whatever
    ``DB_HOST`` points at in the developer's environment. Tests that install
    their own fake ``psycopg.connect`` override this one."""

    def _blocked(*args, **kwargs):
        raise RuntimeError(
            "Tests must not connect to a real database: stub "
            "systems._db_connection or psycopg.connect"
        )

    monkeypatch.setattr(psycopg, "connect", _blocked)
