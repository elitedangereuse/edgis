import psycopg
import pytest


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
