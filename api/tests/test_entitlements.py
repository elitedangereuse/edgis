import os
import pathlib
import sys
from datetime import datetime, timezone

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Keep the route index from prebuilding when systems.app is imported anywhere.
os.environ.setdefault("ROUTE_PREBUILD", "0")

from api import entitlements


def _guarded_app():
    app = FastAPI()

    @app.get("/guarded")
    def guarded(access: dict = Depends(entitlements.require_entitlement("route"))):
        return access

    return TestClient(app)


def _feature(public_after):
    return entitlements.Feature(
        key="route", public_after=public_after, min_tier="supporter"
    )


def test_is_public_cutover():
    past = _feature(datetime(2000, 1, 1, tzinfo=timezone.utc))
    future = _feature(datetime(2999, 1, 1, tzinfo=timezone.utc))
    never = _feature(None)
    assert entitlements.is_public(past)
    assert not entitlements.is_public(future)
    assert not entitlements.is_public(never)


def test_parse_cutover_bad_value_fails_closed():
    assert entitlements._parse_cutover("not-a-date") is None
    assert entitlements._parse_cutover("") is None
    assert entitlements._parse_cutover("2026-11-14") == datetime(
        2026, 11, 14, tzinfo=timezone.utc
    )


def test_blocked_without_key(monkeypatch):
    monkeypatch.setitem(
        entitlements.FEATURES, "route", _feature(datetime(2999, 1, 1, tzinfo=timezone.utc))
    )
    monkeypatch.setenv("ENTITLEMENT_KEYS", "secret-key")
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    resp = _guarded_app().get("/guarded")
    assert resp.status_code == 403
    body = resp.json()["detail"]
    assert body["error"] == "patron_only"
    assert body["public"] is False
    assert body["patreon"]


def test_allowed_with_key(monkeypatch):
    monkeypatch.setitem(
        entitlements.FEATURES, "route", _feature(datetime(2999, 1, 1, tzinfo=timezone.utc))
    )
    monkeypatch.setenv("ENTITLEMENT_KEYS", "secret-key")
    resp = _guarded_app().get("/guarded", headers={"x-route-key": "secret-key"})
    assert resp.status_code == 200
    assert resp.json()["via"] == "key"


def test_wrong_key_blocked(monkeypatch):
    monkeypatch.setitem(
        entitlements.FEATURES, "route", _feature(datetime(2999, 1, 1, tzinfo=timezone.utc))
    )
    monkeypatch.setenv("ENTITLEMENT_KEYS", "secret-key")
    resp = _guarded_app().get("/guarded", headers={"x-route-key": "nope"})
    assert resp.status_code == 403


def _waved_feature():
    # supporter opens in the past, follower/hunter in the future, public far off.
    past = datetime(2000, 1, 1, tzinfo=timezone.utc)
    future = datetime(2999, 1, 1, tzinfo=timezone.utc)
    return entitlements.Feature(
        key="route",
        public_after=future,
        min_tier="supporter",
        waves=(("supporter", past), ("follower", future), ("hunter", future)),
    )


def _patron_app(patron):
    async def resolver(request):
        return patron

    entitlements.set_patron_resolver(resolver)
    app = FastAPI()

    @app.get("/guarded")
    def guarded(access: dict = Depends(entitlements.require_entitlement("route"))):
        return access

    return TestClient(app)


def test_wave_open_level_grants(monkeypatch):
    monkeypatch.setitem(entitlements.FEATURES, "route", _waved_feature())
    monkeypatch.delenv("ENTITLEMENT_KEYS", raising=False)
    monkeypatch.delenv("ROUTE_ACCESS_KEYS", raising=False)
    try:
        # A paid patron: the "supporter" wave is already open -> in.
        resp = _patron_app({"entitled": True, "tier": "supporter"}).get("/guarded")
        assert resp.status_code == 200 and resp.json()["via"] == "patron"
    finally:
        entitlements.set_patron_resolver(None)


def test_wave_not_yet_open_blocks_with_date(monkeypatch):
    monkeypatch.setitem(entitlements.FEATURES, "route", _waved_feature())
    monkeypatch.delenv("ENTITLEMENT_KEYS", raising=False)
    monkeypatch.delenv("ROUTE_ACCESS_KEYS", raising=False)
    try:
        # A free follower: their wave is in the future -> 403 with their date.
        resp = _patron_app(
            {"entitled": False, "tier": "follower", "follower": True}
        ).get("/guarded")
        assert resp.status_code == 403
        body = resp.json()["detail"]
        assert body["your_access_after"] == "2999-01-01"
    finally:
        entitlements.set_patron_resolver(None)


def test_hunter_submissions_threshold(monkeypatch):
    feat = entitlements.Feature(
        key="route",
        public_after=datetime(2999, 1, 1, tzinfo=timezone.utc),
        waves=(("hunter", datetime(2000, 1, 1, tzinfo=timezone.utc)),),
    )
    monkeypatch.setitem(entitlements.FEATURES, "route", feat)
    monkeypatch.delenv("ENTITLEMENT_KEYS", raising=False)
    monkeypatch.delenv("ROUTE_ACCESS_KEYS", raising=False)
    try:
        enough = _patron_app({"hunter_submissions": 5}).get("/guarded")
        assert enough.status_code == 200
        entitlements.set_patron_resolver(None)
        too_few = _patron_app({"hunter_submissions": 4}).get("/guarded")
        assert too_few.status_code == 403
    finally:
        entitlements.set_patron_resolver(None)


def test_public_after_cutover_open_to_all(monkeypatch):
    monkeypatch.setitem(
        entitlements.FEATURES, "route", _feature(datetime(2000, 1, 1, tzinfo=timezone.utc))
    )
    monkeypatch.delenv("ENTITLEMENT_KEYS", raising=False)
    monkeypatch.delenv("ROUTE_ACCESS_KEYS", raising=False)
    resp = _guarded_app().get("/guarded")
    assert resp.status_code == 200
    assert resp.json()["via"] == "public"
