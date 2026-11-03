"""Reusable feature-entitlement gate for EDGIS.

A single dependency — ``require_entitlement("<feature>")`` — guards any
endpoint behind a time-boxed, Patreon-style access window:

* Before a feature's ``public_after`` date the caller must prove entitlement
  (access key today; a Patreon session in Layer 2).
* On/after that date the feature opens to everyone automatically — no deploy,
  no intervention.

Layer 1 (this file) ships an **access-key** backend so the route plotter can
go live immediately: a key is posted in a Patreon patrons-only post and/or
handed to selected members. Layer 2 will populate ``request.state.patron``
from a real Patreon OAuth session; the feature registry and date cutover are
backend-agnostic, so endpoints that already depend on ``require_entitlement``
need no change when that lands.
"""

from __future__ import annotations

import hmac
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException, Request

logger = logging.getLogger(__name__)

PATREON_URL = os.getenv(
    "PATREON_URL", "https://www.patreon.com/cw/EliteDangereuse"
)


def _parse_cutover(raw: Optional[str]) -> Optional[datetime]:
    """Parse a ``YYYY-MM-DD`` (or ISO) cutover into a UTC datetime.

    Returns ``None`` (never auto-public) when unset or unparseable, so a typo
    fails closed rather than silently opening a feature to the world.
    """

    if not raw or not raw.strip():
        return None
    text = raw.strip()
    try:
        if len(text) == 10:  # date only -> midnight UTC
            return datetime.strptime(text, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        logger.warning("Unparseable cutover date %r; feature stays gated", raw)
        return None


@dataclass(frozen=True)
class Feature:
    key: str
    public_after: Optional[datetime]
    # Minimum Patreon tier that unlocks the feature during the gated window.
    # Enforced in Layer 2; recorded now so the registry is the single source
    # of truth. Tiers follow the PHP app: follower < supporter < elite <
    # triple_elite.
    min_tier: str = "supporter"


# The one place features are declared. Add a row per gated feature.
FEATURES: dict[str, Feature] = {
    "route": Feature(
        key="route",
        public_after=_parse_cutover(os.getenv("ROUTE_PUBLIC_AFTER", "2026-11-14")),
        min_tier=os.getenv("ROUTE_MIN_TIER", "supporter"),
    ),
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def is_public(feature: Feature, *, now: Optional[datetime] = None) -> bool:
    return (
        feature.public_after is not None
        and (now or _now()) >= feature.public_after
    )


def _configured_keys() -> list[str]:
    """Access keys that unlock gated features (comma-separated).

    ``ENTITLEMENT_KEYS`` applies to all features; ``ROUTE_ACCESS_KEYS`` is an
    alias kept for the route feature. The admin token always works too.
    """

    raw = ",".join(
        v
        for v in (
            os.getenv("ENTITLEMENT_KEYS"),
            os.getenv("ROUTE_ACCESS_KEYS"),
        )
        if v
    )
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    admin = (os.getenv("ADMIN_TOKEN") or "").strip()
    if admin:
        keys.append(admin)
    return keys


def _extract_key(request: Request) -> str:
    header = (request.headers.get("x-route-key") or "").strip()
    if header:
        return header
    bearer = (request.headers.get("authorization") or "").strip()
    if bearer.lower().startswith("bearer "):
        return bearer[7:].strip()
    return (request.query_params.get("key") or "").strip()


def _key_is_valid(provided: str) -> bool:
    if not provided:
        return False
    # Constant-time compare against every configured key.
    return any(
        hmac.compare_digest(provided, candidate)
        for candidate in _configured_keys()
    )


def entitlement_status(feature_key: str) -> dict:
    """Public-facing status for a feature (safe to expose to the frontend)."""

    feature = FEATURES[feature_key]
    public = is_public(feature)
    return {
        "feature": feature_key,
        "public": public,
        "public_after": (
            feature.public_after.date().isoformat()
            if feature.public_after
            else None
        ),
        "requires": None if public else "patron",
        "min_tier": None if public else feature.min_tier,
        "patreon": None if public else PATREON_URL,
    }


def _patron_is_entitled(request: Request, feature: Feature) -> bool:
    """Layer 2 seam: a Patreon OAuth session sets ``request.state.patron``.

    Expected shape once Layer 2 lands::

        request.state.patron = {"entitled": bool, "tier": str, "name": str}

    Until then this is always falsy and the access key carries the gate.
    """

    patron = getattr(request.state, "patron", None)
    return bool(patron and patron.get("entitled"))


def require_entitlement(feature_key: str):
    """FastAPI dependency factory guarding ``feature_key``.

    Returns a dict describing how access was granted
    (``{"via": "public"|"patron"|"key"}``) or raises ``403`` with a structured,
    frontend-friendly payload (how to get in, when it opens).
    """

    if feature_key not in FEATURES:
        raise KeyError(f"Unknown entitlement feature {feature_key!r}")

    def dependency(request: Request) -> dict:
        feature = FEATURES[feature_key]
        if is_public(feature):
            return {"via": "public", "feature": feature_key}
        if _patron_is_entitled(request, feature):
            patron = getattr(request.state, "patron", {}) or {}
            return {
                "via": "patron",
                "feature": feature_key,
                "tier": patron.get("tier"),
            }
        if _key_is_valid(_extract_key(request)):
            return {"via": "key", "feature": feature_key}
        raise HTTPException(
            status_code=403,
            detail={
                "error": "patron_only",
                "message": (
                    "This feature is in early access for patrons. "
                    "It opens to everyone soon."
                ),
                **entitlement_status(feature_key),
            },
        )

    return dependency
