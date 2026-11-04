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
    # Tiers follow the PHP app: follower < supporter < elite < triple_elite.
    min_tier: str = "supporter"
    # Phased rollout: access level -> unlock datetime. A caller unlocks the
    # feature once now >= the earliest date among the levels they qualify for
    # (see ``_caller_levels``). ``public_after`` is the final, open-to-all wave.
    # Empty tuple = legacy behaviour (any entitled patron is in immediately).
    waves: tuple[tuple[str, Optional[datetime]], ...] = ()


# Paid Patreon tiers (all unlock at the "supporter" wave; follower is free).
_PAID_TIERS = frozenset({"supporter", "elite", "triple_elite"})
# Galactic Hunters need at least this many confirmed submissions (see
# galactic-hunters-access): once the identity endpoint reports the count.
HUNTER_MIN_SUBMISSIONS = int(os.getenv("ROUTE_HUNTER_MIN_SUBMISSIONS") or "5")


# The one place features are declared. Add a row per gated feature.
FEATURES: dict[str, Feature] = {
    "route": Feature(
        key="route",
        public_after=_parse_cutover(os.getenv("ROUTE_PUBLIC_AFTER", "2026-11-30")),
        min_tier=os.getenv("ROUTE_MIN_TIER", "supporter"),
        waves=(
            # Paid patrons (tier 1 and up).
            ("supporter", _parse_cutover(
                os.getenv("ROUTE_WAVE_SUPPORTER", "2026-11-07"))),
            # Free Patreon followers.
            ("follower", _parse_cutover(
                os.getenv("ROUTE_WAVE_FOLLOWER", "2026-11-13"))),
            # Galactic Hunters with >= HUNTER_MIN_SUBMISSIONS confirmed targets.
            ("hunter", _parse_cutover(
                os.getenv("ROUTE_WAVE_HUNTER", "2026-11-13"))),
        ),
    ),
}


def _caller_levels(patron: dict) -> set[str]:
    """Access levels a resolved patron qualifies for, matched to wave names.

    Reads optional enriched identity fields; degrades gracefully when the site
    endpoint only returns the legacy ``entitled`` boolean (-> "supporter")."""

    levels: set[str] = set()
    tier = str(patron.get("tier") or "").strip().lower()
    if tier in _PAID_TIERS or (patron.get("entitled") and tier not in {"follower"}):
        levels.add("supporter")
    if tier == "follower" or patron.get("follower"):
        levels.add("follower")
    subs = patron.get("hunter_submissions")
    if patron.get("hunter") or (
        isinstance(subs, (int, float)) and subs >= HUNTER_MIN_SUBMISSIONS
    ):
        levels.add("hunter")
    return levels


def _earliest_wave_unlock(
    feature: Feature, levels: set[str]
) -> Optional[datetime]:
    """Earliest unlock datetime among the waves the caller qualifies for, or
    ``None`` if they qualify for no dated wave."""

    dates = [d for (lvl, d) in feature.waves if lvl in levels and d is not None]
    return min(dates) if dates else None


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
        "waves": None if public else [
            {"level": lvl, "after": d.date().isoformat()}
            for (lvl, d) in feature.waves
            if d is not None
        ],
    }


# Layer 2 seam: a resolver that maps a request to the caller's patron status
# ``{"entitled": bool, "tier": str|None, "name": str|None}`` (or ``None`` for a
# guest). Registered by patreon_auth at import; kept here so entitlements.py
# stays free of HTTP/site specifics and remains trivially unit-testable.
_patron_resolver = None  # Optional[Callable[[Request], Awaitable[Optional[dict]]]]


def set_patron_resolver(resolver) -> None:
    global _patron_resolver
    _patron_resolver = resolver


async def _resolve_patron(request: Request) -> Optional[dict]:
    # A middleware (or prior dependency) may have stamped it already.
    patron = getattr(request.state, "patron", None)
    if patron is not None:
        return patron
    if _patron_resolver is None:
        return None
    try:
        patron = await _patron_resolver(request)
    except Exception:  # never let an identity hiccup 500 the request
        logger.exception("patron resolver failed; treating caller as guest")
        patron = None
    request.state.patron = patron
    return patron


def require_entitlement(feature_key: str):
    """FastAPI dependency factory guarding ``feature_key``.

    Returns a dict describing how access was granted
    (``{"via": "public"|"patron"|"key"}``) or raises ``403`` with a structured,
    frontend-friendly payload (how to get in, when it opens).
    """

    if feature_key not in FEATURES:
        raise KeyError(f"Unknown entitlement feature {feature_key!r}")

    async def dependency(request: Request) -> dict:
        feature = FEATURES[feature_key]
        if is_public(feature):
            return {"via": "public", "feature": feature_key}
        # Local key check first (no network); then the patron identity call.
        if _key_is_valid(_extract_key(request)):
            return {"via": "key", "feature": feature_key}
        patron = await _resolve_patron(request)
        your_wave: Optional[datetime] = None
        if patron:
            if feature.waves:
                # Phased rollout: the caller is in once their earliest
                # qualifying wave has opened.
                your_wave = _earliest_wave_unlock(
                    feature, _caller_levels(patron)
                )
                if your_wave is not None and _now() >= your_wave:
                    return {
                        "via": "patron",
                        "feature": feature_key,
                        "tier": patron.get("tier"),
                    }
            elif patron.get("entitled"):  # legacy: no wave schedule
                return {
                    "via": "patron",
                    "feature": feature_key,
                    "tier": patron.get("tier"),
                }
        detail = {
            "error": "patron_only",
            "message": (
                "This feature is in early access. It opens to everyone soon."
            ),
            **entitlement_status(feature_key),
        }
        if your_wave is not None:
            # The caller qualifies for a wave that hasn't opened yet; tell them
            # exactly when they're in.
            detail["your_access_after"] = your_wave.date().isoformat()
            detail["message"] = (
                "You're in early access. Your access opens on "
                f"{your_wave.date().isoformat()}."
            )
        raise HTTPException(status_code=403, detail=detail)

    return dependency
