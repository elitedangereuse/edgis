"""Layer 2: Patreon entitlement via the main site's identity endpoint.

The ecosystem already does Patreon OAuth once, on the PHP site
(``elitedangereuselight/patreon.php``), which sets an opaque
``ED_LOGGED_CMDR_ID`` cookie. Satellite apps don't re-implement OAuth — they
forward that cookie to a site identity endpoint (as the mini-shipinteriors
relay does via ``outils/mini-shipinteriors-cmdr.php``) and read back
``{"cmdr": ..., "patreon": bool}``.

EDGIS does the same: given the shared cookie it asks the site whether the
caller is an active supporter, caches the answer briefly, and exposes it to
the entitlement gate as ``{"entitled": bool, "name": str, "tier": str|None}``.

Requirements: EDGIS must be served on a domain that carries the
``ED_LOGGED_CMDR_ID`` cookie (same site / parent domain), and
``EDGIS_IDENTITY_URL`` must point at the site identity endpoint. No Patreon
client secret lives here — the site owns the OAuth.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

import httpx
from fastapi import Request

logger = logging.getLogger(__name__)

COOKIE = "ED_LOGGED_CMDR_ID"
IDENTITY_URL = os.getenv("EDGIS_IDENTITY_URL", "").strip()
# Optional; the site returns `patreon` without it, but a relay key is accepted
# for parity / future-proofing.
RELAY_KEY = os.getenv("EDGIS_RELAY_KEY", "").strip()
IDENTITY_TTL = max(0, int(os.getenv("EDGIS_IDENTITY_TTL") or "60"))
IDENTITY_TIMEOUT = float(os.getenv("EDGIS_IDENTITY_TIMEOUT") or "3")

# Small per-worker TTL cache keyed by cookie token; identity calls are
# infrequent (gated feature) and this keeps the site off the hot path.
_cache: dict[str, tuple[float, dict]] = {}


def _cache_get(token: str) -> Optional[dict]:
    hit = _cache.get(token)
    if not hit:
        return None
    expires, value = hit
    if expires < time.monotonic():
        _cache.pop(token, None)
        return None
    return value


def _cache_put(token: str, value: dict) -> None:
    if IDENTITY_TTL:
        _cache[token] = (time.monotonic() + IDENTITY_TTL, value)


def is_configured() -> bool:
    return bool(IDENTITY_URL)


async def resolve_patron(request: Request) -> Optional[dict]:
    """Return ``{"entitled", "name", "tier"}`` for the caller, or ``None``.

    ``None`` means "treated as a guest" (no cookie, not configured, or the site
    was unreachable) — the entitlement gate then falls back to the access key.
    """

    if not IDENTITY_URL:
        return None
    token = request.cookies.get(COOKIE)
    if not token:
        return None

    cached = _cache_get(token)
    if cached is not None:
        return cached

    headers = {"Accept": "application/json", "Cookie": f"{COOKIE}={token}"}
    if RELAY_KEY:
        headers["X-Relay-Key"] = RELAY_KEY
    try:
        async with httpx.AsyncClient(timeout=IDENTITY_TIMEOUT) as client:
            response = await client.get(IDENTITY_URL, headers=headers)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        logger.warning("Patreon identity lookup failed: %s", exc)
        return None

    entitled = data.get("patreon") is True
    # Enriched fields are optional: the site endpoint returns `patreon` (bool)
    # today, and may later add `tier`, `follower`, and a Galactic-Hunter
    # confirmed-submission count. Map the legacy boolean to the "supporter"
    # tier so paid patrons land in the right rollout wave either way.
    tier = data.get("tier")
    if not tier and entitled:
        tier = "supporter"
    hunter_submissions = (
        data.get("hunter_submissions")
        or data.get("validated_targets")
        or data.get("confirmed_submissions")
    )
    patron = {
        "entitled": entitled,
        "name": data.get("cmdr"),
        "tier": tier,
        "follower": bool(data.get("follower")) or tier == "follower",
        "hunter_submissions": hunter_submissions,
    }
    _cache_put(token, patron)
    return patron
