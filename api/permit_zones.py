"""Permit-locked regions of the galaxy, as exclusion spheres.

Generated from the bundled EDTS data (api/edts/edtslib/pgdata.py: every
``HARegion(..., needs_permit=True)``). EDTS is the authoritative in-repo source
for permit regions; the main systems DB stores no permit flag.

In Elite a hyperspace jump is point-to-point (you never traverse systems in
between), so the only rule is "never ARRIVE at a locked system". The router
therefore drops any neutron waypoint or bridge star inside these spheres; it
does NOT care whether a jump's straight line crosses one. The plotted route's
own start/destination are exempt (the commander may hold that permit).

Regenerate: python3 -c "import api.permit_zones as p; p._regen()"  (see _regen).

Covers the 28 permit regions EDTS knows. Individual bubble permit SYSTEMS (Sol,
Sirius, Beta Hydri, ...) are not regions and are not yet covered; see the
route-plotter handoff for that follow-up.
"""

import numpy as np

# (name, x, y, z, radius_ly)
PERMIT_REGIONS = [
    ('NGC 1647 Sector', 11.76172, -508.69531, -1684.8418, 205.0),
    ('Col 70 Sector', 508.68359, -372.59375, -1090.87891, 514.0),
    ('Col 97 Sector', 878.88281, -64.39062, -1850.92383, 250.0),
    ('NGC 2264 Sector', 851.16406, 83.68359, -2005.2207, 510.0),
    ('M41 Sector', 1731.03125, -400.21094, -1396.76758, 350.0),
    ('NGC 2286 Sector', 5456.35547, -379.24609, -7706.28711, 385.0),
    ('Col 121 Sector', 1246.80469, -278.0, -860.11328, 459.0),
    ('NGC 3603 Sector', 18594.82031, -174.53125, 7362.21094, 150.0),
    ('Cone Sector', 855.44141, 84.45312, -2025.11328, 100.0),
    ('Horsehead Dark Region', 608.46094, -404.64453, -1194.16992, 200.0),
    ('IC 4673 Sector', -840.65625, -561.13281, 13361.82812, 100.0),
    ('Regor Sector', 1099.23828, -146.67188, -133.58008, 100.0),
    ('Bleia2', -43.0, 155.0, 37000.0, 512.0),
    ('Bleia3', -43.0, 155.0, 36500.0, 512.0),
    ('Bleia4', 450.0, 155.0, 37000.0, 512.0),
    ('Bleia5', -450.0, 155.0, 37000.0, 512.0),
    ('Bleia1', -43.0, 155.0, 37500.0, 512.0),
    ('Bovomit', -20070.0, 90.0, -6930.0, 512.0),
    ('Dryman', 19100.0, 20.0, 21160.0, 512.0),
    ('Froadik', -18860.0, -200.0, 14300.0, 512.0),
    ('Hyponia', -23020.0, -10.0, 24080.0, 512.0),
    ('Praei3', -1000.0, -155.0, 53600.0, 512.0),
    ('Praei1', -1000.0, -155.0, 54000.0, 512.0),
    ('Praei2', -1000.0, -155.0, 54400.0, 512.0),
    ('Praei4', -1000.0, -555.0, 54000.0, 512.0),
    ('Praei5', -1000.0, 455.0, 54000.0, 512.0),
    ('Praei6', -500.0, -100.0, 53500.0, 512.0),
    ('Sidgoir', -24120.0, 10.0, -1220.0, 100.0),
]

# (centers[N,3], radii[N]) for vectorized point-in-sphere tests.
_CENTERS = np.asarray([(x, y, z) for _, x, y, z, _ in PERMIT_REGIONS], dtype=np.float64)
_RADII = np.asarray([r for *_, r in PERMIT_REGIONS], dtype=np.float64)
_R2 = _RADII ** 2


def locked_mask(coords: np.ndarray) -> np.ndarray:
    """Boolean mask: True where a point (N,3) falls inside any permit sphere."""
    if len(coords) == 0:
        return np.zeros(0, dtype=bool)
    d2 = ((coords[:, None, :] - _CENTERS[None, :, :]) ** 2).sum(axis=2)
    return (d2 <= _R2[None, :]).any(axis=1)


def is_locked(point) -> bool:
    """Whether a single (x, y, z) falls inside any permit sphere."""
    p = np.asarray(point, dtype=np.float64)
    d2 = ((p - _CENTERS) ** 2).sum(axis=1)
    return bool((d2 <= _R2).any())
