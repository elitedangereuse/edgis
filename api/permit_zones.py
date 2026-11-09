"""Permit-locked systems, matched by NAME (no geometry).

A system is permit-locked if its procedural sector prefix is in
``PERMIT_SECTORS`` (e.g. "Col 70 Sector AB-C d1-2" -> "col 70 sector") or its
full name is in ``PERMIT_SYSTEMS``. This replaced the earlier geometric
exclusion spheres, which over-blocked legitimate named systems that merely sat
near a permit region (e.g. LV Orionis inside Col 70's 514 ly sphere).

Source: the curated EDGIS LoreExplorer permit set
(https://elitedangereuse.github.io/LoreExplorer/#tags=Permit) -- every lore
entry tagged ``:Permit:``. Named entries -> PERMIT_SYSTEMS (the bubble permit
systems: Sol, Sirius, Achenar, Shinrarta Dezhra, ...); procedural / "... Sector"
/ region entries -> PERMIT_SECTORS prefixes (Col 70/97/121 Sector, NGC/M41/Cone/
Regor Sectors, Horsehead Dark Region, and the deep Bleia/Praei/Bovomit/Dryman/
Froadik/Hyponia/Sidgoir regions). 28 sectors, 86 named systems.

Regenerate: clone elitedangereuse/LoreExplorer, grep ``src/*.org`` for the
``:Permit:`` filetag, take each ``#+title:``, and split into procedural-sector
prefixes vs named systems.

In Elite a jump is point-to-point, so this is pure node filtering: the router
drops any neutron waypoint or bridge star whose name is locked. The plotted
route's own start/destination are exempt (the commander may hold that permit).
"""

import re

_PROC = re.compile(r"^(.*?) [A-Za-z][A-Za-z]-[A-Za-z] [a-h](?:\d|$)")

PERMIT_SECTORS = frozenset({
    'bleia1',
    'bleia2',
    'bleia3',
    'bleia4',
    'bleia5',
    'bovomit',
    'col 121 sector',
    'col 70 sector',
    'col 97 sector',
    'cone sector',
    'dryio flyuae',
    'dryman',
    'froadik',
    'horsehead dark region',
    'hyponia',
    'm41 sector',
    'ngc 1647 sector',
    'ngc 2264 sector',
    'ngc 2286 sector',
    'ngc 3603 sector',
    'plaa ain',
    'praei1',
    'praei2',
    'praei3',
    'praei4',
    'regor sector',
    'scheau bli',
    'sidgoir',
})

PERMIT_SYSTEMS = frozenset({
    '18 mu canis majoris',
    '20 iota canis majoris',
    '26 g. canis majoris',
    '35 geminorum',
    '4 sextantis',
    '6 lambda leporis',
    'achenar',
    'alioth',
    'alpha hydri',
    'azoth',
    'bellica',
    'beta hydri',
    'cd-43 11917',
    'cd-44 1695',
    'crom',
    'diso 5 c',
    'dromi',
    'exbeur',
    'facece',
    'fms2006 400',
    'gamma velorum',
    'hbc 531',
    'hd 33766',
    'hen 2-333',
    'hip 10332',
    'hip 104941',
    'hip 22182',
    'hip 22460',
    'hip 23874',
    'hip 39425',
    'hip 51073',
    'hip 54530',
    'hip 87621',
    'hodack',
    'hors',
    'hr 4413',
    'isinor',
    'jotun',
    'kamba',
    'lave 2',
    'lft 509',
    'lhs 2894',
    'lhs 2921',
    'lhs 3091',
    'lia fail',
    'ltt 198',
    "luna (earth's moon)",
    'luyten 347-14',
    'matet',
    'mbooni',
    'mingfu',
    'monocerotis',
    'nastrond',
    'nsv 3128',
    'orionis',
    'orna',
    'otegine',
    'peregrina',
    'phekda',
    'pi mensae',
    'plx 695',
    'polaris',
    'r leporis',
    'red rectangle',
    'ross 128',
    'ross 354',
    'sharur',
    'shinrarta dezhra',
    'sigma canis majoris',
    'sirius',
    'sol',
    'summerland',
    'tarnkappe',
    'terra mater',
    'tiliala',
    'triton',
    'tyc 1275-1976-1',
    'tyc 5961-3335-1',
    'tyet',
    "van maanen's star",
    'vega',
    'wasp-49',
    'wasp-64',
    "witch's reach",
    'wolf 262',
    'wolfsegen',
})


def _sector_of(name):
    """Procedural sector prefix of a system name, or None for a named system."""
    m = _PROC.match(name or "")
    return m.group(1) if m else None


def is_locked(name) -> bool:
    """Whether a system name is permit-locked (sector prefix or exact name)."""
    if not name:
        return False
    low = name.strip().lower()
    if low in PERMIT_SYSTEMS:
        return True
    sector = _sector_of(name)
    return sector is not None and sector.lower() in PERMIT_SECTORS
