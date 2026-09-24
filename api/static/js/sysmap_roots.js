'use strict';

// Single source of truth for the system-map tree topology: parent resolution,
// barycenter member association, mass computation and layout ordering.
// Loaded by api/static/js/sysmap.js as the global `SysmapRoots` and required
// directly by the Node test suite (api/tests/js).

const EARTH_MASS_TO_SOLAR = 1 / 332946.0487; // 1 earth mass in solar masses
const SPEED_OF_LIGHT_METRES_PER_SECOND = 299792458;
const MIN_STATION_ATTACHMENT_TOLERANCE_LS = 0.1;
const MAX_STATION_ATTACHMENT_TOLERANCE_LS = 1;

function toId(value){
    if(value === null || value === undefined || value === '') return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
}

function isBarycenter(node){
    return Boolean(node) && typeof node.type === 'string' && node.type.toLowerCase().includes('bary');
}

function isPlanetaryRingNode(body){
    return (body?.type || '').toLowerCase().includes('planetaryring');
}

function isStellarRingNode(body){
    return (body?.type || '').toLowerCase().includes('stellarring');
}

function isAsteroidClusterNode(body){
    const type = (body?.type || '').toLowerCase();
    return Boolean(type) && type.replace(/\s+/g, '').includes('asteroidcluster');
}

// EDDN and some Spansh root-level stations do not name their host body. Their
// distance from the arrival star can still identify a nearby planet or star,
// but only as a display-time inference: it must never replace a game body ID.
function inferStationHostByArrivalDistance(station, nodes){
    const stationDistance = Number(station?.distance_from_arrival_ls);
    if(!Number.isFinite(stationDistance)) return null;
    const candidates = (Array.isArray(nodes) ? nodes : [...(nodes?.values?.() || [])])
        .filter(node => ['planet', 'star'].includes(String(node?.type || '').toLowerCase()))
        .map(node => {
            const bodyDistance = Number(node.distanceToArrival);
            if(!Number.isFinite(bodyDistance)) return null;
            const radius = Number(node.radius);
            const radiusTolerance = Number.isFinite(radius) && radius > 0
                ? (radius / SPEED_OF_LIGHT_METRES_PER_SECOND) * 8
                : 0;
            return {
                node,
                difference: Math.abs(stationDistance - bodyDistance),
                tolerance: Math.max(
                    MIN_STATION_ATTACHMENT_TOLERANCE_LS,
                    Math.min(MAX_STATION_ATTACHMENT_TOLERANCE_LS, radiusTolerance)
                )
            };
        })
        .filter(Boolean)
        .filter(candidate => candidate.difference <= candidate.tolerance)
        .sort((left, right) => left.difference - right.difference);
    if(candidates.length === 0) return null;

    // Two hosts at virtually the same radial distance cannot be disambiguated
    // from a one-dimensional arrival distance alone.
    if(candidates.length > 1 && candidates[1].difference - candidates[0].difference < 0.05){
        return null;
    }
    return candidates[0].node;
}

function isSpaceStation(station){
    return Boolean(station)
        && !station.is_planetary
        && !station.is_carrier
        && station.station_type !== 'FleetCarrier';
}

// In the stations table, body_id deliberately keeps the in-game ID of the
// celestial body that hosts the station. MarketID is the station's own stable
// identity, so it must not be used for this relationship.
function resolveStationHostId(station){
    return toId(station?.body_id);
}

function stationIconAsset(stationType){
    const type = String(stationType || '').toLowerCase();
    if(type.includes('asteroid')) return 'asteroidstation.svg';
    if(type.includes('ocellus')) return 'ocelusstation.svg';
    if(type.includes('dodec')) return 'dodecstation.svg';
    if(type.includes('coriolis')) return 'coriolisstation.svg';
    if(type.includes('orbis')) return 'orbisstation.svg';
    if(type.includes('outpost')) return 'outpoststation.svg';
    if(type.includes('mega')) return 'megashipstation.svg';
    if(type.includes('squadron')) return 'squadroncarrierstation.svg';
    if(type.includes('installation')) return 'installationstation.svg';
    if(type.includes('disabledfleetfixme')) return 'disabledfleetcarrierstation.svg';
    if(type.includes('fleet')) return 'fleetcarrierstation.svg';
    return null;
}

// The parents array is ordered nearest ancestor first. Barycenter links are the
// entries typed 'Null' (EDDN/Spansh) or 'Barycentre'/'Barycenter'.
// - parentId: first non-bary parent (the host row the body is drawn in)
// - directBaryParentId: the barycenter the body directly orbits, when that
//   link is the immediate parent entry
function resolveParentRefs(parents){
    let parentId = null;
    let directBaryParentId = null;
    if(Array.isArray(parents)){
        parents.forEach((entry, index) => {
            const [type, value] = Object.entries(entry)[0];
            const lower = type.toLowerCase();
            const isBaryLink = type === 'Null' || lower.includes('barycentre') || lower.includes('barycenter');
            if(isBaryLink){
                if(index === 0) directBaryParentId = value;
            } else if(parentId === null){
                parentId = value;
            }
        });
    }
    return { parentId, directBaryParentId };
}

function resolveRingHostId(body){
    if(body.parent_body_id != null) return body.parent_body_id;
    if(body.parentbody_id != null) return body.parentbody_id;
    if(body.parentBodyId != null) return body.parentBodyId;
    const { parentId, directBaryParentId } = resolveParentRefs(body.parents || []);
    return parentId ?? directBaryParentId ?? null;
}

function normalizeInlineRing(ring){
    if(!ring) return null;
    return {
        name: ring.name || ring.body_name || ring.label || 'Ring',
        type: ring.type || ring.ring_class || ring.class || 'Ring',
        innerRadius: ring.innerRadius ?? ring.ring_inner_rad ?? ring.inner_radius ?? null,
        outerRadius: ring.outerRadius ?? ring.ring_outer_rad ?? ring.outer_radius ?? null,
        mass: ring.mass ?? ring.ring_mass_mt ?? ring.mass_mt ?? null,
        bodyId: toId(ring.bodyId ?? ring.body_id ?? ring.id)
    };
}

function normalizeRingRecord(body){
    return {
        name: body.body_name || body.name || 'Ring',
        type: body.ring_class || body.type || 'Ring',
        innerRadius: body.ring_inner_rad ?? body.ring_inner_radius ?? body.inner_radius ?? body.innerRadius ?? null,
        outerRadius: body.ring_outer_rad ?? body.ring_outer_radius ?? body.outer_radius ?? body.outerRadius ?? null,
        mass: body.ring_mass_mt ?? body.mass_em ?? body.mass ?? null,
        bodyId: toId(body.body_id ?? body.bodyId ?? body.id)
    };
}

function determineBodySubType(body){
    if(body.type === 'Star') return body.star_type;
    if(isStellarRingNode(body) || isPlanetaryRingNode(body)){
        return body.ring_class || body.type;
    }
    if(isAsteroidClusterNode(body)){
        return body.subType || body.type;
    }
    return body.planet_class;
}

function resolveBodyMassValue(body){
    if(!body) return null;
    const castNumber = (value) => {
        const num = Number(value);
        return Number.isFinite(num) ? num : null;
    };
    if(body.type === 'Star' || body.stellar_mass != null || body.star_type){
        return castNumber(body.stellar_mass ?? body.mass_em ?? body.mass ?? body.mass_mt ?? body.massMT);
    }
    return castNumber(body.mass_em ?? body.mass ?? body.mass_mt ?? body.massMT);
}

function buildNodeRecord(body, parentsMeta){
    return {
        id64: body.system_id64,
        id: body.body_id,
        name: body.body_name,
        type: body.type,
        subType: determineBodySubType(body),
        temperature: body.surface_temperature,
        parentId: parentsMeta.parentId,
        children: [],
        x: 0, y: 0,
        width: 0, height: 0,
        radius: body.radius,
        radiusScaled: 0,
        axialTilt: body.axial_tilt,
        rotationalPeriod: body.rotation_period,
        orbitalPeriod: body.orbital_period,
        semiMajorAxis: body.semi_major_axis ?? body.semiMajorAxis ?? null,
        orbitalEccentricity: body.orbital_eccentricity ?? body.eccentricity ?? null,
        orbitalInclination: body.orbital_inclination ?? body.orbitalInclination ?? null,
        rings: Array.isArray(body.rings) ? body.rings.map(normalizeInlineRing).filter(Boolean) : [],
        isLandable: body.landable,
        tidallyLocked: body.tidally_locked ?? body.is_tidally_locked ?? body.tidallyLocked ?? null,
        atmosphereType: body.atmosphere_type,
        atmosphereComposition: body.atmosphere_composition,
        surfacePressure: body.surface_pressure,
        distanceToArrival: body.distance_from_arrival_ls ?? null,
        earthMasses: body.mass_em ?? null,
        gravity: body.gravity ?? body.surface_gravity ?? null,
        terraformingState: body.terraforming_state ?? body.terraformingState ?? null,
        volcanism: body.volcanism_type ?? body.volcanism ?? null,
        materials: body.materials ?? null,
        directBaryParentId: parentsMeta.directBaryParentId,
        baryChildren: [],
        baryNodeTarget: null,
        baryConnectorPoint: null,
        massValue: resolveBodyMassValue(body),
        discovery: body.discovery ?? null,
        wasMapped: body.was_mapped ?? body.mapped ?? null,
        isStation: body.is_station === true,
        station: body.station ?? null,
        unresolvedStationHost: body.station_unresolved === true,
        raw: body
    };
}

// Recover the member names of a barycenter from its own name:
// "X AB 2+3" -> ["X AB 2", "X AB 3"]; "X ABC" -> ["X A", "X B", "X C"] (with
// partial matches like "AB" + "C" preferred when both split bodies exist).
function guessBarycenterChildNames(name, nameIndex){
    if(!name) return [];
    // Only treat "+" as a member separator when it sits in the final segment
    // ("X AB 2+3"): designations like "2MASS J02351897+6131236 ABC" carry a
    // "+" inside the system prefix and must fall through to the letter logic.
    const lastSpace = name.lastIndexOf(' ');
    const plusIndex = name.indexOf('+', lastSpace + 1);
    if(plusIndex !== -1){
        return guessBarycenterPlusNames(name, plusIndex);
    }
    return guessBarycenterLetterNames(name, nameIndex);
}

function guessBarycenterPlusNames(name, plusIndex){
    const prefixEnd = name.lastIndexOf(' ', plusIndex);
    const prefix = prefixEnd >= 0 ? name.slice(0, prefixEnd + 1) : '';
    const suffix = name.slice(prefixEnd + 1);
    return suffix.split('+')
        .map(part => (prefix + part.trim()).replace(/\s+/g, ' ').trim())
        .filter(Boolean);
}

function guessBarycenterLetterNames(name, nameIndex){
    const segments = name.trim().split(/\s+/);
    if(segments.length < 2) return [];
    const suffix = segments[segments.length - 1];
    if(!/^[A-Za-z]{2,}$/.test(suffix)) return [];
    const prefix = segments.slice(0, -1).join(' ');
    if(nameIndex instanceof Map){
        for(let split = 1; split < suffix.length; split++){
            const left = `${prefix} ${suffix.slice(0, split)}`.trim();
            const right = `${prefix} ${suffix.slice(split)}`.trim();
            if(nameIndex.has(left) && nameIndex.has(right)){
                return [left, right];
            }
        }
    }
    return suffix.split('')
        .map(ch => `${prefix} ${ch}`.trim())
        .filter(Boolean);
}

function hasStarDescendant(node, seen = new Set()){
    if(!node || seen.has(node)) return false;
    seen.add(node);
    if((node.type || '').toLowerCase() === 'star') return true;
    if(!isBarycenter(node)) return false;
    return (node.baryChildren || []).some(child => hasStarDescendant(child, seen));
}

function usesSolarMassUnits(node){
    if(!node) return false;
    const type = (node.type || '').toLowerCase();
    if(type === 'star') return true;
    if(type === 'planet') return false;
    if(isBarycenter(node)) return hasStarDescendant(node);
    return false;
}

function normalizeMassToUnit(massValue, fromSolarUnits, toSolarUnits){
    if(!Number.isFinite(massValue)) return 0;
    if(fromSolarUnits === toSolarUnits) return massValue;
    return toSolarUnits ? massValue * EARTH_MASS_TO_SOLAR : massValue / EARTH_MASS_TO_SOLAR;
}

function getNodeMass(node){
    const value = Number(node?.massValue);
    return Number.isFinite(value) ? value : 0;
}

// Members of a barycenter are the bodies directly orbiting it. They are
// identified from the barycenter name when possible, falling back to the two
// heaviest direct children (masses normalized to the barycenter's unit system).
// Sets node.baryChildren (mass-sorted) and node.massValue (member mass sum).
function associateBarycenterMembers(nodes){
    const arrayNodes = [...nodes.values()];
    const nameIndex = new Map();
    arrayNodes.forEach(n => {
        if(n.name) nameIndex.set(n.name.trim(), n);
        n.baryChildren = [];
    });

    const directChildren = new Map();
    arrayNodes.forEach(n => {
        const baryId = n.directBaryParentId;
        if(baryId != null && nodes.has(baryId)){
            if(!directChildren.has(baryId)) directChildren.set(baryId, []);
            directChildren.get(baryId).push(n);
        }
    });

    const massCache = new Map();
    const massOf = (node) => {
        if(!node) return 0;
        if(massCache.has(node)) return massCache.get(node);
        let mass;
        if(isBarycenter(node)){
            // Never cache a barycenter sum before its members are associated:
            // the fallback pass below sorts by mass and would otherwise freeze
            // a zero sum for nested barycenters into the cache.
            const solar = usesSolarMassUnits(node);
            mass = (node.baryChildren || []).reduce((sum, child) => {
                return sum + normalizeMassToUnit(massOf(child), usesSolarMassUnits(child), solar);
            }, 0);
            if((node.baryChildren || []).length > 0){
                massCache.set(node, mass);
            }
        } else {
            mass = getNodeMass(node);
            massCache.set(node, mass);
        }
        return mass;
    };

    const inBaryUnits = (child, bary) => {
        return normalizeMassToUnit(massOf(child), usesSolarMassUnits(child), usesSolarMassUnits(bary));
    };

    // 1) name-matched members for every barycenter
    directChildren.forEach((children, baryId) => {
        const bary = nodes.get(baryId);
        const wanted = new Set(guessBarycenterChildNames(bary.name, nameIndex));
        bary.baryChildren = children.filter(c => c.name && wanted.has(c.name));
    });

    // 2) fallback: unclassified barycenters keep their two heaviest children
    directChildren.forEach((children, baryId) => {
        const bary = nodes.get(baryId);
        if(bary.baryChildren.length >= 2) return;
        bary.baryChildren = [...children]
            .sort((a, b) => inBaryUnits(b, bary) - inBaryUnits(a, bary))
            .slice(0, 2);
    });

    // 3) sort members by mass (heaviest first) and store member mass sums
    arrayNodes.forEach(node => {
        if(!isBarycenter(node)) return;
        node.baryChildren.sort((a, b) => inBaryUnits(b, node) - inBaryUnits(a, node));
        node.massValue = massOf(node);
    });
}

// Layout tree: every body hangs off
// - its first non-bary parent (host row: moons under their planet, rings under
//   their star, barycenter members under their host star), or
// - when it only orbits barycenters: as a member, surfaced next to its
//   barycenter (same layout parent), or as a branch child of the barycenter
//   for non-member bodies (circumbinary planets and nested barycenter pairs).
// Returns the top-level layout nodes in visual order: for each barycenter the
// heaviest member subtree comes first, then the barycenter node itself, then
// the remaining members by descending mass.
function buildLayoutTree(nodes){
    const arrayNodes = [...nodes.values()];
    const layoutParent = new Map();
    const resolveLayoutParentId = (node, path = new Set()) => {
        if(layoutParent.has(node.id)) return layoutParent.get(node.id);
        if(path.has(node.id)) return null; // defensive: malformed parent cycle
        path.add(node.id);
        let result;
        if(node.parentId != null && nodes.has(node.parentId)){
            result = node.parentId;
        } else {
            const bary = node.directBaryParentId != null ? nodes.get(node.directBaryParentId) : null;
            if(!bary){
                result = null;
            } else if(bary.baryChildren.includes(node)){
                result = resolveLayoutParentId(bary, path);
            } else {
                result = bary.id;
            }
        }
        layoutParent.set(node.id, result);
        return result;
    };

    arrayNodes.forEach(node => {
        node.children = [];
        const pid = resolveLayoutParentId(node);
        if(pid != null && nodes.has(pid)){
            nodes.get(pid).children.push(node);
        }
    });
    arrayNodes.forEach(node => node.children.sort((a, b) => {
        const hasArrivalDistances = a.distanceToArrival != null && b.distanceToArrival != null;
        const distanceDifference = Number(a.distanceToArrival) - Number(b.distanceToArrival);
        if(hasArrivalDistances && Number.isFinite(distanceDifference) && distanceDifference !== 0){
            return distanceDifference;
        }
        if(a.isStation && b.isStation){
            return String(a.name || '').localeCompare(String(b.name || ''));
        }
        if(a.isStation) return -1;
        if(b.isStation) return 1;
        return (a.id ?? 0) - (b.id ?? 0);
    }));

    const emitted = [];
    const emit = (node, seen) => {
        if(seen.has(node.id)) return;
        seen.add(node.id);
        if(!isBarycenter(node)){
            emitted.push(node);
            return;
        }
        const members = node.baryChildren || [];
        if(members.length === 0){
            emitted.push(node);
            return;
        }
        emit(members[0], seen);
        emitted.push(node);
        members.slice(1).forEach(member => emit(member, seen));
    };
    // Roots may mix star-based and planet-based barycenters: compare their
    // masses normalized to a common unit (solar masses).
    const solarMassOf = (node) => {
        return normalizeMassToUnit(getNodeMass(node), usesSolarMassUnits(node), true);
    };
    const roots = arrayNodes
        .filter(n => {
            const pid = layoutParent.get(n.id);
            return pid == null || !nodes.has(pid);
        })
        .sort((a, b) => solarMassOf(b) - solarMassOf(a));
    const seen = new Set();
    roots.forEach(root => emit(root, seen));
    return emitted;
}

// Builds the full system tree from raw API bodies:
// returns { nodes: Map<body_id, node>, roots: [node] } where roots are the
// top-level layout nodes in draw order.
function buildSystemTree(bodies, stations = []){
    const nodes = new Map();
    const pendingRings = new Map();
    (Array.isArray(bodies) ? bodies : []).forEach(body => {
        if(isPlanetaryRingNode(body)){
            const hostId = resolveRingHostId(body);
            if(hostId != null){
                if(!pendingRings.has(hostId)) pendingRings.set(hostId, []);
                pendingRings.get(hostId).push(normalizeRingRecord(body));
            }
            return;
        }
        const id = toId(body.body_id);
        if(id == null) return;
        const parentsMeta = resolveParentRefs(body.parents || []);
        nodes.set(id, buildNodeRecord(body, parentsMeta));
    });

    (Array.isArray(stations) ? stations : []).forEach(station => {
        const marketId = toId(station?.market_id);
        // A station market ID is its stable identity. body_id is the game body
        // it orbits, so it is its layout parent rather than the station node ID.
        const stationId = marketId != null ? -marketId : null;
        if(stationId == null || nodes.has(stationId)) return;
        const requestedHostId = resolveStationHostId(station);
        const hasResolvedHost = requestedHostId != null && nodes.has(requestedHostId);
        const hostId = hasResolvedHost ? requestedHostId : 0;
        if(hostId == null) return;
        const stationBody = {
            ...station,
            body_id: stationId,
            body_name: station.name || station.body_name || 'Station',
            type: 'Station',
            radius: 1600,
            parents: [{ Station: hostId }],
            is_station: true,
            station_unresolved: !hasResolvedHost,
            station
        };
        nodes.set(stationId, buildNodeRecord(
            stationBody,
            resolveParentRefs(stationBody.parents)
        ));
    });

    pendingRings.forEach((ringList, hostId) => {
        const host = nodes.get(hostId);
        if(host && Array.isArray(host.rings)){
            host.rings.push(...ringList);
        }
    });

    associateBarycenterMembers(nodes);
    const roots = buildLayoutTree(nodes);
    return { nodes, roots };
}

function buildRootIds(bodies){
    return buildSystemTree(bodies).roots.map(node => node.id);
}

function pairKey(aId, bId){
    if(aId == null || bId == null) return '';
    return (aId < bId) ? `${aId}|${bId}` : `${bId}|${aId}`;
}

// Sibling links that must not be drawn because barycenter brackets replace
// them. Nested barycenters are flattened to their visible, non-barycenter
// members so the outer bracket also suppresses the link at each nested-group
// boundary (for example: A+B, then C must not draw B--C).
function computeBarycenterSkipPairs(nodes){
    const skip = new Set();
    const collectVisibleMembers = (node, ancestors = new Set()) => {
        if(!isBarycenter(node)) return [node];
        if(ancestors.has(node)) return [];
        const members = (node.baryChildren || []).filter(Boolean);
        if(members.length === 0) return [];
        const nextAncestors = new Set(ancestors);
        nextAncestors.add(node);
        return members.flatMap(member => collectVisibleMembers(member, nextAncestors));
    };

    (Array.isArray(nodes) ? nodes : [...nodes.values()]).forEach(bary => {
        if(!isBarycenter(bary)) return;
        const members = collectVisibleMembers(bary);
        if(members.length < 2) return;
        for(let i = 0; i < members.length; i++){
            for(let j = i + 1; j < members.length; j++){
                skip.add(pairKey(members[i].id, members[j].id));
            }
        }
    });
    return skip;
}

const api = {
    buildSystemTree,
    buildRootIds,
    computeBarycenterSkipPairs,
    isBarycenter,
    isPlanetaryRingNode,
    isStellarRingNode,
    isAsteroidClusterNode,
    inferStationHostByArrivalDistance,
    resolveStationHostId,
    isSpaceStation,
    stationIconAsset,
    hasStarDescendant,
    getNodeMass,
    normalizeMassToUnit,
    pairKey,
    EARTH_MASS_TO_SOLAR,
    SPEED_OF_LIGHT_METRES_PER_SECOND
};

if(typeof module === 'object' && module.exports){
    module.exports = api;
} else {
    globalThis.SysmapRoots = api;
}
