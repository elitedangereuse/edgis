'use strict';

// Keep this in sync with root ordering logic in api/static/js/sysmap.js.
const EARTH_MASS_TO_SOLAR = 1 / 332946.0487;

function parseBodyId(value){
    if(value === null || value === undefined || value === ''){
        return null;
    }
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
}

function resolveParentIds(parents){
    let primaryId = null;
    let baryId = null;
    let directBaryParentId = null;
    if(Array.isArray(parents)){
        let encounteredNonBary = false;
        parents.forEach(entry => {
            const [[type, value]] = Object.entries(entry);
            if(type === 'Null') return;
            const typeLower = type.toLowerCase();
            const isBaryEntry = typeLower.includes('barycentre') || typeLower.includes('barycenter');
            if(isBaryEntry){
                if(baryId == null) baryId = value;
                if(!encounteredNonBary){
                    directBaryParentId = value;
                }
                return;
            }
            if(primaryId == null) primaryId = value;
            encounteredNonBary = true;
        });
    }
    return { parentId: primaryId, baryParentId: baryId, directBaryParentId };
}

function isBarycenter(node){
    if(!node || typeof node.type !== 'string') return false;
    return node.type.toLowerCase().includes('bary');
}

function guessBarycenterChildNames(name, nameIndex){
    if(!name || typeof name !== 'string') return [];
    const trimmed = name.trim();
    if(!trimmed) return [];
    if(trimmed.includes('+')){
        let prefixEnd = trimmed.indexOf(' ');
        if(prefixEnd === -1) prefixEnd = -1;
        const prefix = prefixEnd >= 0 ? trimmed.slice(0, prefixEnd + 1) : '';
        const suffix = trimmed.slice(prefixEnd + 1);
        return suffix.split('+')
            .map(part => (prefix + part.trim()).replace(/\s+/g, ' ').trim())
            .filter(Boolean);
    }

    const segments = trimmed.split(/\s+/);
    if(segments.length >= 2){
        const suffix = segments[segments.length - 1];
        if(/^[A-Za-z]{2,}$/.test(suffix)){
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
    }
    return [];
}

function ensureBarycenterChildren(nodes){
    const nameIndex = new Map();
    const arrayNodes = [...nodes.values()];
    arrayNodes.forEach(n => {
        if(n.name) nameIndex.set(n.name.trim(), n);
        n.baryChildren = [];
    });

    const addUniqueChild = (bary, child) => {
        if(!child) return;
        if(!bary.baryChildren.some(c => c.id === child.id)){
            bary.baryChildren.push(child);
        }
    };

    const baryMap = new Map();
    arrayNodes.forEach(n => {
        if(isBarycenter(n)) baryMap.set(n.id, n);
    });

    arrayNodes.forEach(node => {
        const baryId = node.directBaryParentId;
        if(baryId != null && baryMap.has(baryId)){
            addUniqueChild(baryMap.get(baryId), node);
        }
    });

    arrayNodes.forEach(node => {
        if(!isBarycenter(node)) return;
        if(node.baryChildren.length >= 2) return;
        const targets = guessBarycenterChildNames(node.name, nameIndex);
        if(targets.length === 0) return;
        const matches = targets.map(n => nameIndex.get(n)).filter(Boolean);
        for(const child of matches){
            addUniqueChild(node, child);
            if(node.baryChildren.length >= 2) break;
        }
    });
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

function computeBarycenterMasses(nodes){
    const arrayNodes = Array.isArray(nodes) ? nodes : [...nodes.values()];
    const massCache = new Map();
    const resolveMass = (node) => {
        if(!node) return 0;
        if(massCache.has(node)) return massCache.get(node);
        const base = Number(node.massValue);
        if(!isBarycenter(node)){
            const mass = Number.isFinite(base) ? base : 0;
            massCache.set(node, mass);
            return mass;
        }
        const children = (node.baryChildren || []).filter(Boolean);
        if(children.length === 0){
            node.massValue = Number.isFinite(base) ? base : 0;
            massCache.set(node, node.massValue);
            return node.massValue;
        }
        const baryUsesSolarMass = usesSolarMassUnits(node);
        const total = children.reduce((sum, child) => {
            const childMass = resolveMass(child);
            const childUsesSolarMass = usesSolarMassUnits(child);
            return sum + normalizeMassToUnit(childMass, childUsesSolarMass, baryUsesSolarMass);
        }, 0);
        node.massValue = total;
        massCache.set(node, total);
        return total;
    };
    arrayNodes.forEach(node => {
        if(isBarycenter(node)) resolveMass(node);
    });
}

function getNodeMass(node){
    const value = Number(node?.massValue);
    return Number.isFinite(value) ? value : 0;
}

function sortBarycenterChildrenByMass(nodes){
    const arrayNodes = Array.isArray(nodes) ? nodes : [...nodes.values()];
    arrayNodes.forEach(node => {
        if(!isBarycenter(node) || !Array.isArray(node.baryChildren)) return;
        node.baryChildren.sort((a, b) => getNodeMass(b) - getNodeMass(a));
    });
}

function buildBaryRootOrderMap(nodes){
    const arrayNodes = Array.isArray(nodes) ? nodes : [...nodes.values()];
    const baryNodes = arrayNodes.filter(isBarycenter);
    if(baryNodes.length === 0){
        return new Map();
    }
    const childToBary = new Map();
    baryNodes.forEach(bary => {
        (bary.baryChildren || []).forEach(child => {
            if(child && child.id != null && !childToBary.has(child.id)){
                childToBary.set(child.id, bary);
            }
        });
    });
    let baryRoots = baryNodes.filter(bary => !childToBary.has(bary.id));
    if(baryRoots.length === 0){
        baryRoots = baryNodes;
    }
    baryRoots.sort((a, b) => getNodeMass(b) - getNodeMass(a));
    const orderMap = new Map();
    const visitedBary = new Set();
    const visitedNodes = new Set();

    const assignFromBary = (bary) => {
        if(!bary || visitedBary.has(bary.id)) return;
        visitedBary.add(bary.id);
        const kids = (bary.baryChildren || []).filter(Boolean);
        kids.sort((a, b) => getNodeMass(b) - getNodeMass(a));
        kids.forEach(child => {
            if(isBarycenter(child)){
                assignFromBary(child);
            } else if(child.id != null && !visitedNodes.has(child.id)){
                orderMap.set(child.id, orderMap.size);
                visitedNodes.add(child.id);
            }
        });
    };

    baryRoots.forEach(assignFromBary);
    return orderMap;
}

function compareRootNodes(a, b, orderMap){
    const orderA = orderMap.get(a?.id);
    const orderB = orderMap.get(b?.id);
    if(orderA != null || orderB != null){
        if(orderA == null) return 1;
        if(orderB == null) return -1;
        if(orderA !== orderB) return orderA - orderB;
    }
    if(a.isMainStar && !b.isMainStar) return -1;
    if(b.isMainStar && !a.isMainStar) return 1;
    return (a.id ?? 0) - (b.id ?? 0);
}

function resolveBodyMassValue(body){
    if(!body) return 0;
    const candidates = [
        body.massValue,
        body.mass_value,
        body.mass_em,
        body.mass
    ];
    for(const value of candidates){
        const num = Number(value);
        if(Number.isFinite(num)) return num;
    }
    return 0;
}

function buildNodeRecord(body, parentsMeta){
    return {
        id: parseBodyId(body.body_id ?? body.bodyId ?? body.id),
        name: body.body_name ?? body.name ?? '',
        type: body.type ?? '',
        parentId: parentsMeta.parentId,
        baryParentId: parentsMeta.baryParentId,
        directBaryParentId: parentsMeta.directBaryParentId,
        isMainStar: Boolean(body.isMainStar ?? body.is_main_star),
        massValue: resolveBodyMassValue(body),
        children: [],
        baryChildren: []
    };
}

function buildRootNodesFromBodies(bodies){
    const nodes = new Map();
    (Array.isArray(bodies) ? bodies : []).forEach(body => {
        const parentsMeta = resolveParentIds(body.parents || []);
        const node = buildNodeRecord(body, parentsMeta);
        if(node.id != null){
            nodes.set(node.id, node);
        }
    });

    for(const node of nodes.values()){
        if(node.parentId != null && nodes.has(node.parentId)){
            nodes.get(node.parentId).children.push(node);
        }
    }

    ensureBarycenterChildren(nodes);
    computeBarycenterMasses([...nodes.values()]);
    sortBarycenterChildrenByMass([...nodes.values()]);
    const baryRootOrderMap = buildBaryRootOrderMap([...nodes.values()]);
    return [...nodes.values()]
        .filter(n => n.parentId == null || !nodes.has(n.parentId))
        .sort((a, b) => compareRootNodes(a, b, baryRootOrderMap));
}

function buildRootIds(bodies){
    return buildRootNodesFromBodies(bodies).map(node => node.id);
}

module.exports = {
    buildRootIds,
    buildRootNodesFromBodies,
    buildBaryRootOrderMap,
    compareRootNodes,
    isBarycenter
};
