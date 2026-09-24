'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
    buildRootIds,
    buildSystemTree,
    computeBarycenterSkipPairs,
    inferStationHostByArrivalDistance,
    isSpaceStation,
    isBarycenter,
    pairKey,
    resolveStationHostId,
    stationIconAsset
} = require('../../static/js/sysmap_roots');

test('station attachment: resolves the direct host from parents, not BodyID', () => {
    assert.equal(
        resolveStationHostId({ body_id: 69, parents: [{ Planet: 14 }, { Star: 1 }] }),
        14
    );
    assert.equal(resolveStationHostId({ body_id: 69, parents: [] }), null);
});

test('station attachment: only space stations are eligible for the map', () => {
    assert.equal(isSpaceStation({ station_type: 'Orbis' }), true);
    assert.equal(isSpaceStation({ station_type: 'PlanetaryOutpost', is_planetary: true }), false);
    assert.equal(isSpaceStation({ station_type: 'FleetCarrier', is_carrier: true }), false);
    assert.equal(isSpaceStation({ station_type: 'FleetCarrier' }), false);
});

test('station icon: maps known station types to their SVG assets', () => {
    assert.equal(stationIconAsset('Orbis'), 'orbisstation.svg');
    assert.equal(stationIconAsset('Ocellus Starport'), 'ocelusstation.svg');
    assert.equal(stationIconAsset('Dodec Starport'), 'dodecstation.svg');
    assert.equal(stationIconAsset('Coriolis Starport'), 'coriolisstation.svg');
    assert.equal(stationIconAsset('Asteroid Base'), 'asteroidstation.svg');
    assert.equal(stationIconAsset('Space Construction Depot'), null);
});

test('station attachment: uses the reconstructed host and the primary star fallback', () => {
    const { nodes } = buildSystemTree(
        [
            { body_id: 1, body_name: 'Shinrarta Dezhra', type: 'Star', radius: 1 },
            { body_id: 14, body_name: 'Founders World', type: 'Planet', radius: 6000, parents: [{ Star: 1 }] }
        ],
        [
            { body_id: 69, name: 'Jameson Memorial', parents: [{ Planet: 14 }, { Star: 1 }] },
            { body_id: 70, name: 'Unresolved Station', parents: [] }
        ]
    );

    assert.equal(nodes.get(69).parentId, 14);
    assert.equal(nodes.get(69).isStation, true);
    assert.deepEqual(nodes.get(1).children.map(node => node.id), [70, 14, 69]);
    assert.equal(nodes.get(70).parentId, 1);
    assert.equal(nodes.get(70).unresolvedStationHost, true);
});

test('station attachment: uses a map-only market ID when its BodyID is absent', () => {
    const { nodes } = buildSystemTree(
        [{ body_id: 1, body_name: 'Example', type: 'Star', radius: 1 }],
        [{ market_id: 4341179395, name: 'Holzman Vision', parents: [{ Planet: 5 }]}]
    );

    assert.equal(nodes.get(-4341179395).isStation, true);
});

test('station attachment: infers Earth from a root station arrival distance', () => {
    const earth = {
        id: 3, name: 'Earth', type: 'Planet', radius: 6371000,
        distanceToArrival: 502.233897
    };
    const host = inferStationHostByArrivalDistance(
        { distance_from_arrival_ls: 502.254085 },
        [
            { id: 2, name: 'Venus', type: 'Planet', radius: 6051800, distanceToArrival: 362.282323 },
            earth,
            { id: 4, name: 'Mars', type: 'Planet', radius: 3389500, distanceToArrival: 811.214371 }
        ]
    );
    assert.equal(host, earth);
});

test('station attachment: leaves ambiguous radial matches unattached', () => {
    const host = inferStationHostByArrivalDistance(
        { distance_from_arrival_ls: 100 },
        [
            { id: 1, name: 'A', type: 'Planet', radius: 6371000, distanceToArrival: 99.99 },
            { id: 2, name: 'B', type: 'Planet', radius: 6371000, distanceToArrival: 100.01 }
        ]
    );
    assert.equal(host, null);
});

const fixtureDir = __dirname;
const fixtureFiles = fs.readdirSync(fixtureDir)
    .filter((name) => name.endsWith('.json') && !name.endsWith('.roots.json'));

const loadBodies = (filePath) => {
    const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    if(Array.isArray(raw)) return raw;
    if(raw && Array.isArray(raw.bodies)) return raw.bodies;
    throw new Error(`Fixture must be an array or { bodies: [...] }: ${filePath}`);
};

fixtureFiles.forEach((filename) => {
    const fixturePath = path.join(fixtureDir, filename);
    const expectedPath = path.join(fixtureDir, filename.replace(/\.json$/, '.roots.json'));
    const testName = path.basename(filename, '.json');

    test(`roots: ${testName}`, () => {
        assert.ok(fs.existsSync(expectedPath), `Missing expected roots file: ${expectedPath}`);
        const bodies = loadBodies(fixturePath);
        const expected = JSON.parse(fs.readFileSync(expectedPath, 'utf8'));
        assert.ok(Array.isArray(expected), `Expected roots must be an array: ${expectedPath}`);
        const rootIds = buildRootIds(bodies);
        assert.deepStrictEqual(rootIds, expected);
    });
});

// Barycenter member sums drive layout ordering; they must aggregate nested
// barycenter masses instead of collapsing to zero (2MASS designations carry a
// "+" inside the system prefix, which used to break the member-name guesses).
test('barycenter masses: 2MASS J02351897+6131236', () => {
    const bodies = loadBodies(path.join(fixtureDir, '2MASS J02351897+6131236.json'));
    const { nodes } = buildSystemTree(bodies);
    const expectedMasses = {
        0: 132.226563, // ABCD = ABC + D
        1: 100.84375,  // ABC = A + BC
        3: 25.152344,  // BC = B + C
        38: 1.046875   // 6+7
    };
    for(const [id, expected] of Object.entries(expectedMasses)){
        const node = nodes.get(Number(id));
        assert.ok(node && isBarycenter(node), `missing barycenter ${id}`);
        assert.ok(
            Math.abs(node.massValue - expected) < 1e-6,
            `barycenter ${id} (${node.name}) mass ${node.massValue} != ${expected}`
        );
    }
});

test('nested barycenters suppress visible member links at every bracket boundary', () => {
    const bodies = loadBodies(path.join(fixtureDir, 'PHREIA FLYOU FG-V D3-116.json'));
    const { nodes } = buildSystemTree(bodies);
    const skipped = computeBarycenterSkipPairs(nodes);

    for(const [left, right] of [[10, 11], [11, 12], [12, 13], [13, 14]]){
        assert.ok(
            skipped.has(pairKey(left, right)),
            `expected nested barycenter boundary ${left}|${right} to be suppressed`
        );
    }
});
