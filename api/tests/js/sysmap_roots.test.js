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

test('station attachment: resolves the host from its reconstructed parents', () => {
    assert.equal(
        resolveStationHostId({ body_id: 14, parents: [{ Planet: 99 }, { Star: 1 }] }),
        99
    );
    assert.equal(resolveStationHostId({ body_id: 14, parents: [] }), null);
    assert.equal(
        resolveStationHostId({ body_id: 14, parents: [{ Null: 0 }, { Star: 1 }] }),
        0
    );
});

test('station attachment: only space stations are eligible for the map', () => {
    assert.equal(isSpaceStation({ station_type: 'Orbis' }), true);
    assert.equal(isSpaceStation({ station_type: 'PlanetaryOutpost', is_planetary: true }), false);
    assert.equal(isSpaceStation({ station_type: 'CraterOutpost' }), false);
    assert.equal(isSpaceStation({ station_type: 'Crater Port' }), false);
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

test('station attachment: uses the reconstructed host and body 0 fallback', () => {
    const { nodes } = buildSystemTree(
        [
            { body_id: 0, body_name: 'Shinrarta Dezhra', type: 'Star', radius: 1 },
            { body_id: 14, body_name: 'Founders World', type: 'Planet', radius: 6000, parents: [{ Star: 0 }] }
        ],
        [
            {
                market_id: 69, body_id: 69, name: 'Jameson Memorial',
                parents: [{ Planet: 14 }, { Star: 0 }]
            },
            { market_id: 70, name: 'Unresolved Station' }
        ]
    );

    assert.equal(nodes.get(-69).parentId, 14);
    assert.equal(nodes.get(-69).isStation, true);
    assert.deepEqual(nodes.get(0).children.map(node => node.id), [-70, 14]);
    assert.deepEqual(nodes.get(14).children.map(node => node.id), [-69]);
    assert.equal(nodes.get(-70).parentId, 0);
    assert.equal(nodes.get(-70).unresolvedStationHost, true);
});

test('station attachment: uses a map-only market ID when its BodyID is absent', () => {
    const { nodes } = buildSystemTree(
        [{ body_id: 1, body_name: 'Example', type: 'Star', radius: 1 }],
        [{ market_id: 4341179395, name: 'Holzman Vision', parents: [{ Planet: 5 }]}]
    );

    assert.equal(nodes.get(-4341179395).isStation, true);
});

test('station attachment: orders direct station siblings by arrival distance', () => {
    const { nodes } = buildSystemTree(
        [
            { body_id: 0, body_name: 'Lave', type: 'Star', radius: 1, distance_from_arrival_ls: 0 },
            { body_id: 1, body_name: 'Planet Lave', type: 'Planet', radius: 6000, distance_from_arrival_ls: 279.357954, parents: [{ Star: 0 }] },
            { body_id: 2, body_name: 'Castellan Belt', type: 'StellarRing', radius: 1, distance_from_arrival_ls: 2398.64421, parents: [{ Star: 0 }] }
        ],
        [{
            market_id: 1000,
            body_id: 69,
            name: 'Warinus',
            parents: [{ Star: 0 }],
            distance_from_arrival_ls: 864.919012
        }]
    );

    assert.deepEqual(nodes.get(0).children.map(node => node.id), [1, -1000, 2]);
});

test('celestial siblings retain game body order despite arrival-distance drift', () => {
    const { nodes } = buildSystemTree([
        { body_id: 7, body_name: 'Jupiter', type: 'Planet', parents: [{ Star: 0 }] },
        { body_id: 9, body_name: 'Io', type: 'Planet', distance_from_arrival_ls: 2618.798905, parents: [{ Planet: 7 }, { Star: 0 }] },
        { body_id: 10, body_name: 'Europa', type: 'Planet', distance_from_arrival_ls: 2618.640205, parents: [{ Planet: 7 }, { Star: 0 }] },
        { body_id: 11, body_name: 'Ganymede', type: 'Planet', distance_from_arrival_ls: 2619.303117, parents: [{ Planet: 7 }, { Star: 0 }] },
        { body_id: 12, body_name: 'Callisto', type: 'Planet', distance_from_arrival_ls: 2615.653817, parents: [{ Planet: 7 }, { Star: 0 }] }
    ]);

    assert.deepEqual(nodes.get(7).children.map(node => node.id), [9, 10, 11, 12]);
});

test('station attachment: keeps a station BodyID distinct from its host', () => {
    const { nodes } = buildSystemTree(
        [
            { body_id: 0, body_name: 'Fujin', type: 'Star', radius: 1 },
            { body_id: 1, body_name: 'Futen', type: 'Planet', radius: 6000, parents: [{ Star: 0 }] },
            { body_id: 2, body_name: 'Fujin 2', type: 'Planet', radius: 6000, parents: [{ Star: 0 }] }
        ],
        [{
            market_id: 128134392,
            body_id: 2,
            name: 'Futen Spaceport',
            parents: [{ Planet: 1 }, { Star: 0 }]
        }]
    );

    assert.equal(nodes.get(-128134392).parentId, 1);
    assert.equal(nodes.get(-128134392).unresolvedStationHost, false);
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

test('nested barycenter leaves keep game body order despite radial differences', () => {
    const distances = {
        10: 2438.900602,
        11: 2438.002831,
        12: 2438.783367,
        13: 2435.399442,
        14: 2434.459137
    };
    const bodies = loadBodies(path.join(fixtureDir, 'PHREIA FLYOU FG-V D3-116.json'))
        .map(body => ({
            ...body,
            distance_from_arrival_ls: distances[body.body_id]
                ?? body.distance_from_arrival_ls
        }));
    const { nodes } = buildSystemTree(bodies);
    const nestedLeafIds = nodes.get(0).children
        .filter(node => !isBarycenter(node) && node.id >= 10 && node.id <= 14)
        .map(node => node.id);

    assert.deepEqual(nestedLeafIds, [10, 11, 12, 13, 14]);
});

test('zero-distance barycenters sort with their orbital members', () => {
    const { nodes } = buildSystemTree([
        { body_id: 0, body_name: 'Sol', type: 'Star', distance_from_arrival_ls: 0 },
        { body_id: 1, body_name: 'Mercury', type: 'Planet', distance_from_arrival_ls: 225, parents: [{ Star: 0 }] },
        { body_id: 28, body_name: 'Neptune', type: 'Planet', distance_from_arrival_ls: 14920, parents: [{ Star: 0 }] },
        { body_id: 31, body_name: 'Barycenter31', type: 'Barycenter', distance_from_arrival_ls: 0, parents: [{ Star: 0 }] },
        { body_id: 32, body_name: 'Pluto', type: 'Planet', distance_from_arrival_ls: 20460, parents: [{ Null: 31 }, { Star: 0 }] },
        { body_id: 33, body_name: 'Charon', type: 'Planet', distance_from_arrival_ls: 20461, parents: [{ Null: 31 }, { Star: 0 }] }
    ]);

    const visibleChildren = nodes.get(0).children
        .filter(node => !isBarycenter(node))
        .map(node => node.id);

    assert.deepEqual(visibleChildren, [1, 28, 32, 33]);
});
