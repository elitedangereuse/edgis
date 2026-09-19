'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
    buildRootIds,
    buildSystemTree,
    computeBarycenterSkipPairs,
    isBarycenter,
    pairKey
} = require('../../static/js/sysmap_roots');

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
