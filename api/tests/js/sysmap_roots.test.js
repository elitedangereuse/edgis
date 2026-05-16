'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { buildRootIds } = require('../../static/js/sysmap_roots');

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
