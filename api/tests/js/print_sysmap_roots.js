'use strict';

const fs = require('node:fs');
const path = require('node:path');
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

const output = fixtureFiles.map((filename) => {
    const fixturePath = path.join(fixtureDir, filename);
    const bodies = loadBodies(fixturePath);
    return {
        fixture: filename,
        rootIds: buildRootIds(bodies)
    };
});

console.log(JSON.stringify(output, null, 2));
