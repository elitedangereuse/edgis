'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { buildSystemTree } = require('../../static/js/sysmap_roots');

const fixturePath = path.join(
    __dirname,
    '..',
    '..',
    'static',
    'fixtures',
    'sysmap-body-palette.json'
);
const bodies = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));

test('body palette covers system-map render categories and interpolators', () => {
    const starTypes = new Set(
        bodies.filter(body => body.type === 'Star').map(body => body.star_type)
    );
    const planetClasses = new Set(
        bodies.filter(body => body.type === 'Planet').map(body => body.planet_class)
    );
    const bodyTypes = new Set(bodies.map(body => body.type));

    assert.ok(starTypes.has('O (Blue-White) Star'));
    assert.ok(starTypes.has('T Tauri Star'));
    assert.ok(starTypes.has('White Dwarf (DA) Star'));
    assert.ok(starTypes.has('White Dwarf (DB) Star'));
    assert.ok(starTypes.has('Supermassive Black Hole'));
    assert.ok(planetClasses.has('Earthlike body'));
    assert.ok(planetClasses.has('Class IV gas giant'));
    assert.ok(planetClasses.has('Gas giant with ammonia-based life'));
    assert.ok(planetClasses.has('Helium-rich gas giant'));
    for(const type of ['Barycenter', 'PlanetaryRing', 'StellarRing', 'AsteroidCluster']){
        assert.ok(bodyTypes.has(type), `missing ${type}`);
    }
});

test('body palette contains mass-ordered and nested barycenter cases', () => {
    const { nodes } = buildSystemTree(bodies);
    assert.deepEqual(
        nodes.get(200).baryChildren.map(body => body.id),
        [202, 201]
    );
    assert.deepEqual(
        nodes.get(210).baryChildren.map(body => body.id),
        [211, 214]
    );
    assert.deepEqual(
        nodes.get(211).baryChildren.map(body => body.id),
        [212, 213]
    );
});
