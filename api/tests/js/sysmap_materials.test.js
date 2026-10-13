'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
    RAW_MATERIAL_GRADES,
    getRawMaterialGrade
} = require('../../static/js/sysmap_materials');

test('raw material grades cover the complete in-game material grid', () => {
    assert.equal(Object.keys(RAW_MATERIAL_GRADES).length, 28);
    assert.deepEqual(getRawMaterialGrade('Iron'), { grade: 1, rarity: 'Very common' });
    assert.deepEqual(getRawMaterialGrade('germanium'), { grade: 2, rarity: 'Common' });
    assert.deepEqual(getRawMaterialGrade('  Tungsten  '), { grade: 3, rarity: 'Standard' });
    assert.deepEqual(getRawMaterialGrade('Tellurium'), { grade: 4, rarity: 'Rare' });
});

test('unknown material names do not receive a misleading grade', () => {
    assert.equal(getRawMaterialGrade('Guardian Relic'), null);
});
