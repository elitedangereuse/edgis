'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
    RAW_MATERIAL_GRADES,
    RAW_MATERIAL_GRADE_ICONS,
    getRawMaterialGrade,
    normalizePercentageEntries
} = require('../../static/js/sysmap_materials');

test('raw material grades cover the complete in-game material grid', () => {
    assert.equal(Object.keys(RAW_MATERIAL_GRADES).length, 28);
    assert.deepEqual(getRawMaterialGrade('Iron'), {
        grade: 1, rarity: 'Very common', icon: RAW_MATERIAL_GRADE_ICONS[1]
    });
    assert.deepEqual(getRawMaterialGrade('germanium'), {
        grade: 2, rarity: 'Common', icon: RAW_MATERIAL_GRADE_ICONS[2]
    });
    assert.deepEqual(getRawMaterialGrade('  Tungsten  '), {
        grade: 3, rarity: 'Standard', icon: RAW_MATERIAL_GRADE_ICONS[3]
    });
    assert.deepEqual(getRawMaterialGrade('Tellurium'), {
        grade: 4, rarity: 'Rare', icon: RAW_MATERIAL_GRADE_ICONS[4]
    });
});

test('unknown material names do not receive a misleading grade', () => {
    assert.equal(getRawMaterialGrade('Guardian Relic'), null);
});

test('system-map material entries accept the API Name and Percent fields', () => {
    assert.deepEqual(
        normalizePercentageEntries([
            { Name: 'Ruthenium', Percent: 1.3 },
            { Name: 'Iron', Percent: 20.8 }
        ]),
        [
            { name: 'Iron', percent: 20.8 },
            { name: 'Ruthenium', percent: 1.3 }
        ]
    );
});
