'use strict';

// Raw-material grades are fixed by Elite Dangerous.  Keeping the lookup here
// lets the system map label live body data without requiring an API change.
(function(root, factory){
    const api = factory();
    if(typeof module === 'object' && module.exports){
        module.exports = api;
    }
    root.SysmapMaterials = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function(){
    const RAW_MATERIAL_GRADES = Object.freeze({
        carbon: { grade: 1, rarity: 'Very common' },
        phosphorus: { grade: 1, rarity: 'Very common' },
        sulphur: { grade: 1, rarity: 'Very common' },
        iron: { grade: 1, rarity: 'Very common' },
        nickel: { grade: 1, rarity: 'Very common' },
        rhenium: { grade: 1, rarity: 'Very common' },
        lead: { grade: 1, rarity: 'Very common' },

        vanadium: { grade: 2, rarity: 'Common' },
        chromium: { grade: 2, rarity: 'Common' },
        manganese: { grade: 2, rarity: 'Common' },
        zinc: { grade: 2, rarity: 'Common' },
        germanium: { grade: 2, rarity: 'Common' },
        arsenic: { grade: 2, rarity: 'Common' },
        zirconium: { grade: 2, rarity: 'Common' },

        niobium: { grade: 3, rarity: 'Standard' },
        molybdenum: { grade: 3, rarity: 'Standard' },
        cadmium: { grade: 3, rarity: 'Standard' },
        tin: { grade: 3, rarity: 'Standard' },
        tungsten: { grade: 3, rarity: 'Standard' },
        mercury: { grade: 3, rarity: 'Standard' },
        boron: { grade: 3, rarity: 'Standard' },

        yttrium: { grade: 4, rarity: 'Rare' },
        technetium: { grade: 4, rarity: 'Rare' },
        polonium: { grade: 4, rarity: 'Rare' },
        selenium: { grade: 4, rarity: 'Rare' },
        tellurium: { grade: 4, rarity: 'Rare' },
        antimony: { grade: 4, rarity: 'Rare' },
        ruthenium: { grade: 4, rarity: 'Rare' }
    });
    const RAW_MATERIAL_GRADE_ICONS = Object.freeze({
        1: '/static/assets/materials/1-very%20common.svg',
        2: '/static/assets/materials/2-common.svg',
        3: '/static/assets/materials/3-standard.svg',
        4: '/static/assets/materials/4-rare.svg'
    });

    function normalizeMaterialName(name){
        return String(name ?? '').trim().toLowerCase().replace(/\s+/g, ' ');
    }

    function getRawMaterialGrade(name){
        const material = RAW_MATERIAL_GRADES[normalizeMaterialName(name)];
        return material
            ? { ...material, icon: RAW_MATERIAL_GRADE_ICONS[material.grade] }
            : null;
    }

    // The body API uses `Name` and `Percent`, while Spansh fixtures commonly
    // use lower camel-case equivalents.  Normalize both before rendering.
    function normalizePercentageEntries(source){
        if(!source) return [];
        const entries = [];
        if(Array.isArray(source)){
            source.forEach(item => {
                const name = item?.name ?? item?.Name ?? item?.material ?? item?.Material
                    ?? item?.label ?? item?.Label ?? item?.type ?? item?.Type;
                const percent = Number(
                    item?.percent ?? item?.Percent ?? item?.percentage ?? item?.Percentage
                    ?? item?.share ?? item?.amount ?? item?.value
                );
                if(name && Number.isFinite(percent)) entries.push({ name, percent });
            });
        } else if(typeof source === 'object'){
            Object.entries(source).forEach(([name, percent]) => {
                const num = Number(percent);
                if(name && Number.isFinite(num)) entries.push({ name, percent: num });
            });
        }
        return entries.sort((left, right) => right.percent - left.percent);
    }

    return {
        RAW_MATERIAL_GRADES,
        RAW_MATERIAL_GRADE_ICONS,
        getRawMaterialGrade,
        normalizeMaterialName,
        normalizePercentageEntries
    };
});
