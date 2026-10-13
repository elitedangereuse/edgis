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

    function normalizeMaterialName(name){
        return String(name ?? '').trim().toLowerCase().replace(/\s+/g, ' ');
    }

    function getRawMaterialGrade(name){
        const material = RAW_MATERIAL_GRADES[normalizeMaterialName(name)];
        return material ? { ...material } : null;
    }

    return { RAW_MATERIAL_GRADES, getRawMaterialGrade, normalizeMaterialName };
});
