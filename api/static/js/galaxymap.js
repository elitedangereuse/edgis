     const urlParams = new URLSearchParams(window.location.search);
     const sameHostBaseUrl =
       window.location?.origin || `${window.location?.protocol}//${window.location?.host || ''}`;
     let manualSystemsLookup = new Map();
     let systemData = null;
     let lastClickedSystemName = null;

     function parseSolutionJsonParam(rawValue) {
       if (!rawValue) return null;
       const attempts = [rawValue];
       try {
         attempts.push(atob(rawValue));
       } catch (err) {
         // Not base64, ignore.
       }

       for (const attempt of attempts) {
         try {
           const parsed = JSON.parse(attempt);
           if (parsed && typeof parsed === 'object') {
             return parsed;
           }
         } catch (err) {
           // Continue trying other decodings.
         }
       }

       return null;
     }

     const solutionJsonParam = urlParams.get('solutionjson');
     let externalSolutionJson = null;
     if (solutionJsonParam) {
       externalSolutionJson = parseSolutionJsonParam(solutionJsonParam);
       if (!externalSolutionJson) {
         console.warn('Failed to parse solutionjson parameter.');
       }
     }

     function initSolutionJson(x, y, z, mode = "simple") {
       if (mode === "expert") {
         return {
           categories: {
             EDGIS: {
               "Target": {
                 name: `Target (${x}, ${y}, ${z})`,
                 color: "00ff1c"
               },
               "Neighbors": {
                 name: "Neighbors",
               },

               // Most common main sequence stars
               "M (Red dwarf) Star": { name: "M (Red dwarf) Star", color: "f8ce9d" },
               "K (Yellow-Orange) Star": { name: "K (Yellow-Orange) Star", color: "feeace" },
               "G (White-Yellow) Star": { name: "G (White-Yellow) Star", color: "faefcd" },
               "F (White) Star": { name: "F (White) Star", color: "fcf8e3" },
               "A (Blue-White) Star": { name: "A (Blue-White) Star", color: "f8fafd" },
               "B (Blue-White) Star": { name: "B (Blue-White) Star", color: "f1fdfd" },
               "O (Blue-White) Star": { name: "O (Blue-White) Star", color: "f5fcfe" },
               "T Tauri Star": { name: "T Tauri Star", color: "e2f2fe" },

               // Brown dwarfs (likely very common, but faint)
               "L (Brown dwarf) Star": { name: "L (Brown dwarf) Star", color: "a52a2a" },
               "T (Brown dwarf) Star": { name: "T (Brown dwarf) Star", color: "8b4513" },
               "Y (Brown dwarf) Star": { name: "Y (Brown dwarf) Star", color: "a0522d" },

               // Giants & Supergiants (rarer evolutionary phases)
               "M (Red giant) Star": { name: "M (Red giant) Star", color: "f0b955" },
               "K (Yellow-Orange giant) Star": { name: "K (Yellow-Orange giant) Star", color: "fee3ab" },
               "G (White-Yellow super giant) Star": { name: "G (White-Yellow super giant) Star", color: "f6e5b4" },
               "F (White super giant) Star": { name: "F (White super giant) Star", color: "fdf1cb" },
               "A (Blue-White super giant) Star": { name: "A (Blue-White super giant) Star", color: "fafdfe" },
               "B (Blue-White super giant) Star": { name: "B (Blue-White super giant) Star", color: "e5e9f1" },
               "M (Red super giant) Star": { name: "M (Red super giant) Star", color: "e48c46" },

               // Carbon-rich & chemically peculiar stars
               "C Star": { name: "C Star", color: "981055" },
               "CN Star": { name: "CN Star", color: "fecd8f" },
               "CJ Star": { name: "CJ Star", color: "f9b66a" },
               "S-type Star": { name: "S-type Star", color: "ffdead" },
               "MS-type Star": { name: "MS-type Star", color: "fcca88" },

               // Pre-main sequence stars
               "Herbig Ae/Be Star": { name: "Herbig Ae/Be Star", color: "ffe6b1" },

               // Wolf-Rayet stars (very rare)
               "Wolf-Rayet Star": { name: "Wolf-Rayet Star", color: "fec2fe" },
               "Wolf-Rayet N Star": { name: "Wolf-Rayet N Star", color: "f5fcfb" },
               "Wolf-Rayet C Star": { name: "Wolf-Rayet C Star", color: "f0fafb" },
               "Wolf-Rayet O Star": { name: "Wolf-Rayet O Star", color: "e1e8f1" },
               "Wolf-Rayet NC Star": { name: "Wolf-Rayet NC Star", color: "e2e7f0" },

               // Stellar remnants (white dwarfs are actually numerous)
               "White Dwarf (DA) Star": { name: "White Dwarf (DA) Star", color: "f8f8ff" },
               "White Dwarf (DAZ) Star": { name: "White Dwarf (DAZ) Star", color: "ffffff" },
               "White Dwarf (DAB) Star": { name: "White Dwarf (DAB) Star", color: "fffafa" },
               "White Dwarf (DAV) Star": { name: "White Dwarf (DAV) Star", color: "e6e6fa" },
               "White Dwarf (DB) Star": { name: "White Dwarf (DB) Star", color: "f5f5f5" },
               "White Dwarf (DBZ) Star": { name: "White Dwarf (DBZ) Star", color: "f0f8ff" },
               "White Dwarf (DBV) Star": { name: "White Dwarf (DBV) Star", color: "f5f5dc" },
               "White Dwarf (DC) Star": { name: "White Dwarf (DC) Star", color: "f0fff0" },
               "White Dwarf (DCV) Star": { name: "White Dwarf (DCV) Star", color: "fafad2" },
               "White Dwarf (DQ) Star": { name: "White Dwarf (DQ) Star", color: "f5f5f5" },
               "White Dwarf (D) Star": { name: "White Dwarf (D) Star", color: "f8f8ff" },

               // Ultra-rare remnants
               "Neutron Star": { name: "Neutron Star", color: "696969" },
               "Black Hole": { name: "Black Hole", color: "000000" },
               "Supermassive Black Hole": { name: "Supermassive Black Hole", color: "0a0a0a" },

               // Fallback
               "Unknown Star": { name: "Unknown Star", color: "ffffff" }
             }
           },
           systems: [],
           routes: []
         };
       } else {
         return {
           categories: {
             EDGIS: {
               "Target": {
                 name: `Target (${x}, ${y}, ${z})`,
                 color: "00ff1c"
               },
               "Neighbors": {
                 name: "Neighbors",
               },

               // Main sequence
               "O-type Stars": { name: "O-type Stars", color: "0001fb" },
               "B-type Stars": { name: "B-type Stars", color: "1209f2" },
               "A-type Stars": { name: "A-type Stars", color: "3e1fdb" },
               "F-type Stars": { name: "F-type Stars", color: "656496" },
               "G-type Stars": { name: "G-type Stars", color: "959648" },
               "K-type Stars": { name: "K-type Stars", color: "b23d1b" },
               "M-type Stars": { name: "M-type Stars", color: "fbb3a9" },

               // Brown dwarfs
               "L-type Stars": { name: "L-type Stars", color: "fe1f04" },
               "T-type Stars": { name: "T-type Stars", color: "800100" },
               "Y-type Stars": { name: "Y-type Stars", color: "7e0100" },

               // Proto
               "Proto Stars": { name: "Proto Stars", color: "7d0000" },

               // Carbon
               "Carbon Stars": { name: "Carbon Stars", color: "969548" },

               // Wolf-Rayet
               "Wolf-Rayet Stars": { name: "Wolf-Rayet Stars", color: "c9cafc" },

               // White dwarfs
               "White Dwarf Stars": { name: "White Dwarf Stars", color: "6067ef" },

               // Other
               "Non Sequence Stars": { name: "Non Sequence Stars", color: "646464" }
             }
           },
           systems: [],
           routes: []
         };
       }
     }

     async function getSystemCoordinates(url) {
       const response = await fetch(url);
       if(!response.ok)
         throw new Error(response.statusText);

       const data = await response.json();
       return data;
     }

     async function drawSolution(x, y, z, radius, res, mode) {
       const sphereurl = sameHostBaseUrl + "/neighbors?x=" +
                         x + "&y=" + y + "&z=" + z +
                         "&radius=" + radius;
       const spherejson = await getSystemCoordinates(sphereurl);
       if (spherejson && spherejson.length > 0) {
         populateResult(spherejson, res, radius, mode);
       }
     }


     function populateResult(spherejson, res, radius, mode = "simple") {
       const starNameMap = {
         "TTS": "T Tauri Star",
         "M": "M (Red dwarf) Star",
         "M_RedGiant": "M (Red giant) Star",
         "L": "L (Brown dwarf) Star",
         "K": "K (Yellow-Orange) Star",
         "K_OrangeGiant": "K (Yellow-Orange giant) Star",
         "G": "G (White-Yellow) Star",
         "F": "F (White) Star",
         "F_WhiteSuperGiant": "F (White super giant) Star",
         "O": "O (Blue-White) Star",
         "B": "B (Blue-White) Star",
         "A": "A (Blue-White) Star",
         "CJ": "CJ Star",
         "Y": "Y (Brown dwarf) Star",
         "DA": "White Dwarf (DA) Star",
         "DAZ": "White Dwarf (DAZ) Star",
         "DB": "White Dwarf (DB) Star",
         "DBZ": "White Dwarf (DBZ) Star",
         "DC": "White Dwarf (DC) Star",
         "DCV": "White Dwarf (DCV) Star",
         "DQ": "White Dwarf (DQ) Star",
         "DAV": "White Dwarf (DAV) Star",
         "DAB": "White Dwarf (DAB) Star",
         "DBV": "White Dwarf (DBV) Star",
         "D": "White Dwarf (D) Star",
         "WR": "Wolf-Rayet Star",
         "WRN": "Wolf-Rayet N Star",
         "WRC": "Wolf-Rayet C Star",
         "WRNC": "Wolf-Rayet NC Star",
         "WRO": "Wolf-Rayet O Star",
         "NS": "Neutron Star",
         "BH": "Black Hole",
         "SMBH": "Supermassive Black Hole",
         "T": "T (Brown dwarf) Star",
         "N": "Neutron Star",
         "S": "S-type Star",
         "null": "Unknown Star"
       };

       // Map detailed star names → simplified categories
       const categoryMap = {
         // Main sequence
         "O (Blue-White) Star": "O-type Stars",
         "B (Blue-White) Star": "B-type Stars",
         "A (Blue-White) Star": "A-type Stars",
         "F (White) Star": "F-type Stars",
         "F (White super giant) Star": "F-type Stars",
         "G (White-Yellow) Star": "G-type Stars",
         "G (White-Yellow super giant) Star": "G-type Stars",
         "K (Yellow-Orange) Star": "K-type Stars",
         "K (Yellow-Orange giant) Star": "K-type Stars",
         "M (Red dwarf) Star": "M-type Stars",
         "M (Red giant) Star": "M-type Stars",
         "M (Red super giant) Star": "M-type Stars",

         // Brown dwarfs
         "L (Brown dwarf) Star": "L-type Stars",
         "T (Brown dwarf) Star": "T-type Stars",
         "Y (Brown dwarf) Star": "Y-type Stars",

         // Proto
         "T Tauri Star": "Proto Stars",
         "Herbig Ae/Be Star": "Proto Stars",

         // Carbon
         "C Star": "Carbon Stars",
         "CN Star": "Carbon Stars",
         "CJ Star": "Carbon Stars",
         "S-type Star": "Carbon Stars",
         "MS-type Star": "Carbon Stars",

         // Wolf-Rayet
         "Wolf-Rayet Star": "Wolf-Rayet Stars",
         "Wolf-Rayet N Star": "Wolf-Rayet Stars",
         "Wolf-Rayet C Star": "Wolf-Rayet Stars",
         "Wolf-Rayet O Star": "Wolf-Rayet Stars",
         "Wolf-Rayet NC Star": "Wolf-Rayet Stars",

         // White dwarfs
         "White Dwarf (DA) Star": "White Dwarf Stars",
         "White Dwarf (DAZ) Star": "White Dwarf Stars",
         "White Dwarf (DAB) Star": "White Dwarf Stars",
         "White Dwarf (DAV) Star": "White Dwarf Stars",
         "White Dwarf (DB) Star": "White Dwarf Stars",
         "White Dwarf (DBZ) Star": "White Dwarf Stars",
         "White Dwarf (DBV) Star": "White Dwarf Stars",
         "White Dwarf (DC) Star": "White Dwarf Stars",
         "White Dwarf (DCV) Star": "White Dwarf Stars",
         "White Dwarf (DQ) Star": "White Dwarf Stars",
         "White Dwarf (D) Star": "White Dwarf Stars",

         // Other
         "Neutron Star": "Non Sequence Stars",
         "Black Hole": "Non Sequence Stars",
         "Supermassive Black Hole": "Non Sequence Stars",
         "Unknown Star": "Non Sequence Stars"
       };

       const starCategories = res.categories.EDGIS;

       res.systems.push(
         ...spherejson.map((s, i) => {
           let mainStar = s.mainstar;
           if (i === 0) {
             return {
               name: `${s.name} (${s.distance.toFixed(2)} LY)`,
               coords: { ...s.coords, radius: radius },
               cat: ["Target"],
               infos: { ...s, mainStar, radius }
             };
           }

           // Step 1: normalize star code (e.g. "M" → "M (Red dwarf) Star")
           if (starNameMap.hasOwnProperty(mainStar)) {
             mainStar = starNameMap[mainStar];
           }

           // Step 2: pick category depending on mode
           let category = mode === "expert" ? mainStar : (categoryMap[mainStar] || "Non Sequence Stars");

           if (!starCategories.hasOwnProperty(category)) {
             console.warn(`Warning: Main star "${mainStar}" maps to missing category "${category}"`);
           }

           return {
             name: `${s.name} (${s.distance.toFixed(2)} LY)`,
             coords: { ...s.coords, radius: 0 },
             cat: [category, "Neighbors"],
             infos: { ...s, mainStar, radius }
           };
         })
       );
     }

    function computeEffectScales(systemCount) {
      const count = Math.max(systemCount || 0, 1);
      let effectScaleMax = 10000 / count;
      let effectScaleMin = 1000 / count;
      if (effectScaleMin < 1) effectScaleMin = 1;
      if (effectScaleMin > 3) effectScaleMin = 3;
      if (effectScaleMax < 10) effectScaleMax = 10;
      return [effectScaleMin, effectScaleMax];
    }

    function startEd3dMap(solutionjson, playerPos, cameraPos, effectScaleMin, effectScaleMax) {
      const hudpanel = true;
      Ed3d.init({
        container   : 'edmap',
        json : solutionjson,
        basePath: "/static/ed3d/",
        withHudPanel : hudpanel,
        startAnim : false,
        hudMultipleSelect : true,
        withOptionsPanel: false,
        withFullscreenToggle: false,
        showGalaxyInfos: true,
        showNameNear: false,
        playerPos: playerPos,
        cameraPos: cameraPos,
        effectScaleSystem : [effectScaleMin, effectScaleMax]
      });
    }

    let autoSelectRequestId = 0;

    function queueAutoSelectByName(systemName, fallbackInfos, attemptsLeft = 25) {
      if (!systemName) {
        return;
      }

      const requestId = ++autoSelectRequestId;

      const trySelect = (remaining) => {
        if (requestId !== autoSelectRequestId || remaining <= 0) {
          return;
        }

        const vertices = window.System?.particleGeo?.vertices;
        const actionReady = window.Action && typeof window.Action.moveToObj === 'function';
        const canSelect = vertices && actionReady;

        if (canSelect) {
          for (let index = 0; index < vertices.length; index++) {
            const vertex = vertices[index];
            if (vertex && vertex.name === systemName) {
              try {
                if (
                  window.Action
                  && (!window.Action.cursor || !window.Action.cursor.hover)
                  && typeof window.Action.addCursorOnHover === 'function'
                ) {
                  window.Action.addCursorOnHover({
                    x: vertex.x,
                    y: vertex.y,
                    z: vertex.z,
                    name: vertex.name
                  });
                }

                window.Action.moveToObj(index, vertex);
                const payload = vertex.infos ?? fallbackInfos ?? null;
                if (window.$) {
                  $(document).trigger('systemClick', [vertex.name, payload, vertex.url ?? null]);
                }
              } catch (error) {
                console.warn('Failed to auto-select system:', error);
              }
              return;
            }
          }
        }

        setTimeout(() => trySelect(remaining - 1), 300);
      };

      setTimeout(() => trySelect(attemptsLeft), 0);
    }

    function autoSelectZeroDistanceSystem(solutionjson) {
      if (!solutionjson || !Array.isArray(solutionjson.systems)) {
        return;
      }

      const systemsWithDistance = solutionjson.systems.filter((system) => typeof system?.infos?.distance === 'number');
      if (systemsWithDistance.length === 0) {
        return;
      }

      const zeroDistanceEntry = systemsWithDistance.find((system) => Math.abs(system.infos.distance) < 1e-4);
      const fallbackEntry = systemsWithDistance.reduce((closest, system) => {
        if (!closest) {
          return system;
        }
        return Math.abs(system.infos.distance) < Math.abs(closest.infos.distance) ? system : closest;
      }, null);

      const targetEntry = zeroDistanceEntry ?? fallbackEntry;
      if (!targetEntry || typeof targetEntry.name !== 'string') {
        return;
      }

      const targetName = targetEntry?.infos?.name ?? targetEntry.name;
      lastClickedSystemName = targetName;
      if (!systemData || systemData.name !== targetName) {
        systemData = { name: targetName, bodies: [] };
      }

      queueAutoSelectByName(targetEntry.name, targetEntry.infos ?? { name: targetName });
    }

    function calculatePositionsFromSystems(systems) {
      if (!Array.isArray(systems) || systems.length === 0) {
        return {
          playerPos: [0, 0, 0],
          cameraPos: [0, 100, -100]
        };
      }

      let minX = Infinity, minY = Infinity, minZ = Infinity;
      let maxX = -Infinity, maxY = -Infinity, maxZ = -Infinity;
      let sumX = 0, sumY = 0, sumZ = 0;
      let count = 0;

      for (const system of systems) {
        if (!system || typeof system !== 'object') continue;
        const coords = system.coords || {};
        const x = Number(coords.x);
        const y = Number(coords.y);
        const z = Number(coords.z);

        if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) {
          continue;
        }

        minX = Math.min(minX, x);
        minY = Math.min(minY, y);
        minZ = Math.min(minZ, z);
        maxX = Math.max(maxX, x);
        maxY = Math.max(maxY, y);
        maxZ = Math.max(maxZ, z);

        sumX += x;
        sumY += y;
        sumZ += z;
        count++;
      }

      if (count === 0) {
        return {
          playerPos: [0, 0, 0],
          cameraPos: [0, 100, -100]
        };
      }

      const meanX = sumX / count;
      const meanY = sumY / count;
      const meanZ = sumZ / count;

      const rangeY = (maxY - minY) || 100;
      const rangeZ = (maxZ - minZ) || 100;

      return {
        playerPos: [meanX, meanY, meanZ],
        cameraPos: [meanX, meanY + rangeY, meanZ - rangeZ]
      };
    }

    function renderManualSolution(manualSolutionJson) {
      const systems = Array.isArray(manualSolutionJson?.systems) ? manualSolutionJson.systems : [];

      manualSystemsLookup = new Map();

      systems.forEach((system, index) => {
        if (!system || typeof system !== 'object') {
          return;
        }

        if (!system.coords || typeof system.coords !== 'object') {
          system.coords = { x: 0, y: 0, z: 0, radius: 0 };
        }

        const fallbackRadius = typeof system.radius === 'number'
          ? system.radius
          : (typeof system.coords.radius === 'number' ? system.coords.radius : 0);

        const entry = {
          name: system.name ?? `System ${index + 1}`,
          coords: system.coords,
          radius: fallbackRadius,
          mainStar: system.mainStar ?? system?.infos?.mainStar ?? 'Unknown',
          distance: typeof system.distance === 'number'
            ? system.distance
            : (typeof system?.infos?.distance === 'number' ? system.infos.distance : null)
        };

        manualSystemsLookup.set(entry.name, entry);

        if (!system.infos) {
          system.infos = {
            name: entry.name,
            coords: entry.coords,
            radius: entry.radius,
            mainStar: entry.mainStar,
            distance: entry.distance
          };
        }
      });

      const { playerPos, cameraPos } = calculatePositionsFromSystems(systems);
      const [effectScaleMin, effectScaleMax] = computeEffectScales(systems.length);
      startEd3dMap(manualSolutionJson, playerPos, cameraPos, effectScaleMin, effectScaleMax);
      autoSelectZeroDistanceSystem(manualSolutionJson);
    }

    async function drawSystems(x, y, z, radius, mode) {
      try {
        const solutionjson = initSolutionJson(x, y, z, mode);
        await drawSolution(x, y, z, radius, solutionjson, mode);
        const [effectScaleMin, effectScaleMax] = computeEffectScales(solutionjson['systems'].length);
        const playerPos = [x, y, z];
        const cameraPos = [x, y + (1.5 * radius), z - (1.5 * radius)];
        startEd3dMap(solutionjson, playerPos, cameraPos, effectScaleMin, effectScaleMax);
        autoSelectZeroDistanceSystem(solutionjson);
      } catch (error) {
        console.error(error);
      }
    }

     function convertEdUrl(u) {
       const src = new URL(u);
       const dst = new URL("/neighbors", sameHostBaseUrl);

       // Copy all parameters from src
       src.searchParams.forEach((value, key) => {
         if (key === "r") {
           dst.searchParams.set("radius", value);
         } else {
           dst.searchParams.set(key, value);
         }
       });

       return dst.toString();
     }

     (function() {
       const params = urlParams;
       const raw = {
         x: params.get('x'),
         y: params.get('y'),
         z: params.get('z'),
         radius: params.get('radius') ?? params.get('r'), // allow r alias
         mode: params.get('mode')
       };
       // Parse numbers safely
       const parsed = Object.fromEntries(Object.entries(raw).map(([k,v]) => [k, v === null ? null : Number(v)]));

       // Validation
       const problems = [];
       ['x','y','z','radius'].forEach(k => {
         if (raw[k] === null) problems.push(`Missing parameter: ${k}`);
         else if (!Number.isFinite(parsed[k])) problems.push(`Invalid number for ${k}: "${raw[k]}"`);
       });
       if (parsed.radius !== null && parsed.radius <= 0) problems.push('radius must be > 0');
       const edgisHref = document.getElementById('edgis');

       if (externalSolutionJson) {
         edgisHref.removeAttribute('href');
         renderManualSolution(externalSolutionJson);
         return;
       }

       edgisHref.href = convertEdUrl(window.location.href);
       drawSystems(parsed.x, parsed.y, parsed.z, parsed.radius, raw.mode);

       // Optional: If someone changes params manually via form encoded hash, live-update
       window.addEventListener('popstate', () => location.reload());
     })();

     $( document ).on( "systemClick", async function( event, name, infos, url ) {
       document.getElementById("InfoPanel").style.display = "block";
       let s = infos;

       if (!s && name) {
         s = manualSystemsLookup.get(name) || null;
       }

       if (!s) {
         console.warn('No system info available for selection:', name);
         return;
       }

       lastClickedSystemName = s.name ?? name ?? null;

       async function getSystemBodies(systemName) {
         try {
           const response = await fetch(
             `${sameHostBaseUrl}/edsm/bodies?systemName=${encodeURIComponent(systemName)}`,
             {
               method: 'GET',
               referrerPolicy: 'strict-origin-when-cross-origin', // avoids referrer warnings
               headers: {
                 'Accept': 'application/json'
               }
             }
           );

           if (!response.ok) {
             throw new Error(`HTTP error! status: ${response.status}`);
           }

           const data = await response.json();
           return data;

         } catch (error) {
           console.error("Error fetching data:", error);
           return null; // keeps behavior consistent
         }
       }

       const systemName = s.name ?? name;
       if (systemName) {
         systemData = await getSystemBodies(systemName) ?? { name: systemName, bodies: [] };
       } else {
         systemData = null;
       }

       function buildStarCard(star) {
         const card = document.createElement("article");
         card.className = "card";

         const coords = s.coords || manualSystemsLookup.get(systemName)?.coords || { x: 0, y: 0, z: 0 };
         const radius = typeof s.radius === 'number' ? s.radius : (coords?.radius ?? 0);
         const mainStar = s.mainStar ?? s?.infos?.mainStar ?? 'Unknown';
         const distanceValue = typeof s.distance === 'number'
           ? `${s.distance.toFixed(2)} LY`
           : (typeof s?.infos?.distance === 'number' ? `${s.infos.distance.toFixed(2)} LY` : 'Unknown');

         card.innerHTML = `
           <header>
             <h2>SYSTEM INFORMATION</h2>
             <ul>
               <li><span style="font-size: x-large;margin-top: -7px;">${systemName ?? 'Unknown'}</span> <span><a title="CENTER VIEW" href="/static/galaxymap.html?x=${coords.x ?? 0}&y=${coords.y ?? 0}&z=${coords.z ?? 0}&radius=${radius ?? 0}"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-border-center" viewBox="0 0 16 16">
                 <path d="M.969 0H0v.969h.5V1h.469V.969H1V.5H.969zm.937 1h.938V0h-.938zm1.875 0h.938V0H3.78v1zm1.875 0h.938V0h-.938zM7.531.969V1h.938V.969H8.5V.5h-.031V0H7.53v.5H7.5v.469zM9.406 1h.938V0h-.938zm1.875 0h.938V0h-.938zm1.875 0h.938V0h-.938zm1.875 0h.469V.969h.5V0h-.969v.5H15v.469h.031zM1 2.844v-.938H0v.938zm6.5-.938v.938h1v-.938zm7.5 0v.938h1v-.938zM1 4.719V3.78H0v.938h1zm6.5-.938v.938h1V3.78h-1zm7.5 0v.938h1V3.78h-1zM1 6.594v-.938H0v.938zm6.5-.938v.938h1v-.938zm7.5 0v.938h1v-.938zM0 8.5v-1h16v1zm0 .906v.938h1v-.938zm7.5 0v.938h1v-.938zm8.5.938v-.938h-1v.938zm-16 .937v.938h1v-.938zm7.5 0v.938h1v-.938zm8.5.938v-.938h-1v.938zm-16 .937v.938h1v-.938zm7.5 0v.938h1v-.938zm8.5.938v-.938h-1v.938zM0 16h.969v-.5H1v-.469H.969V15H.5v.031H0zm1.906 0h.938v-1h-.938zm1.875 0h.938v-1H3.78v1zm1.875 0h.938v-1h-.938zm1.875-.5v.5h.938v-.5H8.5v-.469h-.031V15H7.53v.031H7.5v.469zm1.875.5h.938v-1h-.938zm1.875 0h.938v-1h-.938zm1.875 0h.938v-1h-.938zm1.875-.5v.5H16v-.969h-.5V15h-.469v.031H15v.469z"/>
               </svg></a></span></li>
             </ul>
             <h2>ASTRONOMICAL INFORMATION</h2>
             <ul>
               <li><span class="label">STAR TYPE: </span>${star?.spectralClass ?? 'Unknown'} ${star?.luminosity ?? ''}</li>
               <li><span class="label">DISTANCE: </span>${distanceValue}<br/></li>
               <li><span class="label">STAR CLASS: </span>${mainStar}</li>
               <div>
                 <span class="pill">Scoopable: <b>${star?.isScoopable ?? 'Unknown'}</b></span>
               </div>
           </header>

               <section>
                 <h2>DISCOVERY</h2>
                 <ul>
                   <li><span class="label">COMMANDER:</span> ${star?.discovery.commander ?? "N/A"}</li>
                   <li><span class="label">DATE:</span> ${star?.discovery.date ?? "N/A"}</li>
                 </ul>
               </section>

               <section>
                 <h2>PHYSICAL PROPERTIES</h2>
                 <ul>
                   <li><span class="label">AGE:</span> ${star?.age ?? "Unknown"} Myr</li>
                   <li><span class="label">ABSOLUTE MAGNITUDE:</span> ${star?.absoluteMagnitude?.toFixed(5) ?? "Unknown"}</li>
                   <li><span class="label">SOLAR MASSES:</span> ${star?.solarMasses?.toFixed(5) ?? "Unknown"}</li>
                   <li><span class="label">SOLAR RADIUS:</span> ${star?.solarRadius?.toFixed(5) ?? "Unknown"}</li>
                   <li><span class="label">SURFACE TEMPERATURE:</span> ${star?.surfaceTemperature ?? "Unknown"} K</li>
                   <li><span class="label">ROTATIONAL PERIOD:</span> ${star?.rotationalPeriod?.toFixed(5) ?? "Unknown"} d</li>
                   <li><span class="label">AXIAL TILT:</span> ${star?.axialTilt?.toFixed(2) ?? "N/A"}°</li>
                 </ul>
               </section>
         `;

         return card;
       }
       document.getElementById("InfoPanel").innerHTML = "";
       // fetch main star data
       const body = systemData?.bodies?.find(b => b.isMainStar === true) ?? null;
       document.getElementById("InfoPanel").appendChild(buildStarCard(body));
     });

      const systemInfoButton = document.querySelector('button[title="System Info"]');
      const systemMapButton = document.querySelector('button[title="System Map"]');
      const infoPanel = document.getElementById("InfoPanel");

      systemInfoButton.addEventListener('click', () => {
      if (infoPanel.style.display === "none" || infoPanel.style.display === "") {
      infoPanel.style.display = "block";
      } else {
      infoPanel.style.display = "none";
      }
      });

      systemMapButton.addEventListener('click', () => {
      const systemName = systemData?.name ?? lastClickedSystemName;
      if (!systemName) {
        console.warn('No system selected. Click a system first.');
        return;
      }
      const url = `${sameHostBaseUrl}/static/sysmap.html?system=${encodeURIComponent(systemName)}`;
      window.open(url, "_blank");
      });
