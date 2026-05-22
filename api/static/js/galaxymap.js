     const urlParams = new URLSearchParams(window.location.search);
     const sameHostBaseUrl =
       window.location?.origin || `${window.location?.protocol}//${window.location?.host || ''}`;
     const CAMERA_REFRESH_DEBOUNCE_MS = 700;
     const CAMERA_DRAG_REFRESH_INTERVAL_MS = 220;
     const CAMERA_CENTER_CHANGE_EPSILON = 0.05;
     const LIVE_REFRESH_DISTANCE_RATIO = 0.25;
     const NEIGHBORHOOD_CACHE_TTL_MS = 120000;
     const NEIGHBORHOOD_CACHE_MAX_ENTRIES = 16;
     let manualSystemsLookup = new Map();
     let systemData = null;
     let lastClickedSystemName = null;
     let cameraRefreshTimer = null;
     let lastAutoLoadCenter = null;
     let lastCameraTarget = null;
     let controlsEndListenerAttached = false;
     let autoRefreshRequestId = 0;
     let autoRefreshInFlight = false;
     let systemInfoRequestId = 0;
     let lastSelectedSystemInfo = null;
     let suppressCameraRefreshUntil = 0;
     let currentSolidSystemNames = [];
     let activeNeighborhoodRadius = 20;
     let neighborhoodPrefetchCache = new Map();
     let neighborhoodFetchPromises = new Map();

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

    function parsePositiveNumber(value, fallbackValue) {
      const parsed = Number(value);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : fallbackValue;
    }

    function formatCoord(value) {
      return Number(value).toFixed(2);
    }

    function formatRadiusValue(value) {
      const numericValue = Number(value);
      if (!Number.isFinite(numericValue) || numericValue <= 0) {
        return '20';
      }
      if (Number.isInteger(numericValue)) {
        return String(numericValue);
      }
      return numericValue.toFixed(2).replace(/\.?0+$/, '');
    }

    function getCurrentRadiusParamValue() {
      return formatRadiusValue(activeNeighborhoodRadius || getRequestedNeighborhoodRadius());
    }

    function buildNeighborsInputValue(center) {
      if (!center) {
        return null;
      }
      return [
        formatCoord(center.x),
        formatCoord(center.y),
        formatCoord(center.z),
        getCurrentRadiusParamValue()
      ].join(',');
    }

    function buildEdgisHomeUrl(center) {
      const url = new URL('/static/index.html', sameHostBaseUrl);
      const neighborsValue = buildNeighborsInputValue(center);
      if (neighborsValue) {
        url.searchParams.set('neighbors', neighborsValue);
      }
      return url.toString();
    }

    function buildNeighborsJsonUrl(center) {
      const url = new URL('/neighbors', sameHostBaseUrl);
      if (!center) {
        return url.toString();
      }
      url.searchParams.set('x', formatCoord(center.x));
      url.searchParams.set('y', formatCoord(center.y));
      url.searchParams.set('z', formatCoord(center.z));
      url.searchParams.set('radius', getCurrentRadiusParamValue());
      return url.toString();
    }

    function updateEdgisLinks(center) {
      const homeButton = document.getElementById('openEdgisButton');
      const jsonLink = document.getElementById('edgis');
      const homeHref = buildEdgisHomeUrl(center);

      if (homeButton) {
        homeButton.dataset.href = homeHref;
        homeButton.setAttribute('title', 'Open in EDGIS');
        homeButton.setAttribute('aria-label', 'Open in EDGIS');
      }

      if (jsonLink) {
        jsonLink.href = buildNeighborsJsonUrl(center);
      }
    }

    function setEdgisHomeLoadingState(isLoading) {
      const homeButton = document.getElementById('openEdgisButton');
      if (!homeButton) {
        return;
      }
      homeButton.classList.toggle('is-loading', Boolean(isLoading));
      homeButton.setAttribute('aria-busy', isLoading ? 'true' : 'false');
      homeButton.title = isLoading
        ? 'Refreshing nearby systems... Open in EDGIS'
        : 'Open in EDGIS';
      homeButton.setAttribute('aria-label', homeButton.title);
    }

    function getRequestedNeighborhoodRadius() {
      const liveParams = new URLSearchParams(window.location.search);
      const rawRadius = liveParams.get('radius') ?? liveParams.get('r');
      return parsePositiveNumber(rawRadius, activeNeighborhoodRadius);
    }

    function getAutoRefreshRadius() {
      return getRequestedNeighborhoodRadius();
    }

    function updateBrowserUrlFromCurrentCenter(center) {
      if (!center || externalSolutionJson) {
        return;
      }

      const nextUrl = new URL(window.location.href);
      nextUrl.searchParams.set('x', Number(center.x).toFixed(2));
      nextUrl.searchParams.set('y', Number(center.y).toFixed(2));
      nextUrl.searchParams.set('z', Number(center.z).toFixed(2));
      nextUrl.searchParams.set('radius', getCurrentRadiusParamValue());
      window.history.replaceState({}, '', nextUrl);
      updateEdgisLinks(center);
    }

    function suppressCameraRefresh(durationMs = 1500) {
      suppressCameraRefreshUntil = Math.max(suppressCameraRefreshUntil, Date.now() + durationMs);
    }

    function isCameraRefreshSuppressed() {
      return Date.now() < suppressCameraRefreshUntil;
    }

    window.EDGIS_SUPPRESS_CAMERA_REFRESH = suppressCameraRefresh;

     function initSolutionJson(x, y, z, mode = "simple") {
       if (mode === "expert") {
         return {
           categories: {
             EDGIS: {
               "Target": {
                 name: `Target (${formatCoord(x)}, ${formatCoord(y)}, ${formatCoord(z)})`,
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
                 name: `Target (${formatCoord(x)}, ${formatCoord(y)}, ${formatCoord(z)})`,
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

     async function drawSolution(x, y, z, radius, res, mode, focusTarget = true, targetCenter = null) {
      const spherejson = await fetchNeighborsDataset(x, y, z, radius, "initial");
      if (spherejson && spherejson.length > 0) {
        populateResult(spherejson, res, radius, mode, focusTarget, targetCenter);
      }
     }


     function populateResult(spherejson, res, radius, mode = "simple", focusTarget = true, targetCenter = null) {
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

       if (!focusTarget && targetCenter) {
         res.systems.push({
           name: `Target (${formatCoord(targetCenter.x)}, ${formatCoord(targetCenter.y)}, ${formatCoord(targetCenter.z)})`,
           coords: {
             x: Number(targetCenter.x),
             y: Number(targetCenter.y),
             z: Number(targetCenter.z),
             radius: radius
           },
           hidePoint: true,
           cat: ["Target"],
           infos: {
             name: 'Target',
             distance: 0,
             mainStar: 'Target',
             radius
           }
         });
       }

       res.systems.push(
         ...spherejson.map((s, i) => {
           let mainStar = s.mainstar;
           if (focusTarget && i === 0) {
             return {
               name: `${s.name} (${s.distance.toFixed(2)} LY)`,
               coords: { ...s.coords, radius: radius },
               hidePoint: true,
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

    function getCurrentMapCenter() {
      if (typeof controls === 'undefined' || !controls?.target) {
        return null;
      }
      return {
        x: Number(controls.target.x),
        y: Number(controls.target.y),
        z: Number(-controls.target.z)
      };
    }

    function getCurrentWorldCamera() {
      if (typeof camera === 'undefined' || !camera) {
        return null;
      }
      return {
        x: Number(camera.position.x),
        y: Number(camera.position.y),
        z: Number(camera.position.z)
      };
    }

    function getCurrentInternalTarget() {
      if (typeof controls === 'undefined' || !controls?.target) {
        return null;
      }
      return {
        x: Number(controls.target.x),
        y: Number(controls.target.y),
        z: Number(controls.target.z)
      };
    }

    function distanceBetweenCenters(a, b) {
      if (!a || !b) {
        return Number.POSITIVE_INFINITY;
      }
      const dx = a.x - b.x;
      const dy = a.y - b.y;
      const dz = a.z - b.z;
      return Math.sqrt(dx * dx + dy * dy + dz * dz);
    }

    function hasMovedFarEnoughForLiveRefresh(center, radius) {
      if (!center) {
        return false;
      }
      if (!lastAutoLoadCenter) {
        return true;
      }
      const effectiveRadius = parsePositiveNumber(radius, getAutoRefreshRadius());
      const threshold = Math.max(CAMERA_CENTER_CHANGE_EPSILON, effectiveRadius * LIVE_REFRESH_DISTANCE_RATIO);
      return distanceBetweenCenters(center, lastAutoLoadCenter) >= threshold;
    }

    function roundCacheCoord(value) {
      return Number(value).toFixed(2);
    }

    function buildNeighborhoodCacheKey(center, radius) {
      return [
        roundCacheCoord(center.x),
        roundCacheCoord(center.y),
        roundCacheCoord(center.z),
        formatRadiusValue(radius)
      ].join(':');
    }

    function pruneNeighborhoodPrefetchCache() {
      const now = Date.now();
      for (const [key, entry] of neighborhoodPrefetchCache.entries()) {
        if (!entry || (now - entry.timestamp) > NEIGHBORHOOD_CACHE_TTL_MS) {
          neighborhoodPrefetchCache.delete(key);
        }
      }

      while (neighborhoodPrefetchCache.size > NEIGHBORHOOD_CACHE_MAX_ENTRIES) {
        const oldestKey = neighborhoodPrefetchCache.keys().next().value;
        if (!oldestKey) {
          break;
        }
        neighborhoodPrefetchCache.delete(oldestKey);
      }
    }

    function cacheNeighborhoodDataset(center, radius, dataset, source = 'fetch') {
      if (!center || !Array.isArray(dataset) || !dataset.length) {
        return;
      }

      const key = buildNeighborhoodCacheKey(center, radius);
      neighborhoodPrefetchCache.delete(key);
      neighborhoodPrefetchCache.set(key, {
        center: { x: Number(center.x), y: Number(center.y), z: Number(center.z) },
        radius: Number(radius),
        dataset,
        source,
        timestamp: Date.now()
      });
      pruneNeighborhoodPrefetchCache();
    }

    async function fetchAndCacheNeighborhood(center, radius, source = 'fetch') {
      const key = buildNeighborhoodCacheKey(center, radius);
      const exactCached = neighborhoodPrefetchCache.get(key);
      if (exactCached && Array.isArray(exactCached.dataset) && exactCached.dataset.length) {
        exactCached.timestamp = Date.now();
        return exactCached.dataset;
      }

      const existingPromise = neighborhoodFetchPromises.get(key);
      if (existingPromise) {
        return await existingPromise;
      }

      const fetchPromise = (async () => {
        const sphereurl = `${sameHostBaseUrl}/neighbors?x=${center.x}&y=${center.y}&z=${center.z}&radius=${radius}`;
        const dataset = await getSystemCoordinates(sphereurl);
        cacheNeighborhoodDataset(center, radius, dataset, source);
        return dataset;
      })();

      neighborhoodFetchPromises.set(key, fetchPromise);

      try {
        return await fetchPromise;
      } finally {
        neighborhoodFetchPromises.delete(key);
      }
    }

    function refreshHudFilterCounts() {
      if (!window.$) {
        return;
      }
      $('.map_filter').each(function () {
        const idCat = $(this).data('filter');
        const label = $(this).data('label') || $(this).text().replace(/\s*\(\d+\)\s*$/, '').trim();
        const count = Array.isArray(Ed3d.catObjs[idCat]) ? Ed3d.catObjs[idCat].length : 0;
        const checkHtml = $(this).find('.check').prop('outerHTML') || '';
        const countHtml = count > 1 ? ` (${count})` : '';
        $(this).html(`${checkHtml}${label}${countHtml}`);
      });
    }

    function showInfoPanel() {
      const infoPanel = document.getElementById("InfoPanel");
      if (infoPanel) {
        infoPanel.style.display = "block";
      }
    }

    function renderInfoPanelLoading(selectedInfo) {
      const infoPanel = document.getElementById("InfoPanel");
      if (!infoPanel || !selectedInfo) {
        return;
      }

      const systemName = selectedInfo.name ?? 'Unknown';
      const coords = selectedInfo.coords || manualSystemsLookup.get(systemName)?.coords || { x: 0, y: 0, z: 0 };
      const radius = typeof selectedInfo.radius === 'number'
        ? selectedInfo.radius
        : (coords?.radius ?? 0);
      const mainStar = selectedInfo.mainStar ?? selectedInfo?.infos?.mainStar ?? 'Unknown';
      const distanceValue = typeof selectedInfo.distance === 'number'
        ? `${selectedInfo.distance.toFixed(2)} LY`
        : (typeof selectedInfo?.infos?.distance === 'number' ? `${selectedInfo.infos.distance.toFixed(2)} LY` : 'Unknown');

      infoPanel.innerHTML = `
        <article class="card">
          <header>
            <h2>SYSTEM INFORMATION</h2>
            <ul>
              <li><span style="font-size: x-large;margin-top: -7px;">${systemName}</span> <span><a title="CENTER VIEW" href="/static/galaxymap.html?x=${coords.x ?? 0}&y=${coords.y ?? 0}&z=${coords.z ?? 0}&radius=${radius ?? 0}"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-border-center" viewBox="0 0 16 16">
                <path d="M.969 0H0v.969h.5V1h.469V.969H1V.5H.969zm.937 1h.938V0h-.938zm1.875 0h.938V0H3.78v1zm1.875 0h.938V0h-.938zM7.531.969V1h.938V.969H8.5V.5h-.031V0H7.53v.5H7.5v.469zM9.406 1h.938V0h-.938zm1.875 0h.938V0h-.938zm1.875 0h.938V0h-.938zm1.875 0h.469V.969h.5V0h-.969v.5H15v.469h.031zM1 2.844v-.938H0v.938zm6.5-.938v.938h1v-.938zm7.5 0v.938h1v-.938zM1 4.719V3.78H0v.938h1zm6.5-.938v.938h1V3.78h-1zm7.5 0v.938h1V3.78h-1zM1 6.594v-.938H0v.938zm6.5-.938v.938h1v-.938zm7.5 0v.938h1v-.938zM0 8.5v-1h16v1zm0 .906v.938h1v-.938zm7.5 0v.938h1v-.938zm8.5.938v-.938h-1v.938zm-16 .937v.938h1v-.938zm7.5 0v.938h1v-.938zm8.5.938v-.938h-1v.938zm-16 .937v.938h1v-.938zm7.5 0v.938h1v-.938zm8.5.938v-.938h-1v.938zM0 16h.969v-.5H1v-.469H.969V15H.5v.031H0zm1.906 0h.938v-1h-.938zm1.875 0h.938v-1H3.78v1zm1.875 0h.938v-1h-.938zm1.875-.5v.5h.938v-.5H8.5v-.469h-.031V15H7.53v.031H7.5v.469zm1.875.5h.938v-1h-.938zm1.875 0h.938v-1h-.938zm1.875 0h.938v-1h-.938zm1.875-.5v.5H16v-.969h-.5V15h-.469v.031H15v.469z"/>
              </svg></a></span></li>
            </ul>
            <h2>ASTRONOMICAL INFORMATION</h2>
            <ul>
              <li><span class="label">DISTANCE: </span>${distanceValue}</li>
              <li><span class="label">STAR CLASS: </span>${mainStar}</li>
            </ul>
          </header>
          <section>
            <h2>LOADING</h2>
            <ul>
              <li>Fetching bodies and main star details...</li>
            </ul>
          </section>
        </article>
      `;
    }

    function collectInactiveFilterIds() {
      if (!window.$) {
        return [];
      }
      const inactive = [];
      $('.map_filter').each(function () {
        if (Number($(this).data('active')) !== 1) {
          inactive.push(String($(this).data('filter')));
        }
      });
      return inactive;
    }

    function restoreInactiveFilterIds(filterIds) {
      if (!window.$ || !Array.isArray(filterIds)) {
        return;
      }
      filterIds.forEach((filterId) => {
        const filterEl = $(`.map_filter[data-filter="${filterId}"]`);
        if (filterEl.length && Number(filterEl.data('active')) === 1) {
          filterEl.trigger('click');
        }
      });
    }

    function isCategoryVisible(categoryId) {
      if (!window.$ || !categoryId) {
        return true;
      }
      const filterEl = $(`.map_filter[data-filter="${categoryId}"]`);
      if (!filterEl.length) {
        return true;
      }
      return Number(filterEl.data('active')) === 1;
    }

    async function fetchNeighborsDataset(x, y, z, radius, source = 'fetch') {
      return await fetchAndCacheNeighborhood({ x, y, z }, radius, source);
    }

    function updateTrackedSolidSystemNames(solutionjson) {
      currentSolidSystemNames = Array.isArray(solutionjson?.systems)
        ? solutionjson.systems
            .filter((systemObj) => Number(systemObj?.coords?.radius) !== 0)
            .map((systemObj) => systemObj.name)
        : [];
    }

    function removeTrackedSolidSystems() {
      currentSolidSystemNames.forEach((name) => {
        const obj = scene?.getObjectByName?.(name);
        if (obj) {
          scene.remove(obj);
        }
      });
      currentSolidSystemNames = [];
    }

    function resetDynamicMapState() {
      if (typeof Action !== 'undefined') {
        Action.disableSelection();
      }
      removeTrackedSolidSystems();
      System.remove();
      System.particleInfos = [];
      HUD.removeFilters();
      Route.remove();
      Route.systems = [];
      Route.active = false;
      routes = [];
      Ed3d.catObjs = [];
      Ed3d.catObjsRoutes = [];
      Ed3d.systems = [];
    }

    function restoreView(viewState) {
      if (!viewState?.camera || !viewState?.target || typeof controls === 'undefined' || !controls || typeof camera === 'undefined' || !camera) {
        return;
      }
      camera.position.set(viewState.camera.x, viewState.camera.y, viewState.camera.z);
      controls.target.set(viewState.target.x, viewState.target.y, viewState.target.z);
      controls.update();
    }

    function reloadDynamicNeighborhood(solutionjson, viewState, inactiveFilterIds) {
      resetDynamicMapState();
      Ed3d.loadDatas(solutionjson);
      System.endParticleSystem();
      HUD.init();
      updateTrackedSolidSystemNames(solutionjson);
      restoreInactiveFilterIds(inactiveFilterIds);
      refreshHudFilterCounts();
      restoreView(viewState);
    }

    async function reloadSystemsAroundCurrentCamera(center, options = {}) {
      if (!center || externalSolutionJson) {
        setEdgisHomeLoadingState(false);
        return;
      }
      if (isCameraRefreshSuppressed()) {
        setEdgisHomeLoadingState(false);
        return;
      }
      if (autoRefreshInFlight) {
        scheduleCameraNeighborhoodRefresh(options);
        return;
      }
      autoRefreshInFlight = true;
      setEdgisHomeLoadingState(true);
      const requestId = ++autoRefreshRequestId;
      try {
        const autoRefreshRadius = getAutoRefreshRadius();
        const spherejson = await fetchNeighborsDataset(center.x, center.y, center.z, autoRefreshRadius, 'reload');
        if (requestId !== autoRefreshRequestId || !Array.isArray(spherejson) || !spherejson.length) {
          return;
        }
        const viewState = {
          camera: getCurrentWorldCamera(),
          target: getCurrentInternalTarget()
        };
        const inactiveFilterIds = collectInactiveFilterIds();
        const nextResult = initSolutionJson(center.x, center.y, center.z, "simple");
        populateResult(spherejson, nextResult, autoRefreshRadius, "simple", false, center);
        reloadDynamicNeighborhood(nextResult, viewState, inactiveFilterIds);
        cacheNeighborhoodDataset(center, autoRefreshRadius, spherejson, 'reload');
        lastAutoLoadCenter = center;
        activeNeighborhoodRadius = autoRefreshRadius;
        updateBrowserUrlFromCurrentCenter(center);
      } catch (error) {
        console.error('Failed to reload nearby systems', error);
      } finally {
        autoRefreshInFlight = false;
        setEdgisHomeLoadingState(false);
      }
    }

    function scheduleCameraNeighborhoodRefresh(options = {}) {
      if (externalSolutionJson) {
        return;
      }
      if (isCameraRefreshSuppressed()) {
        return;
      }
      const center = options.center || getCurrentMapCenter();
      if (!center) {
        return;
      }
      const resetExistingTimer = options.resetExistingTimer !== false;
      if (cameraRefreshTimer && !resetExistingTimer) {
        return;
      }
      if (cameraRefreshTimer) {
        clearTimeout(cameraRefreshTimer);
      }
      const debounceMs = Number.isFinite(options.debounceMs) ? options.debounceMs : CAMERA_REFRESH_DEBOUNCE_MS;
      setEdgisHomeLoadingState(true);
      cameraRefreshTimer = setTimeout(() => {
        cameraRefreshTimer = null;
        reloadSystemsAroundCurrentCamera(center, options);
      }, debounceMs);
    }

    function attachCameraNeighborhoodRefresh() {
      if (controlsEndListenerAttached || typeof controls === 'undefined' || !controls?.addEventListener) {
        return;
      }
      controlsEndListenerAttached = true;
      const initialCenter = getCurrentMapCenter();
      if (initialCenter) {
        lastAutoLoadCenter = initialCenter;
        lastCameraTarget = initialCenter;
      }
      controls.addEventListener('change', () => {
        const center = getCurrentMapCenter();
        if (!center || isCameraRefreshSuppressed()) {
          return;
        }
        if (!hasMovedFarEnoughForLiveRefresh(center, getAutoRefreshRadius())) {
          return;
        }
        const centerMoved = !lastCameraTarget || distanceBetweenCenters(center, lastCameraTarget) > CAMERA_CENTER_CHANGE_EPSILON;
        if (!centerMoved) {
          return;
        }
        scheduleCameraNeighborhoodRefresh({
          center,
          debounceMs: CAMERA_DRAG_REFRESH_INTERVAL_MS,
          resetExistingTimer: false
        });
      });
      controls.addEventListener('end', () => {
        const center = getCurrentMapCenter();
        if (!center) {
          return;
        }
        updateBrowserUrlFromCurrentCenter(center);
        if (isCameraRefreshSuppressed()) {
          lastCameraTarget = center;
          return;
        }
        const centerMoved = !lastCameraTarget || distanceBetweenCenters(center, lastCameraTarget) > CAMERA_CENTER_CHANGE_EPSILON;
        lastCameraTarget = center;
        if (centerMoved) {
          scheduleCameraNeighborhoodRefresh({
            center
          });
        }
      });
    }

    function clamp(value, minValue, maxValue) {
      return Math.min(Math.max(value, minValue), maxValue);
    }

    function computeDensityProfile(systemCount, radius) {
      const count = Math.max(systemCount || 0, 1);
      const effectiveRadius = parsePositiveNumber(radius, activeNeighborhoodRadius || 20);
      const volume = (4 / 3) * Math.PI * Math.pow(effectiveRadius, 3);
      const meanSpacing = Math.cbrt(volume / count);
      const densityScale = clamp((meanSpacing - 3) / 10, 0.55, 1.3);
      const crowdingPenalty = clamp(6 / Math.max(meanSpacing, 0.1), 0.85, 3.8);
      const normalizedRadius = clamp((effectiveRadius - 20) / 130, 0, 1);
      const radiusCurve = normalizedRadius * normalizedRadius * (3 - (2 * normalizedRadius));
      const radiusScaleFactor = 0.72 + (0.33 * radiusCurve);
      const smallRadiusBlend = clamp((80 - effectiveRadius) / 60, 0, 1);
      const smallRadiusPenalty = 1 - (0.5 * smallRadiusBlend * smallRadiusBlend);
      const particleScaleFactor = clamp((densityScale * radiusScaleFactor * smallRadiusPenalty) / crowdingPenalty, 0.08, 1.05);
      const glowVisibilityFactor = clamp((meanSpacing - 1.5) / 5.5, 0.22, 1);
      const pointCountGlowPenalty = count <= 2500
        ? 1
        : clamp(1 - ((Math.log10(count) - Math.log10(2500)) * 0.32), 0.45, 1);
      const largeRadiusBlend = clamp((effectiveRadius - 80) / 120, 0, 1);
      const largeRadiusBoost = 1 + (1.9 * largeRadiusBlend * largeRadiusBlend);
      const compactRegionBlend = clamp((25 - effectiveRadius) / 10, 0, 1);
      const ultraDenseBlend = clamp((4.5 - meanSpacing) / 3, 0, 1);
      const compactDenseGlowBoost = 1 + (12 * compactRegionBlend * ultraDenseBlend);

      return {
        effectScaleMin: clamp(0.45 * particleScaleFactor * largeRadiusBoost, 0.3, 2.4),
        effectScaleMax: clamp(((7 * particleScaleFactor) + 1.4) * largeRadiusBoost, 2.2, 24),
        particleScaleFactor,
        particleOpacity: clamp(
          (((((0.16 + (densityScale * 0.08) + (radiusScaleFactor * 0.05)) / Math.pow(crowdingPenalty, 0.18)) * glowVisibilityFactor) * pointCountGlowPenalty) * compactDenseGlowBoost) * 4,
          0.18,
          1
        )
      };
    }

    function startEd3dMap(solutionjson, playerPos, cameraPos, densityProfile) {
      const hudpanel = true;
      if (window.System && densityProfile?.particleOpacity) {
        window.System.opacity = densityProfile.particleOpacity;
      }
      Ed3d.systemSizeScaleFactor = densityProfile?.particleScaleFactor || 1;
      Ed3d.init({
        container   : 'edmap',
        json : solutionjson,
        basePath: "/static/ed3d/",
        withHudPanel : hudpanel,
        recenterOnFilterToggle : false,
        startAnim : false,
        hudMultipleSelect : true,
        withOptionsPanel: false,
        withFullscreenToggle: false,
        showGalaxyInfos: true,
        showNameNear: false,
        playerPos: playerPos,
        cameraPos: cameraPos,
        effectScaleSystem : [densityProfile.effectScaleMin, densityProfile.effectScaleMax],
        finished: function () {
          attachCameraNeighborhoodRefresh();
          refreshHudFilterCounts();
        }
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

    function autoSelectNearestVisibleSystem(solutionjson) {
      if (!solutionjson || !Array.isArray(solutionjson.systems)) {
        return;
      }

      const systemsWithDistance = solutionjson.systems.filter((system) => {
        if (typeof system?.infos?.distance !== 'number') {
          return false;
        }
        if (system?.hidePoint === true) {
          return false;
        }
        if (Array.isArray(system?.cat) && system.cat.indexOf('Target') !== -1) {
          return false;
        }
        return true;
      });
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
      const densityProfile = computeDensityProfile(systems.length, activeNeighborhoodRadius);
      startEd3dMap(manualSolutionJson, playerPos, cameraPos, densityProfile);
      updateTrackedSolidSystemNames(manualSolutionJson);
      autoSelectNearestVisibleSystem(manualSolutionJson);
    }

    async function drawSystems(x, y, z, radius, mode) {
      setEdgisHomeLoadingState(true);
      try {
       activeNeighborhoodRadius = radius;
       const solutionjson = initSolutionJson(x, y, z, mode);
       await drawSolution(x, y, z, radius, solutionjson, mode, false, { x, y, z });
       lastAutoLoadCenter = { x, y, z };
       lastCameraTarget = { x, y, z };
       updateEdgisLinks({ x, y, z });
       const densityProfile = computeDensityProfile(solutionjson['systems'].length, radius);
       const playerPos = [x, y, z];
       const cameraPos = [x, y + (1.5 * radius), z - (1.5 * radius)];
       startEd3dMap(solutionjson, playerPos, cameraPos, densityProfile);
        updateTrackedSolidSystemNames(solutionjson);
        autoSelectNearestVisibleSystem(solutionjson);
      } catch (error) {
        console.error(error);
      } finally {
        setEdgisHomeLoadingState(false);
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
         setEdgisHomeLoadingState(false);
         renderManualSolution(externalSolutionJson);
         return;
       }

       edgisHref.href = convertEdUrl(window.location.href);
       updateEdgisLinks({
         x: parsed.x,
         y: parsed.y,
         z: parsed.z
       });
       drawSystems(parsed.x, parsed.y, parsed.z, parsed.radius, raw.mode);

       // Optional: If someone changes params manually via form encoded hash, live-update
       window.addEventListener('popstate', () => location.reload());
     })();

     $( document ).on( "systemClick", async function( event, name, infos, url ) {
       const requestId = ++systemInfoRequestId;
       let s = infos;
       const infoPanel = document.getElementById("InfoPanel");

       if (!s && name) {
         s = manualSystemsLookup.get(name) || null;
       }

       if (!s) {
         console.warn('No system info available for selection:', name);
         return;
       }

       lastSelectedSystemInfo = s;
       lastClickedSystemName = s.name ?? name ?? null;
       showInfoPanel();
       if (infoPanel) {
         renderInfoPanelLoading(s);
       }

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
         const nextSystemData = await getSystemBodies(systemName) ?? { name: systemName, bodies: [] };
         if (requestId !== systemInfoRequestId) {
           return;
         }
         systemData = nextSystemData;
       } else {
         if (requestId !== systemInfoRequestId) {
           return;
         }
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
       if (requestId !== systemInfoRequestId) {
         return;
       }
       if (!infoPanel) {
         return;
       }
       infoPanel.innerHTML = "";
       // fetch main star data
       const body = systemData?.bodies?.find(b => b.isMainStar === true) ?? null;
       infoPanel.appendChild(buildStarCard(body));
     });

      const systemInfoButton = document.querySelector('button[title="System Info"]');
      const systemMapButton = document.querySelector('button[title="System Map"]');
      const openEdgisButton = document.getElementById('openEdgisButton');
      const infoPanel = document.getElementById("InfoPanel");

      systemInfoButton.addEventListener('click', () => {
      if (infoPanel.style.display === "none" || infoPanel.style.display === "") {
        showInfoPanel();
        if (!infoPanel.innerHTML && lastSelectedSystemInfo) {
          renderInfoPanelLoading(lastSelectedSystemInfo);
        }
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

      if (openEdgisButton) {
        openEdgisButton.addEventListener('click', () => {
          const targetUrl = openEdgisButton.dataset.href || buildEdgisHomeUrl(getCurrentMapCenter());
          window.location.href = targetUrl;
        });
      }
