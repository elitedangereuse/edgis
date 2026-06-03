     const urlParams = new URLSearchParams(window.location.search);
     const sameHostBaseUrl =
       window.location?.origin || `${window.location?.protocol}//${window.location?.host || ''}`;
     const CAMERA_REFRESH_DEBOUNCE_MS = 700;
     const CAMERA_DRAG_REFRESH_INTERVAL_MS = 220;
     const CAMERA_CENTER_CHANGE_EPSILON = 0.05;
     const LIVE_REFRESH_DISTANCE_RATIO = 0.25;
     const NEIGHBORHOOD_CACHE_TTL_MS = 120000;
     const NEIGHBORHOOD_CACHE_MAX_ENTRIES = 16;
     const STAR_SETTINGS_STORAGE_KEY = 'edgis_galaxymap_star_settings_v1';
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
     let activeFilterMode = 'simple';
     let activeFilterDimension = 'spectral';
     let activeNeighborhoodFilters = {
       atmosphereGas: '',
       material: ''
     };
     let pendingSharedFilterIds = null;
     let neighborhoodPrefetchCache = new Map();
     let neighborhoodFetchPromises = new Map();
     let userStarSizeScale = 1;
     let userStarBrightnessScale = 1;
     let userStarOpacityScale = 1;
     let baseParticleOpacity = 0.76;
     let baseParticleScaleFactor = 1;
     let baseEffectScaleMin = 1;
     let baseEffectScaleMax = 24;
     let isHudPanelVisible = true;
     let hudStatusObserver = null;
     let suppressFacetColorSync = false;
     let hudFacetSearchTerm = '';
     let hudFacetSortMode = 'count_desc';
     let distanceHeatEnabled = false;
     let boxelOverlayEnabled = false;
     let predictedSystemsEnabled = false;
     let experimentalFeaturesEnabled = false;
     let regionNamesEnabled = true;
     let reverseDebugGridDetailOrder = false;
     const boxelOverlayMode = 'adaptive';
     const boxelOverlayMinMcode = 'b';
     const boxelOverlayMaxMcode = 'g';
     const boxelOverlayMaxRequestLimit = 30000;
     const boxelOverlayShowLabels = true;
     let dynamicBaseGridEnabled = true;
     let boxelOverlayGroup = null;
     let boxelOverlayCells = [];
     let boxelLabelGroup = null;
     let boxelLabelRefreshTimer = null;
     let boxelOverlayRequestId = 0;
     let dynamicBaseGridGroup = null;
     let dynamicBaseGridRefreshTimer = null;
     let cachedVisibleStarCount = null;
     let cachedVisibleStarCountUpdatedAt = 0;

     function parseEmbedVariant(rawValue) {
       const normalized = String(rawValue || '').trim().toLowerCase();
       if (
         normalized === 'light'
         || normalized === 'lite'
         || normalized === 'minimal'
       ) {
         return 'light';
       }
       if (
         normalized === '1'
         || normalized === 'true'
         || normalized === 'yes'
         || normalized === 'on'
         || normalized === 'embed'
         || normalized === 'full'
       ) {
         return 'default';
       }
       return null;
     }

     const embedVariant = parseEmbedVariant(
       urlParams.get('embed') ?? urlParams.get('embedded')
     );
     const isEmbeddedMode = embedVariant !== null;
     document.documentElement.dataset.embedMode = isEmbeddedMode ? 'true' : 'false';
     document.documentElement.dataset.embedVariant = embedVariant || 'none';

     function loadStoredStarSettings() {
       try {
         const raw = window.localStorage?.getItem(STAR_SETTINGS_STORAGE_KEY);
         if (!raw) {
           return;
         }
         const parsed = JSON.parse(raw);
         userStarSizeScale = clamp(Number(parsed?.size), 0.01, 10) || 1;
         userStarBrightnessScale = clamp(Number(parsed?.brightness), 0.01, 1) || 1;
         userStarOpacityScale = clamp(Number(parsed?.opacity), 0.01, 1) || 1;
       } catch (error) {
         console.warn('Failed to load stored star settings', error);
       }
     }

     function saveStoredStarSettings() {
       try {
         const payload = {
           size: userStarSizeScale,
           brightness: userStarBrightnessScale,
           opacity: userStarOpacityScale
         };
         window.localStorage?.setItem(STAR_SETTINGS_STORAGE_KEY, JSON.stringify(payload));
       } catch (error) {
         console.warn('Failed to save star settings', error);
       }
     }

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
     loadStoredStarSettings();

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

    function buildGalaxyMapViewUrl(coords, radius) {
      const url = new URL('/static/galaxymap.html', sameHostBaseUrl);
      url.searchParams.set('x', coords?.x ?? 0);
      url.searchParams.set('y', coords?.y ?? 0);
      url.searchParams.set('z', coords?.z ?? 0);
      url.searchParams.set('radius', radius ?? 0);
      if (isEmbeddedMode) {
        url.searchParams.set('embed', embedVariant === 'light' ? 'light' : '1');
      }
      return url.toString();
    }

    function normalizeBlankTargetLink(link) {
      if (!isEmbeddedMode || !link) {
        return;
      }
      if (link.getAttribute('target') === '_blank') {
        link.setAttribute('target', '_self');
      }
      link.removeAttribute('rel');
    }

    function normalizeBlankTargetLinks(root = document) {
      if (!isEmbeddedMode || !root?.querySelectorAll) {
        return;
      }
      root
        .querySelectorAll('a[target="_blank"]')
        .forEach((link) => normalizeBlankTargetLink(link));
    }

    function normalizeFilterMode(mode) {
      return String(mode || '').toLowerCase() === 'expert' ? 'expert' : 'simple';
    }

    function normalizeFilterDimension(dimension) {
      const normalized = String(dimension || '').toLowerCase();
      if (normalized === 'atmosphere' || normalized === 'material' || normalized === 'spectral') {
        return normalized;
      }
      return 'spectral';
    }

    function updateFilterDimensionButtonState() {
      const spectralButton = document.getElementById('filterDimensionSpectralButton');
      const atmosphereButton = document.getElementById('filterDimensionAtmosphereButton');
      const materialButton = document.getElementById('filterDimensionMaterialButton');
      const byDimension = {
        spectral: spectralButton,
        atmosphere: atmosphereButton,
        material: materialButton
      };
      Object.entries(byDimension).forEach(([dimension, button]) => {
        if (!button) {
          return;
        }
        button.classList.toggle('is-active', activeFilterDimension === dimension);
      });
    }

    function setHudPanelVisibility(visible) {
      isHudPanelVisible = Boolean(visible);
      const hud = document.getElementById('hud');
      if (!hud) {
        return;
      }
      hud.classList.toggle('is-collapsed', !isHudPanelVisible);
      hud.classList.remove('hidden');
    }

    function disposeLegacyGridCoordMesh(gridObj) {
      const coordMesh = gridObj?.coordGrid;
      if (!coordMesh) {
        return;
      }
      if (typeof scene !== 'undefined' && scene && typeof scene.remove === 'function') {
        scene.remove(coordMesh);
      }
      if (coordMesh.geometry?.dispose) {
        coordMesh.geometry.dispose();
      }
      if (Array.isArray(coordMesh.material)) {
        coordMesh.material.forEach((material) => material?.dispose?.());
      } else if (coordMesh.material?.dispose) {
        coordMesh.material.dispose();
      }
      gridObj.coordGrid = null;
      gridObj.coordTxt = '';
    }

    function suppressLegacyEd3dGrid() {
      if (!window.Ed3d) {
        return;
      }
      ['grid1H', 'grid1K', 'grid1XL'].forEach((gridKey) => {
        const gridObj = Ed3d?.[gridKey];
        if (!gridObj || typeof gridObj !== 'object') {
          return;
        }
        gridObj.visible = false;
        if (gridObj.obj) {
          gridObj.obj.visible = false;
          gridObj.obj.customUpdateCallback = null;
        }
        if (gridKey !== 'grid1XL') {
          gridObj.addCoords = function noopLegacyGridCoords() {};
          disposeLegacyGridCoordMesh(gridObj);
        }
        if (typeof gridObj.hide === 'function') {
          gridObj.hide();
        }
      });
    }

    function removeLegacyHudToggle() {
      if (window.$) {
        $(document).off('click', '#hud-toggle');
      }
      const legacyToggle = document.getElementById('hud-toggle');
      if (legacyToggle) {
        legacyToggle.remove();
      }
    }

    function updateBottomRightStatusDisplay() {
      const distanceDisplay = document.getElementById('mapStatusDistance');
      const coordsDisplay = document.getElementById('mapStatusCoords');
      const starsDisplay = document.getElementById('mapStatusStars');
      if (!distanceDisplay || !coordsDisplay || !starsDisplay) {
        return;
      }

      const distanceText = String(document.getElementById('distsol')?.textContent || '').trim();
      const xText = String(document.getElementById('cx')?.textContent || '').trim();
      const yText = String(document.getElementById('cy')?.textContent || '').trim();
      const zText = String(document.getElementById('cz')?.textContent || '').trim();

      distanceDisplay.textContent = distanceText ? `Dist. Sol: ${distanceText}` : 'Dist. Sol: --';
      if (xText || yText || zText) {
        coordsDisplay.textContent = `Pos: ${xText || '--'}, ${yText || '--'}, ${zText || '--'}`;
      } else {
        coordsDisplay.textContent = 'Pos: --, --, --';
      }

      const visibleStars = getVisibleStarsCount();
      starsDisplay.textContent = Number.isFinite(visibleStars)
        ? `Stars: ${visibleStars.toLocaleString('en-US')}`
        : 'Stars: --';
    }

    function getVisibleStarsCount() {
      const now = Date.now();
      if (
        cachedVisibleStarCount !== null
        && (now - cachedVisibleStarCountUpdatedAt) < 250
      ) {
        return cachedVisibleStarCount;
      }

      let count = null;
      const vertices = (typeof System !== 'undefined' && System && System.particleGeo)
        ? System.particleGeo.vertices
        : null;
      if (Array.isArray(vertices)) {
        count = 0;
        for (let i = 0; i < vertices.length; i += 1) {
          if (vertices[i]?.visible === true) {
            count += 1;
          }
        }
      }

      cachedVisibleStarCount = count;
      cachedVisibleStarCountUpdatedAt = now;
      return count;
    }

    function attachHudStatusObserver() {
      if (hudStatusObserver) {
        hudStatusObserver.disconnect();
        hudStatusObserver = null;
      }

      const watchedIds = ['distsol', 'cx', 'cy', 'cz'];
      const nodes = watchedIds
        .map((id) => document.getElementById(id))
        .filter(Boolean);
      if (!nodes.length || typeof MutationObserver === 'undefined') {
        updateBottomRightStatusDisplay();
        return;
      }

      hudStatusObserver = new MutationObserver(() => {
        updateBottomRightStatusDisplay();
      });
      nodes.forEach((node) => {
        hudStatusObserver.observe(node, {
          childList: true,
          characterData: true,
          subtree: true
        });
      });
      updateBottomRightStatusDisplay();
    }

    function syncHudPanelUi() {
      removeLegacyHudToggle();
      suppressLegacyEd3dGrid();
      setHudPanelVisibility(isHudPanelVisible);
      attachHudStatusObserver();
      applyHudFilterPanelTitle();
      removeUtilityFiltersFromHud();
      ensureHudFacetTools();
      ensureHudSelectionActions();
      bindHudFilterColorSync();
      applyHudFilterSearchAndSort();
      applyPendingSharedFacetSelection();
      applyActivePointColorMode();
      if (boxelOverlayEnabled) {
        refreshBoxelOverlay();
      }
    }

    function getFilterDimensionPanelTitle() {
      if (activeFilterDimension === 'atmosphere') {
        return 'Atmospheres';
      }
      if (activeFilterDimension === 'material') {
        return 'Materials';
      }
      return 'Spectral Type';
    }

    function applyHudFilterPanelTitle() {
      const filterTitle = document.querySelector('#hud #filters > h8');
      if (!filterTitle) {
        return;
      }
      filterTitle.textContent = getFilterDimensionPanelTitle();
    }

    function removeUtilityFiltersFromHud() {
      const hiddenFilterIds = ['Target', 'Neighbors'];
      hiddenFilterIds.forEach((filterId) => {
        const filterEl = document.querySelector(`#hud #filters .map_filter[data-filter="${filterId}"]`);
        if (filterEl) {
          filterEl.remove();
        }
      });

      const predictedFilterEl = document.querySelector('#hud #filters .map_filter[data-filter="Predicted Systems"]');
      if (predictedFilterEl) {
        predictedFilterEl.style.display = experimentalFeaturesEnabled ? '' : 'none';
      }
    }

    function getHudSelectableFilters() {
      const allFilters = Array.from(document.querySelectorAll('#hud #filters .map_filter'));
      return allFilters.filter((filterEl) => {
        const filterId = String(filterEl.getAttribute('data-filter') || '');
        if (filterId === 'Target' || filterId === 'Neighbors') {
          return false;
        }
        if (!experimentalFeaturesEnabled && filterId === 'Predicted Systems') {
          return false;
        }
        return true;
      });
    }

    function getHudFacetFilterCount(filterId) {
      return Array.isArray(Ed3d?.catObjs?.[filterId]) ? Ed3d.catObjs[filterId].length : 0;
    }

    function applyHudFilterSearchAndSort() {
      const filtersRoot = document.getElementById('filters');
      if (!filtersRoot) {
        return;
      }
      const filterGroup = filtersRoot.querySelector(':scope > div[id^="group_"]');
      if (!filterGroup) {
        return;
      }

      const filters = Array.from(filterGroup.querySelectorAll('.map_filter'));
      filters.sort((a, b) => {
        const aId = String(a.getAttribute('data-filter') || '').trim();
        const bId = String(b.getAttribute('data-filter') || '').trim();
        const aLabel = String(a.getAttribute('data-label') || a.textContent || '').trim();
        const bLabel = String(b.getAttribute('data-label') || b.textContent || '').trim();

        if (hudFacetSortMode === 'alpha_asc') {
          return aLabel.localeCompare(bLabel);
        }
        if (hudFacetSortMode === 'alpha_desc') {
          return bLabel.localeCompare(aLabel);
        }

        const countDiff = getHudFacetFilterCount(bId) - getHudFacetFilterCount(aId);
        if (countDiff !== 0) {
          return countDiff;
        }
        return aLabel.localeCompare(bLabel);
      });
      filters.forEach((filterEl) => {
        filterGroup.appendChild(filterEl);
      });

      const term = hudFacetSearchTerm.toLowerCase();
      filters.forEach((filterEl) => {
        const filterId = String(filterEl.getAttribute('data-filter') || '').trim();
        if (!experimentalFeaturesEnabled && filterId === 'Predicted Systems') {
          filterEl.style.display = 'none';
          return;
        }
        if (!term) {
          filterEl.style.display = '';
          return;
        }
        const label = String(filterEl.getAttribute('data-label') || filterEl.textContent || '').toLowerCase();
        filterEl.style.display = label.includes(term) ? '' : 'none';
      });
    }

    function ensureHudFacetTools() {
      const filtersRoot = document.getElementById('filters');
      if (!filtersRoot) {
        return;
      }

      const existing = document.getElementById('hudFacetTools');
      if (existing) {
        existing.remove();
      }

      const filterTitle = filtersRoot.querySelector(':scope > h8');
      if (!filterTitle) {
        return;
      }

      const tools = document.createElement('div');
      tools.id = 'hudFacetTools';
      tools.className = 'hud-facet-tools';
      tools.innerHTML = `
        <input type="search" id="hudFacetSearchInput" placeholder="Search..." aria-label="Search facets">
        <select id="hudFacetSortSelect" aria-label="Sort facets">
          <option value="count_desc">Sort: Count</option>
          <option value="alpha_asc">Sort: A-Z</option>
          <option value="alpha_desc">Sort: Z-A</option>
        </select>
      `;

      const searchInput = tools.querySelector('#hudFacetSearchInput');
      const sortSelect = tools.querySelector('#hudFacetSortSelect');
      if (searchInput) {
        searchInput.value = hudFacetSearchTerm;
        searchInput.addEventListener('input', () => {
          hudFacetSearchTerm = String(searchInput.value || '').trim();
          applyHudFilterSearchAndSort();
        });
      }
      if (sortSelect) {
        sortSelect.value = hudFacetSortMode;
        sortSelect.addEventListener('change', () => {
          hudFacetSortMode = String(sortSelect.value || 'count_desc');
          applyHudFilterSearchAndSort();
        });
      }

      filterTitle.insertAdjacentElement('afterend', tools);
    }

    function getActiveHudFilterIds() {
      return getHudSelectableFilters()
        .filter((filterEl) => !filterEl.classList.contains('disabled'))
        .map((filterEl) => String(filterEl.getAttribute('data-filter') || '').trim())
        .filter(Boolean);
    }

    function normalizeSharedFilterIds(values) {
      if (!Array.isArray(values)) {
        return null;
      }
      const seen = new Set();
      const normalized = [];
      values.forEach((value) => {
        const id = String(value ?? '').trim();
        if (!id) {
          return;
        }
        const key = id.toLowerCase();
        if (seen.has(key)) {
          return;
        }
        seen.add(key);
        normalized.push(id);
      });
      return normalized;
    }

    function parseSharedFilterIdsParam(rawValues) {
      if (rawValues == null) {
        return null;
      }
      const values = Array.isArray(rawValues) ? rawValues : [rawValues];
      if (!values.length) {
        return null;
      }

      const cleanedValues = values
        .map((value) => String(value ?? '').trim())
        .filter((value) => value.length > 0);
      if (!cleanedValues.length) {
        return [];
      }

      const firstValue = cleanedValues[0];
      if (firstValue.startsWith('[') && firstValue.endsWith(']')) {
        try {
          const parsed = JSON.parse(firstValue);
          if (Array.isArray(parsed)) {
            return normalizeSharedFilterIds(parsed);
          }
        } catch (error) {
          // Fallback to comma-separated parsing.
        }
      }

      const splitValues = cleanedValues.flatMap((value) => value.split(','));
      return normalizeSharedFilterIds(splitValues);
    }

    function applyFacetSelectionToSearchParams(searchParams) {
      const selectableFilters = getHudSelectableFilters();
      if (!selectableFilters.length) {
        if (Array.isArray(pendingSharedFilterIds)) {
          searchParams.set('filter_ids', pendingSharedFilterIds.join(','));
        } else {
          searchParams.delete('filter_ids');
        }
        return;
      }

      const activeIds = getActiveHudFilterIds();
      if (activeIds.length === selectableFilters.length) {
        searchParams.delete('filter_ids');
        return;
      }
      searchParams.set('filter_ids', activeIds.join(','));
    }

    function syncFacetSelectionUrlParam() {
      if (externalSolutionJson) {
        return;
      }
      const nextUrl = new URL(window.location.href);
      applyFacetSelectionToSearchParams(nextUrl.searchParams);
      window.history.replaceState({}, '', nextUrl);
    }

    function applyPendingSharedFacetSelection() {
      if (!Array.isArray(pendingSharedFilterIds)) {
        return;
      }

      const selectableFilters = getHudSelectableFilters();
      if (!selectableFilters.length) {
        return;
      }

      const desiredIds = new Set(pendingSharedFilterIds.map((id) => String(id).toLowerCase()));
      const filtersById = new Map(
        selectableFilters.map((filterEl) => [
          String(filterEl.getAttribute('data-filter') || '').trim().toLowerCase(),
          filterEl
        ])
      );

      suppressFacetColorSync = true;
      try {
        // Phase 1: clear current selection so multi-facet stars are reset consistently.
        selectableFilters.forEach((filterEl) => {
          const isActive = !filterEl.classList.contains('disabled');
          if (isActive) {
            filterEl.click();
          }
        });

        // Phase 2: activate only requested facets.
        desiredIds.forEach((id) => {
          const filterEl = filtersById.get(id);
          if (!filterEl) {
            return;
          }
          const isActive = !filterEl.classList.contains('disabled');
          if (!isActive) {
            filterEl.click();
          }
        });
      } finally {
        suppressFacetColorSync = false;
      }

      pendingSharedFilterIds = null;
      applyActivePointColorMode();
      syncFacetSelectionUrlParam();
    }

    function restoreVisiblePointBaseColors() {
      if (!window.System?.particleGeo?.vertices || !Array.isArray(window.System.particleGeo.colors)) {
        return;
      }
      const vertices = window.System.particleGeo.vertices;
      const colors = window.System.particleGeo.colors;
      for (let index = 0; index < vertices.length; index++) {
        const vertex = vertices[index];
        if (!vertex || vertex.visible !== true || !vertex.color) {
          continue;
        }
        colors[index] = vertex.color;
      }
      window.System.syncParticleColors();
    }

    function getInternalHeatCenter() {
      const cameraCenter = getCurrentInternalTarget();
      if (cameraCenter) {
        return cameraCenter;
      }
      if (lastAutoLoadCenter && Number.isFinite(lastAutoLoadCenter.x) && Number.isFinite(lastAutoLoadCenter.y) && Number.isFinite(lastAutoLoadCenter.z)) {
        return {
          x: Number(lastAutoLoadCenter.x),
          y: Number(lastAutoLoadCenter.y),
          z: -Number(lastAutoLoadCenter.z)
        };
      }
      return null;
    }

    function colorFromHeatRatio(ratio) {
      const stops = [
        { at: 0, rgb: [42, 98, 255] },
        { at: 0.45, rgb: [0, 226, 255] },
        { at: 0.75, rgb: [255, 196, 0] },
        { at: 1, rgb: [255, 72, 0] }
      ];
      const t = clamp(ratio, 0, 1);
      for (let idx = 0; idx < stops.length - 1; idx++) {
        const left = stops[idx];
        const right = stops[idx + 1];
        if (t >= left.at && t <= right.at) {
          const span = Math.max(right.at - left.at, 0.0001);
          const local = (t - left.at) / span;
          const r = Math.round(left.rgb[0] + ((right.rgb[0] - left.rgb[0]) * local));
          const g = Math.round(left.rgb[1] + ((right.rgb[1] - left.rgb[1]) * local));
          const b = Math.round(left.rgb[2] + ((right.rgb[2] - left.rgb[2]) * local));
          return new THREE.Color(`rgb(${r}, ${g}, ${b})`);
        }
      }
      return new THREE.Color('rgb(255,72,0)');
    }

    function applyDistanceHeatColoring() {
      if (!window.System?.particleGeo?.vertices || !Array.isArray(window.System.particleGeo.colors)) {
        return;
      }
      const center = getInternalHeatCenter();
      if (!center) {
        return;
      }
      const vertices = window.System.particleGeo.vertices;
      const colors = window.System.particleGeo.colors;
      const maxDistance = Math.max(1, Number(activeNeighborhoodRadius) || Number(getAutoRefreshRadius()) || 20);

      for (let index = 0; index < vertices.length; index++) {
        const vertex = vertices[index];
        if (!vertex || vertex.visible !== true) {
          continue;
        }
        const dx = Number(vertex.x) - center.x;
        const dy = Number(vertex.y) - center.y;
        const dz = Number(vertex.z) - center.z;
        const distance = Math.sqrt((dx * dx) + (dy * dy) + (dz * dz));
        const ratio = distance / maxDistance;
        colors[index] = colorFromHeatRatio(ratio);
      }
      window.System.syncParticleColors();
    }

    function applyFacetSelectionColoring() {
      if (!window.System?.particleGeo?.vertices || !Array.isArray(window.System.particleGeo.colors)) {
        return;
      }

      const facetKey = activeFilterDimension === 'atmosphere' ? 'atmosphere_gases' : 'materials';
      const unknownFacetName = activeFilterDimension === 'atmosphere' ? 'Unknown Atmosphere' : 'Unknown Material';
      const activeFilterIds = new Set(getActiveHudFilterIds());
      const vertices = window.System.particleGeo.vertices;
      const colors = window.System.particleGeo.colors;

      for (let index = 0; index < vertices.length; index++) {
        const vertex = vertices[index];
        if (!vertex || vertex.visible !== true) {
          continue;
        }

        const rawFacetValues = normalizeFacetValues(vertex?.infos?.[facetKey]);
        const facetValues = rawFacetValues.length ? rawFacetValues : [unknownFacetName];
        let selectedFacet = facetValues.find((name) => activeFilterIds.has(name)) || facetValues[0];
        if (!selectedFacet) {
          continue;
        }

        const mappedColor = Ed3d?.colors?.[selectedFacet];
        if (!mappedColor) {
          continue;
        }
        colors[index] = mappedColor;
      }

      window.System.syncParticleColors();
    }

    function applyActivePointColorMode() {
      if (distanceHeatEnabled) {
        applyDistanceHeatColoring();
        return;
      }
      restoreVisiblePointBaseColors();
      if (normalizeFilterDimension(activeFilterDimension) !== 'spectral') {
        applyFacetSelectionColoring();
      }
    }

    function bindHudFilterColorSync() {
      if (!window.$) {
        return;
      }
      $(document)
        .off('click.edgisfacetcolor', '#hud #filters .map_filter')
        .on('click.edgisfacetcolor', '#hud #filters .map_filter', function () {
          if (suppressFacetColorSync) {
            return;
          }
          setTimeout(() => {
            applyActivePointColorMode();
            syncFacetSelectionUrlParam();
          }, 0);
        });
    }

    function applyHudFilterBulkAction(action) {
      const selectableFilters = getHudSelectableFilters();
      suppressFacetColorSync = true;
      try {
        selectableFilters.forEach((filterEl) => {
          const isActive = !filterEl.classList.contains('disabled');
          if (action === 'all' && !isActive) {
            filterEl.click();
            return;
          }
          if (action === 'none' && isActive) {
            filterEl.click();
            return;
          }
          if (action === 'invert') {
            filterEl.click();
          }
        });
      } finally {
        suppressFacetColorSync = false;
      }
      applyActivePointColorMode();
      syncFacetSelectionUrlParam();
    }

    function ensureHudSelectionActions() {
      const filtersRoot = document.getElementById('filters');
      if (!filtersRoot) {
        return;
      }

      const existing = document.getElementById('hudFilterActions');
      if (existing) {
        existing.remove();
      }

      const filterTitle = filtersRoot.querySelector(':scope > h8');
      if (!filterTitle) {
        return;
      }
      const insertionAnchor = document.getElementById('hudFacetTools') || filterTitle;

      const actions = document.createElement('div');
      actions.id = 'hudFilterActions';
      actions.className = 'hud-filter-actions';
      actions.innerHTML = `
        <button type="button" data-action="all" aria-label="Select all filters">All</button>
        <button type="button" data-action="none" aria-label="Clear all filters">None</button>
        <button type="button" data-action="invert" aria-label="Invert filter selection">Invert</button>
      `;
      actions.addEventListener('click', (event) => {
        const eventTarget = event.target instanceof Element ? event.target : null;
        const actionButton = eventTarget ? eventTarget.closest('button[data-action]') : null;
        if (!actionButton) {
          return;
        }
        const action = String(actionButton.getAttribute('data-action') || '');
        if (!action) {
          return;
        }
        applyHudFilterBulkAction(action);
      });

      insertionAnchor.insertAdjacentElement('afterend', actions);
    }

    function normalizeNeighborhoodFilterValue(value) {
      return String(value || '').trim();
    }

    function normalizeNeighborhoodFilters(filters = {}) {
      return {
        atmosphereGas: normalizeNeighborhoodFilterValue(filters.atmosphereGas),
        material: normalizeNeighborhoodFilterValue(filters.material)
      };
    }

    function getCurrentNeighborhoodFilters() {
      return normalizeNeighborhoodFilters(activeNeighborhoodFilters);
    }

    function applyNeighborhoodFiltersToSearchParams(searchParams) {
      const filters = getCurrentNeighborhoodFilters();
      if (filters.atmosphereGas) {
        searchParams.set('atmosphere_gas', filters.atmosphereGas);
      } else {
        searchParams.delete('atmosphere_gas');
      }
      if (filters.material) {
        searchParams.set('material', filters.material);
      } else {
        searchParams.delete('material');
      }
    }

    function applyFilterDimensionToSearchParams(searchParams) {
      if (activeFilterDimension === 'spectral') {
        searchParams.delete('filter_dimension');
        return;
      }
      searchParams.set('filter_dimension', activeFilterDimension);
    }

    function applyDebugGridOrderToSearchParams(searchParams) {
      if (reverseDebugGridDetailOrder) {
        searchParams.set('grid_detail', 'reverse');
      } else {
        searchParams.delete('grid_detail');
      }
    }

    function applyRegionNamesToSearchParams(searchParams) {
      if (!regionNamesEnabled) {
        searchParams.set('region_names', '0');
      } else {
        searchParams.delete('region_names');
      }
    }

    function applyBaseGridToSearchParams(searchParams) {
      if (!dynamicBaseGridEnabled) {
        searchParams.set('base_grid', '0');
      } else {
        searchParams.delete('base_grid');
      }
    }

    function applyExperimentalFeaturesToSearchParams(searchParams) {
      if (experimentalFeaturesEnabled) {
        searchParams.set('experimental_features', '1');
      } else {
        searchParams.delete('experimental_features');
      }
    }

    function getCurrentRadiusParamValue() {
      return formatRadiusValue(activeNeighborhoodRadius || getRequestedNeighborhoodRadius());
    }

    function updateRadiusDisplay() {
      const radiusDisplay = document.getElementById('radiusDisplay');
      if (!radiusDisplay) {
        return;
      }
      const value = getCurrentRadiusParamValue();
      const label = `Radius: ${value} LY`;
      radiusDisplay.textContent = label;
      radiusDisplay.title = label;
      radiusDisplay.setAttribute('aria-label', `Current ${label}`);
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
      applyNeighborhoodFiltersToSearchParams(url.searchParams);
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
        normalizeBlankTargetLink(jsonLink);
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
      if (activeFilterMode === 'expert') {
        nextUrl.searchParams.set('mode', 'expert');
      } else {
        nextUrl.searchParams.delete('mode');
      }
      applyFilterDimensionToSearchParams(nextUrl.searchParams);
      applyNeighborhoodFiltersToSearchParams(nextUrl.searchParams);
      if (distanceHeatEnabled) {
        nextUrl.searchParams.set('heat', '1');
      } else {
        nextUrl.searchParams.delete('heat');
      }
      if (boxelOverlayEnabled) {
        nextUrl.searchParams.set('boxels', '1');
      } else {
        nextUrl.searchParams.delete('boxels');
      }
      if (predictedSystemsEnabled) {
        nextUrl.searchParams.set('predicted', '1');
      } else {
        nextUrl.searchParams.delete('predicted');
      }
      applyFacetSelectionToSearchParams(nextUrl.searchParams);
      applyDebugGridOrderToSearchParams(nextUrl.searchParams);
      applyRegionNamesToSearchParams(nextUrl.searchParams);
      applyBaseGridToSearchParams(nextUrl.searchParams);
      applyExperimentalFeaturesToSearchParams(nextUrl.searchParams);
      window.history.replaceState({}, '', nextUrl);
      updateEdgisLinks(center);
      updateRadiusDisplay();
    }

    function updateDistanceHeatButtonState() {
      const button = document.getElementById('distanceHeatButton');
      if (!button) {
        return;
      }
      button.classList.toggle('is-active', Boolean(distanceHeatEnabled));
      button.title = distanceHeatEnabled ? 'Distance Heat: On' : 'Distance Heat: Off';
      button.setAttribute('aria-label', button.title);
    }

    function applyExperimentalFeatureButtonsVisibility() {
      const shouldShow = Boolean(experimentalFeaturesEnabled);
      const predictedSystemsButton = document.getElementById('predictedSystemsButton');
      const boxelOverlayButton = document.getElementById('boxelOverlayButton');
      if (predictedSystemsButton) {
        predictedSystemsButton.style.display = shouldShow ? '' : 'none';
      }
      if (boxelOverlayButton) {
        boxelOverlayButton.style.display = shouldShow ? '' : 'none';
      }
    }

    function applyRegionNamesVisibility() {
      if (typeof Ed3d === 'undefined' || !Ed3d) {
        return;
      }
      Ed3d.showGalaxyInfos = Boolean(regionNamesEnabled);
      if (typeof Galaxy === 'undefined' || !Galaxy) {
        return;
      }
      if (!regionNamesEnabled) {
        if (typeof Galaxy.infosHide === 'function') {
          Galaxy.infosHide();
        }
        return;
      }
      if (typeof isFarView !== 'undefined' && isFarView && typeof Galaxy.infosShow === 'function') {
        Galaxy.infosShow();
      }
    }

    function clearBoxelOverlay() {
      if (boxelLabelRefreshTimer) {
        clearTimeout(boxelLabelRefreshTimer);
        boxelLabelRefreshTimer = null;
      }
      boxelOverlayCells = [];
      clearBoxelLabels();
      if (!boxelOverlayGroup || typeof scene === 'undefined' || !scene) {
        boxelOverlayGroup = null;
        updateDebugGridLegend([]);
        return;
      }
      scene.remove(boxelOverlayGroup);
      boxelOverlayGroup.traverse((obj) => {
        if (obj.geometry) {
          obj.geometry.dispose?.();
        }
        if (obj.material) {
          if (Array.isArray(obj.material)) {
            obj.material.forEach((material) => material?.dispose?.());
          } else {
            obj.material.dispose?.();
          }
        }
      });
      boxelOverlayGroup = null;
      updateDebugGridLegend([]);
    }

    function clearBoxelLabels() {
      if (!boxelLabelGroup || typeof scene === 'undefined' || !scene) {
        boxelLabelGroup = null;
        return;
      }
      scene.remove(boxelLabelGroup);
      boxelLabelGroup.traverse((obj) => {
        if (obj.geometry) {
          obj.geometry.dispose?.();
        }
        if (obj.material) {
          if (obj.material.map) {
            obj.material.map.dispose?.();
          }
          obj.material.dispose?.();
        }
      });
      boxelLabelGroup = null;
    }

    function clearDynamicBaseGrid() {
      if (dynamicBaseGridRefreshTimer) {
        clearTimeout(dynamicBaseGridRefreshTimer);
        dynamicBaseGridRefreshTimer = null;
      }
      if (!dynamicBaseGridGroup || typeof scene === 'undefined' || !scene) {
        dynamicBaseGridGroup = null;
        return;
      }
      scene.remove(dynamicBaseGridGroup);
      dynamicBaseGridGroup.traverse((obj) => {
        if (obj.geometry) {
          obj.geometry.dispose?.();
        }
        if (obj.material) {
          if (obj.material.map) {
            obj.material.map.dispose?.();
          }
          obj.material.dispose?.();
        }
      });
      dynamicBaseGridGroup = null;
    }

    function getNiceGridStep(value) {
      const safe = Math.max(1, Number(value) || 1);
      const pow10 = Math.pow(10, Math.floor(Math.log10(safe)));
      const normalized = safe / pow10;
      if (normalized <= 1) return 1 * pow10;
      if (normalized <= 2) return 2 * pow10;
      if (normalized <= 5) return 5 * pow10;
      return 10 * pow10;
    }

    function formatGridCoordValue(value) {
      const rounded = Math.round(Number(value) || 0);
      if (Math.abs(rounded) >= 1000) {
        return rounded.toLocaleString('en-US');
      }
      return String(rounded);
    }

    function getDynamicBaseGridStep() {
      const target = getCurrentInternalTarget();
      if (!target || typeof camera === 'undefined' || !camera) {
        return 50;
      }
      const radius = Number(activeNeighborhoodRadius || getAutoRefreshRadius() || 20);
      const dx = Number(camera.position.x) - Number(target.x);
      const dy = Number(camera.position.y) - Number(target.y);
      const dz = Number(camera.position.z) - Number(target.z);
      const cameraDistance = Math.sqrt((dx * dx) + (dy * dy) + (dz * dz));
      const targetStep = Math.max(radius * 0.45, cameraDistance * 0.16, 10);
      return getNiceGridStep(targetStep);
    }

    function createFlatGridTextMesh(text, colorHex, scale = 2.8) {
      if (typeof THREE === 'undefined') {
        return null;
      }
      const effectiveScale = Math.max(0.6, Number(scale) || 2.8);
      const canvas = document.createElement('canvas');
      const context = canvas.getContext('2d');
      if (!context) {
        return null;
      }
      const fontSize = Math.max(10, Math.min(Math.round(effectiveScale * 6), 140));
      const paddingX = Math.max(2, Math.round(fontSize * 0.2));
      const paddingY = Math.max(2, Math.round(fontSize * 0.18));
      context.font = `${fontSize}px Arial`;
      const metrics = context.measureText(text);
      const textWidth = Math.ceil(metrics.width);
      const width = Math.max(14, textWidth + (paddingX * 2));
      const height = fontSize + (paddingY * 2);
      canvas.width = width;
      canvas.height = height;

      context.font = `${fontSize}px Arial`;
      context.fillStyle = `#${colorHex.toString(16).padStart(6, '0')}`;
      context.fillText(text, paddingX, paddingY + fontSize - 1);

      const texture = new THREE.CanvasTexture(canvas);
      texture.minFilter = THREE.LinearFilter;
      texture.magFilter = THREE.LinearFilter;
      texture.needsUpdate = true;

      const material = new THREE.MeshBasicMaterial({
        map: texture,
        transparent: true,
        opacity: 0.72,
        depthWrite: false,
        side: THREE.DoubleSide
      });
      const geometry = new THREE.PlaneGeometry(1, 1);
      const mesh = new THREE.Mesh(geometry, material);
      const aspect = width / Math.max(1, height);
      mesh.scale.set(effectiveScale * aspect, effectiveScale, 1);
      return mesh;
    }

    function buildDynamicBaseGridOverlay() {
      clearDynamicBaseGrid();
      if (!dynamicBaseGridEnabled) {
        return;
      }
      if (typeof THREE === 'undefined' || typeof scene === 'undefined' || !scene) {
        return;
      }
      const center = getCurrentMapCenter();
      const target = getCurrentInternalTarget();
      if (!center || !target) {
        return;
      }
      const step = getDynamicBaseGridStep();
      const halfCount = 10;
      const extent = step * halfCount;
      const gridY = Number(target.y) || 0;
      const originX = Math.round((Number(center.x) || 0) / step) * step;
      const originZ = Math.round((Number(center.z) || 0) / step) * step;

      const group = new THREE.Group();
      group.name = 'dynamicBaseGridGroup';

      const majorStride = 10;
      const minorMaterial = new THREE.LineBasicMaterial({
        color: 0x2a6ea0,
        transparent: true,
        opacity: 0.2
      });
      const microMaterial = new THREE.LineBasicMaterial({
        color: 0x2a6ea0,
        transparent: true,
        opacity: 0.08
      });
      const majorMaterial = new THREE.LineBasicMaterial({
        color: 0x58b5ff,
        transparent: true,
        opacity: 0.42
      });

      for (let offset = -halfCount; offset <= halfCount; offset++) {
        const lineX = originX + (offset * step);
        const xLinePoints = [
          new THREE.Vector3(lineX, gridY, -(originZ - extent)),
          new THREE.Vector3(lineX, gridY, -(originZ + extent))
        ];
        const xGeom = new THREE.BufferGeometry().setFromPoints(xLinePoints);
        const xMat = (offset % majorStride === 0) ? majorMaterial.clone() : minorMaterial.clone();
        group.add(new THREE.LineSegments(xGeom, xMat));

        const lineZ = originZ + (offset * step);
        const zLinePoints = [
          new THREE.Vector3(originX - extent, gridY, -lineZ),
          new THREE.Vector3(originX + extent, gridY, -lineZ)
        ];
        const zGeom = new THREE.BufferGeometry().setFromPoints(zLinePoints);
        const zMat = (offset % majorStride === 0) ? majorMaterial.clone() : minorMaterial.clone();
        group.add(new THREE.LineSegments(zGeom, zMat));
      }

      // Add a finer subdivision: each base cell gets split into 10x10.
      const microDivisionsPerCell = 10;
      const microStep = step / microDivisionsPerCell;
      const microHalfCount = halfCount * microDivisionsPerCell;
      for (let microOffset = -microHalfCount; microOffset <= microHalfCount; microOffset++) {
        // Skip lines already drawn by the base grid.
        if (microOffset % microDivisionsPerCell === 0) {
          continue;
        }
        const lineX = originX + (microOffset * microStep);
        const xLinePoints = [
          new THREE.Vector3(lineX, gridY, -(originZ - extent)),
          new THREE.Vector3(lineX, gridY, -(originZ + extent))
        ];
        const xGeom = new THREE.BufferGeometry().setFromPoints(xLinePoints);
        group.add(new THREE.LineSegments(xGeom, microMaterial.clone()));

        const lineZ = originZ + (microOffset * microStep);
        const zLinePoints = [
          new THREE.Vector3(originX - extent, gridY, -lineZ),
          new THREE.Vector3(originX + extent, gridY, -lineZ)
        ];
        const zGeom = new THREE.BufferGeometry().setFromPoints(zLinePoints);
        group.add(new THREE.LineSegments(zGeom, microMaterial.clone()));
      }

      const labelStride = step < 20 ? 4 : step < 100 ? 3 : 2;
      const labelOffsetX = -step * 0.12;
      const labelOffsetZ = step * 0.12;
      const labelScale = Math.max(1.6, step * 0.085);
      const tupleColor = 0x58b5ff;
      for (let ix = -halfCount; ix <= halfCount; ix++) {
        if (ix % labelStride !== 0) {
          continue;
        }
        for (let iz = -halfCount; iz <= halfCount; iz++) {
          if (iz % labelStride !== 0) {
            continue;
          }
          const lineX = originX + (ix * step);
          const lineZ = originZ + (iz * step);
          const tupleLabel = createFlatGridTextMesh(
            `${formatGridCoordValue(lineX)} : ${formatGridCoordValue(gridY)} : ${formatGridCoordValue(lineZ)}`,
            tupleColor,
            labelScale
          );
          if (!tupleLabel) {
            continue;
          }
          tupleLabel.rotation.set(-Math.PI / 2, 0, 0);
          tupleLabel.position.set(
            lineX + labelOffsetX,
            gridY + 0.03,
            -(lineZ + labelOffsetZ)
          );
          group.add(tupleLabel);
        }
      }

      scene.add(group);
      dynamicBaseGridGroup = group;
    }

    function scheduleDynamicBaseGridRefresh(delayMs = 120) {
      if (!dynamicBaseGridEnabled) {
        clearDynamicBaseGrid();
        return;
      }
      if (dynamicBaseGridRefreshTimer) {
        clearTimeout(dynamicBaseGridRefreshTimer);
      }
      dynamicBaseGridRefreshTimer = setTimeout(() => {
        dynamicBaseGridRefreshTimer = null;
        buildDynamicBaseGridOverlay();
      }, delayMs);
    }

    function updateBoxelOverlayButtonState() {
      const button = document.getElementById('boxelOverlayButton');
      if (!button) {
        return;
      }
      button.classList.toggle('is-active', Boolean(boxelOverlayEnabled));
      button.title = boxelOverlayEnabled ? 'Octree: On' : 'Octree: Off';
      button.setAttribute('aria-label', button.title);
    }

    function updatePredictedSystemsButtonState() {
      const button = document.getElementById('predictedSystemsButton');
      if (!button) {
        return;
      }
      button.classList.toggle('is-active', Boolean(predictedSystemsEnabled));
      button.title = predictedSystemsEnabled ? 'Predicted Systems: On' : 'Predicted Systems: Off';
      button.setAttribute('aria-label', button.title);
    }

    function colorForBoxelMcode(mcodeRaw) {
      const mcode = String(mcodeRaw || '').toLowerCase();
      if (mcode === 'a') return 0xff4d4d;
      if (mcode === 'b') return 0xff8a3d;
      if (mcode === 'c') return 0xffbf3d;
      if (mcode === 'd') return 0x9ad14b;
      if (mcode === 'e') return 0x3ccf91;
      if (mcode === 'f') return 0x2f8ecf;
      if (mcode === 'g') return 0x7f6cff;
      if (mcode === 'h') return 0xbf6cff;
      return 0x2f8ecf;
    }

    function mcodeCubeSizeLy(mcodeRaw) {
      const mcode = String(mcodeRaw || '').toLowerCase();
      const idx = mcode.charCodeAt(0) - 'a'.charCodeAt(0);
      if (!Number.isFinite(idx) || idx < 0 || idx > 7) {
        return null;
      }
      return 10 * Math.pow(2, idx);
    }

    function createBoxelLabelMesh(text, colorHex, baseScale = 3.2) {
      if (typeof THREE === 'undefined') {
        return null;
      }
      const canvas = document.createElement('canvas');
      const context = canvas.getContext('2d');
      if (!context) {
        return null;
      }
      const fontSize = 9;
      const paddingX = 3;
      const paddingY = 2;
      context.font = `${fontSize}px Arial`;
      const metrics = context.measureText(text);
      const textWidth = Math.ceil(metrics.width);
      const width = Math.max(20, textWidth + (paddingX * 2));
      const height = fontSize + (paddingY * 2);
      canvas.width = width;
      canvas.height = height;

      context.font = `${fontSize}px Arial`;
      context.fillStyle = `#${colorHex.toString(16).padStart(6, '0')}`;
      context.fillText(text, paddingX, paddingY + fontSize - 1);

      const texture = new THREE.CanvasTexture(canvas);
      texture.minFilter = THREE.LinearFilter;
      texture.magFilter = THREE.LinearFilter;
      texture.needsUpdate = true;

      const material = new THREE.MeshBasicMaterial({
        map: texture,
        transparent: true,
        depthWrite: false,
        side: THREE.DoubleSide
      });
      const geometry = new THREE.PlaneGeometry(1, 1);
      const mesh = new THREE.Mesh(geometry, material);
      const aspect = width / Math.max(1, height);
      const widthScale = baseScale * aspect;
      const heightScale = baseScale;
      mesh.scale.set(widthScale, heightScale, 1);
      return mesh;
    }

    function orientLabelMeshToSegment(mesh, directionVec3) {
      if (!mesh || typeof THREE === 'undefined') {
        return;
      }
      const xAxis = directionVec3.clone().normalize();
      const yAxis = new THREE.Vector3(0, 1, 0);
      if (Math.abs(xAxis.dot(yAxis)) > 0.99) {
        yAxis.set(0, 0, 1);
      }
      const zAxis = new THREE.Vector3().crossVectors(xAxis, yAxis).normalize();
      const correctedYAxis = new THREE.Vector3().crossVectors(zAxis, xAxis).normalize();
      const basis = new THREE.Matrix4().makeBasis(xAxis, correctedYAxis, zAxis);
      mesh.quaternion.setFromRotationMatrix(basis);
    }

    function updateBoxelProximityLabels() {
      if (!boxelOverlayShowLabels) {
        clearBoxelLabels();
        return;
      }
      clearBoxelLabels();
      if (!boxelOverlayEnabled || !Array.isArray(boxelOverlayCells) || !boxelOverlayCells.length || typeof scene === 'undefined' || !scene) {
        return;
      }
      const cameraPos = getCurrentWorldCamera();
      if (!cameraPos) {
        return;
      }
      const candidates = [];
      for (let i = 0; i < boxelOverlayCells.length; i++) {
        const cell = boxelOverlayCells[i];
        const c = cell?.center;
        const size = Number(cell?.size);
        if (!c || !Number.isFinite(size) || size <= 0) {
          continue;
        }
        const worldX = Number(c.x) || 0;
        const worldY = Number(c.y) || 0;
        const worldZ = -(Number(c.z) || 0);
        const dx = worldX - cameraPos.x;
        const dy = worldY - cameraPos.y;
        const dz = worldZ - cameraPos.z;
        const dist = Math.sqrt((dx * dx) + (dy * dy) + (dz * dz));
        const threshold = Math.max(45, size * 2.4);
        if (dist <= threshold) {
          candidates.push({ cell, dist });
        }
      }
      if (!candidates.length) {
        return;
      }
      candidates.sort((a, b) => a.dist - b.dist);
      const selected = candidates.slice(0, 5);
      const labelGroup = new THREE.Group();
      labelGroup.name = 'boxelLabelGroup';

      selected.forEach(({ cell }) => {
        const c = cell.center;
        const size = Number(cell.size);
        const half = size * 0.5;
        const worldX = Number(c.x) || 0;
        const worldY = Number(c.y) || 0;
        const worldZ = -(Number(c.z) || 0);
        const color = colorForBoxelMcode(cell.mcode);
        const labelScale = clamp(size * 0.03, 0.9, 3.2);

        const xMin = Math.round((Number(c.x) || 0) - half);
        const xMax = Math.round((Number(c.x) || 0) + half);
        const yTop = Math.round((Number(c.y) || 0) + half);
        const zMin = Math.round((Number(c.z) || 0) - half);
        const zMax = Math.round((Number(c.z) || 0) + half);

        const cornerLabels = [
          {
            text: `(${xMin},${yTop},${zMin})`,
            worldPos: [worldX - half, worldY + half + (size * 0.02), worldZ + half],
            dir: new THREE.Vector3(1, 0, 0)
          },
          {
            text: `(${xMax},${yTop},${zMin})`,
            worldPos: [worldX + half, worldY + half + (size * 0.02), worldZ + half],
            dir: new THREE.Vector3(0, 0, -1)
          },
          {
            text: `(${xMax},${yTop},${zMax})`,
            worldPos: [worldX + half, worldY + half + (size * 0.02), worldZ - half],
            dir: new THREE.Vector3(-1, 0, 0)
          },
          {
            text: `(${xMin},${yTop},${zMax})`,
            worldPos: [worldX - half, worldY + half + (size * 0.02), worldZ - half],
            dir: new THREE.Vector3(0, 0, 1)
          }
        ];

        cornerLabels.forEach((corner) => {
          const mesh = createBoxelLabelMesh(corner.text, color, labelScale);
          if (!mesh) {
            return;
          }
          orientLabelMeshToSegment(mesh, corner.dir);
          mesh.position.set(corner.worldPos[0], corner.worldPos[1], corner.worldPos[2]);
          labelGroup.add(mesh);
        });
      });

      if (!labelGroup.children.length) {
        return;
      }
      scene.add(labelGroup);
      boxelLabelGroup = labelGroup;
    }

    function scheduleBoxelLabelRefresh(delayMs = 90) {
      if (!boxelOverlayShowLabels) {
        return;
      }
      if (!boxelOverlayEnabled) {
        return;
      }
      if (boxelLabelRefreshTimer) {
        clearTimeout(boxelLabelRefreshTimer);
      }
      boxelLabelRefreshTimer = setTimeout(() => {
        boxelLabelRefreshTimer = null;
        updateBoxelProximityLabels();
      }, delayMs);
    }

    function getAdaptiveMinMcodeForRadius(radiusLy) {
      const radius = Number(radiusLy);
      if (!Number.isFinite(radius) || radius <= 0) {
        return boxelOverlayMinMcode;
      }
      if (reverseDebugGridDetailOrder) {
        if (radius < 70) return 'a';
        if (radius < 180) return 'b';
        if (radius < 450) return 'c';
        return 'd';
      }
      if (radius < 70) return 'd';
      if (radius < 180) return 'c';
      if (radius < 450) return 'b';
      return 'a';
    }

    function getDebugGridLimitForRadius(radiusLy) {
      const radius = Number(radiusLy);
      if (!Number.isFinite(radius) || radius <= 0) {
        return 12000;
      }
      if (radius < 80) return 8000;
      if (radius < 220) return 12000;
      if (radius < 500) return 18000;
      return boxelOverlayMaxRequestLimit;
    }

    function updateDebugGridLegend(boxels = [], payload = null) {
      const legend = document.getElementById('debugGridLegend');
      if (!legend) {
        return;
      }
      if (!boxelOverlayEnabled || !Array.isArray(boxels) || !boxels.length) {
        legend.style.display = 'none';
        legend.innerHTML = '';
        return;
      }

      const countsByMcode = new Map();
      boxels.forEach((item) => {
        const mcode = String(item?.mcode || '').toLowerCase();
        if (mcode < 'a' || mcode > 'h') {
          return;
        }
        countsByMcode.set(mcode, (countsByMcode.get(mcode) || 0) + 1);
      });

      const ordered = Array.from(countsByMcode.entries())
        .sort((a, b) => a[0].localeCompare(b[0]));
      const rows = ordered.map(([mcode, count]) => {
        const hex = colorForBoxelMcode(mcode).toString(16).padStart(6, '0');
        const size = mcodeCubeSizeLy(mcode);
        const sizeLabel = Number.isFinite(size) ? `${size} LY` : '?';
        return `
          <div class="legend-row">
            <span class="legend-chip" style="background:#${hex}"></span>
            <span>${mcode.toUpperCase()} · ${sizeLabel} · ${count}</span>
          </div>
        `;
      }).join('');

      const modeLabel = String(payload?.mode || 'adaptive');
      const visibleCells = Array.isArray(boxels) ? boxels.length : 0;
      const isTruncated = Boolean(payload?.truncated);
      const truncationLabel = isTruncated ? ' - truncated' : '';
      legend.innerHTML = `
        <div class="legend-title">Debug Grid (${modeLabel})</div>
        <div>Cells: ${visibleCells}${truncationLabel}</div>
        ${rows}
      `;
      legend.style.display = 'block';
    }

    function renderBoxelOverlay(boxels = [], payload = null) {
      clearBoxelOverlay();
      if (!Array.isArray(boxels) || !boxels.length || typeof THREE === 'undefined' || typeof scene === 'undefined' || !scene) {
        updateDebugGridLegend([], payload);
        return;
      }
      const group = new THREE.Group();
      group.name = 'boxelOverlayGroup';
      boxels.forEach((boxel) => {
        const center = boxel?.center;
        const size = Number(boxel?.size);
        const mcode = String(boxel?.mcode || '').toLowerCase();
        if (!center || !Number.isFinite(size) || size <= 0) {
          return;
        }
        const geometry = new THREE.BoxGeometry(size, size, size);
        const edges = new THREE.EdgesGeometry(geometry);
        geometry.dispose();
        const lines = new THREE.LineSegments(
          edges,
          new THREE.LineBasicMaterial({
            color: colorForBoxelMcode(mcode),
            transparent: true,
            opacity: 0.44
          })
        );
        lines.position.set(
          Number(center.x) || 0,
          Number(center.y) || 0,
          -(Number(center.z) || 0)
        );
        group.add(lines);
      });
      scene.add(group);
      boxelOverlayGroup = group;
      boxelOverlayCells = boxels;
      updateDebugGridLegend(boxels, payload);
      updateBoxelProximityLabels();
    }

    async function refreshBoxelOverlay() {
      if (!boxelOverlayEnabled) {
        clearBoxelOverlay();
        return;
      }
      const center = getCurrentMapCenter() || lastAutoLoadCenter;
      if (!center) {
        clearBoxelOverlay();
        return;
      }
      const requestId = ++boxelOverlayRequestId;
      const radius = Number(activeNeighborhoodRadius || getAutoRefreshRadius() || 20);
      const adaptiveMinMcode = getAdaptiveMinMcodeForRadius(radius);
      const requestLimit = getDebugGridLimitForRadius(radius);
      const url = new URL('/boxels', sameHostBaseUrl);
      url.searchParams.set('x', String(center.x));
      url.searchParams.set('y', String(center.y));
      url.searchParams.set('z', String(center.z));
      url.searchParams.set('radius', String(radius));
      url.searchParams.set('mode', boxelOverlayMode);
      url.searchParams.set('min_mcode', adaptiveMinMcode);
      url.searchParams.set('max_mcode', boxelOverlayMaxMcode);
      url.searchParams.set('limit', String(requestLimit));

      try {
        const response = await fetch(url.toString());
        if (!response.ok) {
          throw new Error(`Debug grid query failed (${response.status})`);
        }
        const payload = await response.json();
        if (requestId !== boxelOverlayRequestId || !boxelOverlayEnabled) {
          return;
        }
        renderBoxelOverlay(Array.isArray(payload?.items) ? payload.items : [], payload);
      } catch (error) {
        console.error('Failed to render boxel overlay', error);
        if (requestId === boxelOverlayRequestId) {
          clearBoxelOverlay();
        }
      }
    }

    async function toggleBoxelOverlay() {
      if (!experimentalFeaturesEnabled) {
        return;
      }
      boxelOverlayEnabled = !boxelOverlayEnabled;
      updateBoxelOverlayButtonState();
      if (boxelOverlayEnabled) {
        await refreshBoxelOverlay();
      } else {
        clearBoxelOverlay();
      }
    }

    function toggleDistanceHeat() {
      distanceHeatEnabled = !distanceHeatEnabled;
      updateDistanceHeatButtonState();
      applyActivePointColorMode();
    }

    async function togglePredictedSystems() {
      if (!experimentalFeaturesEnabled) {
        return;
      }
      predictedSystemsEnabled = !predictedSystemsEnabled;
      updatePredictedSystemsButtonState();
      const center = getCurrentMapCenter() || lastAutoLoadCenter;
      if (!center || externalSolutionJson) {
        const nextUrl = new URL(window.location.href);
        if (predictedSystemsEnabled) {
          nextUrl.searchParams.set('predicted', '1');
        } else {
          nextUrl.searchParams.delete('predicted');
        }
        window.history.replaceState({}, '', nextUrl);
        return;
      }
      updateBrowserUrlFromCurrentCenter(center);
      await reloadSystemsAroundCurrentCamera(center, { force: true });
    }

    function syncNeighborhoodFilterControls() {
      const atmosphereGasFilterInput = document.getElementById('atmosphereGasFilter');
      const materialFilterInput = document.getElementById('materialFilter');
      const filters = getCurrentNeighborhoodFilters();
      if (atmosphereGasFilterInput) {
        atmosphereGasFilterInput.value = filters.atmosphereGas;
      }
      if (materialFilterInput) {
        materialFilterInput.value = filters.material;
      }
    }

    function updateModeButtonState() {
      const expertModeToggle = document.getElementById('expertModeToggle');
      if (expertModeToggle) {
        const isExpert = activeFilterMode === 'expert';
        expertModeToggle.checked = isExpert;
        expertModeToggle.title = 'Expert Mode';
        expertModeToggle.setAttribute('aria-label', 'Expert Mode');
      }
      const reverseDebugGridDetailToggle = document.getElementById('reverseDebugGridDetailToggle');
      if (reverseDebugGridDetailToggle) {
        reverseDebugGridDetailToggle.checked = reverseDebugGridDetailOrder;
        reverseDebugGridDetailToggle.title = 'Reverse Debug Grid Detail';
        reverseDebugGridDetailToggle.setAttribute('aria-label', 'Reverse Debug Grid Detail');
      }
      const regionNamesToggle = document.getElementById('regionNamesToggle');
      if (regionNamesToggle) {
        regionNamesToggle.checked = regionNamesEnabled;
        regionNamesToggle.title = 'Show Region Names';
        regionNamesToggle.setAttribute('aria-label', 'Show Region Names');
      }
      const showBaseGridToggle = document.getElementById('showBaseGridToggle');
      if (showBaseGridToggle) {
        showBaseGridToggle.checked = dynamicBaseGridEnabled;
        showBaseGridToggle.title = 'Show Base Grid';
        showBaseGridToggle.setAttribute('aria-label', 'Show Base Grid');
      }
      const experimentalFeaturesToggle = document.getElementById('experimentalFeaturesToggle');
      if (experimentalFeaturesToggle) {
        experimentalFeaturesToggle.checked = experimentalFeaturesEnabled;
        experimentalFeaturesToggle.title = 'Experimental Features';
        experimentalFeaturesToggle.setAttribute('aria-label', 'Experimental Features');
      }
      applyExperimentalFeatureButtonsVisibility();
      removeUtilityFiltersFromHud();
    }

    async function setFilterMode(nextMode) {
      const normalizedMode = normalizeFilterMode(nextMode);
      if (activeFilterMode === normalizedMode) {
        updateModeButtonState();
        return;
      }
      activeFilterMode = normalizedMode;
      updateModeButtonState();

      if (externalSolutionJson) {
        return;
      }

      const center = getCurrentMapCenter() || lastAutoLoadCenter;
      if (!center) {
        const nextUrl = new URL(window.location.href);
        if (activeFilterMode === 'expert') {
          nextUrl.searchParams.set('mode', 'expert');
        } else {
          nextUrl.searchParams.delete('mode');
        }
        applyFilterDimensionToSearchParams(nextUrl.searchParams);
        applyNeighborhoodFiltersToSearchParams(nextUrl.searchParams);
        window.history.replaceState({}, '', nextUrl);
        return;
      }

      updateBrowserUrlFromCurrentCenter(center);
      await reloadSystemsAroundCurrentCamera(center, { force: true });
    }

    async function setFilterDimension(nextDimension) {
      const normalizedDimension = normalizeFilterDimension(nextDimension);
      if (activeFilterDimension === normalizedDimension) {
        applyHudFilterPanelTitle();
        updateFilterDimensionButtonState();
        return;
      }
      activeFilterDimension = normalizedDimension;
      applyHudFilterPanelTitle();
      updateFilterDimensionButtonState();

      if (externalSolutionJson) {
        return;
      }

      const center = getCurrentMapCenter() || lastAutoLoadCenter;
      if (!center) {
        const nextUrl = new URL(window.location.href);
        applyFilterDimensionToSearchParams(nextUrl.searchParams);
        applyNeighborhoodFiltersToSearchParams(nextUrl.searchParams);
        window.history.replaceState({}, '', nextUrl);
        return;
      }

      updateBrowserUrlFromCurrentCenter(center);
      await reloadSystemsAroundCurrentCamera(center, { force: true });
    }

    async function toggleOrSetFilterDimension(nextDimension) {
      const normalizedDimension = normalizeFilterDimension(nextDimension);
      if (activeFilterDimension === normalizedDimension) {
        setHudPanelVisibility(!isHudPanelVisible);
        return;
      }
      setHudPanelVisibility(true);
      await setFilterDimension(normalizedDimension);
    }

    async function setNeighborhoodFilters(nextFilters) {
      const normalizedFilters = normalizeNeighborhoodFilters(nextFilters);
      const currentFilters = getCurrentNeighborhoodFilters();
      const unchanged = (
        currentFilters.atmosphereGas === normalizedFilters.atmosphereGas
        && currentFilters.material === normalizedFilters.material
      );
      activeNeighborhoodFilters = normalizedFilters;
      syncNeighborhoodFilterControls();

      if (unchanged) {
        return;
      }

      if (externalSolutionJson) {
        return;
      }

      const center = getCurrentMapCenter() || lastAutoLoadCenter;
      if (!center) {
        const nextUrl = new URL(window.location.href);
        applyNeighborhoodFiltersToSearchParams(nextUrl.searchParams);
        window.history.replaceState({}, '', nextUrl);
        return;
      }

      updateBrowserUrlFromCurrentCenter(center);
      await reloadSystemsAroundCurrentCamera(center, { force: true });
    }

    function suppressCameraRefresh(durationMs = 1500) {
      suppressCameraRefreshUntil = Math.max(suppressCameraRefreshUntil, Date.now() + durationMs);
    }

    function isCameraRefreshSuppressed() {
      return Date.now() < suppressCameraRefreshUntil;
    }

    window.EDGIS_SUPPRESS_CAMERA_REFRESH = suppressCameraRefresh;

     function initSolutionJson(x, y, z, mode = "simple", dimension = "spectral") {
       const normalizedDimension = normalizeFilterDimension(dimension);
       if (normalizedDimension !== 'spectral') {
         const unknownCategoryName = normalizedDimension === 'atmosphere'
           ? 'Unknown Atmosphere'
           : 'Unknown Material';
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
               "Predicted Systems": {
                 name: "Predicted Systems",
                 color: "49d3ff"
               },
               [unknownCategoryName]: {
                 name: unknownCategoryName,
                 color: "999999"
               }
             }
           },
           systems: [],
           routes: []
         };
       }
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
               "Predicted Systems": {
                 name: "Predicted Systems",
                 color: "49d3ff"
               },

               // Main sequence (hot to cool)
               "O (Blue-White) Star": { name: "O (Blue-White) Star", color: "f5fcfe" },
               "B (Blue-White) Star": { name: "B (Blue-White) Star", color: "f1fdfd" },
               "A (Blue-White) Star": { name: "A (Blue-White) Star", color: "f8fafd" },
               "F (White) Star": { name: "F (White) Star", color: "fcf8e3" },
               "G (White-Yellow) Star": { name: "G (White-Yellow) Star", color: "faefcd" },
               "K (Yellow-Orange) Star": { name: "K (Yellow-Orange) Star", color: "feeace" },
               "M (Red dwarf) Star": { name: "M (Red dwarf) Star", color: "f8ce9d" },

               // Giants & supergiants
               "B (Blue-White super giant) Star": { name: "B (Blue-White super giant) Star", color: "e5e9f1" },
               "A (Blue-White super giant) Star": { name: "A (Blue-White super giant) Star", color: "fafdfe" },
               "F (White super giant) Star": { name: "F (White super giant) Star", color: "fdf1cb" },
               "G (White-Yellow super giant) Star": { name: "G (White-Yellow super giant) Star", color: "f6e5b4" },
               "K (Yellow-Orange giant) Star": { name: "K (Yellow-Orange giant) Star", color: "fee3ab" },
               "M (Red giant) Star": { name: "M (Red giant) Star", color: "f0b955" },
               "M (Red super giant) Star": { name: "M (Red super giant) Star", color: "e48c46" },

               // Pre-main sequence
               "T Tauri Star": { name: "T Tauri Star", color: "e2f2fe" },
               "Herbig Ae/Be Star": { name: "Herbig Ae/Be Star", color: "ffe6b1" },

               // Wolf-Rayet
               "Wolf-Rayet Star": { name: "Wolf-Rayet Star", color: "fec2fe" },
               "Wolf-Rayet N Star": { name: "Wolf-Rayet N Star", color: "f5fcfb" },
               "Wolf-Rayet C Star": { name: "Wolf-Rayet C Star", color: "f0fafb" },
               "Wolf-Rayet O Star": { name: "Wolf-Rayet O Star", color: "e1e8f1" },
               "Wolf-Rayet NC Star": { name: "Wolf-Rayet NC Star", color: "e2e7f0" },

               // Carbon-rich & chemically peculiar
               "C Star": { name: "C Star", color: "981055" },
               "CN Star": { name: "CN Star", color: "fecd8f" },
               "CJ Star": { name: "CJ Star", color: "f9b66a" },
               "S-type Star": { name: "S-type Star", color: "ffdead" },
               "MS-type Star": { name: "MS-type Star", color: "fcca88" },

               // Brown dwarfs
               "L (Brown dwarf) Star": { name: "L (Brown dwarf) Star", color: "a52a2a" },
               "T (Brown dwarf) Star": { name: "T (Brown dwarf) Star", color: "8b4513" },
               "Y (Brown dwarf) Star": { name: "Y (Brown dwarf) Star", color: "a0522d" },

               // White dwarfs
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

               // Compact remnants
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
               "Predicted Systems": {
                 name: "Predicted Systems",
                 color: "49d3ff"
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
      const [spherejson, predictedjson] = await Promise.all([
        fetchNeighborsDataset(x, y, z, radius, "initial"),
        fetchPredictedDataset(x, y, z, radius)
      ]);
      if ((spherejson && spherejson.length > 0) || (predictedjson && predictedjson.length > 0)) {
        populateResult(
          Array.isArray(spherejson) ? spherejson : [],
          res,
          radius,
          mode,
          focusTarget,
          targetCenter,
          Array.isArray(predictedjson) ? predictedjson : []
        );
      }
     }

     function hslToHex(h, s, l) {
       const a = s * Math.min(l, 1 - l);
       const f = (n) => {
         const k = (n + h * 12) % 12;
         const color = l - a * Math.max(Math.min(k - 3, 9 - k, 1), -1);
         return Math.round(255 * color).toString(16).padStart(2, '0');
       };
       return `${f(0)}${f(8)}${f(4)}`;
     }

     function colorFromFacetName(name) {
       let hash = 0;
       const input = String(name || '');
       for (let i = 0; i < input.length; i++) {
         hash = ((hash << 5) - hash) + input.charCodeAt(i);
         hash |= 0;
       }
       const hue = Math.abs(hash % 360) / 360;
       return hslToHex(hue, 0.62, 0.56);
     }

     function normalizeFacetValues(values) {
       if (!Array.isArray(values)) {
         return [];
       }
       const unique = new Set();
       values.forEach((value) => {
         const normalized = String(value || '').trim();
         if (normalized) {
           unique.add(normalized);
         }
       });
       return Array.from(unique);
     }

     function ensureFacetCategories(res, spherejson, dimension) {
       const facetKey = dimension === 'atmosphere' ? 'atmosphere_gases' : 'materials';
       const unknownName = dimension === 'atmosphere' ? 'Unknown Atmosphere' : 'Unknown Material';
       const categoryBucket = res?.categories?.EDGIS;
       if (!categoryBucket) {
         return;
       }

       const discovered = new Set();
       spherejson.forEach((systemObj) => {
         normalizeFacetValues(systemObj?.[facetKey]).forEach((name) => discovered.add(name));
       });
       discovered.add(unknownName);

       discovered.forEach((categoryName) => {
         if (!categoryBucket[categoryName]) {
           categoryBucket[categoryName] = {
             name: categoryName,
             color: categoryName === unknownName ? '999999' : colorFromFacetName(categoryName)
           };
         }
       });
     }


     function populateResult(spherejson, res, radius, mode = "simple", focusTarget = true, targetCenter = null, predictedjson = []) {
       const normalizedDimension = normalizeFilterDimension(activeFilterDimension);
       if (normalizedDimension !== 'spectral') {
         ensureFacetCategories(res, spherejson, normalizedDimension);
       }
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

           let categories = [];
           if (normalizedDimension === 'spectral') {
             // Step 2: pick category depending on mode
             const category = mode === "expert" ? mainStar : (categoryMap[mainStar] || "Non Sequence Stars");
             categories = [category];
             if (!starCategories.hasOwnProperty(category)) {
               console.warn(`Warning: Main star "${mainStar}" maps to missing category "${category}"`);
             }
           } else {
             const facetKey = normalizedDimension === 'atmosphere' ? 'atmosphere_gases' : 'materials';
             const unknownName = normalizedDimension === 'atmosphere' ? 'Unknown Atmosphere' : 'Unknown Material';
             const facetCategories = normalizeFacetValues(s?.[facetKey]);
             categories = facetCategories.length ? facetCategories : [unknownName];
           }

           return {
             name: `${s.name} (${s.distance.toFixed(2)} LY)`,
             coords: { ...s.coords, radius: 0 },
             cat: [...categories, "Neighbors"],
             infos: { ...s, mainStar, radius }
          };
         })
       );

       if (Array.isArray(predictedjson) && predictedjson.length > 0) {
         res.systems.push(
           ...predictedjson.map((item, index) => {
             const px = Number(item?.coords?.x);
             const py = Number(item?.coords?.y);
             const pz = Number(item?.coords?.z);
             if (!Number.isFinite(px) || !Number.isFinite(py) || !Number.isFinite(pz)) {
               return null;
             }
             const predictedName = String(item?.name || `Predicted ${index + 1}`);
             return {
               name: predictedName,
               coords: { x: px, y: py, z: pz, radius: 0 },
               cat: ["Predicted Systems"],
               infos: {
                 name: predictedName,
                 distance: null,
                 mainStar: 'Predicted (EDTS)',
                 predicted: true,
                 uncertainty: Number(item?.uncertainty) || null,
                 mcode: String(item?.mcode || '')
               }
             };
           }).filter(Boolean)
         );
       }
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
      const filters = getCurrentNeighborhoodFilters();
      return [
        roundCacheCoord(center.x),
        roundCacheCoord(center.y),
        roundCacheCoord(center.z),
        formatRadiusValue(radius),
        filters.atmosphereGas.toLowerCase(),
        filters.material.toLowerCase()
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
        const filters = getCurrentNeighborhoodFilters();
        const sphereurl = new URL('/neighbors', sameHostBaseUrl);
        sphereurl.searchParams.set('x', String(center.x));
        sphereurl.searchParams.set('y', String(center.y));
        sphereurl.searchParams.set('z', String(center.z));
        sphereurl.searchParams.set('radius', String(radius));
        sphereurl.searchParams.set('include_facets', '1');
        if (filters.atmosphereGas) {
          sphereurl.searchParams.set('atmosphere_gas', filters.atmosphereGas);
        }
        if (filters.material) {
          sphereurl.searchParams.set('material', filters.material);
        }
        const dataset = await getSystemCoordinates(sphereurl.toString());
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
      applyHudFilterSearchAndSort();
    }

    function showInfoPanel() {
      const infoPanel = document.getElementById("InfoPanel");
      const controlsPanel = document.getElementById('controlsPanel');
      const settingsPanel = document.getElementById('settingsPanel');
      if (controlsPanel) {
        controlsPanel.style.display = 'none';
      }
      if (settingsPanel) {
        settingsPanel.style.display = 'none';
      }
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
              <li><span style="font-size: x-large;margin-top: -7px;">${systemName}</span> <span><a title="CENTER VIEW" href="${buildGalaxyMapViewUrl(coords, radius)}"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-border-center" viewBox="0 0 16 16">
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

    function collectActiveFilterIds() {
      if (!window.$) {
        return [];
      }
      const active = [];
      $('.map_filter').each(function () {
        if (Number($(this).data('active')) === 1) {
          active.push(String($(this).data('filter')));
        }
      });
      return active;
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

    async function fetchPredictedDataset(x, y, z, radius) {
      if (!predictedSystemsEnabled || externalSolutionJson) {
        return [];
      }
      const requestedLimit = 10000;
      try {
        const predictedUrl = new URL('/predicted-systems', sameHostBaseUrl);
        predictedUrl.searchParams.set('x', String(x));
        predictedUrl.searchParams.set('y', String(y));
        predictedUrl.searchParams.set('z', String(z));
        predictedUrl.searchParams.set('radius', String(radius));
        predictedUrl.searchParams.set('limit', String(requestedLimit));
        let response = await fetch(predictedUrl.toString());
        if (response.status === 422) {
          // Graceful fallback if a stale client/build sends an out-of-range limit.
          predictedUrl.searchParams.set('limit', '10000');
          response = await fetch(predictedUrl.toString());
        }
        if (!response.ok) {
          throw new Error(`Predicted query failed (${response.status})`);
        }
        const payload = await response.json();
        return Array.isArray(payload?.items) ? payload.items : [];
      } catch (error) {
        console.error('Failed to fetch predicted systems', error);
        return [];
      }
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
      clearDynamicBaseGrid();
      clearBoxelOverlay();
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

    function reloadDynamicNeighborhood(solutionjson, viewState, selectedFilterIds) {
      resetDynamicMapState();
      Ed3d.loadDatas(solutionjson);
      System.endParticleSystem();
      HUD.init();
      pendingSharedFilterIds = normalizeSharedFilterIds(selectedFilterIds);
      syncHudPanelUi();
      buildDynamicBaseGridOverlay();
      updateTrackedSolidSystemNames(solutionjson);
      refreshHudFilterCounts();
      restoreView(viewState);
    }

    async function reloadSystemsAroundCurrentCamera(center, options = {}) {
      if (!center || externalSolutionJson) {
        setEdgisHomeLoadingState(false);
        return;
      }
      if (!options.force && isCameraRefreshSuppressed()) {
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
        const [spherejson, predictedjson] = await Promise.all([
          fetchNeighborsDataset(center.x, center.y, center.z, autoRefreshRadius, 'reload'),
          fetchPredictedDataset(center.x, center.y, center.z, autoRefreshRadius)
        ]);
        if (
          requestId !== autoRefreshRequestId
          || !Array.isArray(spherejson)
          || (!spherejson.length && !(Array.isArray(predictedjson) && predictedjson.length))
        ) {
          return;
        }
        const viewState = {
          camera: getCurrentWorldCamera(),
          target: getCurrentInternalTarget()
        };
        const activeFilterIds = collectActiveFilterIds();
        const nextResult = initSolutionJson(center.x, center.y, center.z, activeFilterMode, activeFilterDimension);
        populateResult(
          spherejson,
          nextResult,
          autoRefreshRadius,
          activeFilterMode,
          false,
          center,
          predictedjson
        );
        reloadDynamicNeighborhood(nextResult, viewState, activeFilterIds);
        cacheNeighborhoodDataset(center, autoRefreshRadius, spherejson, 'reload');
        lastAutoLoadCenter = center;
        activeNeighborhoodRadius = autoRefreshRadius;
        updateRadiusDisplay();
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
      let controlsMovedDuringInteraction = false;
      const initialCenter = getCurrentMapCenter();
      if (initialCenter) {
        lastAutoLoadCenter = initialCenter;
        lastCameraTarget = initialCenter;
      }
      controls.addEventListener('start', () => {
        controlsMovedDuringInteraction = false;
      });
      controls.addEventListener('change', () => {
        controlsMovedDuringInteraction = true;
        scheduleDynamicBaseGridRefresh(140);
        if (boxelOverlayEnabled) {
          scheduleBoxelLabelRefresh(110);
        }
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
        if (controlsMovedDuringInteraction) {
          suppressSelectionNeighborhoodJump(500);
        }
        updateBrowserUrlFromCurrentCenter(center);
        if (isCameraRefreshSuppressed()) {
          lastCameraTarget = center;
          scheduleDynamicBaseGridRefresh(0);
          if (boxelOverlayEnabled) {
            scheduleBoxelLabelRefresh(0);
          }
          return;
        }
        const centerMoved = !lastCameraTarget || distanceBetweenCenters(center, lastCameraTarget) > CAMERA_CENTER_CHANGE_EPSILON;
        lastCameraTarget = center;
        if (centerMoved) {
          scheduleCameraNeighborhoodRefresh({
            center
          });
        }
        scheduleDynamicBaseGridRefresh(0);
        if (boxelOverlayEnabled) {
          scheduleBoxelLabelRefresh(0);
        }
      });
    }

    function clamp(value, minValue, maxValue) {
      return Math.min(Math.max(value, minValue), maxValue);
    }

    function applyUserStarVisualSettings() {
      const effectiveOpacity = clamp(baseParticleOpacity * userStarBrightnessScale * userStarOpacityScale, 0.01, 1);
      const effectiveScaleFactor = clamp(baseParticleScaleFactor * userStarSizeScale, 0.02, 10);
      const effectiveScaleMin = clamp(baseEffectScaleMin * userStarSizeScale, 0.02, 30);
      const effectiveScaleMax = clamp(baseEffectScaleMax * userStarSizeScale, effectiveScaleMin + 0.1, 300);

      if (window.System) {
        window.System.opacity = effectiveOpacity;
        if (window.System.particle?.material) {
          window.System.particle.material.opacity = effectiveOpacity;
          window.System.particle.material.needsUpdate = true;
        }
      }

      Ed3d.systemSizeScaleFactor = effectiveScaleFactor;
      Ed3d.effectScaleSystem = [effectiveScaleMin, effectiveScaleMax];

      if (
        window.Action
        && typeof window.Action.sizeOnScroll === 'function'
        && typeof window.distanceFromTarget === 'function'
        && typeof camera !== 'undefined'
        && camera
      ) {
        const scale = distanceFromTarget(camera) / 200;
        window.Action.sizeOnScroll(scale);
      }
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
      baseParticleOpacity = densityProfile?.particleOpacity || 0.76;
      baseParticleScaleFactor = densityProfile?.particleScaleFactor || 1;
      baseEffectScaleMin = densityProfile?.effectScaleMin || 1;
      baseEffectScaleMax = densityProfile?.effectScaleMax || 24;
      const initialEffectScaleMin = clamp(baseEffectScaleMin * userStarSizeScale, 0.02, 30);
      const initialEffectScaleMax = clamp(baseEffectScaleMax * userStarSizeScale, initialEffectScaleMin + 0.1, 300);
      applyUserStarVisualSettings();
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
        showGalaxyInfos: regionNamesEnabled,
        showNameNear: false,
        playerPos: playerPos,
        cameraPos: cameraPos,
        effectScaleSystem : [initialEffectScaleMin, initialEffectScaleMax],
        finished: function () {
          attachCameraNeighborhoodRefresh();
          refreshHudFilterCounts();
          syncHudPanelUi();
          buildDynamicBaseGridOverlay();
          applyRegionNamesVisibility();
          applyUserStarVisualSettings();
        }
      });
    }

    let autoSelectRequestId = 0;
    let suppressSelectionNeighborhoodJumpUntil = 0;

    function suppressSelectionNeighborhoodJump(durationMs = 1200) {
      suppressSelectionNeighborhoodJumpUntil = Math.max(suppressSelectionNeighborhoodJumpUntil, Date.now() + durationMs);
    }

    function shouldSuppressSelectionNeighborhoodJump() {
      return Date.now() < suppressSelectionNeighborhoodJumpUntil;
    }

    function normalizeSystemNameForSelection(value) {
      if (value == null) {
        return '';
      }
      return String(value).replace(/\s+\(\d+(?:\.\d+)?\s+LY\)\s*$/i, '').trim().toLowerCase();
    }

    function queueAutoSelectByName(systemName, fallbackInfos, attemptsLeft = 25) {
      if (!systemName) {
        return Promise.resolve(false);
      }

      const requestId = ++autoSelectRequestId;
      const normalizedTargetName = normalizeSystemNameForSelection(systemName);

      return new Promise((resolve) => {
        const trySelect = (remaining) => {
          if (requestId !== autoSelectRequestId || remaining <= 0) {
            resolve(false);
            return;
          }

          const vertices = window.System?.particleGeo?.vertices;
          const actionReady = window.Action && typeof window.Action.moveToObj === 'function';
          const canSelect = vertices && actionReady;

          if (canSelect) {
            for (let index = 0; index < vertices.length; index++) {
              const vertex = vertices[index];
              const vertexDisplayName = vertex?.name ?? '';
              const vertexCanonicalName = vertex?.infos?.name ?? '';
              const normalizedDisplayName = normalizeSystemNameForSelection(vertexDisplayName);
              const normalizedCanonicalName = normalizeSystemNameForSelection(vertexCanonicalName);
              const matchesName = normalizedDisplayName === normalizedTargetName || normalizedCanonicalName === normalizedTargetName;
              if (!vertex || !matchesName) {
                continue;
              }
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
                  $(document).trigger('systemClick', [vertex.name, payload, vertex.url ?? null, { auto: true }]);
                }
              } catch (error) {
                console.warn('Failed to auto-select system:', error);
              }
              resolve(true);
              return;
            }
          }

          setTimeout(() => trySelect(remaining - 1), 300);
        };

        setTimeout(() => trySelect(attemptsLeft), 0);
      });
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

    async function focusOnSelectedSystemNeighborhood(selectedInfo, radius = 20) {
      if (!selectedInfo?.coords) {
        return;
      }

      const x = Number(selectedInfo.coords.x);
      const y = Number(selectedInfo.coords.y);
      const z = Number(selectedInfo.coords.z);
      if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) {
        return;
      }

      const nextRadius = Number(radius);
      const viewCamera = getCurrentWorldCamera();
      const viewTarget = getCurrentInternalTarget();
      const currentOffset = (viewCamera && viewTarget)
        ? {
            x: viewCamera.x - viewTarget.x,
            y: viewCamera.y - viewTarget.y,
            z: viewCamera.z - viewTarget.z
          }
        : { x: 0, y: nextRadius * 1.5, z: -nextRadius * 1.5 };

      setEdgisHomeLoadingState(true);
      try {
        const [spherejson, predictedjson] = await Promise.all([
          fetchNeighborsDataset(x, y, z, nextRadius, 'selection'),
          fetchPredictedDataset(x, y, z, nextRadius)
        ]);
        if (!Array.isArray(spherejson) || (!spherejson.length && !(Array.isArray(predictedjson) && predictedjson.length))) {
          return;
        }
        const nextResult = initSolutionJson(x, y, z, activeFilterMode, activeFilterDimension);
        populateResult(
          spherejson,
          nextResult,
          nextRadius,
          activeFilterMode,
          false,
          { x, y, z },
          predictedjson
        );
        const activeFilterIds = collectActiveFilterIds();
        const viewState = {
          camera: {
            x: x + currentOffset.x,
            y: y + currentOffset.y,
            z: (-z) + currentOffset.z
          },
          target: {
            x,
            y,
            z: -z
          }
        };
        reloadDynamicNeighborhood(nextResult, viewState, activeFilterIds);
        cacheNeighborhoodDataset({ x, y, z }, nextRadius, spherejson, 'selection');
        lastAutoLoadCenter = { x, y, z };
        lastCameraTarget = { x, y, z };
        activeNeighborhoodRadius = nextRadius;
        updateRadiusDisplay();
        updateBrowserUrlFromCurrentCenter({ x, y, z });
        suppressSelectionNeighborhoodJump();
        const didSelectTarget = await queueAutoSelectByName(selectedInfo.name, selectedInfo);
        if (!didSelectTarget) {
          autoSelectNearestVisibleSystem(nextResult);
        }
      } catch (error) {
        console.error('Failed to focus selected system neighborhood', error);
      } finally {
        setEdgisHomeLoadingState(false);
      }
    }

    async function drawSystems(x, y, z, radius, mode = activeFilterMode) {
      setEdgisHomeLoadingState(true);
      try {
       const effectiveMode = normalizeFilterMode(mode);
       activeFilterMode = effectiveMode;
       updateModeButtonState();
       activeNeighborhoodRadius = radius;
       updateRadiusDisplay();
       const solutionjson = initSolutionJson(x, y, z, effectiveMode, activeFilterDimension);
       await drawSolution(x, y, z, radius, solutionjson, effectiveMode, false, { x, y, z });
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
         mode: params.get('mode'),
         filter_dimension: params.get('filter_dimension'),
         atmosphere_gas: params.get('atmosphere_gas'),
         material: params.get('material'),
         heat: params.get('heat'),
         boxels: params.get('boxels'),
         predicted: params.get('predicted'),
         filter_ids: params.getAll('filter_ids'),
         region_names: params.get('region_names'),
         grid_detail: params.get('grid_detail'),
         base_grid: params.get('base_grid'),
         experimental_features: params.get('experimental_features')
       };
       activeFilterMode = normalizeFilterMode(raw.mode);
       activeFilterDimension = normalizeFilterDimension(raw.filter_dimension);
       activeNeighborhoodFilters = normalizeNeighborhoodFilters({
         atmosphereGas: raw.atmosphere_gas,
         material: raw.material
       });
       distanceHeatEnabled = raw.heat === '1';
       boxelOverlayEnabled = raw.boxels === '1';
       predictedSystemsEnabled = raw.predicted === '1';
       regionNamesEnabled = String(raw.region_names || '1') !== '0';
       reverseDebugGridDetailOrder = String(raw.grid_detail || '').toLowerCase() === 'reverse';
       dynamicBaseGridEnabled = String(raw.base_grid || '1') !== '0';
       experimentalFeaturesEnabled = raw.experimental_features === '1';
       pendingSharedFilterIds = parseSharedFilterIdsParam(raw.filter_ids);
       if (!experimentalFeaturesEnabled) {
         boxelOverlayEnabled = false;
         predictedSystemsEnabled = false;
       }
       // Parse numbers safely
       const parsed = {
         x: raw.x === null ? null : Number(raw.x),
         y: raw.y === null ? null : Number(raw.y),
         z: raw.z === null ? null : Number(raw.z),
         radius: raw.radius === null ? null : Number(raw.radius)
       };

       // Validation
       const problems = [];
       ['x','y','z','radius'].forEach(k => {
         if (raw[k] === null) problems.push(`Missing parameter: ${k}`);
         else if (!Number.isFinite(parsed[k])) problems.push(`Invalid number for ${k}: "${raw[k]}"`);
       });
       if (parsed.radius !== null && parsed.radius <= 0) problems.push('radius must be > 0');
       const edgisHref = document.getElementById('edgis');
       normalizeBlankTargetLinks();

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
       drawSystems(parsed.x, parsed.y, parsed.z, parsed.radius, activeFilterMode);

       // Optional: If someone changes params manually via form encoded hash, live-update
       window.addEventListener('popstate', () => location.reload());
     })();

     $( document ).on( "systemClick", async function( event, name, infos, url, meta ) {
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
       const currentSystemInput = document.getElementById('system');
       if (currentSystemInput && s.name) {
         currentSystemInput.value = s.name;
       }
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
             <li><span style="font-size: x-large;margin-top: -7px;">${systemName ?? 'Unknown'}</span> <span><a title="CENTER VIEW" href="${buildGalaxyMapViewUrl(coords, radius)}"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-border-center" viewBox="0 0 16 16">
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
      const systemMapButton = document.getElementById('systemMapButton');
      const openEdgisButton = document.getElementById('openEdgisButton');
      const distanceHeatButton = document.getElementById('distanceHeatButton');
      const boxelOverlayButton = document.getElementById('boxelOverlayButton');
      const predictedSystemsButton = document.getElementById('predictedSystemsButton');
      const searchSystemButton = document.getElementById('searchSystemButton');
      const settingsButton = document.getElementById('settingsButton');
      const filterDimensionSpectralButton = document.getElementById('filterDimensionSpectralButton');
      const filterDimensionAtmosphereButton = document.getElementById('filterDimensionAtmosphereButton');
      const filterDimensionMaterialButton = document.getElementById('filterDimensionMaterialButton');
      const expertModeToggle = document.getElementById('expertModeToggle');
      const experimentalFeaturesToggle = document.getElementById('experimentalFeaturesToggle');
      const reverseDebugGridDetailToggle = document.getElementById('reverseDebugGridDetailToggle');
      const regionNamesToggle = document.getElementById('regionNamesToggle');
      const showBaseGridToggle = document.getElementById('showBaseGridToggle');
      const radiusDownButton = document.getElementById('radiusDownButton');
      const radiusUpButton = document.getElementById('radiusUpButton');
      const controlsPanel = document.getElementById('controlsPanel');
      const settingsPanel = document.getElementById('settingsPanel');
      const systemInput = document.getElementById('system');
      const systemSuggestions = document.getElementById('systemSuggestions');
      const loadButton = document.getElementById('load');
      const starSizeRange = document.getElementById('starSizeRange');
      const starBrightnessRange = document.getElementById('starBrightnessRange');
      const starOpacityRange = document.getElementById('starOpacityRange');
      const starSizeValue = document.getElementById('starSizeValue');
      const starBrightnessValue = document.getElementById('starBrightnessValue');
      const starOpacityValue = document.getElementById('starOpacityValue');
      const resetStarSettingsButton = document.getElementById('resetStarSettingsButton');
      const infoPanel = document.getElementById("InfoPanel");
      const SYSTEM_AUTOCOMPLETE_DELAY_MS = 200;
      let systemAutocompleteTimer = null;

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

      if (systemMapButton && !isEmbeddedMode) {
        systemMapButton.addEventListener('click', () => {
          const systemName = systemData?.name ?? lastClickedSystemName;
          if (!systemName) {
            console.warn('No system selected. Click a system first.');
            return;
          }
          const url = `${sameHostBaseUrl}/static/sysmap.html?system=${encodeURIComponent(systemName)}`;
          window.open(url, "_blank");
        });
      } else if (systemMapButton) {
        systemMapButton.addEventListener('click', () => {
          const systemName = systemData?.name ?? lastClickedSystemName;
          if (!systemName) {
            console.warn('No system selected. Click a system first.');
            return;
          }
          const url = `${sameHostBaseUrl}/static/sysmap.html?system=${encodeURIComponent(systemName)}`;
          window.location.href = url;
        });
      }

      if (openEdgisButton) {
        openEdgisButton.addEventListener('click', () => {
          const targetUrl = openEdgisButton.dataset.href || buildEdgisHomeUrl(getCurrentMapCenter());
          window.location.href = targetUrl;
        });
      }

      if (distanceHeatButton) {
        distanceHeatButton.addEventListener('click', () => {
          toggleDistanceHeat();
        });
      }

      if (boxelOverlayButton) {
        boxelOverlayButton.addEventListener('click', async () => {
          await toggleBoxelOverlay();
        });
      }

      if (predictedSystemsButton) {
        predictedSystemsButton.addEventListener('click', async () => {
          await togglePredictedSystems();
        });
      }

      const isSearchPanelVisible = () => controlsPanel && controlsPanel.style.display !== 'none';
      const isSettingsPanelVisible = () => settingsPanel && settingsPanel.style.display !== 'none';
      const hideInfoPanel = () => {
        if (infoPanel) {
          infoPanel.style.display = 'none';
        }
      };
      const showSearchPanel = () => {
        if (!controlsPanel) {
          return;
        }
        hideSettingsPanel();
        hideInfoPanel();
        controlsPanel.style.display = 'block';
        if (systemInput) {
          setTimeout(() => {
            systemInput.focus();
            systemInput.select();
          }, 0);
        }
      };
      const hideSearchPanel = () => {
        if (!controlsPanel) {
          return;
        }
        controlsPanel.style.display = 'none';
      };

      const showSettingsPanel = () => {
        if (!settingsPanel) {
          return;
        }
        hideSearchPanel();
        hideInfoPanel();
        settingsPanel.style.display = 'block';
      };

      const hideSettingsPanel = () => {
        if (!settingsPanel) {
          return;
        }
        settingsPanel.style.display = 'none';
      };

      const updateStarSettingsLabels = () => {
        if (starSizeValue) {
          starSizeValue.textContent = `${Math.round(userStarSizeScale * 100)}%`;
        }
        if (starBrightnessValue) {
          starBrightnessValue.textContent = `${Math.round(userStarBrightnessScale * 100)}%`;
        }
        if (starOpacityValue) {
          starOpacityValue.textContent = `${Math.round(userStarOpacityScale * 100)}%`;
        }
      };

      const syncStarSettingsControls = () => {
        if (starSizeRange) {
          starSizeRange.value = String(Math.round(userStarSizeScale * 100));
        }
        if (starBrightnessRange) {
          starBrightnessRange.value = String(Math.round(userStarBrightnessScale * 100));
        }
        if (starOpacityRange) {
          starOpacityRange.value = String(Math.round(userStarOpacityScale * 100));
        }
        updateStarSettingsLabels();
      };

      const resetStarSettings = () => {
        userStarSizeScale = 1;
        userStarBrightnessScale = 1;
        userStarOpacityScale = 1;
        saveStoredStarSettings();
        syncStarSettingsControls();
        applyUserStarVisualSettings();
      };

      const adjustRadiusByFactor = async (factor) => {
        const center = getCurrentMapCenter();
        if (!center) {
          return;
        }
        const currentRadius = getAutoRefreshRadius();
        const nextRadius = clamp(currentRadius * factor, 1, 10000);
        activeNeighborhoodRadius = nextRadius;
        updateRadiusDisplay();
        updateBrowserUrlFromCurrentCenter(center);
        await reloadSystemsAroundCurrentCamera(center, { force: true });
      };

      const renderSystemSuggestions = (names) => {
        if (!systemSuggestions) {
          return;
        }
        systemSuggestions.innerHTML = '';
        names.forEach((name) => {
          const option = document.createElement('option');
          option.value = name;
          systemSuggestions.appendChild(option);
        });
      };

      const fetchSystemSuggestions = async (prefix) => {
        const trimmed = (prefix || '').trim();
        if (trimmed.length < 2) {
          renderSystemSuggestions([]);
          return;
        }
        try {
          const res = await fetch(`${sameHostBaseUrl}/systems/autocomplete?q=${encodeURIComponent(trimmed)}`);
          if (!res.ok) {
            return;
          }
          const data = await res.json();
          const suggestions = Array.isArray(data?.suggestions) ? data.suggestions : [];
          renderSystemSuggestions(suggestions);
        } catch (error) {
          console.error('Failed to fetch system suggestions', error);
        }
      };

      const runSystemSearch = async () => {
        const trimmedQuery = (systemInput?.value ?? '').toString().trim();
        if (!trimmedQuery) {
          return;
        }
        try {
          const response = await fetch(`${sameHostBaseUrl}/coords?q=${encodeURIComponent(trimmedQuery)}`);
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
          const payload = await response.json();
          const resolvedCoords = payload?.coords ?? payload;
          const x = Number(resolvedCoords?.x);
          const y = Number(resolvedCoords?.y);
          const z = Number(resolvedCoords?.z);
          if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) {
            return;
          }
          await focusOnSelectedSystemNeighborhood({
            name: payload.name ?? trimmedQuery,
            coords: {
              x,
              y,
              z
            }
          }, 20);
          if (systemInput) {
            systemInput.value = payload.name ?? trimmedQuery;
          }
          hideSearchPanel();
        } catch (error) {
          console.error('Failed to search system', error);
        }
      };

      if (loadButton) {
        loadButton.addEventListener('click', () => {
          runSystemSearch();
        });
      }

      if (systemInput) {
        systemInput.addEventListener('keydown', (event) => {
          if (event.key === 'Enter') {
            event.preventDefault();
            runSystemSearch();
          }
        });
        systemInput.addEventListener('input', () => {
          if (systemAutocompleteTimer) {
            clearTimeout(systemAutocompleteTimer);
          }
          systemAutocompleteTimer = setTimeout(() => {
            fetchSystemSuggestions(systemInput.value);
          }, SYSTEM_AUTOCOMPLETE_DELAY_MS);
        });
        systemInput.addEventListener('focus', () => {
          fetchSystemSuggestions(systemInput.value);
        });
      }

      if (searchSystemButton) {
        searchSystemButton.addEventListener('click', () => {
          if (isSearchPanelVisible()) {
            hideSearchPanel();
          } else {
            showSearchPanel();
          }
        });
      }

      if (settingsButton) {
        settingsButton.addEventListener('click', () => {
          if (isSettingsPanelVisible()) {
            hideSettingsPanel();
          } else {
            showSettingsPanel();
          }
        });
      }

      if (expertModeToggle) {
        expertModeToggle.addEventListener('change', async () => {
          await setFilterMode(expertModeToggle.checked ? 'expert' : 'simple');
        });
      }

      if (experimentalFeaturesToggle) {
        experimentalFeaturesToggle.addEventListener('change', () => {
          const nextValue = Boolean(experimentalFeaturesToggle.checked);
          if (experimentalFeaturesEnabled === nextValue) {
            updateModeButtonState();
            return;
          }

          experimentalFeaturesEnabled = nextValue;
          if (!experimentalFeaturesEnabled) {
            boxelOverlayEnabled = false;
            predictedSystemsEnabled = false;
            updateBoxelOverlayButtonState();
            updatePredictedSystemsButtonState();
            applyActivePointColorMode();
            clearBoxelOverlay();
          }
          updateModeButtonState();

          const center = getCurrentMapCenter() || lastAutoLoadCenter;
          if (center) {
            updateBrowserUrlFromCurrentCenter(center);
            return;
          }

          const nextUrl = new URL(window.location.href);
          if (boxelOverlayEnabled) {
            nextUrl.searchParams.set('boxels', '1');
          } else {
            nextUrl.searchParams.delete('boxels');
          }
          if (predictedSystemsEnabled) {
            nextUrl.searchParams.set('predicted', '1');
          } else {
            nextUrl.searchParams.delete('predicted');
          }
          applyExperimentalFeaturesToSearchParams(nextUrl.searchParams);
          window.history.replaceState({}, '', nextUrl);
        });
      }

      if (reverseDebugGridDetailToggle) {
        reverseDebugGridDetailToggle.addEventListener('change', async () => {
          const nextValue = Boolean(reverseDebugGridDetailToggle.checked);
          if (reverseDebugGridDetailOrder === nextValue) {
            return;
          }
          reverseDebugGridDetailOrder = nextValue;
          const center = getCurrentMapCenter() || lastAutoLoadCenter;
          if (center) {
            updateBrowserUrlFromCurrentCenter(center);
          }
          if (boxelOverlayEnabled) {
            await refreshBoxelOverlay();
          } else {
            updateModeButtonState();
          }
        });
      }

      if (regionNamesToggle) {
        regionNamesToggle.addEventListener('change', () => {
          const nextValue = Boolean(regionNamesToggle.checked);
          if (regionNamesEnabled === nextValue) {
            return;
          }
          regionNamesEnabled = nextValue;
          applyRegionNamesVisibility();
          const center = getCurrentMapCenter() || lastAutoLoadCenter;
          if (center) {
            updateBrowserUrlFromCurrentCenter(center);
          } else {
            const nextUrl = new URL(window.location.href);
            applyRegionNamesToSearchParams(nextUrl.searchParams);
            window.history.replaceState({}, '', nextUrl);
          }
          updateModeButtonState();
        });
      }

      if (showBaseGridToggle) {
        showBaseGridToggle.addEventListener('change', () => {
          const nextValue = Boolean(showBaseGridToggle.checked);
          if (dynamicBaseGridEnabled === nextValue) {
            return;
          }
          dynamicBaseGridEnabled = nextValue;
          const center = getCurrentMapCenter() || lastAutoLoadCenter;
          if (center) {
            updateBrowserUrlFromCurrentCenter(center);
          } else {
            const nextUrl = new URL(window.location.href);
            applyBaseGridToSearchParams(nextUrl.searchParams);
            window.history.replaceState({}, '', nextUrl);
          }
          if (dynamicBaseGridEnabled) {
            buildDynamicBaseGridOverlay();
          } else {
            clearDynamicBaseGrid();
          }
          updateModeButtonState();
        });
      }

      if (filterDimensionSpectralButton) {
        filterDimensionSpectralButton.addEventListener('click', async () => {
          await toggleOrSetFilterDimension('spectral');
        });
      }

      if (filterDimensionAtmosphereButton) {
        filterDimensionAtmosphereButton.addEventListener('click', async () => {
          await toggleOrSetFilterDimension('atmosphere');
        });
      }

      if (filterDimensionMaterialButton) {
        filterDimensionMaterialButton.addEventListener('click', async () => {
          await toggleOrSetFilterDimension('material');
        });
      }

      if (starSizeRange) {
        starSizeRange.addEventListener('input', () => {
          userStarSizeScale = clamp(Number(starSizeRange.value) / 100, 0.01, 10);
          updateStarSettingsLabels();
          saveStoredStarSettings();
          applyUserStarVisualSettings();
        });
      }

      if (starBrightnessRange) {
        starBrightnessRange.addEventListener('input', () => {
          userStarBrightnessScale = clamp(Number(starBrightnessRange.value) / 100, 0.01, 1);
          updateStarSettingsLabels();
          saveStoredStarSettings();
          applyUserStarVisualSettings();
        });
      }

      if (starOpacityRange) {
        starOpacityRange.addEventListener('input', () => {
          userStarOpacityScale = clamp(Number(starOpacityRange.value) / 100, 0.01, 1);
          updateStarSettingsLabels();
          saveStoredStarSettings();
          applyUserStarVisualSettings();
        });
      }

      if (resetStarSettingsButton) {
        resetStarSettingsButton.addEventListener('click', () => {
          resetStarSettings();
        });
      }

      if (radiusDownButton) {
        radiusDownButton.addEventListener('click', () => {
          adjustRadiusByFactor(0.9);
        });
      }

      if (radiusUpButton) {
        radiusUpButton.addEventListener('click', () => {
          adjustRadiusByFactor(1.1);
        });
      }

      syncStarSettingsControls();
      updateRadiusDisplay();
      updateModeButtonState();
      updateFilterDimensionButtonState();
      updateDistanceHeatButtonState();
      updateBoxelOverlayButtonState();
      updatePredictedSystemsButtonState();

      document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
          hideSearchPanel();
          hideSettingsPanel();
          if (systemInput && document.activeElement === systemInput) {
            systemInput.blur();
          }
          return;
        }
        if (event.key !== 'Enter') {
          return;
        }
        if (systemInput && document.activeElement === systemInput) {
          return;
        }
        if (isSearchPanelVisible()) {
          return;
        }
        if (isSettingsPanelVisible()) {
          return;
        }
        if (event.target && typeof event.target.closest === 'function' && event.target.closest('.map_filter, .toolbar button, .info-panel')) {
          return;
        }
        showSearchPanel();
      });
