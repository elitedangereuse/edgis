(async function(){
    const svg = document.getElementById('svg');
    const config = globalThis.EDGIS_SYSMAP_CONFIG || {};
    const svgOnlyMode = Boolean(config.svgOnly);
    const defaultSystemName = config.defaultSystemName || 'Maia';
    const infoPanel = document.getElementById('InfoPanel');
    const bodyInfoButton = document.getElementById('bodyInfoButton');
    const controlsPanel = document.getElementById('controlsPanel');
    const controlsToggleButton = document.getElementById('controlsToggleButton');
    const downloadButton = document.getElementById('downloadSvgButton');
    const openGalaxyMapButton = document.getElementById('openGalaxyMapButton');
    const openEdgisButton = document.getElementById('openEdgisButton');
    const copyEmbedButton = document.getElementById('copyEmbedButton');
    const embedPanel = document.getElementById('embedPanel');
    const embedLinkInput = document.getElementById('embedLinkInput');
    const embedLinkCopyButton = document.getElementById('embedLinkCopyButton');
    const embedCodeOutput = document.getElementById('embedCodeOutput');
    const embedCodeCopyButton = document.getElementById('embedCodeCopyButton');
    const systemSuggestions = document.getElementById('systemSuggestions');
    const sameHostBaseUrl =
          globalThis.location?.origin || `${globalThis.location?.protocol}//${globalThis.location?.host || ''}`;
    const radius = 8;
    const vGap = 25;      // vertical spacing unit
    const hGap = 30;      // horizontal spacing unit
    const rootX = 60;
    const SOL_RADIUS_KM = 696340; // actual km radius
    const EARTH_MASS_TO_SOLAR = 1 / 332946.0487; // 1 earth mass in solar masses
    const AU_IN_METERS = 149597870700;
    const BARY_CLEARANCE = 16;
    const BARY_BRACKET_STROKE = '#333';
    const BARY_ICON_OFFSET_Y = 12;
    const BARY_LABEL_OFFSET_Y = 22;
    const RING_ROTATION_DEG = -10;
    const RING_ROTATION_RAD = RING_ROTATION_DEG * (Math.PI / 180);
    const RING_LABEL_OFFSET_X = 22;
    const RING_LABEL_OFFSET_Y = -12;
    const RING_LABEL_SEGMENT_LENGTH = 3;
    let selectedNodeGroup = null;
    let selectedBodyNode = null;
    let selectionMarkerEl = null;
    let baryLayerGroup = null;
    let baryBracketLayer = null;
    let baryIconLayer = null;
    let nodeElementsById = new Map();
    let labelsLayerGroup = null;

    const systemInput = document.getElementById('system');
    const loadButton = document.getElementById('load');
    const urlParams = new URLSearchParams(globalThis.location?.search || '');
    const systemFromURL = (urlParams.get('system') || '').trim();
    const parseBodyIdParam = (value) => {
        if(value === null || value === undefined || value === ''){
            return null;
        }
        const num = Number(value);
        return Number.isFinite(num) ? num : null;
    };
    const bodyIdFromURL = parseBodyIdParam(urlParams.get('body_id') ?? urlParams.get('bodyId') ?? urlParams.get('body'));
    if (systemFromURL && systemInput) {
        systemInput.value = systemFromURL;
    }
    updateNavigationButtonState();
    const downloadMode = (() => {
        const pngFlag = (urlParams.get('png') || urlParams.get('png_only') || urlParams.get('pngOnly') || '').toLowerCase();
        if(['1', 'true', 'yes', 'on', 'png'].includes(pngFlag)){
            return 'png';
        }
        const flag = (urlParams.get('download') || '').toLowerCase();
        if(['png', 'image', 'img'].includes(flag)){
            return 'png';
        }
        if(flag && !['0', 'false', 'no', 'off'].includes(flag)){
            return 'svg';
        }
        return null;
    })();

    const isInfoPanelVisible = () => infoPanel && infoPanel.style.display !== 'none';
    const hideInfoPanel = () => {
        if(infoPanel){
            infoPanel.style.display = 'none';
        }
    };

    const isControlsPanelVisible = () => controlsPanel && controlsPanel.style.display !== 'none';
    const setControlsPanelVisible = (isVisible) => {
        if(!controlsPanel) return;
        controlsPanel.style.display = isVisible ? 'block' : 'none';
    };

    const hideControlsPanel = () => setControlsPanelVisible(false);

    const showBarycenterInfo = (node) => {
        if(!node) return;
        if(selectBodyById(node.id)){
            return;
        }
        selectedBodyNode = node;
        hideControlsPanel();
        setEmbedPanelVisible(false);
        renderBodyInfo(node);
        handleSelectionChange();
    };

    function updateUrlState(systemName, bodyId){
        if(typeof globalThis === 'undefined' || !globalThis.history || !globalThis.location) return;
        const url = new URL(globalThis.location.href);
        const activeSystem = systemName ?? resolveActiveSystemName();
        if(activeSystem){
            url.searchParams.set('system', activeSystem);
        } else {
            url.searchParams.delete('system');
        }
        const candidateId = bodyId != null ? bodyId : resolveNodeBodyId(selectedBodyNode);
        const normalizedBodyId = parseBodyIdParam(candidateId);
        if(normalizedBodyId != null){
            url.searchParams.set('body_id', normalizedBodyId);
        } else {
            url.searchParams.delete('body_id');
        }
        globalThis.history.replaceState({}, '', url);
    }

    function handleSelectionChange(){
        if(isEmbedPanelVisible()){
            updateEmbedLinkField();
        }
        updateUrlState();
    };

    const buildIframeSnippet = (url) => {
        if(!url) return '';
        const safeUrl = url.replace(/"/g, '&quot;');
        return `<iframe src="${safeUrl}" width="1200" height="800" frameborder="0" loading="lazy" style="border:0;max-width:100%;"></iframe>`;
    };

    const updateEmbedLinkField = () => {
        const link = buildEmbedUrl();
        if(embedLinkInput){
            embedLinkInput.value = link;
        }
        if(embedLinkCopyButton){
            embedLinkCopyButton.disabled = !link;
        }
        const snippet = buildIframeSnippet(link);
        if(embedCodeOutput){
            embedCodeOutput.value = snippet;
        }
        if(embedCodeCopyButton){
            embedCodeCopyButton.disabled = !snippet;
        }
    };

    const isEmbedPanelVisible = () => {
        if(!embedPanel) return false;
        return embedPanel.style.display !== 'none' && embedPanel.style.display !== '';
    };

    const setEmbedPanelVisible = (isVisible) => {
        if(!embedPanel) return;
        embedPanel.style.display = isVisible ? 'block' : 'none';
        if(isVisible){
            updateEmbedLinkField();
            if(embedLinkInput){
                embedLinkInput.focus();
                embedLinkInput.select();
            }
        }
    };

    if (loadButton && systemInput) {
        loadButton.addEventListener('click', () => {
            renderSystem(systemInput.value.trim());
        });
    }

    // Lightweight autocomplete for system names
    const SYSTEM_AUTOCOMPLETE_DELAY_MS = 200;
    let systemAutocompleteTimer = null;
    const renderSystemSuggestions = (names) => {
        if(!systemSuggestions) return;
        systemSuggestions.innerHTML = '';
        names.forEach(name => {
            const option = document.createElement('option');
            option.value = name;
            systemSuggestions.appendChild(option);
        });
    };
    const fetchSystemSuggestions = async (prefix) => {
        const trimmed = (prefix || '').trim();
        if(trimmed.length < 2){
            renderSystemSuggestions([]);
            return;
        }
        try {
            const res = await fetch(`${sameHostBaseUrl}/systems/autocomplete?q=${encodeURIComponent(trimmed)}`);
            if(!res.ok){
                return;
            }
            const data = await res.json();
            const suggestions = Array.isArray(data?.suggestions) ? data.suggestions : [];
            renderSystemSuggestions(suggestions);
        } catch(err){
            console.error('Failed to fetch system suggestions', err);
        }
    };
    if(systemInput){
        systemInput.addEventListener('input', () => {
            if(systemAutocompleteTimer){
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

    if(controlsToggleButton && controlsPanel){
        controlsToggleButton.addEventListener('click', () => {
            const shouldShow = !isControlsPanelVisible();
            if(shouldShow){
                hideInfoPanel();
                setEmbedPanelVisible(false);
                if(systemInput){
                    setTimeout(() => {
                        systemInput.focus();
                        systemInput.select();
                    }, 0);
                }
            }
            setControlsPanelVisible(shouldShow);
        });
    }

    // Show search when pressing Enter (if hidden)
    document.addEventListener('keydown', (event) => {
        if(event.key !== 'Enter'){
            return;
        }
        if(event.target && typeof event.target.closest === 'function' && event.target.closest('.bary-layer')){
            return;
        }
        if(systemInput && document.activeElement === systemInput){
            return; // let the input's own Enter handler run
        }
        if(isControlsPanelVisible()){
            return;
        }
        hideInfoPanel();
        setEmbedPanelVisible(false);
        setControlsPanelVisible(true);
        if(systemInput){
            setTimeout(() => {
                systemInput.focus();
                systemInput.select();
            }, 0);
        }
    });

    // Hide the search controls when pressing Escape
    document.addEventListener('keydown', (event) => {
        if(event.key === 'Escape'){
            hideControlsPanel();
            if(systemInput){
                systemInput.blur();
            }
        }
    });

    if(bodyInfoButton){
        bodyInfoButton.addEventListener('click', () => {
            if(!infoPanel) return;
            const shouldShow = !isInfoPanelVisible();
            if(shouldShow){
                hideControlsPanel();
                setEmbedPanelVisible(false);
                if(selectedBodyNode){
                    renderBodyInfo(selectedBodyNode);
                } else {
                    infoPanel.innerHTML = '<p class="small">Select a body to view details.</p>';
                }
                infoPanel.style.display = 'block';
            } else {
                hideInfoPanel();
            }
        });
    }

    // Enter key handler
    if (systemInput) {
        systemInput.addEventListener('keydown', (event) => {
            if (event.key === 'Enter') {
                event.preventDefault(); // avoid form submit/reload
                renderSystem(systemInput.value.trim());
            }
        });
        systemInput.addEventListener('input', () => {
            if(isEmbedPanelVisible()){
                updateEmbedLinkField();
            }
            updateNavigationButtonState();
        });
    }
    if(openGalaxyMapButton){
        openGalaxyMapButton.addEventListener('click', () => {
            const targetUrl = buildGalaxyMapUrl();
            if(!targetUrl){
                return;
            }
            const galaxyMapWindow = window.open(targetUrl, '_blank', 'noopener');
            if(galaxyMapWindow){
                galaxyMapWindow.opener = null;
            }
        });
    }
    if(downloadButton){
        downloadButton.addEventListener('click', downloadSVG);
    }
    if(openEdgisButton){
        openEdgisButton.addEventListener('click', () => {
            const targetUrl = buildEdgisLookupUrl();
            if(!targetUrl){
                return;
            }
            const edgisWindow = window.open(targetUrl, '_blank', 'noopener');
            if(edgisWindow){
                edgisWindow.opener = null;
            }
        });
    }
    if(copyEmbedButton){
        copyEmbedButton.addEventListener('click', () => {
            const shouldShow = !isEmbedPanelVisible();
            if(shouldShow){
                hideControlsPanel();
                hideInfoPanel();
            }
            setEmbedPanelVisible(shouldShow);
        });
    }

    const setupCopyIconButton = (button, getValue, successLabel, errorLabel) => {
        if(!button) return;
        const defaultLabel = button.getAttribute('aria-label') || 'Copy';
        let resetTimeout = null;
        const setState = (label, stateClass) => {
            button.setAttribute('aria-label', label);
            button.classList.remove('copied', 'error');
            if(stateClass){
                button.classList.add(stateClass);
            }
            if(resetTimeout){
                clearTimeout(resetTimeout);
            }
            resetTimeout = globalThis.setTimeout?.(() => {
                button.setAttribute('aria-label', defaultLabel);
                button.classList.remove('copied', 'error');
                resetTimeout = null;
            }, 2000) ?? null;
        };
        button.addEventListener('click', async () => {
            try {
                const value = typeof getValue === 'function' ? getValue() : '';
                if(!value){
                    throw new Error('Nothing to copy');
                }
                await copyTextToClipboard(value);
                setState(successLabel, 'copied');
            } catch (err) {
                console.error(errorLabel, err);
                setState(errorLabel, 'error');
            }
        });
    };

    setupCopyIconButton(
        embedLinkCopyButton,
        () => (embedLinkInput ? (embedLinkInput.value || buildEmbedUrl()) : buildEmbedUrl()),
        'Embed link copied',
        'Unable to copy embed link'
    );

    setupCopyIconButton(
        embedCodeCopyButton,
        () => (embedCodeOutput ? embedCodeOutput.value : ''),
        'Iframe code copied',
        'Unable to copy iframe code'
    );

    if(svg){
        svg.addEventListener('click', (event) => {
            if(!(event.target instanceof Element)) return;
            if(!event.target.closest('.node')){
                clearSelection();
            }
        });
        svg.addEventListener('touchstart', (event) => {
            const target = event.target instanceof Element ? event.target : null;
            if(target && !target.closest('.node')){
                clearSelection();
            }
        }, { passive: true });
    }
    const ringTypeInfo = {
        rocky: { color: 'rgba(174,156,151,1)', label: 'Rocky' },
        icy: { color: 'rgba(181,189,202,1)', label: 'Icy' },
        metalrich: { color: 'rgba(192,192,192,1)', label: 'Metal Rich' },
        metalic: { color: 'rgba(220,220,220,1)', label: 'Metallic' },
        metallic: { color: 'rgba(220,220,220,1)', label: 'Metallic' }
    };

    function getRingShortName(name){
        if(typeof name !== 'string') return 'Ring';
        const parts = name.trim().split(/\s+/).filter(Boolean);
        if(!parts.length) return 'Ring';
        if(parts.length === 1) return parts[0];
        return parts.slice(-2).join(' ');
    }

    function stripTrailingDigits(value){
        let end = value.length;
        while(end > 0){
            const code = value.charCodeAt(end - 1);
            if(code < 48 || code > 57) break;
            end--;
        }
        return end === value.length ? value : value.slice(0, end);
    }

    function normalizeRingTypeKey(type) {
        if(typeof type !== 'string') return '';
        let normalized = type.trim();
        if(normalized.startsWith('eRingClass_')){
            normalized = normalized.slice('eRingClass_'.length);
        }
        return stripTrailingDigits(normalized)
            .replace(/[_\s]+/g, '')
            .toLowerCase();
    }

    function getRingInfo(type) {
        const key = normalizeRingTypeKey(type || '');
        return ringTypeInfo[key];
    }

    function getRingColor(type) {
        const info = getRingInfo(type);
        return (info && info.color) || 'rgba(88,88,88,1)';
    }

    function getRingDisplayName(type) {
        if(!type) return 'Unknown';
        const info = getRingInfo(type);
        if(info && info.label) return info.label;
        return 'Unknown';
    }
    const planetColors = {
        "Class V gas giant": { color: "#ef9a67" },
        // Fallback
        "Unknown Planet": { color: "#ffffff" }
    };
    const starTypeMap = {
        'B': 'B (Blue-White) Star',
        'G_WhiteSuperGiant': 'G (White-Yellow super giant) Star',
        'AeBe': 'Herbig Ae/Be Star',
        'DQ': 'White Dwarf (DQ) Star',
        'WN': 'Wolf-Rayet N Star',
        'N': 'Neutron Star',
        'DAZ': 'White Dwarf (DAZ) Star',
        'DAV': 'White Dwarf (DAV) Star',
        'H': 'Black Hole',
        'CJ': 'CJ Star',
        'WO': 'Wolf-Rayet O Star',
        'DA': 'White Dwarf (DA) Star',
        'M_RedGiant': 'M (Red giant) Star',
        'Y': 'Y (Brown dwarf) Star',
        'DAB': 'White Dwarf (DAB) Star',
        'A_BlueWhiteSuperGiant': 'A (Blue-White super giant) Star',
        'TTS': 'T Tauri Star',
        'CN': 'CN Star',
        'MS': 'MS-type Star',
        'WC': 'Wolf-Rayet C Star',
        'DC': 'White Dwarf (DC) Star',
        'C': 'C Star',
        'O': 'O (Blue-White) Star',
        'M': 'M (Red dwarf) Star',
        'DB': 'White Dwarf (DB) Star',
        'F': 'F (White) Star',
        'K_OrangeGiant': 'K (Yellow-Orange giant) Star',
        'DBZ': 'White Dwarf (DBZ) Star',
        'DBV': 'White Dwarf (DBV) Star',
        'D': 'White Dwarf (D) Star',
        'WNC': 'Wolf-Rayet NC Star',
        'K': 'K (Yellow-Orange) Star',
        'M_RedSuperGiant': 'M (Red super giant) Star',
        'T': 'T (Brown dwarf) Star',
        'G': 'G (White-Yellow) Star',
        'S': 'S-type Star',
        'W': 'Wolf-Rayet Star',
        'DCV': 'White Dwarf (DCV) Star',
        'A': 'A (Blue-White) Star',
        'SupermassiveBlackHole': 'Supermassive Black Hole',
        'B_BlueWhiteSuperGiant': 'B (Blue-White super giant) Star',
        'F_WhiteSuperGiant': 'F (White super giant) Star',
        'L': 'L (Brown dwarf) Star'
    };

    const starColors = {
        "M (Red dwarf) Star": { color: "#ef9a67" },
        "K (Yellow-Orange) Star": { color: "#feeace" },
        "G (White-Yellow) Star": { color: "#faefcd" },
        "F (White) Star": { color: "#fcf8e3" },
        "A (Blue-White) Star": { color: "#f8fafd" },
        "B (Blue-White) Star": { color: "#f1fdfd" },
        "O (Blue-White) Star": { color: "#f5fcfe" },
        "T Tauri Star": { color: "#640037" },

        // Brown dwarfs
        "L (Brown dwarf) Star": { color: "#b1003e" },
        "T (Brown dwarf) Star": { color: "#640037" },
        "Y (Brown dwarf) Star": { color: "#640036" },

        // Giants & Supergiants
        "M (Red giant) Star": { color: "#f0b955" },
        "K (Yellow-Orange giant) Star": { color: "#fee3ab" },
        "G (White-Yellow super giant) Star": { color: "#f6e5b4" },
        "F (White super giant) Star": { color: "#fdf1cb" },
        "A (Blue-White super giant) Star": { color: "#fafdfe" },
        "B (Blue-White super giant) Star": { color: "#e5e9f1" },
        "M (Red super giant) Star": { color: "#e48c46" },

        // Carbon / exotic
        "C Star": { color: "#981055" },
        "CN Star": { color: "#fecd8f" },
        "CJ Star": { color: "#f9b66a" },
        "S-type Star": { color: "#ffdead" },
        "MS-type Star": { color: "#fcca88" },
        "Herbig Ae/Be Star": { color: "#ffe6b1" },

        // Wolf-Rayet
        "Wolf-Rayet Star": { color: "#fec2fe" },
        "Wolf-Rayet N Star": { color: "#f5fcfb" },
        "Wolf-Rayet C Star": { color: "#f0fafb" },
        "Wolf-Rayet O Star": { color: "#e1e8f1" },
        "Wolf-Rayet NC Star": { color: "#e2e7f0" },

        // White dwarfs
        "White Dwarf (DA) Star": { color: "#f8f8ff" },
        "White Dwarf (DAZ) Star": { color: "#ffffff" },
        "White Dwarf (DAB) Star": { color: "#fffafa" },
        "White Dwarf (DAV) Star": { color: "#e6e6fa" },
        "White Dwarf (DB) Star": { color: "#f5f5f5" },
        "White Dwarf (DBZ) Star": { color: "#f0f8ff" },
        "White Dwarf (DBV) Star": { color: "#f5f5dc" },
        "White Dwarf (DC) Star": { color: "#f0fff0" },
        "White Dwarf (DCV) Star": { color: "#fafad2" },
        "White Dwarf (DQ) Star": { color: "#f5f5f5" },
        "White Dwarf (D) Star": { color: "#f8f8ff" },

        // Remnants
        "Neutron Star": { color: "#65d2f4" },
        "N": { color: "#65d2f4" },
        "Black Hole": { color: "#0a0a0a" },
        "Supermassive Black Hole": { color: "#0a0a0a" },

        // Fallback
        "Unknown Star": { color: "#ffffff" }
    };

    function isBarycenter(node){
        if(!node || typeof node.type !== 'string') return false;
        return node.type.toLowerCase().includes('bary');
    }

    function buildMaskId(node){
        const base = node && (node.id ?? node.name ?? 'mask');
        return `mask-${String(base).replace(/[^a-zA-Z0-9_-]/g, '-')}`;
    }

    function normalizeStarType(type){
        if(!type || typeof type !== 'string') return 'Unknown Star';
        const trimmed = type.trim();
        if(!trimmed) return 'Unknown Star';

        if(starColors[trimmed]) return trimmed;
        if(starTypeMap[trimmed]) return starTypeMap[trimmed];

        const upper = trimmed.toUpperCase();
        if(starTypeMap[upper]) return starTypeMap[upper];

        const lower = trimmed.toLowerCase();
        const directMatch = Object.keys(starColors).find(name => name.toLowerCase() === lower);
        if(directMatch) return directMatch;

        return 'Unknown Star';
    }

    function resolveStarColor(subType, temperature){
        const normalized = normalizeStarType(subType);
        const baseColor = (starColors[normalized] || starColors['Unknown Star']).color;

        if (normalized === 'T Tauri Star' && Number.isFinite(temperature)) {
            const coolHold = 974;   // keep base color up to here
            const midOne = 1564;    // #b0204e target
            const midTwo = 6186;    // #f1e5c0 target
            const hotCutoff = 11937; // #e2eff3 target
            const midColorOne = '#b0204e';
            const midColorTwo = '#f1e5c0';
            const hotColor = '#e2eff3';

            if (temperature <= coolHold) {
                return baseColor;
            }
            if (temperature <= midOne) {
                const blend = Math.min((temperature - coolHold) / (midOne - coolHold), 1);
                return interpolateColor(baseColor, midColorOne, blend);
            }
            if (temperature <= midTwo) {
                const blend = Math.min((temperature - midOne) / (midTwo - midOne), 1);
                return interpolateColor(midColorOne, midColorTwo, blend);
            }
            if (temperature <= hotCutoff) {
                const blend = Math.min((temperature - midTwo) / (hotCutoff - midTwo), 1);
                return interpolateColor(midColorTwo, hotColor, blend);
            }
            return hotColor;
        }

        return baseColor;
    }

    function resolveActiveSystemName(){
        if (systemInput) {
            const current = systemInput.value.trim();
            if (current) {
                return current;
            }
        }
        if (systemFromURL) {
            return systemFromURL;
        }
        return defaultSystemName;
    }

    const initialSystem = resolveActiveSystemName();
    if (initialSystem) {
        const initialRender = renderSystem(initialSystem, { bodyId: bodyIdFromURL });
        if (downloadMode === 'svg') {
            initialRender.then((success) => {
                if (success) {
                    downloadSVG();
                }
            });
        } else if (downloadMode === 'png') {
            initialRender.then((success) => {
                if (success) {
                    downloadPNG().catch((err) => console.error('PNG export failed', err));
                }
            });
        }
    }

    function interpolateColor(c1, c2, t) {
        const parse = c => c.match(/\w\w/g).map(x => parseInt(x, 16));
        const [r1,g1,b1] = parse(c1), [r2,g2,b2] = parse(c2);
        const r = Math.round(r1 + (r2 - r1)*t);
        const g = Math.round(g1 + (g2 - g1)*t);
        const b = Math.round(b1 + (b2 - b1)*t);
        return `#${r.toString(16).padStart(2,'0')}${g.toString(16).padStart(2,'0')}${b.toString(16).padStart(2,'0')}`;
    }

    function mixWithWhite(hex, amount) {
        const clamp = Math.min(Math.max(amount ?? 0, 0), 1);
        return interpolateColor(hex, '#ffffff', clamp);
    }

    function rockyPlanetColor(type, temp) {
        if (!type || temp === undefined) return 'var(--planet)';

        const s = type.toLowerCase();

        if (s.includes("earthlike body")) {
            const minT = 150, maxT = 350;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#2e8bff", "#a0522d", p); // blue → brown
        }

        if (s.includes("high metal content world")) {
            const minT = 300, maxT = 1500;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#6a4f52", "#8c7a6f", p); // dark grey → silver
        }

        if (s.includes("icy body") || s.includes("ice world")) {
            const minT = 20, maxT = 250;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#a5add6", "#ffffff", p); // deep blue → white
        }

        if (s.includes("metal-rich body")) {
            const minT = 300, maxT = 1500;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#444444", "#aaaaaa", p); // dark grey → light grey
        }

        if (s.includes("rocky body")) {
            const minT = 250, maxT = 1000;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#454b59", "#6f6880", p); // dark brown → reddish brown
        }

        return 'var(--planet)';
    }

    function gasGiantColor(subType, temp) {
        if (!subType || temp === undefined) return 'var(--planet)';

        const s = subType.toLowerCase();

        if (
            s.includes("class i gas giant") ||
            s.includes("gas giant with water-based life") ||
            s.includes("gas giant with ammonia-based life")
        ) {
            const minT = 50, maxT = 90;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#413981", "#efcba7", p); // dark purple → bright brown
        }

        if (s.includes("class ii gas giant")) {
            const minT = 120, maxT = 250;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#d2a679", "#bbbbbb", p); // brown → grey
        }

        if (s.includes("class iii gas giant")) {
            const minT = 300, maxT = 600;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#272d6f", "#5b5b82", p); // reddish-brown → deep blue
        }

        if (s.includes("class iv gas giant")) {
            const minT = 900, maxT = 1400;
            const p = Math.min(Math.max((temp - minT)/(maxT - minT), 0), 1);
            return interpolateColor("#664c48", "#854e72", p); // brown → dark red
        }

        if (s.includes("class v gas giant")) {
            return "#b3b5c5"; // desaturated brown, static
        }

        return 'var(--planet)';
    }


    function sanitizeRadiusValue(value, bodyType){
        const parsed = Number(value);
        if(Number.isFinite(parsed) && parsed > 0){
            return parsed;
        }
        return bodyType === 'Star' ? 1 : 1000;
    }

    function specialRadiusOverride(subType, radiusKm){
        if(!subType) return null;
        if(subType === 'Black Hole' || subType === 'H') return 20;
        if(subType.includes('Neutron') || subType === 'N') return 5;
        if(subType === 'Supermassive Black Hole' || subType === 'SupermassiveBlackHole'){
            return Math.cbrt(radiusKm);
        }
        return null;
    }

    function scaleRadius(r, bodyType, subType){
        const rawRadius = sanitizeRadiusValue(r, bodyType);
        const radiusKm = bodyType === 'Star' ? rawRadius * SOL_RADIUS_KM : rawRadius;
        const baseScaled = bodyType === 'Star'
            ? Math.cbrt(radiusKm)
            : Math.sqrt(radiusKm) / 9;

        const override = specialRadiusOverride(subType, radiusKm);
        const scaled = override ?? baseScaled;

        if(!Number.isFinite(scaled) || scaled <= 0){
            return 10;
        }

        return scaled;
    }

    const numericIdRegex = /^-?\d+$/;

    async function fetchSystemNameById64(id64){
        const normalizedId64 = (id64 ?? '').toString().trim();
        if(!numericIdRegex.test(normalizedId64)){
            return null;
        }
        try {
            const res = await fetch(`${sameHostBaseUrl}/coords?q=${encodeURIComponent(normalizedId64)}`);
            if(!res.ok){
                return null;
            }
            const payload = await res.json();
            return payload?.name ?? null;
        } catch(err){
            console.error('Failed to resolve system name from id64', err);
            return null;
        }
    }

    async function renderSystem(systemName, { bodyId = null } = {}){
        const normalizedSystemName = (systemName ?? '').toString().trim();
        if (!normalizedSystemName) {
            return false;
        }
        let data;
        try {
            const res = await fetch(`${sameHostBaseUrl}/bodies?name_or_id=${encodeURIComponent(normalizedSystemName)}&mode=edsm`);
            data = await res.json();
        } catch(err){
            console.error(err);
            return false;
        }
        if(!data || !Array.isArray(data)) return false;
        let requestedBodyId = null;
        if(bodyId !== null && bodyId !== undefined){
            const bodyIdNumber = Number(bodyId);
            requestedBodyId = Number.isFinite(bodyIdNumber) ? bodyIdNumber : null;
        }
        const trimmedInputName = normalizedSystemName;
        let resolvedSystemName = trimmedInputName || systemName;
        if(trimmedInputName && numericIdRegex.test(trimmedInputName)){
            const fetchedName = await fetchSystemNameById64(trimmedInputName);
            if(fetchedName){
                resolvedSystemName = fetchedName;
            }
        }
        if(systemInput && resolvedSystemName){
            systemInput.value = resolvedSystemName;
            updateNavigationButtonState();
        }
        updateUrlState(resolvedSystemName, requestedBodyId);

        const nodes = new Map();
        function resolveParentIds(parents){
            let primaryId = null;
            let baryId = null;
            let directBaryParentId = null;
            if(Array.isArray(parents)){
                let encounteredNonBary = false;
                parents.forEach(entry => {
                    const [[type, value]] = Object.entries(entry);
                    if(type === 'Null') return;
                    const typeLower = type.toLowerCase();
                    const isBaryEntry = typeLower.includes('barycentre') || typeLower.includes('barycenter');
                    if(isBaryEntry){
                        if(baryId == null) baryId = value;
                        if(!encounteredNonBary){
                            directBaryParentId = value;
                        }
                        return;
                    }
                    if(primaryId == null) primaryId = value;
                    encounteredNonBary = true;
                });
            }
            return { parentId: primaryId, baryParentId: baryId, directBaryParentId };
        }

        const pendingRings = new Map();

        function processRingBody(body){
            if(!isPlanetaryRingNode(body)) return false;
            const hostId = resolveRingHostId(body);
            if(hostId == null) return true;
            if(!pendingRings.has(hostId)) pendingRings.set(hostId, []);
            pendingRings.get(hostId).push(normalizeRingRecord(body));
            return true;
        }

        function determineBodySubType(body){
            if(body.type === 'Star') return body.star_type;
            if(isStellarRingNode(body) || isPlanetaryRingNode(body)){
                return body.ring_class || body.type;
            }
            if(isAsteroidClusterNode(body)){
                return body.subType || body.type;
            }
            return body.planet_class;
        }

        function buildNodeRecord(body, parentsMeta){
            return {
                id64: body.system_id64,
                id: body.body_id,
                name: body.body_name,
                type: body.type,
                subType: determineBodySubType(body),
                temperature: body.surface_temperature,
                parentId: parentsMeta.parentId,
                children: [],
                x: 0, y: 0,
                width: 0, height: 0,
                radius: body.radius,
                radiusScaled: 0,
                axialTilt: body.axial_tilt,
                rotationalPeriod: body.rotation_period,
                orbitalPeriod: body.orbital_period,
                semiMajorAxis: body.semi_major_axis ?? body.semiMajorAxis ?? null,
                orbitalEccentricity: body.orbital_eccentricity ?? body.eccentricity ?? null,
                orbitalInclination: body.orbital_inclination ?? body.orbitalInclination ?? null,
                rings: Array.isArray(body.rings) ? body.rings.map(normalizeInlineRing).filter(Boolean) : [],
                isLandable: body.landable,
                tidallyLocked: body.tidally_locked ?? body.is_tidally_locked ?? body.tidallyLocked ?? null,
                atmosphereType: body.atmosphere_type,
                atmosphereComposition: body.atmosphere_composition,
                surfacePressure: body.surface_pressure,
                distanceToArrival: body.distance_from_arrival_ls ?? null,
                earthMasses: body.mass_em ?? body.null,
                gravity: body.gravity ?? body.surface_gravity ?? null,
                terraformingState: body.terraforming_state ?? body.terraformingState ?? null,
                volcanism: body.volcanism_type ?? body.volcanism ?? null,
                materials: body.materials ?? null,
                baryParentId: parentsMeta.baryParentId,
                directBaryParentId: parentsMeta.directBaryParentId,
                baryChildren: [],
                baryNodeTarget: null,
                baryConnectorPoint: null,
                massValue: resolveBodyMassValue(body),
                discovery: body.discovery ?? null,
                wasMapped: body.was_mapped ?? body.mapped ?? null,
                raw: body
                // isMainStar: body.isMainStar
            };
        }

        data.forEach(body => {
            if(processRingBody(body)) return;
            const parentsMeta = resolveParentIds(body.parents || []);
            nodes.set(body.body_id, buildNodeRecord(body, parentsMeta));
        });

        for(const node of nodes.values()){
            if(node.parentId != null && nodes.has(node.parentId)){
                nodes.get(node.parentId).children.push(node);
            }
        }
        pendingRings.forEach((ringList, parentId) => {
            if(nodes.has(parentId)){
                const host = nodes.get(parentId);
                if(!Array.isArray(host.rings)) host.rings = [];
                host.rings.push(...ringList);
            }
        });

        ensureBarycenterChildren(nodes);
        computeBarycenterMasses([...nodes.values()]);
        sortBarycenterChildrenByMass([...nodes.values()]);
        logBarycenterChildren([...nodes.values()]);
        const skipSiblingPairs = computeBarycenterSkipPairs([...nodes.values()]);
        const baryRootOrderMap = buildBaryRootOrderMap([...nodes.values()]);
        for(const node of nodes.values()) node.children.sort((a,b)=>a.id-b.id);

        const roots = [...nodes.values()].filter(n => n.parentId == null || !nodes.has(n.parentId))
              .sort((a, b) => compareRootNodes(a, b, baryRootOrderMap));

        // First pass: compute subtree sizes
        roots.forEach(r => computeSize(r, 1));
        // Second pass: place nodes
        let yCursor = 80;
        for(const r of roots){
            const margin = 50; // or dynamically use max root radius
            r.x = rootX * 3;
            r.y = yCursor + r.radiusScaled;
            placeChildren(r, 1);
            yCursor += r.height + vGap * 3.5;
        }

        const bounds = computeBounds([...nodes.values()]);
        draw([...nodes.values()], bounds, resolvedSystemName, requestedBodyId);
        if(isEmbedPanelVisible()){
            updateEmbedLinkField();
        }
        return true;
    }

    function resolveParentIds(parents){
        let primaryId = null;
        let baryId = null;
        if(Array.isArray(parents)){
            parents.forEach(entry => {
                const [[type, value]] = Object.entries(entry);
                if(type === 'Null') return;
                const typeLower = type.toLowerCase();
                if(typeLower.includes('barycentre') || typeLower.includes('barycenter')){
                    if(baryId == null) baryId = value;
                    return;
                }
                if(primaryId == null) primaryId = value;
            });
        }
        return { parentId: primaryId, baryParentId: baryId };
    }

    function isPlanetaryRingNode(body){
        const type = (body?.type || '').toLowerCase();
        return type.includes('planetaryring');
    }

    function isStellarRingNode(body){
        const type = (body?.type || '').toLowerCase();
        return type.includes('stellarring');
    }

    function isAsteroidClusterNode(body){
        const type = (body?.type || '').toLowerCase();
        if(!type) return false;
        return type.replace(/\s+/g, '').includes('asteroidcluster');
    }

    function isRingNode(body){
        return isPlanetaryRingNode(body) || isStellarRingNode(body);
    }

    function normalizeInlineRing(ring){
        if(!ring) return null;
        const bodyId = parseBodyIdParam(ring.bodyId ?? ring.body_id ?? ring.id);
        return {
            name: ring.name || ring.body_name || ring.label || 'Ring',
            type: ring.type || ring.ring_class || ring.class || 'Ring',
            innerRadius: ring.innerRadius ?? ring.ring_inner_rad ?? ring.inner_radius ?? null,
            outerRadius: ring.outerRadius ?? ring.ring_outer_rad ?? ring.outer_radius ?? null,
            mass: ring.mass ?? ring.ring_mass_mt ?? ring.mass_mt ?? null,
            bodyId
        };
    }

    function normalizeRingRecord(body){
        const bodyId = parseBodyIdParam(body.body_id ?? body.bodyId ?? body.id);
        return {
            name: body.body_name || body.name || 'Ring',
            type: body.ring_class || body.type || 'Ring',
            innerRadius: body.ring_inner_rad ?? body.ring_inner_radius ?? body.inner_radius ?? body.innerRadius ?? null,
            outerRadius: body.ring_outer_rad ?? body.ring_outer_radius ?? body.outer_radius ?? body.outerRadius ?? null,
            mass: body.ring_mass_mt ?? body.mass_em ?? body.mass ?? null,
            bodyId
        };
    }

    function resolveRingHostId(body){
        if(body.parent_body_id != null) return body.parent_body_id;
        if(body.parentbody_id != null) return body.parentbody_id;
        if(body.parentBodyId != null) return body.parentBodyId;
        const { parentId, baryParentId } = resolveParentIds(body.parents || []);
        return parentId ?? baryParentId ?? null;
    }

    function resolveBodyMassValue(body){
        if(!body) return null;
        const castNumber = (value) => {
            const num = Number(value);
            return Number.isFinite(num) ? num : null;
        };
        if(body.type === 'Star' || body.stellar_mass != null || body.star_type){
            return castNumber(body.stellar_mass ?? body.mass_em ?? body.mass ?? body.mass_mt ?? body.massMT);
        }
        return castNumber(body.mass_em ?? body.mass ?? body.mass_mt ?? body.massMT);
    }

    function guessBarycenterChildNames(name, nameIndex){
        if(!name) return [];
        const plusIndex = name.indexOf('+');
        if(plusIndex !== -1){
            let prefixEnd = name.lastIndexOf(' ', plusIndex);
            if(prefixEnd === -1) prefixEnd = -1;
            const prefix = prefixEnd >= 0 ? name.slice(0, prefixEnd + 1) : '';
            const suffix = name.slice(prefixEnd + 1);
            return suffix.split('+')
                .map(part => (prefix + part.trim()).replace(/\s+/g, ' ').trim())
                .filter(Boolean);
        }

        const segments = name.trim().split(/\s+/);
        if(segments.length >= 2){
            const suffix = segments[segments.length - 1];
            if(/^[A-Za-z]{2,}$/.test(suffix)){
                const prefix = segments.slice(0, -1).join(' ');
                if(nameIndex instanceof Map){
                    for(let split = 1; split < suffix.length; split++){
                        const left = `${prefix} ${suffix.slice(0, split)}`.trim();
                        const right = `${prefix} ${suffix.slice(split)}`.trim();
                        if(nameIndex.has(left) && nameIndex.has(right)){
                            return [left, right];
                        }
                    }
                }
                return suffix.split('')
                    .map(ch => `${prefix} ${ch}`.trim())
                    .filter(Boolean);
            }
        }
        return [];
    }

    function ensureBarycenterChildren(nodes){
        const nameIndex = new Map();
        const arrayNodes = [...nodes.values()];
        arrayNodes.forEach(n => {
            if(n.name) nameIndex.set(n.name.trim(), n);
            n.baryChildren = [];
            n.baryNodeTarget = null;
            n.baryConnectorPoint = null;
        });

        const addUniqueChild = (bary, child) => {
            if(!child) return;
            if(!bary.baryChildren.some(c => c.id === child.id)){
                bary.baryChildren.push(child);
            }
        };

        const baryMap = new Map();
        arrayNodes.forEach(n => {
            if(isBarycenter(n)) baryMap.set(n.id, n);
        });

        arrayNodes.forEach(node => {
            const baryId = node.directBaryParentId;
            if(baryId != null && baryMap.has(baryId)){
                addUniqueChild(baryMap.get(baryId), node);
            }
        });

        arrayNodes.forEach(node => {
            if(!isBarycenter(node)) return;
            if(node.baryChildren.length >= 2) return;
            const targets = guessBarycenterChildNames(node.name, nameIndex);
            if(targets.length === 0) return;
            const matches = targets.map(name => nameIndex.get(name)).filter(Boolean);
            for(const child of matches){
                addUniqueChild(node, child);
                if(node.baryChildren.length >= 2) break;
            }
        });
    }

    function usesSolarMassUnits(node){
        if(!node) return false;
        const type = (node.type || '').toLowerCase();
        if(type === 'star') return true;
        if(type === 'planet') return false;
        if(isBarycenter(node)) return hasStarDescendant(node);
        return false;
    }

    function normalizeMassToUnit(massValue, fromSolarUnits, toSolarUnits){
        if(!Number.isFinite(massValue)) return 0;
        if(fromSolarUnits === toSolarUnits) return massValue;
        return toSolarUnits ? massValue * EARTH_MASS_TO_SOLAR : massValue / EARTH_MASS_TO_SOLAR;
    }

    function computeBarycenterMasses(nodes){
        const arrayNodes = Array.isArray(nodes) ? nodes : [...nodes.values()];
        const massCache = new Map();
        const resolveMass = (node) => {
            if(!node) return 0;
            if(massCache.has(node)) return massCache.get(node);
            const base = Number(node.massValue);
            if(!isBarycenter(node)){
                const mass = Number.isFinite(base) ? base : 0;
                massCache.set(node, mass);
                return mass;
            }
            const children = (node.baryChildren || []).filter(Boolean);
            if(children.length === 0){
                node.massValue = Number.isFinite(base) ? base : 0;
                massCache.set(node, node.massValue);
                return node.massValue;
            }
            const baryUsesSolarMass = usesSolarMassUnits(node);
            const total = children.reduce((sum, child) => {
                const childMass = resolveMass(child);
                const childUsesSolarMass = usesSolarMassUnits(child);
                return sum + normalizeMassToUnit(childMass, childUsesSolarMass, baryUsesSolarMass);
            }, 0);
            node.massValue = total;
            massCache.set(node, total);
            return total;
        };
        arrayNodes.forEach(node => {
            if(isBarycenter(node)) resolveMass(node);
        });
    }

    function getNodeMass(node){
        const value = Number(node?.massValue);
        return Number.isFinite(value) ? value : 0;
    }

    function sortBarycenterChildrenByMass(nodes){
        const arrayNodes = Array.isArray(nodes) ? nodes : [...nodes.values()];
        arrayNodes.forEach(node => {
            if(!isBarycenter(node) || !Array.isArray(node.baryChildren)) return;
            node.baryChildren.sort((a, b) => getNodeMass(b) - getNodeMass(a));
        });
    }

    function hasStarDescendant(node, seen = new Set()){
        if(!node || seen.has(node)) return false;
        seen.add(node);
        if((node.type || '').toLowerCase() === 'star') return true;
        if(!isBarycenter(node)) return false;
        return (node.baryChildren || []).some(child => hasStarDescendant(child, seen));
    }

    function resolveMassUnitForNode(node){
        if(!node) return '';
        const type = (node.type || '').toLowerCase();
        if(type === 'star') return ' M☉';
        if(type === 'planet' || type.includes('moon')) return ' M⊕';
        if(isBarycenter(node)){
            return hasStarDescendant(node) ? ' M☉' : ' M⊕';
        }
        return '';
    }

    function formatMassDisplay(node){
        const value = getNodeMass(node);
        if(!Number.isFinite(value)) return 'Unknown';
        const unit = resolveMassUnitForNode(node) || '';
        return formatNumber(value, { unit, fractionDigits: 3 });
    }

    function logBarycenterChildren(nodes){
        const baryNodes = nodes.filter(isBarycenter);
        if(baryNodes.length === 0) return;
        console.groupCollapsed('Barycenter masses');
        baryNodes.forEach(bary => {
            const masses = (bary.baryChildren || []).map(child => {
                const childName = child?.name || `#${child?.id ?? '??'}`;
                const childType = child?.type || 'unknown';
                return `${childName} (${childType})`;
            });
            const label = `${bary.name || 'Unnamed barycenter'} (#${bary.id ?? '??'})`;
            const baryMass = formatMassDisplay(bary);
            if(masses.length === 0){
                console.log(`${label}: no bary masses detected [mass=${baryMass}]`);
            } else {
                console.log(`${label}: ${masses.join('  |  ')} [mass=${baryMass}]`);
            }
        });
        console.groupEnd();
    }

    function computeBarycenterSkipPairs(arrayNodes){
        const skip = new Set();
        arrayNodes.forEach(bary => {
            if(!isBarycenter(bary)) return;
            const kids = (bary.baryChildren || []).filter(Boolean);
            if(kids.length < 2) return;
            for(let i = 0; i < kids.length; i++){
                for(let j = i+1; j < kids.length; j++){
                    skip.add(pairKey(kids[i].id, kids[j].id));
                }
            }
        });
        return skip;
    }

    function buildBaryRootOrderMap(arrayNodes){
        const nodes = Array.isArray(arrayNodes) ? arrayNodes : [...arrayNodes.values()];
        const baryNodes = nodes.filter(isBarycenter);
        if(baryNodes.length === 0){
            return new Map();
        }
        const childToBary = new Map();
        baryNodes.forEach(bary => {
            (bary.baryChildren || []).forEach(child => {
                if(child && child.id != null && !childToBary.has(child.id)){
                    childToBary.set(child.id, bary);
                }
            });
        });
        let baryRoots = baryNodes.filter(bary => !childToBary.has(bary.id));
        if(baryRoots.length === 0){
            baryRoots = baryNodes;
        }
        baryRoots.sort((a, b) => getNodeMass(b) - getNodeMass(a));
        const orderMap = new Map();
        const visitedBary = new Set();
        const visitedNodes = new Set();

        const assignFromBary = (bary) => {
            if(!bary || visitedBary.has(bary.id)) return;
            visitedBary.add(bary.id);
            const kids = (bary.baryChildren || []).filter(Boolean);
            kids.sort((a, b) => getNodeMass(b) - getNodeMass(a));
            kids.forEach(child => {
                if(isBarycenter(child)){
                    assignFromBary(child);
                } else if(child.id != null && !visitedNodes.has(child.id)){
                    orderMap.set(child.id, orderMap.size);
                    visitedNodes.add(child.id);
                }
            });
        };

        baryRoots.forEach(assignFromBary);
        return orderMap;
    }

    function compareRootNodes(a, b, orderMap){
        const orderA = orderMap.get(a?.id);
        const orderB = orderMap.get(b?.id);
        if(orderA != null || orderB != null){
            if(orderA == null) return 1;
            if(orderB == null) return -1;
            if(orderA !== orderB) return orderA - orderB;
        }
        if (a.isMainStar && !b.isMainStar) return -1;
        if (b.isMainStar && !a.isMainStar) return 1;
        return (a.id ?? 0) - (b.id ?? 0);
    }

    function pairKey(aId, bId){
        if(aId == null || bId == null) return '';
        return (aId < bId) ? `${aId}|${bId}` : `${bId}|${aId}`;
    }

    // Recursively compute subtree size (width, height)
    function computeSize(node, depth){
        const nodeLabel = `${node.name || 'unnamed'} (#${node.id || '??'})`;
        node.radiusScaled = scaleRadius(node.radius, node.type, node.subType);

        if(node.children.length === 0){
            node.width = node.radiusScaled * 2;
            node.height = node.radiusScaled * 2;
            return {width: node.width, height: node.height};
        }

        let totalW = 0, totalH = 0;

        if(depth % 2 === 1){  // horizontal layout
            let maxH = 0;
            node.children.forEach(c => {
                const childLabel = `${c.name || 'unnamed'} (#${c.id || '??'})`;
                const sz = computeSize(c, depth+1);
                totalW += sz.width + hGap;
                if(sz.height > maxH) maxH = sz.height;
            });
            node.width = Math.max(node.radiusScaled*2, totalW);
            node.height = Math.max(node.radiusScaled*2, maxH);
        } else {  // vertical layout
            let maxW = 0;
            node.children.forEach(c => {
                const childLabel = `${c.name || 'unnamed'} (#${c.id || '??'})`;
                const sz = computeSize(c, depth+1);
                totalH += sz.height + vGap;
                if(sz.width > maxW) maxW = sz.width;
            });
            node.width = Math.max(node.radiusScaled*2, maxW);
            node.height = Math.max(node.radiusScaled*2, totalH);
        }

        return {width: node.width, height: node.height};
    }

    // Place nodes based on computed sizes
    function placeChildren(parent, depth){
        const kids = parent.children.filter(child => !isBarycenter(child));
        if(kids.length === 0) return;
        if(depth % 2 === 1){ // horizontal
            let xCursor = parent.x + parent.radiusScaled + hGap;
            if (depth > 1) {
                xCursor = parent.x + hGap * 0.8;
            }
            kids.forEach(c => {
                c.y = parent.y;
                c.x = xCursor + c.radiusScaled;
                console.log(c.name, c.x, c.radiusScaled);
                placeChildren(c, depth+1);
                if (c.rings) {
                    xCursor += c.width * 1.3 + hGap;
                } else {
                    xCursor += c.width + hGap;
                }
            });
        } else { // vertical
            let yCursor = parent.y + parent.radiusScaled + vGap;
            kids.forEach(c => {
                c.x = parent.x;
                c.y = yCursor + c.radiusScaled;
                placeChildren(c, depth+1);
                yCursor += c.height * 0.5 + vGap;
            });
        }
    }

    function computeBounds(nodes){
        let maxX = 0, maxY = 0;
        for(const n of nodes){
            if(n.x > maxX) maxX = n.x;
            if(n.y > maxY) maxY = n.y;
        }
        return { maxX: maxX + 120, maxY: maxY + 120 };
    }

    function addAtmosphere(n, group) {
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('r', n.radiusScaled * 1.25);

        let strokeColor = "#27667f";
        circle.setAttribute('stroke', strokeColor);
        circle.setAttribute("fill", "url(#atmoGrad)");
        circle.setAttribute('filter', 'url(#blurStroke)');
        circle.setAttribute('stroke-width', '.5');
        circle.setAttribute('stroke-opacity', '1');
        circle.setAttribute('class', 'notnode');
        group.appendChild(circle);
    }

    function addLandable(n, group) {
        let radius = 12;
        if (n.radiusScaled > 10) {
            radius = 17;
        }
        const angleStart = 55 * Math.PI/180;
        const xStart = radius * Math.cos(angleStart);
        const yStart = -radius * Math.sin(angleStart);
        //const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        //dot.setAttribute('cx', xStart);
        //dot.setAttribute('cy', yStart);
        //dot.setAttribute('r', 1.5);
        //dot.setAttribute('fill', 'red');
        //group.appendChild(dot);
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');

        const d = `
   M ${xStart},${yStart}
   A ${radius},${radius} 0 0 0 ${-xStart},${-yStart}
    `;

        path.setAttribute('d', d);
        // path.setAttribute('stroke', '#44b5d2');
        path.setAttribute('stroke', 'url(#fadeStroke)');
        path.setAttribute('stroke-width', '.8');
        path.setAttribute('fill', 'none');
        path.setAttribute('marker-start', 'url(#smoothCap)');
        path.setAttribute('marker-end', 'url(#smoothCap)');
        path.setAttribute('class', 'notnode');
        group.appendChild(path);

        // vertical ticks
        const lineHeight = 2.5;
        const spacing = .15;
        let tickAngleStart = 105 * Math.PI/180;

        for (let i = 0; i < 3; i++) {
            const tickAngle = tickAngleStart + (spacing * i);
            const x = radius * Math.cos(tickAngle);
            const y = -radius * Math.sin(tickAngle);
            const currentLineHeight = (i % 2 === 1) ? lineHeight * 2 : lineHeight;

            const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x1', x);
            line.setAttribute('y1', y);
            line.setAttribute('x2', x);
            line.setAttribute('y2', y - currentLineHeight);

            line.setAttribute('stroke', '#44b5d2');
            line.setAttribute('stroke-width', '.9');
            line.setAttribute('class', 'notnode');
            group.appendChild(line);
        }
    }

    function createRingSelectionNode(hostNode, ring, index, { innerScaled = null, outerScaled = null } = {}){
        const hostName = hostNode?.name || 'Unknown Body';
        const ringName = ring?.name || `${hostName} Ring`;
        const hostIdPart = hostNode?.id != null ? hostNode.id : hostName;
        const ringBodyId = parseBodyIdParam(ring?.bodyId ?? ring?.body_id ?? ring?.id);
        return {
            id: `ring:${hostIdPart}:${index}`,
            type: 'PlanetaryRing',
            name: ringName,
            ring,
            hostBody: hostNode || null,
            radiusScaled: outerScaled ?? hostNode?.radiusScaled ?? 0,
            ringInnerScaled: innerScaled ?? null,
            ringOuterScaled: outerScaled ?? null,
            isRingNode: true,
            raw: ring,
            ringBodyId
        };
    }

    function addRings(n, group, maskId) {
        if (!n.rings || !n.rings.length) return;

        const maxRingKm = Math.max(...n.rings.map(r => r.outerRadius));
        const availableBand = 2 * n.radiusScaled - 1.2 * n.radiusScaled; // = n.radiusScaled
        const ringThickness = availableBand / n.rings.length;

        n.rings.forEach((ring, idx) => {
            const ringGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            ringGroup.classList.add('ring-layer', 'notnode');
            if(n.id != null){
                ringGroup.dataset.hostId = String(n.id);
            }
            ringGroup.dataset.ringIndex = String(idx);
            // Spread rings evenly by index
            const centerFraction = (idx + 0.5) / n.rings.length;

            // Map fraction to scaled radius band
            const centerScaled = n.radiusScaled + centerFraction * availableBand;

            // Apply uniform thickness
            const innerScaled = centerScaled - ringThickness / 2;
            const outerScaled = centerScaled + ringThickness / 2;

            let rxInner = innerScaled * 1.2;
            let ryInner = innerScaled * 1.2 / 5;
            let rxOuter = outerScaled * 1.16;
            let ryOuter = outerScaled * 1.16 / 5;

            if (n.rings.length < 2) {
                rxInner = innerScaled * 1.25;
                ryInner = innerScaled * 1.25 / 5;
                rxOuter = outerScaled * 0.95;
                ryOuter = outerScaled * 0.95 / 5;
            }

            const frontPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            const backPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            const shadowPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            const angleStart = 0;
            const angleEnd = Math.PI;
            const shadowAngleStart = -0.3;
            const shadowAngleEnd = -1.5;
            const largeArcOuter = angleEnd - angleStart > Math.PI ? 1 : 0;
            const largeArcInner = angleEnd - angleStart > Math.PI ? 1 : 0;
            const largeArcShadowOuter = shadowAngleEnd - shadowAngleStart > Math.PI ? 1 : 0;
            const largeArcShadowInner = shadowAngleEnd - shadowAngleStart > Math.PI ? 1 : 0;
            const xStartOuter = rxOuter * Math.cos(angleStart);
            const yStartOuter = ryOuter * Math.sin(angleStart);
            const xEndOuter = rxOuter * Math.cos(angleEnd);
            const yEndOuter = ryOuter * Math.sin(angleEnd);
            const xStartInner = rxInner * Math.cos(angleStart);
            const yStartInner = ryInner * Math.sin(angleStart);
            const xEndInner = rxInner * Math.cos(angleEnd);
            const yEndInner = ryInner * Math.sin(angleEnd);

            const xShadowStartOuter = rxOuter * 1.01 * Math.cos(shadowAngleStart);
            const yShadowStartOuter = ryOuter * 1.01 * Math.sin(shadowAngleStart);
            const xShadowEndOuter   = rxOuter * 1.01 * Math.cos(shadowAngleEnd);
            const yShadowEndOuter   = ryOuter * 1.01 * Math.sin(shadowAngleEnd);
            const xShadowStartInner = rxInner * 0.99 * Math.cos(shadowAngleStart);
            const yShadowStartInner = ryInner * 0.99 * Math.sin(shadowAngleStart);
            const xShadowEndInner   = rxInner * 0.99 * Math.cos(shadowAngleEnd);
            const yShadowEndInner   = ryInner * 0.99 * Math.sin(shadowAngleEnd);

            const front = `
 M ${xStartOuter},${yStartOuter}
 A ${rxOuter},${ryOuter} 0 ${largeArcOuter},1 ${xEndOuter},${yEndOuter}
 L ${xEndInner},${yEndInner}
 A ${rxInner},${ryInner} 0 ${largeArcInner},0 ${xStartInner},${yStartInner}
 Z
      `;

            // full circle for the back so we avoid the thin hairline gap between the two halves
            const back = `
 M ${xStartOuter},${yStartOuter}
 A ${rxOuter},${ryOuter} 0 ${largeArcOuter},0 ${xEndOuter},${yEndOuter}
 L ${xEndInner},${yEndInner}
 A ${rxInner},${ryInner} 0 ${largeArcInner},1 ${xStartInner},${yStartInner}
 M ${xStartOuter},${yStartOuter}
 A ${rxOuter},${ryOuter} 0 ${largeArcOuter},1 ${xEndOuter},${yEndOuter}
 L ${xEndInner},${yEndInner}
 A ${rxInner},${ryInner} 0 ${largeArcInner},0 ${xStartInner},${yStartInner}
 Z
      `;
            // full circle for the back so we avoid the thin hairline gap between the two halves
            const shadow = `
 M ${xShadowStartOuter},${yShadowStartOuter}
 A ${rxOuter},${ryOuter} 0 ${largeArcShadowOuter},0 ${xShadowEndOuter},${yShadowEndOuter}
 L ${xShadowEndInner},${yShadowEndInner}
 A ${rxInner},${ryInner} 0 ${largeArcShadowInner},1 ${xShadowStartInner},${yShadowStartInner}
 Z
      `;
            const ringColor = getRingColor(ring.type);
            // console.log(ring.name, ring.type, ringColor);

            frontPath.setAttribute('d', front);
            frontPath.setAttribute('fill', ringColor);
            frontPath.setAttribute('stroke', 'none');
            frontPath.setAttribute('transform', `rotate(${RING_ROTATION_DEG})`);
            frontPath.setAttribute('class', 'notnode ring-segment');
            ringGroup.appendChild(frontPath);

            backPath.setAttribute('d', back);
            backPath.setAttribute('fill', ringColor);
            backPath.setAttribute('stroke', 'none');
            backPath.setAttribute('transform', `rotate(${RING_ROTATION_DEG})`);
            backPath.setAttribute('mask', `url(#${maskId})`);
            backPath.setAttribute('class', 'notnode ring-segment');
            ringGroup.appendChild(backPath);

            shadowPath.setAttribute('d', shadow);
            shadowPath.setAttribute('fill', 'black');
            shadowPath.setAttribute('opacity', 0.75);
            shadowPath.setAttribute('stroke', 'none');
            shadowPath.setAttribute('transform', `rotate(${RING_ROTATION_DEG})`);
            shadowPath.setAttribute('mask', `url(#${maskId})`);
            shadowPath.setAttribute('class', 'notnode ring-shadow');
            ringGroup.appendChild(shadowPath);

            group.appendChild(ringGroup);

            const ringSelectionNode = createRingSelectionNode(n, ring, idx, { innerScaled, outerScaled });
            let ringLabel = null;
            let ringLabelLine = null;
            if(labelsLayerGroup){
                const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                const labelText = getRingShortName(ring.name || `Ring ${idx + 1}`);
                label.textContent = labelText;
                label.setAttribute('text-anchor', 'start');
                const ringRightX = n.x + (((rxOuter+rxInner)/2) * Math.cos(RING_ROTATION_RAD));
                const ringRightY = n.y + (((rxOuter+rxInner)/2) * Math.sin(RING_ROTATION_RAD));
                const labelX = ringRightX + RING_LABEL_OFFSET_X;
                const labelY = ringRightY + RING_LABEL_OFFSET_Y;
                label.setAttribute('x', labelX);
                label.setAttribute('y', labelY);
                label.setAttribute('class', 'label hidden ring-label');
                labelsLayerGroup.appendChild(label);
                ringLabel = label;

                const connectorGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                connectorGroup.setAttribute('class', 'ring-label-line hidden');
                const horizontal = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                const horizEndX = labelX - RING_LABEL_SEGMENT_LENGTH;
                horizontal.setAttribute('x1', labelX);
                horizontal.setAttribute('y1', labelY - 2);
                horizontal.setAttribute('x2', horizEndX);
                horizontal.setAttribute('y2', labelY - 2);

                const diagonal = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                diagonal.setAttribute('x1', horizEndX);
                diagonal.setAttribute('y1', labelY - 2);
                diagonal.setAttribute('x2', ringRightX);
                diagonal.setAttribute('y2', ringRightY);

                const anchorDot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                anchorDot.setAttribute('cx', ringRightX);
                anchorDot.setAttribute('cy', ringRightY);
                anchorDot.setAttribute('r', 1);

                connectorGroup.appendChild(horizontal);
                connectorGroup.appendChild(diagonal);
                connectorGroup.appendChild(anchorDot);
                labelsLayerGroup.appendChild(connectorGroup);
                ringLabelLine = connectorGroup;
            }

            if(ringSelectionNode){
                const entry = { group: ringGroup, label: ringLabel, node: ringSelectionNode };
                registerSelectableNode(ringSelectionNode.id, entry, ringSelectionNode);
            }

            if(svgOnlyMode){
                return;
            }

            ringGroup.style.cursor = 'pointer';
            ringGroup.style.pointerEvents = 'auto';
            const activate = () => ringGroup.classList.add('ring-active');
            const deactivate = () => ringGroup.classList.remove('ring-active');
            const showRingLabel = () => {
                if(ringLabel) showLabel(ringLabel);
                if(ringLabelLine) showLabel(ringLabelLine);
            };
            const hideRingLabel = () => {
                if(ringLabel) hideLabel(ringLabel);
                if(ringLabelLine) hideLabel(ringLabelLine);
            };
            ringGroup.addEventListener('mouseenter', () => {
                activate();
                showRingLabel();
            });
            ringGroup.addEventListener('mouseleave', () => {
                deactivate();
                hideRingLabel();
            });
            const handleRingSelect = (event) => {
                event.stopPropagation();
                selectNode(ringGroup, ringLabel, ringSelectionNode);
            };
            ringGroup.addEventListener('click', (event) => {
                handleRingSelect(event);
                showRingLabel();
            });
            ringGroup.addEventListener('touchstart', (event) => {
                handleRingSelect(event);
                activate();
                showRingLabel();
            }, { passive: true });
            const endTouch = () => {
                deactivate();
                hideRingLabel();
            };
            ringGroup.addEventListener('touchend', endTouch);
            ringGroup.addEventListener('touchcancel', endTouch);
        });
    }

    function addStrokeWobble(target, values, dur) {
        const animateStroke = document.createElementNS('http://www.w3.org/2000/svg', 'animate');
        animateStroke.setAttribute('attributeName', 'stroke-width');
        animateStroke.setAttribute('values', values); // irregular stroke-widths
        animateStroke.setAttribute('dur', dur);
        animateStroke.setAttribute('repeatCount', 'indefinite');
        target.appendChild(animateStroke);
    }

    function addOpacityWobble(target, values, dur) {
        const animateOpacity = document.createElementNS('http://www.w3.org/2000/svg', 'animate');
        animateOpacity.setAttribute('attributeName', 'opacity');
        animateOpacity.setAttribute('values', values); // irregular opacity values
        animateOpacity.setAttribute('dur', dur);
        animateOpacity.setAttribute('repeatCount', 'indefinite');
        target.appendChild(animateOpacity);
    }

    function addStar(n, group) {
        const starColor = resolveStarColor(
            n.subType,
            n.temperature ?? (n.raw ? n.raw.surface_temperature : undefined)
        );

        const core = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        core.setAttribute('r', n.radiusScaled);
        core.setAttribute('fill', '#000');
        group.appendChild(core);

        const outerGlow = document.createElementNS('http://www.w3.org/2000/svg', 'circle');

        if (n.subType === "Neutron Star" || n.subType === "N") {
            const coneGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');

            // Define a blur filter
            const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
            const filter = document.createElementNS('http://www.w3.org/2000/svg', 'filter');
            filter.setAttribute('x', '-200%');    // move left boundary far outward
            filter.setAttribute('y', '-200%');    // move top boundary far upward
            filter.setAttribute('width', '400%'); // expand width far beyond element
            filter.setAttribute('height', '400%'); // expand height far beyond element
            filter.setAttribute('id', 'motionBlur');
            const gaussian = document.createElementNS('http://www.w3.org/2000/svg', 'feGaussianBlur');
            gaussian.setAttribute('in', 'SourceGraphic');
            gaussian.setAttribute('stdDeviation', '.1 4'); // X and Y blur (slightly elongated for motion effect)
            filter.appendChild(gaussian);
            defs.appendChild(filter);
            group.appendChild(defs);

            const makeCone = (angle) => {
                const cone = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                const base = n.radiusScaled;
                const coneLength = n.radiusScaled * 5;
                const coneWidth = n.radiusScaled * 1.5;

                const d = `
 M 0 0
 L ${-coneWidth} ${-coneLength}
 L ${coneWidth} ${-coneLength}
 Z
        `;
                cone.setAttribute('d', d);
                cone.setAttribute('fill', starColor);
                cone.setAttribute('opacity', 1);
                cone.setAttribute('transform', `rotate(${angle})`);

                // Apply motion blur
                cone.setAttribute('filter', 'url(#motionBlur)');

                return cone;
            };

            // Add two opposite cones
            coneGroup.appendChild(makeCone(0));
            coneGroup.appendChild(makeCone(180));
            coneGroup.setAttribute('class', 'notnode');

            const animate = document.createElementNS('http://www.w3.org/2000/svg', 'animateTransform');
            animate.setAttribute('attributeName', 'transform');
            animate.setAttribute('attributeType', 'XML');
            animate.setAttribute('type', 'rotate');
            animate.setAttribute('values', '-5 0 0; 5 0 0; -5 0 0');
            if (typeof n.axialTilt !== 'number' || isNaN(n.axialTilt)) {
                n.axialTilt = Math.random() * 360 * (Math.PI / 180); // radians
            }
            const rotationDegrees = n.axialTilt * (180 / Math.PI);

            animate.setAttribute('dur', `${n.rotationalPeriod * 1000}s`);
            animate.setAttribute('repeatCount', 'indefinite');
            coneGroup.appendChild(animate);

            const rotationGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            // console.log(n.rotationalPeriod, rotationDegrees);
            rotationGroup.setAttribute('transform', `rotate(${rotationDegrees})`);
            rotationGroup.appendChild(coneGroup);

            const dynamicSurface = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            dynamicSurface.setAttribute('r', n.radiusScaled);
            dynamicSurface.setAttribute('fill', starColor);
            rotationGroup.appendChild(dynamicSurface);

            group.appendChild(rotationGroup);
            group.appendChild(outerGlow);
        }

        else if (n.subType === "Black Hole" || n.subType === "Supermassive Black Hole" || n.subType === "H" || n.subType === "SupermassiveBlackHole") {
            // Path-based dynamic outer glow
            const outerGlowGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');

            // Scale factor based on core radius (assumes original path fits ~radius 5 units)
            const scale = n.radiusScaled / 5;
            outerGlowGroup.setAttribute(
                'transform',
                `translate(${0},${0}) scale(${scale})  rotate(-17) translate(${-14.155},${-5.5562})`
            );

            // Yellow ring
            const circlePath = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circlePath.setAttribute('cx', '14.155');
            circlePath.setAttribute('cy', '5.5562');
            circlePath.setAttribute('r', '4.7625');
            circlePath.setAttribute('stroke', '#ff0');
            circlePath.setAttribute('stroke-width', '1.5875');
            circlePath.setAttribute('fill', 'none');

            // Black inner path
            const blackPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            blackPath.setAttribute('d', 'm27.914 6.0854c-1.9519 0.47831-6.0628 1.5317-13.755 1.5317-7.5591 0-12.417-1.0033-13.761-1.5317');
            blackPath.setAttribute('stroke', '#000');
            blackPath.setAttribute('stroke-width', '.79375');
            blackPath.setAttribute('fill', 'none');

            // Yellow dynamic path
            const yellowPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            yellowPath.setAttribute('d', 'm0.39687 6.0854c6.4033 0 7.2841 0.0384 8.2751-1.2857 0.99096-1.3241 1.951-3.9814 5.4822-4.0078 3.3346-0.0249 4.3186 2.8048 5.4191 3.9427 1.1005 1.1378 1.9314 1.3508 8.3404 1.3508 0 0-5.5415 1.3299-13.736 1.3299-8.0607 0-13.781-1.3299-13.781-1.3299z');
            yellowPath.setAttribute('stroke', '#ff0');
            yellowPath.setAttribute('stroke-width', '.09');
            yellowPath.setAttribute('fill', 'none');

            addStrokeWobble(yellowPath, '0.1;0.15;0.12;.35;0.08', '4s');
            addStrokeWobble(blackPath, '0.7;0.4;0.6;0.1;0.2', '1.7s');
            addStrokeWobble(circlePath, '0.3;0.5;0.3;.1;0.4', '3s');
            addOpacityWobble(circlePath, '1;0.8;0.7;0.9;1', '1.5s');

            outerGlowGroup.appendChild(circlePath);
            outerGlowGroup.appendChild(blackPath);
            outerGlowGroup.appendChild(yellowPath);
            outerGlowGroup.setAttribute('class', 'notnode');

            group.appendChild(outerGlowGroup);
        } else {
            const coreGradientId = `sun-core-${Math.random().toString(36).slice(2)}`;
            const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
            const brightCore = mixWithWhite(starColor, 0.35);
            const softEdge = mixWithWhite(starColor, 0.15);
            defs.innerHTML = `
        <radialGradient id="${coreGradientId}" cx="50%" cy="50%" r="65%">
          <stop offset="0%" stop-color="${starColor}"/>
          <stop offset="55%" stop-color="${starColor}"/>
          <stop offset="100%" stop-color="${softEdge}"/>
        </radialGradient>
      `;
            group.appendChild(defs);

            const outerGlow = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            outerGlow.setAttribute('r', n.radiusScaled);
            outerGlow.setAttribute('fill', starColor);
            outerGlow.setAttribute('opacity', 0.35);
            outerGlow.setAttribute('filter', 'url(#glowBlur)');
            outerGlow.setAttribute('class', 'notnode');

            const core = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            core.setAttribute('r', n.radiusScaled);
            core.setAttribute('fill', `url(#${coreGradientId})`);
            core.setAttribute('filter', 'url(#sunDisplace)');

            const texture = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            texture.setAttribute('r', n.radiusScaled);
            texture.setAttribute('fill', '#000');
            texture.setAttribute('opacity', '0.32');
            texture.setAttribute('filter', 'url(#sunTexture)');
            texture.setAttribute('class', 'notnode');

            group.appendChild(outerGlow);
            group.appendChild(core);
            group.appendChild(texture);
        }

        group.setAttribute('class', 'node');
        const mask = document.createElementNS('http://www.w3.org/2000/svg', 'mask');
        const maskId = buildMaskId(n);
        mask.setAttribute('id', maskId);
        // White rectangle (everything visible by default)
        const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        rect.setAttribute('x', -10000);
        rect.setAttribute('y', -10000);
        rect.setAttribute('width', 20000);
        rect.setAttribute('height', 20000);
        rect.setAttribute('fill', 'white');
        mask.appendChild(rect);

        // Black circle to hide the planet area
        const circleMask = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circleMask.setAttribute('r', n.radiusScaled);
        circleMask.setAttribute('cx', 0);
        circleMask.setAttribute('cy', 0);
        circleMask.setAttribute('fill', 'black');
        mask.appendChild(circleMask);

        group.appendChild(mask);

        addRings(n, group, maskId);
    }

    function addStellarRingBody(n, group){
        const baseRadius = Math.max(n.radiusScaled || 10, 12);
        const iconSize = Math.max(baseRadius * 5, 25);
        const image = document.createElementNS('http://www.w3.org/2000/svg', 'image');
        image.setAttribute('width', iconSize);
        image.setAttribute('height', iconSize);
        image.setAttribute('x', -iconSize / 2);
        image.setAttribute('y', -iconSize / 2);
        image.setAttribute('preserveAspectRatio', 'xMidYMid meet');
        image.setAttribute('class', 'node stellarring-icon');
        const asteroidIconPath = '/static/assets/asteroidbelt.svg';
        image.setAttributeNS('http://www.w3.org/1999/xlink', 'href', asteroidIconPath);
        image.setAttribute('href', asteroidIconPath);
        group.appendChild(image);
    }

    function addAsteroidClusterBody(n, group){
        const baseRadius = Math.max(Math.min(n.radiusScaled || 10, 20), 10);
        const iconSize = Math.max(baseRadius * 1, 10);
        const iconGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        const initialAngle = Math.random() * 360;
        iconGroup.setAttribute('transform', `rotate(${initialAngle})`);

        const image = document.createElementNS('http://www.w3.org/2000/svg', 'image');
        image.setAttribute('width', iconSize);
        image.setAttribute('height', iconSize);
        image.setAttribute('x', -iconSize / 2);
        image.setAttribute('y', -iconSize / 2);
        image.setAttribute('preserveAspectRatio', 'xMidYMid meet');
        image.setAttribute('class', 'node asteroid-cluster-icon');
        const asteroidClusterIconPath = '/static/assets/asteroid.svg';
        image.setAttributeNS('http://www.w3.org/1999/xlink', 'href', asteroidClusterIconPath);
        image.setAttribute('href', asteroidClusterIconPath);

        const rotationAnim = document.createElementNS('http://www.w3.org/2000/svg', 'animateTransform');
        rotationAnim.setAttribute('attributeName', 'transform');
        rotationAnim.setAttribute('attributeType', 'XML');
        rotationAnim.setAttribute('type', 'rotate');
        rotationAnim.setAttribute('from', `${initialAngle}`);
        rotationAnim.setAttribute('to', `${initialAngle + 360}`);
        rotationAnim.setAttribute('dur', `${80 + Math.random() * 40}s`);
        rotationAnim.setAttribute('repeatCount', 'indefinite');

        iconGroup.appendChild(image);
        iconGroup.appendChild(rotationAnim);
        group.appendChild(iconGroup);
    }

    function addPlanet(n, group) {
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('r', n.radiusScaled);

        let fillColor;
        if (n.subType && n.subType.toLowerCase().includes("earthlike body")) {
            fillColor = "#4fa0ff"; // base ocean color
        } else if (n.subType && n.subType.toLowerCase().includes("gas giant")) {
            fillColor = gasGiantColor(n.subType, n.temperature);
        } else if (n.atmosphereType && n.atmosphereType.toLowerCase().includes("sulphur")) {
            fillColor = "#ceab4e";
        } else {
            fillColor = rockyPlanetColor(n.subType, n.temperature);
        }
        circle.setAttribute('fill', fillColor);

        if (n.subType && n.subType.toLowerCase().includes("earthlike body")) {
            circle.setAttribute('filter', 'url(#earthLikeTexture)');
        }
        const mask = document.createElementNS('http://www.w3.org/2000/svg', 'mask');
        const maskId = buildMaskId(n);
        mask.setAttribute('id', maskId);

        // White rectangle (everything visible by default)
        const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        rect.setAttribute('x', -10000);
        rect.setAttribute('y', -10000);
        rect.setAttribute('width', 20000);
        rect.setAttribute('height', 20000);
        rect.setAttribute('fill', 'white');
        mask.appendChild(rect);

        // Black circle to hide the planet area
        const circleMask = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circleMask.setAttribute('r', n.radiusScaled);
        circleMask.setAttribute('cx', 0);
        circleMask.setAttribute('cy', 0);
        circleMask.setAttribute('fill', 'black');
        mask.appendChild(circleMask);

        group.appendChild(mask);
        const rotationGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');

        // Use axialTilt if available, otherwise fallback to random
        const rotationDegrees = (typeof n.axialTilt === 'number' && !isNaN(n.axialTilt))
              ? n.axialTilt * (180 / Math.PI)  // convert from radians to degrees if needed
              : Math.random() * 360;

        rotationGroup.setAttribute('transform', `rotate(${rotationDegrees})`);

        rotationGroup.appendChild(circle);
        const outerGlow = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        outerGlow.setAttribute('r', n.radiusScaled);
        outerGlow.setAttribute('fill', 'url(#shadowAtmosphere)');
        outerGlow.setAttribute('opacity', '0.8');
        group.appendChild(rotationGroup);
        group.appendChild(outerGlow);
        group.setAttribute('class', 'node');

        addRings(n, group, maskId);
        if (n.atmosphereType !== null && n.atmosphereType !== "No atmosphere" && n.surfacePressure > 0) {
            addAtmosphere(n, group);
        }
        if (n.isLandable === true) {
            addLandable(n, group);
        }
    }

    function addSvgDefs(id64) {
        const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
        defs.innerHTML = `
      <filter id="sunDisplace" x="-60%" y="-60%" width="220%" height="220%">
        <feTurbulence type="fractalNoise" baseFrequency="0.28" numOctaves="2" seed="${id64}" result="noise">
          <animate attributeName="baseFrequency" values="0.23;0.32;0.25;0.23" dur="12s" repeatCount="indefinite"/>
        </feTurbulence>
        <feDisplacementMap in="SourceGraphic" in2="noise" scale="2.4" xChannelSelector="R" yChannelSelector="G">
          <animate attributeName="scale" values="1.4;2.8;2;1.4" dur="14s" repeatCount="indefinite"/>
        </feDisplacementMap>
      </filter>
      <filter id="sunTexture" x="-30%" y="-30%" width="160%" height="160%">
        <feTurbulence type="fractalNoise" baseFrequency="0.35" numOctaves="3" seed="${id64}" result="noise">
          <animate attributeName="baseFrequency"
                   values="0.42;0.3;0.3;0.3"
                   keyTimes="0;0.82;0.8201;1"
                   dur="36s"
                   repeatCount="indefinite"/>
        </feTurbulence>
        <feGaussianBlur in="noise" stdDeviation="0.5" result="softNoise"/>
        <feComponentTransfer in="softNoise" result="mask">
          <feFuncA type="table" tableValues="0 0.35 0.75 0.9 1"/>
        </feComponentTransfer>
        <feComposite in="SourceGraphic" in2="mask" operator="in"/>
      </filter>
      <filter id="blurStroke" x="-50%" y="-50%" width="200%" height="200%">
        <feGaussianBlur in="SourceGraphic" stdDeviation=".3" />
      </filter>
      <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
        <feGaussianBlur in="SourceGraphic" stdDeviation="3" result="blur"/>
        <feMerge>
          <feMergeNode in="blur"/>
          <feMergeNode in="SourceGraphic"/>
          </feMerge>
      </filter>
      <filter id="glowBlur" x="-70%" y="-70%" width="240%" height="240%">
        <!-- Strong inner halo -->
        <feGaussianBlur in="SourceGraphic" stdDeviation="6" result="blur1"/>
        <!-- Wider outer glow -->
        <feGaussianBlur in="SourceGraphic" stdDeviation="23" result="blur2"/>
        <!-- Even wider faint aura -->
        <feGaussianBlur in="SourceGraphic" stdDeviation="45" result="blur3"/>
        <!-- Merge everything -->
        <feMerge>
          <feMergeNode in="blur3"/>
          <feMergeNode in="blur2"/>
          <feMergeNode in="blur1"/>
          <feMergeNode in="SourceGraphic"/>
          </feMerge>
      </filter>
      <filter id="earthLikeTexture" x="-50%" y="-50%" width="200%" height="200%" primitiveUnits="objectBoundingBox">
        <!-- Fractal noise base -->
        <feTurbulence type="fractalNoise" baseFrequency="0.09" numOctaves="5" seed="${id64}" result="noise"/>
        <!-- Colorize the noise -->
        <feColorMatrix in="noise" result="coloredNoise"
                       values="1 0 0 0 0
          1 0 0 0 0
          1 0 0 0 0
          0 0 0 0 1"/>
        <!-- Map colors for texture variation -->
        <feComponentTransfer in="coloredNoise" result="mapped">
          <feFuncR type="table" tableValues="0 .02 .03 .03 .05 .08 .12 .35 .25 .03 0 0"/>
          <feFuncG type="table" tableValues=".01 .03 .05 .07 .08 .12 .18 .28 .22 .12 .05 .01"/>
          <feFuncB type="table" tableValues=".015 .08 .15 .14 .22 .28 .32 .45 .22 .08 .015 0"/>
        </feComponentTransfer>
        <!-- Offset for slow horizontal movement -->
        <feOffset in="mapped" dx="0" dy="0" result="shifted">
          <animate attributeName="dx" from="0" to=".5" dur="60s" repeatCount="indefinite"/>
        </feOffset>
        <!-- Tile the offset noise to avoid edges -->
        <feTile in="shifted" result="tiled"/>
        <!-- Apply texture to the planet -->
        <feComposite in="tiled" in2="SourceGraphic" operator="in"/>
      </filter>
      <radialGradient id="shadowAtmosphere" cx="35%" cy="50%" r="55%">
        <stop stop-color="#000" stop-opacity="0" offset="0.82"/>
        <stop offset="1"/>
      </radialGradient>
      <marker id="smoothCap" overflow="visible" markerHeight="1" markerWidth="1" orient="auto-start-reverse" preserveAspectRatio="none" viewBox="0 0 0.525 1">
        <path transform="scale(.5)" d="m0-1 1 1-1 1-0.05-1z" fill="context-stroke"/>
      </marker>
      <radialGradient id="atmoGrad" cx="50%" cy="50%" r="50%">
        <stop offset="60%" stop-color="#27667f" stop-opacity="0" />
        <stop offset="100%" stop-color="#27667f" stop-opacity=".8" />
      </radialGradient>
      <linearGradient id="fadeStroke" gradientUnits="userSpaceOnUse" x1="-5" y1="-3" x2="5" y2="3">
        <stop offset="0%" stop-color="#44b5d2" stop-opacity="1"/>
        <stop offset="50%" stop-color="#44b5d2" stop-opacity="0.2"/>
      </linearGradient>
    `;
        svg.appendChild(defs);
    }

    function draw(nodes, bounds, title, preselectBodyId = null){
        const skipSiblingPairs = computeBarycenterSkipPairs(nodes);
        const drawnLinks = new Set();
        while(svg.firstChild) svg.removeChild(svg.firstChild);
        const w = Math.max(800, Math.ceil(bounds.maxX));
        const h = Math.max(600, Math.ceil(bounds.maxY));
        svg.setAttribute('viewBox', `0 0 ${w} ${h}`);
        addSvgDefs(nodes[0].id64);
        const showDebug = document.getElementById('debugToggle')?.checked ?? null;
        const ns = 'http://www.w3.org/2000/svg';
        nodeElementsById = new Map();
        labelsLayerGroup = null;

        const formatBrandingTime = (date) => {
            const hours = String(date.getUTCHours()).padStart(2, '0');
            const minutes = String(date.getUTCMinutes()).padStart(2, '0');
            const seconds = String(date.getUTCSeconds()).padStart(2, '0');
            return `${hours}:${minutes}:${seconds}`;
        };

        const formatBrandingDate = (date) => {
            const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
            const day = String(date.getUTCDate()).padStart(2, '0');
            const month = months[date.getUTCMonth()];
            const year = date.getUTCFullYear() + 1286; // lean into the ED timeline
            return `${day} ${month} ${year}`;
        };

        const buildBrandingPanel = (systemTitle) => {
            const group = document.createElementNS(ns, 'g');
            group.setAttribute('class', 'uc-branding');
            group.setAttribute('transform', 'translate(0 10)');

            const panelWidth = 145;
            const panelHeight = 25;
            const padding = 0;

            const panel = document.createElementNS(ns, "rect");
            panel.setAttribute("width", panelWidth);
            panel.setAttribute("height", panelHeight);
            panel.setAttribute("fill", "none"); // or a color if you want a filled background
            group.appendChild(panel);

            // top border
            const topBorder = document.createElementNS(ns, "line");
            topBorder.setAttribute("x1", 0);
            topBorder.setAttribute("y1", 0);
            topBorder.setAttribute("x2", panelWidth);
            topBorder.setAttribute("y2", 0);
            topBorder.setAttribute("stroke", "#fff");
            topBorder.setAttribute("stroke-width", ".2");

            // bottom border
            const bottomBorder = document.createElementNS(ns, "line");
            bottomBorder.setAttribute("x1", 0);
            bottomBorder.setAttribute("y1", panelHeight);
            bottomBorder.setAttribute("x2", panelWidth);
            bottomBorder.setAttribute("y2", panelHeight);
            bottomBorder.setAttribute("stroke", "#fff");
            bottomBorder.setAttribute("stroke-width", ".2");

            group.appendChild(topBorder);
            group.appendChild(bottomBorder);

            const logoGroup = document.createElementNS(ns, 'g');
            logoGroup.setAttribute('class', 'uc-branding__logo');
            logoGroup.setAttribute('transform', `translate(${padding + 22} ${panelHeight / 2})`);


            group.appendChild(logoGroup);
            const iconSize = 25;
            const edgisLogo = document.createElementNS('http://www.w3.org/2000/svg', 'image');
            edgisLogo.setAttribute('width', iconSize);
            edgisLogo.setAttribute('height', iconSize);
            edgisLogo.setAttribute('x', -iconSize / 2 - 8);
            edgisLogo.setAttribute('y', -iconSize / 2);
            edgisLogo.setAttribute('preserveAspectRatio', 'xMidYMid meet');
            edgisLogo.setAttribute('class', 'node stellarring-icon');
            const edgisLogoPath = '/static/assets/edgiswhite.svg';
            edgisLogo.setAttributeNS('http://www.w3.org/1999/xlink', 'href', edgisLogoPath);
            edgisLogo.setAttribute('href', edgisLogoPath);
            logoGroup.appendChild(edgisLogo);

            const headline = document.createElementNS(ns, 'text');
            headline.textContent = 'EDGIS';
            headline.setAttribute('class', 'uc-branding__headline');
            headline.setAttribute('x', padding + 26);
            headline.setAttribute('y', padding + 21);
            group.appendChild(headline);

            const accentX = padding + 100;
            const accent = document.createElementNS(ns, 'line');
            accent.setAttribute('class', 'uc-branding__accent');
            accent.setAttribute('x1', accentX);
            accent.setAttribute('y1', padding + 3.5);
            accent.setAttribute('x2', accentX);
            accent.setAttribute('y2', panelHeight - padding - 3.5);
            group.appendChild(accent);

            const now = new Date();
            const time = document.createElementNS(ns, 'text');
            time.textContent = formatBrandingTime(now);
            time.setAttribute('class', 'uc-branding__time');
            time.setAttribute('x', accentX + 4);
            time.setAttribute('y', padding + 11);
            group.appendChild(time);

            const dateText = document.createElementNS(ns, 'text');
            dateText.textContent = formatBrandingDate(now);
            dateText.setAttribute('class', 'uc-branding__date');
            dateText.setAttribute('x', accentX + 7);
            dateText.setAttribute('y', padding + 19);
            group.appendChild(dateText);

            return group;
        };

        const index = new Map(nodes.map(n => [n.id, n]));
        const baryDepthCache = new WeakMap();
        const computeBaryDepth = (node) => {
            if(!node || !isBarycenter(node)) return 0;
            if(baryDepthCache.has(node)) return baryDepthCache.get(node);
            const baryKids = (node.baryChildren || []).filter(isBarycenter);
            if(baryKids.length === 0){
                baryDepthCache.set(node, 0);
                return 0;
            }
            const depth = 1 + Math.max(...baryKids.map(child => computeBaryDepth(child)));
            baryDepthCache.set(node, depth);
            return depth;
        };
        nodes.forEach(parent => {
            if(!index.has(parent.id)) return;
            const kids = (parent.children || []).filter(child => child && index.has(child.id) && !isBarycenter(child));
            if(kids.length === 0) return;

            if(!isBarycenter(parent)){
                drawLink(parent, kids[0]);
            }

            for(let i = 1; i < kids.length; i++){
                if(!skipSiblingPairs.has(pairKey(kids[i-1].id, kids[i].id))){
                    drawLink(kids[i-1], kids[i]);
                }
            }
        });

        function drawLink(a, b){
            const key = pairKey(a.id, b.id);
            if(!key || drawnLinks.has(key)) return;
            drawnLinks.add(key);

            const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x1', a.x);
            line.setAttribute('y1', a.y);
            line.setAttribute('x2', b.x);
            line.setAttribute('y2', b.y);
            line.setAttribute('stroke-width', '1.5');
            line.setAttribute('class', 'link');
            svg.appendChild(line);
        }

        const appendBracketLine = (x1, y1, x2, y2, debugLabel, baryNode) => {
            const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x1', x1);
            line.setAttribute('y1', y1);
            line.setAttribute('x2', x2);
            line.setAttribute('y2', y2);
            line.setAttribute('stroke', BARY_BRACKET_STROKE);
            line.setAttribute('stroke-width', '1.4');
            line.setAttribute('stroke-linecap', 'round');
            line.setAttribute('pointer-events', 'stroke');
            line.setAttribute('class', 'bary-bracket');
            if(debugLabel){
                line.dataset.barycenter = debugLabel;
            }
            if(baryNode){
                line.addEventListener('click', (event) => {
                    event.stopPropagation();
                    showBarycenterInfo(baryNode);
                    infoPanel?.scrollTo({ top: 0, behavior: 'smooth' });
                });
                line.addEventListener('pointerup', (event) => {
                    event.stopPropagation();
                    showBarycenterInfo(baryNode);
                    infoPanel?.scrollTo({ top: 0, behavior: 'smooth' });
                });
            }
            const targetLayer = baryBracketLayer || baryLayerGroup || svg;
            targetLayer.appendChild(line);
        };

        const appendBracketPointer = (x1, y1, x2, y2, orientation, debugLabel) => {
            let d;
            if(orientation === 'horizontal'){
                d = `M ${x1} ${y1} L ${x1} ${y2}`;
            } else {
                d = `M ${x1} ${y1} L ${x2} ${y1}`;
            }
            const pointer = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            pointer.setAttribute('d', d);
            pointer.setAttribute('fill', 'none');
            pointer.setAttribute('stroke', BARY_BRACKET_STROKE);
            pointer.setAttribute('stroke-width', '1.4');
            pointer.setAttribute('stroke-linecap', 'round');
            pointer.setAttribute('stroke-linejoin', 'round');
            pointer.setAttribute('pointer-events', 'stroke');
            pointer.setAttribute('class', 'bary-bracket');
            if(debugLabel){
                pointer.dataset.barycenter = debugLabel;
            }
            const targetLayer = baryBracketLayer || baryLayerGroup || svg;
            targetLayer.appendChild(pointer);
        };

        const drawBarycenterBracket = (node, showLabel = false) => {
            const kids = (node.baryChildren && node.baryChildren.length
                          ? node.baryChildren
                          : (node.children || [])).filter(Boolean);
            const debugLabel = node.name || `barycenter_${node.id || '??'}`;
            if(kids.length < 2){
                if(showLabel){
                    appendBaryLabel(node, node.x + 6, node.y - 6);
                }
                return;
            }
            const childStats = kids.map(child => ({
                x: child.x,
                y: child.y,
                r: child.radiusScaled || radius
            }));

            const minXEdge = Math.min(...childStats.map(c => c.x - c.r));
            const maxXEdge = Math.max(...childStats.map(c => c.x + c.r));
            const minYEdge = Math.min(...childStats.map(c => c.y - c.r));
            const maxYEdge = Math.max(...childStats.map(c => c.y + c.r));
            const horizontal = (maxXEdge - minXEdge) >= (maxYEdge - minYEdge);

            const layoutKids = horizontal ? kids : [...kids].sort((a, b) => getNodeMass(b) - getNodeMass(a));
            const layoutStats = horizontal ? childStats : layoutKids.map(child => ({
                x: child.x,
                y: child.y,
                r: child.radiusScaled || radius
            }));

            const pointerAnchors = layoutKids.map(child => {
                const target = (isBarycenter(child) && child.baryNodeTarget) ? child.baryNodeTarget : null;
                return {
                    pointerX: target ? target.x : child.x,
                    pointerY: target ? target.y : child.y
                };
            });

            const childTopClearance = layoutKids.map((child, idx) => {
                if(isBarycenter(child) && child.baryConnectorPoint){
                    return child.baryConnectorPoint.y;
                }
                return layoutStats[idx].y - layoutStats[idx].r;
            });
            const childLeftClearance = layoutKids.map((child, idx) => {
                if(isBarycenter(child) && child.baryConnectorPoint){
                    return child.baryConnectorPoint.x;
                }
                return layoutStats[idx].x - layoutStats[idx].r;
            });

            const pointerXs = pointerAnchors.map(p => p.pointerX);
            const pointerYs = pointerAnchors.map(p => p.pointerY);

            if(horizontal){
                const clearanceY = Math.min(...childTopClearance);
                const legY = clearanceY - BARY_CLEARANCE;
                const startX = Math.min(...pointerXs);
                const endX = Math.max(...pointerXs);
                const midX = (startX + endX) / 2;
                appendBracketLine(startX, legY, endX, legY, debugLabel, node);
                appendBaryIcon(midX, legY - BARY_ICON_OFFSET_Y, node);
                node.baryConnectorPoint = { x: midX, y: legY };
                node.baryNodeTarget = { x: midX, y: legY - BARY_ICON_OFFSET_Y };
                layoutKids.forEach((child, idx) => {
                    const isChildBary = isBarycenter(child) && child.baryNodeTarget;
                    const pointerX = pointerAnchors[idx].pointerX;
                    const targetY = isChildBary ? child.baryNodeTarget.y
                          : child.y - (child.radiusScaled || radius);
                    appendBracketPointer(pointerX, legY, pointerX, targetY, 'horizontal', debugLabel);
                });
                if(showLabel){
                    appendBaryLabel(node, midX, legY - BARY_LABEL_OFFSET_Y, 'middle');
                }
            } else {
                const clearanceX = Math.min(...childLeftClearance);
                const legX = clearanceX - BARY_CLEARANCE;
                const startY = Math.min(...pointerYs);
                const endY = Math.max(...pointerYs);
                const midY = (startY + endY) / 2;
                appendBracketLine(legX, startY, legX, endY, debugLabel, node);
                appendBaryIcon(legX - 8, midY, node);
                node.baryConnectorPoint = { x: legX, y: midY };
                node.baryNodeTarget = { x: legX - 8, y: midY };
                layoutKids.forEach((child, idx) => {
                    const isChildBary = isBarycenter(child) && child.baryNodeTarget;
                    const pointerY = pointerAnchors[idx].pointerY;
                    const targetX = isChildBary ? child.baryNodeTarget.x
                          : child.x - (child.radiusScaled || radius);
                    appendBracketPointer(legX, pointerY, targetX, pointerY, 'vertical', debugLabel);
                });
                if(showLabel){
                    appendBaryLabel(node, legX - 8, midY - 10, 'middle');
                }
            }
        };

        const appendBaryIcon = (cx, cy, baryNode) => {
            const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            group.setAttribute('transform', `translate(${cx}, ${cy})`);
            if(baryNode?.id != null){
                group.dataset.bodyId = String(baryNode.id);
            }
            group.setAttribute('tabindex', '0');
            group.setAttribute('role', 'button');
            group.setAttribute('aria-label', baryNode?.name ? `Barycenter ${baryNode.name}` : 'Barycenter');

            const outerRadius = 6;
            const innerRadius = 3;

            const outerCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            outerCircle.setAttribute('r', outerRadius);
            outerCircle.setAttribute('stroke', '#8d9bc2');
            outerCircle.setAttribute('stroke-width', '.4');

            const innerCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            innerCircle.setAttribute('r', innerRadius);
            innerCircle.setAttribute('stroke', '#eef2ff');
            innerCircle.setAttribute('stroke-width', '.4');

            const leftDot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            leftDot.setAttribute('cx', -innerRadius);
            leftDot.setAttribute('cy', 0);
            leftDot.setAttribute('r', 1.5);
            leftDot.setAttribute('fill', '#eef2ff');

            const rightDot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            rightDot.setAttribute('cx', outerRadius);
            rightDot.setAttribute('cy', 0);
            rightDot.setAttribute('r', .8);
            rightDot.setAttribute('fill', '#8d9bc2');

            group.appendChild(outerCircle);
            group.appendChild(innerCircle);
            group.appendChild(leftDot);
            group.appendChild(rightDot);
            group.style.cursor = 'pointer';
            group.style.pointerEvents = 'auto';
            group.style.touchAction = 'manipulation';
            group.addEventListener('click', (event) => {
                event.stopPropagation();
                selectNode(group, null, baryNode);
                infoPanel?.scrollTo({ top: 0, behavior: 'smooth' });
            });
            group.addEventListener('pointerup', (event) => {
                event.stopPropagation();
                selectNode(group, null, baryNode);
                infoPanel?.scrollTo({ top: 0, behavior: 'smooth' });
            });
            group.addEventListener('touchend', (event) => {
                event.stopPropagation();
                selectNode(group, null, baryNode);
                infoPanel?.scrollTo({ top: 0, behavior: 'smooth' });
            }, { passive: true });
            group.addEventListener('mouseenter', () => {
                group.classList.add('active');
            });
            group.addEventListener('mouseleave', () => {
                group.classList.remove('active');
            });
            const targetLayer = baryIconLayer || baryLayerGroup || svg;
            targetLayer.appendChild(group);
            if(baryNode){
                registerSelectableNode(baryNode.id, { group, label: null, node: baryNode }, baryNode);
            }
        };

        const appendBaryLabel = (node, x, y, anchor = 'start') => {
            const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            label.textContent = node.name || 'Barycenter';
            label.setAttribute('x', x);
            label.setAttribute('y', y);
            label.setAttribute('class', 'bary-label');
            label.setAttribute('text-anchor', anchor);
            const targetLayer = baryIconLayer || baryLayerGroup || svg;
            targetLayer.appendChild(label);
            if(node){
                let existing = null;
                if(node.id != null){
                    existing = nodeElementsById?.get(node.id) || null;
                }
                if(!existing){
                    const shareableId = resolveNodeBodyId(node);
                    if(shareableId != null){
                        existing = nodeElementsById?.get(shareableId) || null;
                    }
                }
                if(!existing){
                    existing = { group: null, node, label: null };
                }
                existing.label = label;
                registerSelectableNode(node.id, existing, node);
            }
        };

        baryLayerGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        baryLayerGroup.setAttribute('class', 'bary-layer');
        svg.appendChild(baryLayerGroup);

        baryBracketLayer = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        baryBracketLayer.setAttribute('class', 'bary-layer-brackets');
        baryLayerGroup.appendChild(baryBracketLayer);

        baryIconLayer = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        baryIconLayer.setAttribute('class', 'bary-layer-overlay');
        baryLayerGroup.appendChild(baryIconLayer);

        const nodesGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        const labelsGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        labelsGroup.setAttribute('class', 'labels-layer');
        svg.appendChild(nodesGroup);
        svg.appendChild(labelsGroup);
        labelsLayerGroup = labelsGroup;
        clearSelection({ updateShareState: false });

        const baryNodes = nodes.filter(isBarycenter)
              .sort((a, b) => computeBaryDepth(a) - computeBaryDepth(b));
        baryNodes.forEach(n => drawBarycenterBracket(n, true));

        nodes.forEach(n => {
            if (isBarycenter(n)) {
                return;
            }

            if(showDebug){
                const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
                rect.setAttribute('x', n.x);
                rect.setAttribute('y', n.y);
                rect.setAttribute('width', n.width);
                rect.setAttribute('height', n.height);
                rect.setAttribute('class', 'debug');
                nodesGroup.appendChild(rect);
            }

            const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            group.setAttribute('transform', `translate(${n.x}, ${n.y})`);
            group.classList.add('node');
            group.dataset.bodyId = String(n.id);

            if (n.type === 'Star'){ // Star
                addStar(n, group);
            } else if (isStellarRingNode(n)) {
                addStellarRingBody(n, group);
            } else if (isAsteroidClusterNode(n)) {
                addAsteroidClusterBody(n, group);
            } else {
                // Planet
                addPlanet(n, group);
            }
            nodesGroup.appendChild(group);

            const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            label.textContent = `${n.name}`;
            label.setAttribute('text-anchor', 'middle');
            label.setAttribute('x', n.x);
            const labelYOffset = n.radiusScaled + 26;
            label.setAttribute('y', n.y - labelYOffset);
            label.setAttribute('dominant-baseline', 'bottom');
            label.setAttribute('class', 'label hidden');
            label.dataset.node = String(n.id);
            label.id = `${n.name} (#${n.id})`;
            labelsGroup.appendChild(label);
            registerSelectableNode(n.id, { group, label, node: n }, n);

            if(!svgOnlyMode){
                group.addEventListener('mouseenter', () => {
                    showLabel(label);
                });
                group.addEventListener('mouseleave', () => {
                    hideLabel(label);
                });
                group.addEventListener('click', () => selectNode(group, label, n));
                group.addEventListener('touchstart', () => selectNode(group, label, n), {passive: true});
            }
        });

        const normalizedPreselectId = preselectBodyId != null ? Number(preselectBodyId) : NaN;
        if(Number.isFinite(normalizedPreselectId)){
            const matched = selectBodyById(normalizedPreselectId);
            if(!matched){
                handleSelectionChange();
            }
        }

        const titleText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        titleText.textContent = `${title}`;
        titleText.setAttribute('x', 300);
        titleText.setAttribute('y', 34);
        titleText.setAttribute('class', 'systemName');
        titleText.setAttribute('opacity', '.8');
        svg.appendChild(titleText);
        const titleBox = titleText.getBBox();
        const titlePowerSeparator = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        titlePowerSeparator.textContent = "|";
        titlePowerSeparator.setAttribute('x', titleBox.x + titleBox.width + 2);
        titlePowerSeparator.setAttribute('y', 32.2);
        titlePowerSeparator.setAttribute('class', 'titlePowerSeparator');
        titlePowerSeparator.setAttribute('opacity', '.8');
        svg.appendChild(titlePowerSeparator);
        const powerplayText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        powerplayText.textContent = "NONE";
        powerplayText.setAttribute('x', titleBox.x + titleBox.width + 10);
        powerplayText.setAttribute('y', 34);
        powerplayText.setAttribute('class', 'powerName');
        powerplayText.setAttribute('opacity', '.8');
        svg.appendChild(powerplayText);

        const brandingPanel = buildBrandingPanel(title);
        svg.appendChild(brandingPanel);
    }

    function resolveNodeBodyId(node){
        if(!node) return null;
        if(node.isRingNode){
            const ring = node.ring || node.raw || {};
            const ringIdCandidate = node.ringBodyId ?? ring.bodyId ?? ring.body_id ?? ring.id;
            const ringId = parseBodyIdParam(ringIdCandidate);
            if(ringId != null){
                return ringId;
            }
        }
        const raw = node.raw || {};
        const candidate = node.id ?? raw.body_id ?? raw.bodyId ?? raw.id;
        return parseBodyIdParam(candidate);
    }

    function registerSelectableNode(primaryId, entry, nodeOverride = null){
        if(!nodeElementsById || !entry) return;
        const keys = new Set();
        if(primaryId !== undefined && primaryId !== null){
            keys.add(primaryId);
        }
        const targetNode = nodeOverride || entry.node;
        const shareableId = resolveNodeBodyId(targetNode);
        if(shareableId != null){
            keys.add(shareableId);
        }
        if(!keys.size) return;
        keys.forEach((key) => {
            if(key !== undefined && key !== null){
                nodeElementsById.set(key, entry);
            }
        });
    }

    function selectNode(group, label, nodeData){
        if(selectedNodeGroup === group) return;
        if(selectedNodeGroup){
            selectedNodeGroup.classList.remove('selected');
        }
        selectedNodeGroup = group;
        if(group){
            group.classList.add('selected');
        }
        selectedBodyNode = nodeData || null;
        applySelectionMarker(group, nodeData);
        if(nodeData){
            renderBodyInfo(nodeData);
        }
        handleSelectionChange();
    }

    function selectBodyById(bodyId){
        if(!nodeElementsById || !nodeElementsById.size) return false;
        const entry = nodeElementsById.get(bodyId);
        if(!entry) return false;
        selectNode(entry.group, entry.label, entry.node);
        return true;
    }

    function clearSelection({ updateShareState = true } = {}){
        if(selectedNodeGroup){
            selectedNodeGroup.classList.remove('selected');
            selectedNodeGroup = null;
        }
        selectedBodyNode = null;
        removeSelectionMarker();
        if(infoPanel){
            infoPanel.style.display = 'none';
            infoPanel.innerHTML = '';
        }
        if(updateShareState){
            handleSelectionChange();
        }
    }

    function showLabel(label){
        label.classList.remove('hidden');
        label.classList.add('visible');
    }

    function hideLabel(label){
        label.classList.remove('visible');
        label.classList.add('hidden');
    }

    function applySelectionMarker(group, node){
        removeSelectionMarker();
        if(!group || !node) return;
        const markerGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        markerGroup.setAttribute('class', 'selection-marker');
        markerGroup.setAttribute('pointer-events', 'none');
        const baseWidth = 6.6672;
        const baseHeight = 10.3;
        const scale = Math.min(4.5, Math.max(1.8, (node.radiusScaled || radius) / 9));
        const offsetY = -((node.radiusScaled || radius) + baseHeight * scale * 0.9);
        const offsetX = -(baseWidth * scale) / 2;
        markerGroup.setAttribute('transform', `translate(${offsetX}, ${offsetY}) scale(${scale})`);

        const tip = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        tip.setAttribute('d', 'm0 1.6808 3.3922-1.6808 3.275 1.6808-3.275 8.6193z');
        tip.setAttribute('fill', '#6ef2ed');
        const base = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        base.setAttribute('d', 'm0.87825 1.6808 2.5173-1.2562 2.3935 1.2562-2.3935 1.1485z');
        base.setAttribute('fill', '#000');
        markerGroup.appendChild(tip);
        markerGroup.appendChild(base);
        group.appendChild(markerGroup);
        selectionMarkerEl = markerGroup;
    }

    function removeSelectionMarker(){
        if(selectionMarkerEl && selectionMarkerEl.parentNode){
            selectionMarkerEl.parentNode.removeChild(selectionMarkerEl);
        }
        selectionMarkerEl = null;
    }

    function renderBodyInfo(node){
        if(!infoPanel) return;
        if(!node){
            infoPanel.innerHTML = '';
            infoPanel.style.display = 'none';
            return;
        }
        infoPanel.innerHTML = '';
        infoPanel.appendChild(buildBodyCard(node));
        infoPanel.style.display = 'block';
    }

    function buildBodyCard(node){
        if(isRingInfoNode(node)){
            return buildRingCard(node);
        }
        if(isBarycenter(node)){
            return buildBarycenterCard(node);
        }
        const raw = node.raw || {};
        const card = document.createElement('article');
        card.className = 'card';
        const typeText = node.type || raw.type || 'Unknown';
        const isStar = (typeText || '').toLowerCase() === 'star';
        const subTypeText = node.subType || raw.subType || raw.star_type || raw.planet_class || 'Unknown';
        const distanceText = formatLightSeconds(node.distanceToArrival);
        const landableText = formatYesNo(node.isLandable);
        const tidallyLockedText = formatYesNo(node.tidallyLocked ?? raw.tidally_locked ?? raw.isTidallyLocked);
        const bodyIdText = formatNumber(node.id ?? raw.body_id ?? raw.bodyId ?? raw.id, { fractionDigits: 0, minimumFractionDigits: 0 });
        const hideUnknownInfo = isAsteroidClusterNode(node) || isStellarRingNode(node);
        const includeAxialTilt = !hideUnknownInfo;
        const includeAtmosphere = !hideUnknownInfo;
        const includeOrbit = !hideUnknownInfo;
        const includeSubtype = !hideUnknownInfo;
        const atmosphereSummary = formatAtmosphere(node.atmosphereType ?? raw.atmosphere_type ?? raw.atmosphereType, node.atmosphereComposition ?? raw.atmosphere_composition ?? raw.atmosphereComposition);
        const pressureText = formatPressure(node.surfacePressure ?? raw.surface_pressure ?? raw.surfacePressure);
        const gravityText = formatNumber(node.gravity ?? raw.gravity ?? raw.surface_gravity ?? raw.surfaceGravity, { unit: ' m/s²', fractionDigits: 2 });
        const massText = formatNodeMass(node);
        let radiusUnit = ' km';
        if(node.type === 'Star') radiusUnit = ' R☉';
        const radiusText = formatNumber(node.radius, { unit: radiusUnit, fractionDigits: 5 });
        const tempText = formatNumber(node.temperature ?? raw.surface_temperature ?? raw.temperature, { unit: ' K', fractionDigits: 0 });
        const rotationText = formatDuration(node.rotationalPeriod * 86400);
        const orbitItems = buildOrbitItems(node);
        const axialTiltText = formatNumber(toDegrees(node.axialTilt ?? raw.axial_tilt ?? raw.axialTilt), { unit: ' deg', fractionDigits: 2 });
        const volcanismText = escapeHtml(node.volcanism ?? raw.volcanism_type ?? raw.volcanism ?? 'Unknown');
        const terraformingText = escapeHtml(node.terraformingState ?? raw.terraforming_state ?? raw.terraformingState ?? 'Unknown');
        const rings = Array.isArray(node.rings) ? node.rings : [];
        const ringSection = rings.length ? `
     <section>
       <h2>RINGS</h2>
       <ul>
         ${rings.map(r => {
           const ringName = getRingShortName(r.name || 'Ring');
           return `<li><span class="label">${escapeHtml(ringName)}:</span> ${escapeHtml(getRingDisplayName(r.type))} &bull; ${formatDistanceRange(r.innerRadius, r.outerRadius)}</li>`;
         }).join('')}
       </ul>
     </section>
           ` : '';
        const materials = normalizePercentageEntries(node.materials ?? raw.materials);
        const materialsSection = materials.length ? `
      <section>
        <h2>MATERIALS</h2>
        <ul>
          ${materials.slice(0, 8).map(m => `<li><span class="label">${escapeHtml(m.name)}:</span> ${formatPercent(m.percent)}</li>`).join('')}
        </ul>
      </section>
          ` : '';

        const filterUnknown = (items) => {
            if(!hideUnknownInfo) return items;
            return items.filter(item => item.value !== 'Unknown' && item.value !== null && item.value !== undefined && item.value !== '');
        };

        const renderItems = (items) => {
            const list = filterUnknown(items);
            if(!list.length) return '';
            return `<ul>${list.map(item => `<li><span class="label">${item.label}</span> ${item.value}</li>`).join('')}</ul>`;
        };

        const renderSection = (title, items) => {
            const content = renderItems(items);
            if(!content) return '';
            return `
       <section>
         <h2>${title}</h2>
         ${content}
       </section>
      `;
        };

        card.innerHTML = `
      <header>
        <h2>BODY INFORMATION</h2>
        <ul>
          <li><span style="font-size: x-large;margin-top: -7px;">${escapeHtml(node.name || raw.name || 'Unknown')}</span></li>
        </ul>
        <div>
          <span class="pill">Body ID: <b>${bodyIdText}</b></span>
          ${!isStar ? `
            <span class="pill">Landable: <b>${landableText}</b></span>
            <span class="pill">Tidally Locked: <b>${tidallyLockedText}</b></span>
            ` : ''}
        </div>
        ${renderSection('ASTRONOMICAL INFORMATION', (() => {
          const items = [
            { label: 'TYPE:', value: escapeHtml(typeText) },
            { label: 'DISTANCE TO ARRIVAL:', value: distanceText }
          ];
          if(includeSubtype){
            items.splice(1, 0, { label: 'SUBTYPE:', value: escapeHtml(subTypeText) });
          }
          return items;
        })())}
      </header>

      ${renderSection('PHYSICAL PROPERTIES', [
        { label: 'RADIUS:', value: radiusText },
        { label: 'MASS:', value: massText },
        ...(!isStar ? [
        { label: 'GRAVITY:', value: gravityText }
      ] : []),
      { label: 'SURFACE TEMPERATURE:', value: tempText },
      { label: 'ROTATIONAL PERIOD:', value: rotationText },
      ...(!includeAxialTilt ? [] : [{ label: 'AXIAL TILT:', value: axialTiltText }]),
      ...(!isStar ? [
        { label: 'PRESSURE:', value: pressureText },
        ...(!includeAtmosphere ? [] : [{ label: 'ATMOSPHERE:', value: escapeHtml(atmosphereSummary) }]),
        { label: 'VOLCANISM:', value: volcanismText },
        { label: 'TERRAFORMING:', value: terraformingText }
      ] : [])
    ])}

      ${!includeOrbit ? '' : renderSection('ORBITAL CHARACTERISTICS', orbitItems)}
      ${ringSection}
      ${materialsSection}
    `;

        return card;
    }

    function buildBarycenterCard(node){
        const raw = node?.raw || {};
        const card = document.createElement('article');
        card.className = 'card';
        const typeText = node?.type || raw.type || 'Barycenter';
        const bodyIdText = formatNumber(node?.id ?? raw.body_id ?? raw.bodyId ?? raw.id, { fractionDigits: 0, minimumFractionDigits: 0 });
        const baryChildren = (node?.baryChildren || []).filter(Boolean);
        const baryMassText = formatMassDisplay(node);
        const baryOrbitItems = buildOrbitItems(node);
        const baryOrbitSection = baryOrbitItems.length ? `
      <section>
        <h2>ORBITAL CHARACTERISTICS</h2>
        <ul>
          ${baryOrbitItems.map(item => `<li><span class="label">${item.label}</span> ${item.value}</li>`).join('')}
        </ul>
      </section>
    ` : '';
        const memberItems = baryChildren.map(child => {
            const childRaw = child?.raw || {};
            const childName = escapeHtml(child?.name || childRaw.name || `Body #${child?.id ?? '?'}`);
            const childType = escapeHtml(child?.type || childRaw.type || 'Unknown');
            const childMass = formatNodeMass(child);
            const massSnippet = childMass && childMass !== 'Unknown' ? ` &bull; ${childMass}` : '';
            return `<li><span class="label">${childName}:</span> ${childType}${massSnippet}</li>`;
        }).join('');
        const membersSection = `
      <section>
        <h2>BARYCENTER MEMBERS</h2>
        ${baryChildren.length ? `<ul>${memberItems}</ul>` : '<p class="small">No associated bodies.</p>'}
      </section>
        `;

        card.innerHTML = `
      <header>
        <h2>BODY INFORMATION</h2>
        <ul>
          <li><span style="font-size: x-large;margin-top: -7px;">${escapeHtml(node?.name || raw.name || 'Barycenter')}</span></li>
          <li><span class="label">TYPE:</span> ${escapeHtml(typeText)}</li>
        </ul>
        <div>
          <span class="pill">Body ID: <b>${bodyIdText}</b></span>
        </div>
      </header>

      <section>
        <h2>BARYCENTER SUMMARY</h2>
        <ul>
          <li><span class="label">TOTAL MASS:</span> ${baryMassText}</li>
        </ul>
      </section>
      ${membersSection}
      ${baryOrbitSection}
    `;

        return card;
    }

    function isRingInfoNode(node){
        return Boolean(node?.isRingNode && node?.ring);
    }

    function buildRingCard(node){
        const ring = node?.ring || {};
        const host = node?.hostBody || {};
        const card = document.createElement('article');
        card.className = 'card';
        const ringName = getRingShortName(ring.name || 'Ring');
        const hostName = host?.name || 'Unknown Body';
        const typeText = getRingDisplayName(ring.type) || 'Ring';
        const spanText = formatDistanceRange(ring.innerRadius, ring.outerRadius);
        const innerText = formatDistance(ring.innerRadius);
        const outerText = formatDistance(ring.outerRadius);
        const widthText = formatRingWidth(ring.innerRadius, ring.outerRadius);
        const massText = formatNumber(ring.mass, { unit: ' Mt', fractionDigits: 2 });

        const filterItems = (items) => items.filter(item => item.value && item.value !== 'Unknown');
        const renderSection = (title, items) => {
            const filtered = filterItems(items);
            if(!filtered.length) return '';
            return `
      <section>
        <h2>${title}</h2>
        <ul>
          ${filtered.map(item => `<li><span class="label">${item.label}</span> ${item.value}</li>`).join('')}
        </ul>
      </section>
    `;
        };

        card.innerHTML = `
      <header>
        <h2>RING INFORMATION</h2>
        <ul>
          <li><span style="font-size: x-large;margin-top: -7px;">${escapeHtml(ringName)}</span></li>
          <li><span class="label">HOST BODY:</span> ${escapeHtml(hostName)}</li>
          <li><span class="label">RING TYPE:</span> ${escapeHtml(typeText)}</li>
        </ul>
      </header>

      ${renderSection('RING SUMMARY', [
        { label: 'RADIAL SPAN:', value: spanText }
      ])}
      ${renderSection('DIMENSIONS', [
        { label: 'INNER RADIUS:', value: innerText },
        { label: 'OUTER RADIUS:', value: outerText },
        { label: 'WIDTH:', value: widthText }
      ])}
      ${renderSection('MASS / OTHER', [
        { label: 'MASS:', value: massText }
      ])}
    `;

        return card;
    }

    function formatNodeMass(node){
        if(!node) return 'Unknown';
        const raw = node.raw || {};
        if(isBarycenter(node)){
            return formatMassDisplay(node);
        }
        if(node.type === 'Star'){
            const stellarMass = node.massValue ?? raw.stellar_mass ?? raw.mass_em ?? raw.mass ?? raw.mass_mt;
            return formatNumber(stellarMass, { unit: ' M☉', fractionDigits: 3 });
        }
        if(node.type === 'Planet'){
            const planetaryMass = node.massValue ?? node.earthMasses ?? raw.mass_em ?? raw.mass;
            return formatNumber(planetaryMass, { unit: ' M⊕', fractionDigits: 3 });
        }
        if(node.type === 'PlanetaryRing'){
            const ringMass = raw.ring_mass_mt ?? raw.mass_mt ?? node.massValue;
            return formatNumber(ringMass, { unit: ' Mt', fractionDigits: 2 });
        }
        return 'Unknown';
    }

    function formatNumber(value, { unit = '', fractionDigits = 2, minimumFractionDigits = 0, fallback = 'Unknown' } = {}){
        const num = Number(value);
        if(!Number.isFinite(num)) return fallback;
        return `${new Intl.NumberFormat('en-US', {
      minimumFractionDigits,
      maximumFractionDigits: fractionDigits
    }).format(num)}${unit}`;
    }

    function formatDuration(seconds){
        const num = Number(seconds);
        if(!Number.isFinite(num)) return 'Unknown';
        const abs = Math.abs(num);
        const days = abs / 86400;
        if(days >= 1) return `${formatNumber(days, { fractionDigits: 2 })} d`;
        const hours = abs / 3600;
        if(hours >= 1) return `${formatNumber(hours, { fractionDigits: 2 })} h`;
        const minutes = abs / 60;
        if(minutes >= 1) return `${formatNumber(minutes, { fractionDigits: 2 })} min`;
        return `${formatNumber(abs, { fractionDigits: 0 })} s`;
    }

    function formatLightSeconds(value){
        const num = Number(value);
        if(!Number.isFinite(num)) return 'Unknown';
        if(Math.abs(num) >= 1000){
            return `${formatNumber(num / 1000, { fractionDigits: 2 })} kls`;
        }
        return `${formatNumber(num, { fractionDigits: 1 })} ls`;
    }

    function formatDistance(value){
        const num = Number(value);
        if(!Number.isFinite(num)) return 'Unknown';
        const km = num / 1000;
        if(Math.abs(km) >= 1e6) return `${formatNumber(km / 1e6, { fractionDigits: 2 })} Gm`;
        if(Math.abs(km) >= 1e3) return `${formatNumber(km / 1e3, { fractionDigits: 2 })} Mm`;
        if(Math.abs(km) >= 1) return `${formatNumber(km, { fractionDigits: 1 })} km`;
        return `${formatNumber(km * 1000, { fractionDigits: 0 })} m`;
    }

    function formatDistanceRange(inner, outer){
        const innerText = formatDistance(inner);
        const outerText = formatDistance(outer);
        if(innerText === 'Unknown' && outerText === 'Unknown') return 'Unknown';
        if(innerText === 'Unknown') return `Outer ${outerText}`;
        if(outerText === 'Unknown') return `Inner ${innerText}`;
        return `${innerText} - ${outerText}`;
    }

    function formatRingWidth(inner, outer){
        const innerNum = Number(inner);
        const outerNum = Number(outer);
        if(!Number.isFinite(innerNum) || !Number.isFinite(outerNum)) return 'Unknown';
        const width = outerNum - innerNum;
        if(width <= 0) return 'Unknown';
        return formatDistance(width);
    }

    function formatSemiMajorAxisValue(value){
        const axis = Number(value);
        if(!Number.isFinite(axis)) return 'Unknown';
        const distanceText = formatDistance(axis * AU_IN_METERS);
        const auText = formatNumber(axis, { unit: ' AU', fractionDigits: 3 });
        return distanceText === 'Unknown' && auText === 'Unknown'
            ? 'Unknown'
            : `${distanceText} (${auText})`;
    }

    function buildOrbitItems(node){
        if(!node) return [];
        const raw = node.raw || {};
        const orbitalDays = node.orbitalPeriod ?? raw.orbital_period ?? raw.orbitalPeriod;
        const orbitalText = formatDuration((orbitalDays ?? NaN) * 86400);
        const semiMajor = node.semiMajorAxis ?? raw.semi_major_axis ?? raw.semiMajorAxis;
        const semiMajorText = formatSemiMajorAxisValue(semiMajor);
        const eccentricity = node.orbitalEccentricity ?? raw.orbital_eccentricity ?? raw.eccentricity;
        const eccentricityText = formatNumber(eccentricity, { fractionDigits: 3, minimumFractionDigits: 3 });
        const inclination = node.orbitalInclination ?? raw.orbital_inclination ?? raw.orbitalInclination;
        const inclinationText = formatNumber(inclination, { unit: ' deg', fractionDigits: 2 });
        return [
            { label: 'ORBITAL PERIOD:', value: orbitalText },
            { label: 'SEMI-MAJOR AXIS:', value: semiMajorText },
            { label: 'ECCENTRICITY:', value: eccentricityText },
            { label: 'INCLINATION:', value: inclinationText }
        ];
    }

    function formatYesNo(value){
        if(value === null || value === undefined) return 'Unknown';
        return value ? 'Yes' : 'No';
    }

    function toDegrees(value){
        if(!Number.isFinite(value)) return null;
        return value * (180 / Math.PI);
    }

    function formatPressure(value){
        const num = Number(value);
        if(!Number.isFinite(num)) return 'Unknown';
        const atm = num / 101325;
        if(atm >= 0.05 && atm <= 200) return `${formatNumber(atm, { fractionDigits: 2 })} atm`;
        return `${formatNumber(num, { fractionDigits: 0 })} Pa`;
    }

    function normalizePercentageEntries(source){
        if(!source) return [];
        const entries = [];
        if(Array.isArray(source)){
            source.forEach(item => {
                const name = item?.name || item?.material || item?.label || item?.type;
                const percent = Number(item?.percent ?? item?.percentage ?? item?.share ?? item?.amount ?? item?.value);
                if(name && Number.isFinite(percent)) entries.push({ name, percent });
            });
        } else if(typeof source === 'object'){
            Object.entries(source).forEach(([name, percent]) => {
                const num = Number(percent);
                if(name && Number.isFinite(num)) entries.push({ name, percent: num });
            });
        }
        return entries.sort((a, b) => b.percent - a.percent);
    }

    function formatAtmosphere(type, composition){
        const base = type && type !== '' ? type : 'None';
        const entries = normalizePercentageEntries(composition);
        if(!entries.length) return base;
        const list = entries.slice(0, 4).map(entry => `${entry.name} ${formatPercent(entry.percent)}`).join(', ');
        return `${base} (${list})`;
    }

    function formatPercent(value){
        const num = Number(value);
        if(!Number.isFinite(num)) return 'Unknown';
        return `${formatNumber(num, { fractionDigits: 1 })}%`;
    }

    function escapeHtml(str){
        if(typeof str !== 'string') str = String(str ?? '');
        return str.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c] || c));
    }

    function getSerializedSvg(includeRasterDimensions = false){
        if(!svg){
            throw new Error('Missing SVG root');
        }
        const clone = svg.cloneNode(true);
        const viewBox = svg.viewBox?.baseVal;
        if(includeRasterDimensions && viewBox && viewBox.width && viewBox.height){
            clone.setAttribute('width', viewBox.width);
            clone.setAttribute('height', viewBox.height);
        }
        if(!clone.getAttribute('xmlns')){
            clone.setAttribute('xmlns', svg.namespaceURI || 'http://www.w3.org/2000/svg');
        }
        if(!clone.getAttribute('xmlns:xlink')){
            clone.setAttribute('xmlns:xlink', 'http://www.w3.org/1999/xlink');
        }
        const serializer = new XMLSerializer();
        let source = serializer.serializeToString(clone);
        if(!source.startsWith('<?xml')){
            source = `<?xml version="1.0" encoding="UTF-8"?>\n` + source;
        }
        return source;
    }

    function triggerBlobDownload(blob, filename){
        if(!blob){
            return;
        }
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    function getSvgPixelDimensions(){
        const viewBox = svg?.viewBox?.baseVal;
        if(viewBox && viewBox.width && viewBox.height){
            return {
                width: Math.ceil(viewBox.width),
                height: Math.ceil(viewBox.height)
            };
        }
        return {
            width: Math.ceil(svg?.clientWidth || 1200),
            height: Math.ceil(svg?.clientHeight || 800)
        };
    }

    function downloadSVG(){
        try {
            const source = getSerializedSvg(false);
            const blob = new Blob([source], {type: 'image/svg+xml;charset=utf-8'});
            const sys = resolveActiveSystemName() || 'System';
            triggerBlobDownload(blob, `${sys.replace(/\\s+/g,'_')}.svg`);
        } catch(err) {
            console.error('Unable to export SVG', err);
        }
    }

    function loadImageFromUrl(url){
        return new Promise((resolve, reject) => {
            const image = new Image();
            image.decoding = 'async';
            image.crossOrigin = 'anonymous';
            image.onload = () => resolve(image);
            image.onerror = (event) => reject(event?.error || new Error('Image load failed'));
            image.src = url;
        });
    }

    function canvasToBlob(canvas, type = 'image/png', quality = 1){
        return new Promise((resolve, reject) => {
            canvas.toBlob((blob) => {
                if(blob){
                    resolve(blob);
                } else {
                    reject(new Error('Unable to export canvas'));
                }
            }, type, quality);
        });
    }

    async function downloadPNG(){
        if(!svg){
            return;
        }
        const source = getSerializedSvg(true);
        const svgBlob = new Blob([source], {type: 'image/svg+xml;charset=utf-8'});
        const svgUrl = URL.createObjectURL(svgBlob);
        try {
            const image = await loadImageFromUrl(svgUrl);
            const { width, height } = getSvgPixelDimensions();
            const canvas = document.createElement('canvas');
            canvas.width = width;
            canvas.height = height;
            const ctx = canvas.getContext('2d');
            if(!ctx){
                throw new Error('Canvas 2D context unavailable');
            }
            ctx.clearRect(0, 0, width, height);
            ctx.drawImage(image, 0, 0, width, height);
            const pngBlob = await canvasToBlob(canvas, 'image/png');
            const sys = resolveActiveSystemName() || 'System';
            triggerBlobDownload(pngBlob, `${sys.replace(/\\s+/g,'_')}.png`);
        } finally {
            URL.revokeObjectURL(svgUrl);
        }
    }

    function buildEdgisLookupUrl(){
        const systemName = resolveActiveSystemName();
        if(!systemName){
            return '';
        }
        return `${sameHostBaseUrl}/?lookup=${encodeURIComponent(systemName)}`;
    }

    function buildGalaxyMapUrl(){
        const systemName = resolveActiveSystemName();
        if(!systemName){
            return '';
        }
        return `${sameHostBaseUrl}/static/galaxymap.html?q=${encodeURIComponent(systemName)}&radius=20`;
    }

    function updateNavigationButtonState(){
        const systemName = resolveActiveSystemName();
        const hasSystem = Boolean(systemName);

        if(openGalaxyMapButton){
            const galaxyMapLabel = hasSystem ? `Open ${systemName} in Galaxy Map` : 'Open in Galaxy Map';
            openGalaxyMapButton.disabled = !hasSystem;
            openGalaxyMapButton.setAttribute('aria-disabled', hasSystem ? 'false' : 'true');
            openGalaxyMapButton.setAttribute('title', galaxyMapLabel);
            openGalaxyMapButton.setAttribute('aria-label', galaxyMapLabel);
        }

        if(openEdgisButton){
            const edgisLabel = hasSystem ? `Open ${systemName} in EDGIS` : 'Open in EDGIS';
            openEdgisButton.disabled = !hasSystem;
            openEdgisButton.setAttribute('aria-disabled', hasSystem ? 'false' : 'true');
            openEdgisButton.setAttribute('title', edgisLabel);
            openEdgisButton.setAttribute('aria-label', edgisLabel);
        }
    }

    function buildEmbedUrl(){
        const systemName = resolveActiveSystemName();
        if(!systemName){
            return '';
        }
        const url = new URL(globalThis.location?.href || '');
        url.searchParams.set('system', systemName);
        const selectedId = resolveNodeBodyId(selectedBodyNode);
        if(selectedId != null){
            url.searchParams.set('body_id', selectedId);
        } else {
            url.searchParams.delete('body_id');
        }
        url.searchParams.set('svgOnly', '1');
        url.searchParams.delete('svg_only');
        url.searchParams.delete('download');
        return url.toString();
    }

    async function copyTextToClipboard(text){
        if(!text){
            throw new Error('Nothing to copy');
        }
        if(navigator.clipboard && navigator.clipboard.writeText){
            await navigator.clipboard.writeText(text);
            return text;
        }
        return new Promise((resolve, reject) => {
            const textarea = document.createElement('textarea');
            textarea.value = text;
            textarea.setAttribute('readonly', '');
            textarea.style.position = 'absolute';
            textarea.style.left = '-9999px';
            document.body.appendChild(textarea);
            textarea.select();
            try {
                const successful = document.execCommand('copy');
                document.body.removeChild(textarea);
                if(successful){
                    resolve(text);
                } else {
                    reject(new Error('Copy command failed'));
                }
            } catch(copyErr){
                document.body.removeChild(textarea);
                reject(copyErr);
            }
        });
    }
})();
