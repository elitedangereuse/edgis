(function () {
    'use strict';

    if (!globalThis.EDGIS_SYSMAP_CONFIG?.architectModeEnabled) return;

    const DRAFT_VERSION = 1;
    const DRAFT_STORAGE_KEY = 'edgis:architect-draft:v1';
    let enabled = Boolean(globalThis.EDGIS_SYSMAP_CONFIG.architectDraft);
    let draft = loadDraft();
    let mapSnapshot = null;
    let selectedBodyId = null;

    const renderer = () => globalThis.EDGIS_SYSMAP_ARCHITECT_RENDERER;

    function loadDraft() {
        try {
            const value = globalThis.localStorage?.getItem(DRAFT_STORAGE_KEY);
            const candidate = value ? JSON.parse(value) : null;
            if (candidate?.version !== DRAFT_VERSION || !Array.isArray(candidate.bodies)) return null;
            const byId = new Map(candidate.bodies.map((body) => [body.id, body]));
            let migrated = false;
            candidate.bodies.forEach((body) => {
                const parent = byId.get(body.parentId);
                const anchor = byId.get(body.layoutAnchorId);
                // A side insertion on the barycentre is a new member of that
                // barycentre.  Older drafts left it as an unrelated root.
                if (!body.parentId && anchor?.type === 'Barycenter'
                    && ['left', 'right'].includes(body.layoutDirection)) {
                    body.parentId = anchor.id;
                    migrated = true;
                }
                // Earlier drafts could attach a planet to a barycentre when it
                // was added beside one of the barycentre's member stars.
                if (body.type === 'Planet' && parent?.type === 'Barycenter'
                    && anchor?.type === 'Star' && anchor.parentId === parent.id
                    && ['left', 'right'].includes(body.layoutDirection)) {
                    body.parentId = anchor.id;
                    migrated = true;
                }
            });
            if (migrated) globalThis.localStorage?.setItem(DRAFT_STORAGE_KEY, JSON.stringify(candidate));
            return candidate;
        } catch (error) {
            console.warn('Unable to read Architect draft', error);
            return null;
        }
    }

    function saveDraft() {
        if (!draft) return;
        draft.updatedAt = new Date().toISOString();
        try {
            globalThis.localStorage?.setItem(DRAFT_STORAGE_KEY, JSON.stringify(draft));
        } catch (error) {
            console.warn('Unable to save Architect draft', error);
        }
    }

    function newDreamSystem() {
        draft = {
            version: DRAFT_VERSION,
            name: 'Untitled dream system',
            bodies: [],
            stationPlans: [],
            updatedAt: new Date().toISOString(),
        };
        beginDreamSystem();
        saveDraft();
        renderDraftCanvas();
    }

    function updateDraftUrl(isDraft) {
        const url = new URL(globalThis.location.href);
        if (isDraft) {
            url.searchParams.set('architect', 'draft');
            ['system', 'body_id', 'station_id', 'station', 'carrier'].forEach((key) => url.searchParams.delete(key));
        } else {
            url.searchParams.delete('architect');
        }
        globalThis.history.replaceState({}, '', url);
    }

    function beginDreamSystem() {
        globalThis.EDGIS_ARCHITECT_DRAFT_ACTIVE = true;
        updateDraftUrl(true);
        document.getElementById('InfoPanel')?.style.setProperty('display', 'none');
    }

    function svgElement(name, attributes = {}) {
        const element = document.createElementNS('http://www.w3.org/2000/svg', name);
        Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)));
        return element;
    }

    function activateDraftCanvas() {
        const svg = document.getElementById('svg');
        if (!svg || mapSnapshot) return;
        mapSnapshot = { markup: svg.innerHTML, viewBox: svg.getAttribute('viewBox') };
    }

    function restoreSourceCanvas() {
        const svg = document.getElementById('svg');
        if (!svg || !mapSnapshot) return;
        svg.innerHTML = mapSnapshot.markup;
        if (mapSnapshot.viewBox) svg.setAttribute('viewBox', mapSnapshot.viewBox);
        mapSnapshot = null;
    }

    function nextBodyId(kind) {
        return `draft-${kind}-${Date.now()}-${draft.bodies.length + 1}`;
    }

    function createRootBarycenter(rootStar) {
        const barycenter = {
            id: nextBodyId('barycenter'),
            name: 'Dream barycentre',
            type: 'Barycenter',
            parentId: null,
        };
        rootStar.parentId = barycenter.id;
        draft.bodies.push(barycenter);
        return barycenter.id;
    }

    function insertDraftBody(body, insertBeforeId = null, insertAfterId = null) {
        let insertAt = insertBeforeId
            ? draft.bodies.findIndex((candidate) => candidate.id === insertBeforeId)
            : -1;
        if (insertAt < 0 && insertAfterId) {
            const afterIndex = draft.bodies.findIndex((candidate) => candidate.id === insertAfterId);
            insertAt = afterIndex >= 0 ? afterIndex + 1 : -1;
        }
        if (insertAt >= 0) draft.bodies.splice(insertAt, 0, body);
        else draft.bodies.push(body);
    }

    function addBody(selection, requestedParentId = null, siblingId = null, insertBeforeId = null, insertAfterId = null, layoutAnchorId = null, layoutDirection = null) {
        if (!draft || !selection) return;
        const [kind, value] = selection.split(':');
        const count = draft.bodies.length + 1;
        let parentId = requestedParentId || null;
        if (kind === 'star') {
            const starType = renderer()?.starTypes.find((type) => type.code === value);
            if (!starType) return;
            // A second root star is represented as a binary/multi-star barycentre.
            const sibling = siblingId ? draft.bodies.find((body) => body.id === siblingId) : null;
            const requestedParent = parentId ? draft.bodies.find((body) => body.id === parentId) : null;
            const rootStar = sibling || requestedParent;
            if (sibling?.type === 'Barycenter') {
                // The barycentre's side controls add another member to its
                // bracket, not an unrelated root system.
                parentId = sibling.id;
            } else if (rootStar?.type === 'Star' && !rootStar.parentId) {
                parentId = createRootBarycenter(rootStar);
            }
            insertDraftBody({
                id: nextBodyId('star'),
                name: `Dream Star ${count}`,
                type: 'Star',
                starType: starType.code,
                subtype: starType.label,
                parents: [],
                parentId,
                radius: 1,
                mass: 1,
                temperature: 5500,
                rotationalPeriod: 1,
                axialTilt: 0,
                orbitalPeriod: 365,
                semiMajorAxis: 1,
                eccentricity: 0,
                inclination: 0,
                rings: [],
                layoutAnchorId,
                layoutDirection,
            }, insertBeforeId, insertAfterId);
        } else if (kind === 'planet') {
            // Planets cannot be root-level siblings of a lone star.  The
            // sibling affordance stays useful for stars; choosing a planet
            // there makes it orbit that root star instead.
            const sibling = siblingId ? draft.bodies.find((body) => body.id === siblingId) : null;
            if (sibling?.type === 'Barycenter') {
                parentId = sibling.id;
            } else if (sibling?.type === 'Star') {
                parentId = sibling.id;
            }
            insertDraftBody({
                id: nextBodyId('planet'),
                name: `Dream Planet ${count}`,
                type: 'Planet',
                subtype: value,
                parents: [],
                parentId,
                radius: 6000,
                mass: 1,
                gravity: 9.81,
                temperature: 300,
                surfacePressure: 101325,
                rotationalPeriod: 1,
                axialTilt: 0,
                orbitalPeriod: 365,
                semiMajorAxis: 1,
                eccentricity: 0,
                inclination: 0,
                rings: [],
                layoutAnchorId,
                layoutDirection,
            }, insertBeforeId, insertAfterId);
        }
        saveDraft();
        renderDraftCanvas();
    }

    function closeBodyPicker() {
        document.getElementById('architectBodyPicker')?.remove();
    }

    function openBodyPicker(parentId = null, siblingId = null, insertBeforeId = null, insertAfterId = null, layoutAnchorId = null, layoutDirection = null) {
        if (document.getElementById('architectBodyPicker')) return;
        const picker = document.createElement('div');
        picker.id = 'architectBodyPicker';
        picker.className = 'architect-body-picker';
        picker.setAttribute('role', 'dialog');
        picker.setAttribute('aria-label', 'Add body');
        picker.dataset.parentId = parentId || '';
        picker.dataset.siblingId = siblingId || '';
        picker.dataset.insertBeforeId = insertBeforeId || '';
        picker.dataset.insertAfterId = insertAfterId || '';
        picker.dataset.layoutAnchorId = layoutAnchorId || '';
        picker.dataset.layoutDirection = layoutDirection || '';

        const select = document.createElement('select');
        select.setAttribute('aria-label', 'Body type');
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = 'Select a body type…';
        select.appendChild(placeholder);

        const stars = document.createElement('optgroup');
        stars.label = 'Stars and remnants';
        (renderer()?.starTypes || []).forEach((type) => {
            const option = document.createElement('option');
            option.value = `star:${type.code}`;
            option.textContent = type.label;
            stars.appendChild(option);
        });
        select.appendChild(stars);

        const planets = document.createElement('optgroup');
        planets.label = 'Planets and gas giants';
        (renderer()?.planetTypes || []).forEach((type) => {
            const option = document.createElement('option');
            option.value = `planet:${type}`;
            option.textContent = type;
            planets.appendChild(option);
        });
        select.appendChild(planets);

        const add = document.createElement('button');
        add.type = 'button';
        add.textContent = 'Add';
        add.addEventListener('click', () => {
            addBody(
                select.value,
                picker.dataset.parentId || null,
                picker.dataset.siblingId || null,
                picker.dataset.insertBeforeId || null,
                picker.dataset.insertAfterId || null,
                picker.dataset.layoutAnchorId || null,
                picker.dataset.layoutDirection || null,
            );
            closeBodyPicker();
        });
        const cancel = document.createElement('button');
        cancel.type = 'button';
        cancel.textContent = 'Cancel';
        cancel.addEventListener('click', closeBodyPicker);
        picker.append(select, add, cancel);
        document.body.appendChild(picker);
        select.focus();
    }

    function addBodyControl(svg, x, y, parentId, kind, siblingId = null, insertBeforeId = null, insertAfterId = null) {
        const label = kind === 'child' ? 'Add child body'
            : kind === 'sibling-before' ? 'Insert sibling before'
                : kind === 'sibling-after' ? 'Insert sibling after'
                    : kind === 'root' ? 'Add body'
                : 'Add sibling body';
        const control = svgElement('g', {
            class: 'architect-add-body-control',
            transform: `translate(${x} ${y})`,
            role: 'button',
            tabindex: '0',
            'aria-label': label,
        });
        const title = svgElement('title');
        title.textContent = label;
        control.appendChild(title);
        control.appendChild(svgElement('circle', { r: 5 }));
        const plus = svgElement('path', { d: 'M -2 0 H 2 M 0 -2 V 2' });
        control.appendChild(plus);
        const layoutDirection = kind === 'child' ? 'below'
            : kind === 'sibling-before' ? 'left'
                : kind === 'sibling-after' ? 'right' : null;
        const layoutAnchorId = kind === 'child' ? parentId : siblingId;
        const open = () => openBodyPicker(
            parentId, siblingId, insertBeforeId, insertAfterId, layoutAnchorId, layoutDirection,
        );
        control.addEventListener('click', open);
        control.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                open();
            }
        });
        svg.appendChild(control);
    }

    function removeSelectedBody() {
        if (!draft || !selectedBodyId) return;
        const deleted = new Set([selectedBodyId]);
        let changed = true;
        while (changed) {
            changed = false;
            draft.bodies.forEach((body) => {
                if (body.parentId && deleted.has(body.parentId) && !deleted.has(body.id)) {
                    deleted.add(body.id);
                    changed = true;
                }
            });
        }
        draft.bodies = draft.bodies.filter((body) => !deleted.has(body.id));
        selectedBodyId = null;
        saveDraft();
        renderDraftCanvas({ preserveViewBox: true });
        const infoPanel = document.getElementById('InfoPanel');
        if (infoPanel) {
            infoPanel.replaceChildren();
            infoPanel.style.display = 'none';
        }
    }

    function rangeDefinition(body, property) {
        const planet = body.type === 'Planet';
        const definitions = {
            radius: { label: 'Radius', min: planet ? 100 : 0.1, max: planet ? 100000 : 100, step: planet ? 100 : 0.1, unit: planet ? ' km' : ' R☉', fallback: planet ? 6000 : 1 },
            mass: { label: 'Mass', min: 0.01, max: planet ? 100 : 1000, step: 0.01, unit: planet ? ' M⊕' : ' M☉', fallback: 1 },
            gravity: { label: 'Gravity', min: 0, max: 100, step: 0.01, unit: ' m/s²', fallback: 9.81 },
            temperature: { label: 'Surface temperature', min: 0, max: planet ? 3000 : 100000, step: 1, unit: ' K', fallback: planet ? 300 : 5500 },
            surfacePressure: { label: 'Surface pressure', min: 0, max: 10000000, step: 1000, unit: ' Pa', fallback: 101325 },
            rotationalPeriod: { label: 'Rotational period', min: -500, max: 500, step: 0.1, unit: ' d', fallback: 1 },
            axialTilt: { label: 'Axial tilt', min: -180, max: 180, step: 0.1, unit: '°', fallback: 0 },
            orbitalPeriod: { label: 'Orbital period', min: 0.1, max: 100000, step: 0.1, unit: ' d', fallback: 365 },
            semiMajorAxis: { label: 'Semi-major axis', min: 0.001, max: 100000, step: 0.001, unit: ' AU', fallback: 1 },
            eccentricity: { label: 'Eccentricity', min: 0, max: 0.99, step: 0.001, unit: '', fallback: 0 },
            inclination: { label: 'Inclination', min: -180, max: 180, step: 0.1, unit: '°', fallback: 0 },
        };
        return definitions[property];
    }

    function formatDraftValue(value, definition) {
        const precision = definition.step < 1 ? Math.min(3, String(definition.step).split('.')[1]?.length || 0) : 0;
        return `${Number(value).toFixed(precision)}${definition.unit}`;
    }

    function renderArchitectInfo(body) {
        const infoPanel = document.getElementById('InfoPanel');
        if (!infoPanel || !body) return;
        infoPanel.replaceChildren();
        const card = document.createElement('article');
        card.className = 'card architect-body-card';
        const heading = document.createElement('h1');
        heading.textContent = body.name.toUpperCase();
        card.appendChild(heading);

        const typeSection = document.createElement('section');
        typeSection.innerHTML = '<h2>ARCHITECT BODY</h2>';
        const nameLabel = document.createElement('label');
        nameLabel.className = 'architect-field architect-name-field';
        nameLabel.textContent = 'Name';
        const name = document.createElement('input');
        name.type = 'text';
        name.value = body.name;
        name.addEventListener('input', () => {
            body.name = name.value || 'Unnamed body';
            heading.textContent = body.name.toUpperCase();
            saveDraft();
            renderDraftCanvas();
        });
        nameLabel.appendChild(name);
        typeSection.appendChild(nameLabel);

        if (body.type === 'Star') {
            const typeLabel = document.createElement('label');
            typeLabel.className = 'architect-field';
            typeLabel.textContent = 'Star type';
            const starType = document.createElement('select');
            (renderer()?.starTypes || []).forEach((type) => {
                const option = document.createElement('option');
                option.value = type.code;
                option.textContent = type.label;
                option.selected = type.code === body.starType;
                starType.appendChild(option);
            });
            starType.addEventListener('change', () => {
                body.starType = starType.value;
                body.subtype = renderer()?.starTypes.find((type) => type.code === starType.value)?.label || starType.value;
                saveDraft();
                renderDraftCanvas();
            });
            typeLabel.appendChild(starType);
            typeSection.appendChild(typeLabel);
        } else {
            const type = document.createElement('p');
            type.textContent = body.type === 'Barycenter' ? 'Barycentre' : body.subtype;
            typeSection.appendChild(type);
        }
        card.appendChild(typeSection);

        const appendRanges = (title, properties) => {
            const section = document.createElement('section');
            section.innerHTML = `<h2>${title}</h2>`;
            properties.forEach((property) => {
                const definition = rangeDefinition(body, property);
                const field = document.createElement('label');
                field.className = 'architect-field architect-range-field';
                const caption = document.createElement('span');
                caption.textContent = definition.label;
                const value = document.createElement('output');
                const input = document.createElement('input');
                input.type = 'range';
                input.min = definition.min;
                input.max = definition.max;
                input.step = definition.step;
                input.value = Number.isFinite(Number(body[property])) ? body[property] : definition.fallback;
                const update = () => {
                    body[property] = Number(input.value);
                    value.textContent = formatDraftValue(body[property], definition);
                    saveDraft();
                    renderDraftCanvas();
                };
                value.textContent = formatDraftValue(input.value, definition);
                input.addEventListener('input', update);
                field.append(caption, value, input);
                section.appendChild(field);
            });
            card.appendChild(section);
        };

        if (body.type !== 'Barycenter') {
            appendRanges('PHYSICAL PROPERTIES', body.type === 'Star'
                ? ['radius', 'mass', 'temperature', 'rotationalPeriod', 'axialTilt']
                : ['radius', 'mass', 'gravity', 'temperature', 'surfacePressure', 'rotationalPeriod', 'axialTilt']);
            appendRanges('ORBITAL CHARACTERISTICS', ['orbitalPeriod', 'semiMajorAxis', 'eccentricity', 'inclination']);

            const ringsSection = document.createElement('section');
            ringsSection.innerHTML = '<h2>RINGS</h2>';
            const rings = Array.isArray(body.rings) ? body.rings : [];
            rings.forEach((ring, index) => {
                const row = document.createElement('div');
                row.className = 'architect-ring-row';
                const type = document.createElement('select');
                (renderer()?.ringTypes || ['Rocky', 'Icy', 'Metal Rich', 'Metallic']).forEach((ringType) => {
                    const option = document.createElement('option');
                    option.value = ringType;
                    option.textContent = ringType;
                    option.selected = ringType.toLowerCase() === String(ring.type).toLowerCase();
                    type.appendChild(option);
                });
                type.addEventListener('change', () => {
                    ring.type = type.value;
                    saveDraft();
                    renderDraftCanvas();
                });
                const removeRing = document.createElement('button');
                removeRing.type = 'button';
                removeRing.className = 'architect-ring-remove';
                removeRing.textContent = '×';
                removeRing.title = `Remove ring ${index + 1}`;
                removeRing.addEventListener('click', () => {
                    body.rings.splice(index, 1);
                    saveDraft();
                    renderDraftCanvas();
                    renderArchitectInfo(body);
                });
                row.append(type, removeRing);
                ringsSection.appendChild(row);
            });
            const addRing = document.createElement('button');
            addRing.type = 'button';
            addRing.className = 'architect-add-ring';
            addRing.textContent = 'ADD RING';
            addRing.addEventListener('click', () => {
                if (!Array.isArray(body.rings)) body.rings = [];
                const radius = Number(body.radius) || (body.type === 'Star' ? 1 : 6000);
                body.rings.push({
                    name: `${body.name} Ring ${body.rings.length + 1}`,
                    type: 'Icy',
                    innerRadius: radius * 1.2,
                    outerRadius: radius * 1.8,
                });
                saveDraft();
                renderDraftCanvas();
                renderArchitectInfo(body);
            });
            ringsSection.appendChild(addRing);
            card.appendChild(ringsSection);
        }
        const remove = document.createElement('button');
        remove.type = 'button';
        remove.className = 'architect-remove-button';
        remove.textContent = 'REMOVE BODY';
        remove.addEventListener('click', removeSelectedBody);
        card.appendChild(remove);
        infoPanel.appendChild(card);
        infoPanel.style.display = 'block';
    }

    function addRemoveControl(svg, x, y) {
        const control = svgElement('g', {
            class: 'architect-remove-body-control', transform: `translate(${x} ${y})`,
            role: 'button', tabindex: '0', 'aria-label': 'Remove selected body',
        });
        const title = svgElement('title');
        title.textContent = 'Remove selected body and its children';
        control.appendChild(title);
        control.appendChild(svgElement('circle', { r: 5 }));
        control.appendChild(svgElement('path', { d: 'M -2 -2 L 2 2 M 2 -2 L -2 2' }));
        const remove = () => removeSelectedBody();
        control.addEventListener('click', remove);
        control.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                remove();
            }
        });
        svg.appendChild(control);
    }

    function selectDraftBody(body) {
        selectedBodyId = body.id;
        renderDraftCanvas({ preserveViewBox: true });
        renderArchitectInfo(body);
    }

    // This follows Sysmap's barycentre bracket geometry.  The barycentre is
    // an overlay derived from its member bodies, never an ordinary parent node.
    function drawDraftBarycenter(svg, barycenter, body) {
        const members = (barycenter.baryChildren || []).filter(Boolean);
        if (members.length < 2) return;
        const minX = Math.min(...members.map((member) => member.x - member.radiusScaled));
        const maxX = Math.max(...members.map((member) => member.x + member.radiusScaled));
        const minY = Math.min(...members.map((member) => member.y - member.radiusScaled));
        const maxY = Math.max(...members.map((member) => member.y + member.radiusScaled));
        const horizontal = (maxX - minX) >= (maxY - minY);
        const bracket = svgElement('g', { class: 'architect-bary-bracket' });
        const line = (x1, y1, x2, y2) => bracket.appendChild(svgElement('line', { x1, y1, x2, y2 }));
        let iconX;
        let iconY;
        if (horizontal) {
            const legY = Math.min(...members.map((member) => member.y - member.radiusScaled - 16));
            const startX = Math.min(...members.map((member) => member.x));
            const endX = Math.max(...members.map((member) => member.x));
            iconX = (startX + endX) / 2;
            iconY = legY - 12;
            line(startX, legY, endX, legY);
            line(iconX, legY, iconX, iconY + 6);
            members.forEach((member) => line(member.x, legY, member.x, member.y - member.radiusScaled));
        } else {
            const legX = Math.min(...members.map((member) => member.x - member.radiusScaled - 16));
            const startY = Math.min(...members.map((member) => member.y));
            const endY = Math.max(...members.map((member) => member.y));
            iconX = legX - 12;
            iconY = (startY + endY) / 2;
            line(legX, startY, legX, endY);
            line(legX, iconY, iconX + 6, iconY);
            members.forEach((member) => line(legX, member.y, member.x - member.radiusScaled, member.y));
        }
        svg.appendChild(bracket);
        const glyph = svgElement('g', {
            transform: `translate(${iconX} ${iconY})`, class: 'architect-body', role: 'button', tabindex: '0',
            'aria-label': `Select ${body.name}`,
        });
        renderer()?.drawBarycenterIcon(glyph);
        const select = () => selectDraftBody(body);
        glyph.addEventListener('click', select);
        glyph.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                select();
            }
        });
        svg.appendChild(glyph);
        barycenter.x = iconX;
        barycenter.y = iconY;
        barycenter.radiusScaled = 6;
        return glyph;
    }

    function renderDraftCanvas({ preserveViewBox = false } = {}) {
        const svg = document.getElementById('svg');
        if (!svg || !draft) return;
        const activeViewBox = preserveViewBox ? svg.getAttribute('viewBox') : null;
        activateDraftCanvas();
        while (svg.firstChild) svg.removeChild(svg.firstChild);
        renderer()?.prepareCanvas();

        const bodies = draft.bodies;
        if (!bodies.length) {
            svg.setAttribute('viewBox', activeViewBox || '0 0 1200 800');
            const heading = svgElement('text', { x: 600, y: 70, 'text-anchor': 'middle', class: 'architect-draft-title' });
            heading.textContent = draft.name.toUpperCase();
            svg.appendChild(heading);
            const subheading = svgElement('text', { x: 600, y: 98, 'text-anchor': 'middle', class: 'architect-draft-subtitle' });
            subheading.textContent = 'SYNTHETIC DRAFT — NOT EDGIS SURVEY DATA';
            svg.appendChild(subheading);
            const message = svgElement('text', { x: 600, y: 365, 'text-anchor': 'middle', class: 'architect-empty-message' });
            message.textContent = 'ADD YOUR FIRST BODY TO BEGIN';
            svg.appendChild(message);
            addBodyControl(svg, 600, 420, null, 'root');
            return;
        }

        const layout = renderer()?.layoutDraftBodies(bodies);
        if (!layout) return;
        const { byId, bounds } = layout;
        const width = Math.max(800, Math.ceil(bounds.maxX));
        const height = Math.max(600, Math.ceil(bounds.maxY));
        svg.setAttribute('viewBox', activeViewBox || `0 0 ${width} ${height}`);
        const heading = svgElement('text', { x: width / 2, y: 36, 'text-anchor': 'middle', class: 'architect-draft-title' });
        heading.textContent = draft.name.toUpperCase();
        svg.appendChild(heading);
        const subheading = svgElement('text', { x: width / 2, y: 62, 'text-anchor': 'middle', class: 'architect-draft-subtitle' });
        subheading.textContent = 'SYNTHETIC DRAFT — NOT EDGIS SURVEY DATA';
        svg.appendChild(subheading);
        // Match Sysmap's rail topology: parent to the first child, then each
        // successive sibling.  Lines run centre-to-centre and are covered by
        // body artwork at either end, just as in the read-only renderer.
        const drawnLinks = new Set();
        const drawLink = (from, to) => {
            const key = [from.id, to.id].sort().join('|');
            if (drawnLinks.has(key)) return;
            drawnLinks.add(key);
            svg.appendChild(svgElement('line', {
                x1: from.x, y1: from.y, x2: to.x, y2: to.y,
                'stroke-width': 1.5, class: 'link',
            }));
        };
        layout.nodes.forEach((parent) => {
            const children = (parent.children || []).filter((child) => child.type !== 'Barycenter');
            if (!children.length) return;
            if (parent.type !== 'Barycenter') drawLink(parent, children[0]);
            for (let index = 1; index < children.length; index += 1) {
                drawLink(children[index - 1], children[index]);
            }
        });

        const baryGroups = new Map();
        bodies.forEach((body) => {
            if (body.type === 'Barycenter') {
                baryGroups.set(body.id, drawDraftBarycenter(svg, byId.get(body.id), body));
            }
        });

        bodies.forEach((body) => {
            const layoutNode = byId.get(body.id);
            const { x, y } = layoutNode;
            const radius = layoutNode.radiusScaled;
            let group = null;
            if (body.type === 'Star') {
                group = svgElement('g', {
                    transform: `translate(${x} ${y})`, class: 'architect-body', role: 'button', tabindex: '0',
                    'aria-label': `Select ${body.name}`,
                });
                renderer()?.drawStar({
                    id: body.id, name: body.name, type: 'Star', subType: body.starType,
                    radius: body.radius ?? 1, radiusScaled: radius, temperature: body.temperature,
                    rotationalPeriod: body.rotationalPeriod ?? 1, axialTilt: body.axialTilt ?? 0,
                    rings: body.rings || [],
                }, group);
            } else if (body.type === 'Planet') {
                group = svgElement('g', {
                    transform: `translate(${x} ${y})`, class: 'architect-body', role: 'button', tabindex: '0',
                    'aria-label': `Select ${body.name}`,
                });
                renderer()?.drawPlanet({
                    id: body.id, name: body.name, type: 'Planet', subType: body.subtype,
                    radius: body.radius ?? 6000, radiusScaled: radius, temperature: body.temperature ?? 300,
                    axialTilt: body.axialTilt ?? 0, atmosphereType: null,
                    surfacePressure: body.surfacePressure ?? 0, isLandable: false, rings: body.rings || [],
                }, group);
            }
            if (group) {
                const select = () => selectDraftBody(body);
                group.addEventListener('click', select);
                group.addEventListener('keydown', (event) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault();
                        select();
                    }
                });
                svg.appendChild(group);
            }
            if (body.id === selectedBodyId) {
                svg.appendChild(svgElement('circle', { cx: x, cy: y, r: radius + 3, class: 'architect-selected-body' }));
            }
            const label = svgElement('text', {
                x, y: y - radius - 26, 'text-anchor': 'middle',
                'dominant-baseline': 'bottom',
                class: `architect-body-label${body.id === selectedBodyId ? ' selected' : ''}`,
            });
            label.textContent = body.name;
            svg.appendChild(label);
            const hoverTarget = group || baryGroups.get(body.id);
            if (hoverTarget) {
                const showLabel = () => label.classList.add('visible');
                const hideLabel = () => {
                    if (body.id !== selectedBodyId) label.classList.remove('visible');
                };
                hoverTarget.addEventListener('mouseenter', showLabel);
                hoverTarget.addEventListener('mouseleave', hideLabel);
                hoverTarget.addEventListener('focus', showLabel);
                hoverTarget.addEventListener('blur', hideLabel);
            }
            if (body.id === selectedBodyId) {
                // A selected body exposes its complete editing compass:
                // siblings before/after on either side, plus a child below.
                addBodyControl(svg, x - radius - 12, y, body.parentId || null, 'sibling-before', body.id, body.id);
                addBodyControl(svg, x + radius + 12, y, body.parentId || null, 'sibling-after', body.id, null, body.id);
                addBodyControl(svg, x, y + radius + 16, body.id, 'child');
                const deleteOffset = (radius + 14) * Math.SQRT1_2;
                addRemoveControl(svg, x + deleteOffset, y + deleteOffset);
            }
        });
    }

    function addStyle() {
        const style = document.createElement('style');
        style.textContent = `
            svg .architect-draft-title { fill:#fa6d00; font-size:1.25rem; letter-spacing:.12em; }
            svg .architect-draft-subtitle { fill:#9aa3c7; font-size:.65rem; letter-spacing:.08em; }
            svg .architect-body-label { fill:#e6e9ff; font-size:.65rem; opacity:0; transition:opacity .15s ease; pointer-events:none; }
            svg .architect-body-label.visible, svg .architect-body-label.selected { opacity:1; }
            svg .architect-empty-message { fill:#9aa3c7; font-size:.75rem; letter-spacing:.12em; }
            svg .architect-bary-bracket line { stroke:#333; stroke-width:1.4; stroke-linecap:round; }
            svg .architect-body { cursor:pointer; }
            svg .architect-selected-body { fill:none; stroke:#fa6d00; stroke-width:2; pointer-events:none; }
            svg .architect-add-body-control { cursor:pointer; }
            svg .architect-add-body-control circle { fill:rgba(5,29,14,.94); stroke:#49d46b; stroke-width:1.25; stroke-dasharray:2.2 2.2; }
            svg .architect-add-body-control path { fill:none; stroke:#49d46b; stroke-linecap:round; }
            svg .architect-add-body-control:hover circle, svg .architect-add-body-control:focus circle { fill:#49d46b; }
            svg .architect-add-body-control:hover path, svg .architect-add-body-control:focus path { stroke:#051d0e; }
            svg .architect-remove-body-control { cursor:pointer; }
            svg .architect-remove-body-control circle { fill:rgba(46,11,7,.96); stroke:#e85a3c; stroke-width:1.25; }
            svg .architect-remove-body-control path { fill:none; stroke:#e85a3c; stroke-linecap:round; }
            svg .architect-remove-body-control:hover circle, svg .architect-remove-body-control:focus circle { fill:#e85a3c; }
            svg .architect-remove-body-control:hover path, svg .architect-remove-body-control:focus path { stroke:#241607; }
            .architect-body-card .architect-field { display:grid; gap:5px; margin:10px 0; color:#aaa; font-size:.8rem; }
            .architect-body-card .architect-name-field input, .architect-body-card select { width:100%; box-sizing:border-box; border:1px solid rgba(250,109,0,.5); background:#241607; color:#e6e9ff; padding:7px; }
            .architect-body-card .architect-range-field { grid-template-columns:minmax(0, 1fr) max-content; align-items:center; }
            .architect-body-card .architect-range-field input { grid-column:1 / -1; width:100%; accent-color:#fa6d00; }
            .architect-body-card .architect-range-field output { color:#e6e9ff; font-variant-numeric:tabular-nums; }
            .architect-ring-row { display:flex; gap:7px; margin:7px 0; }
            .architect-ring-row select { min-width:0; flex:1; }
            .architect-ring-remove, .architect-add-ring { border:1px solid rgba(250,109,0,.5); background:#241607; color:#e6e9ff; padding:6px 9px; cursor:pointer; }
            .architect-ring-remove { border-color:rgba(232,90,60,.75); color:#ffd0c5; }
            .architect-add-ring { margin-top:5px; width:100%; color:#49d46b; border-color:rgba(73,212,107,.65); }
            .architect-remove-button { width:100%; margin:14px 0 5px; border:1px solid rgba(232,90,60,.75); background:#2e0b07; color:#ffd0c5; padding:9px; letter-spacing:.08em; cursor:pointer; }
            .architect-remove-button:hover { background:#e85a3c; color:#241607; }
            .architect-body-picker { position:fixed; left:50%; top:50%; z-index:1100; display:flex; gap:7px; align-items:center; width:min(540px, calc(100vw - 32px)); padding:12px; transform:translate(-50%, -50%); border:1px solid rgba(250,109,0,.72); background:rgba(7,7,7,.97); box-shadow:0 12px 40px rgba(0,0,0,.58); }
            .architect-body-picker select { min-width:0; flex:1; border:1px solid rgba(250,109,0,.5); background:#241607; color:#e6e9ff; padding:7px; }
            .architect-body-picker button { border:1px solid rgba(250,109,0,.5); background:#241607; color:#e6e9ff; padding:7px 10px; }
        `;
        document.head.appendChild(style);
    }

    function updateMode(button) {
        button.classList.toggle('active', enabled);
        button.setAttribute('aria-pressed', String(enabled));
        button.title = enabled ? 'Exit System Builder' : 'Enter System Builder (dream system)';
        button.setAttribute('aria-label', button.title);
        if (enabled) {
            if (!draft) newDreamSystem();
            beginDreamSystem();
            renderDraftCanvas();
        } else {
            closeBodyPicker();
            globalThis.EDGIS_ARCHITECT_DRAFT_ACTIVE = false;
            updateDraftUrl(false);
            restoreSourceCanvas();
            const infoPanel = document.getElementById('InfoPanel');
            if (infoPanel) {
                infoPanel.replaceChildren();
                infoPanel.style.display = 'none';
            }
        }
    }

    function bindArchitectKeyboardShortcuts() {
        document.addEventListener('keydown', (event) => {
            if (!enabled || !globalThis.EDGIS_ARCHITECT_DRAFT_ACTIVE || event.key !== 'Delete') return;
            const target = event.target;
            const isEditing = target instanceof HTMLElement
                && (target.matches('input, select, textarea, button') || target.isContentEditable);
            if (isEditing || !selectedBodyId) return;
            event.preventDefault();
            removeSelectedBody();
        });
    }

    function addArchitectButton() {
        const toolbar = document.querySelector('.toolbar');
        if (!toolbar || document.getElementById('architectModeButton')) return;
        addStyle();
        const button = document.createElement('button');
        button.id = 'architectModeButton';
        button.type = 'button';
        button.title = 'Enter System Builder (dream system)';
        button.setAttribute('aria-label', button.title);
        button.innerHTML = '<svg width="26" height="26" viewBox="0 0 24 24" aria-hidden="true"><path d="M8 3v15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/><path d="M9 4h8v7l-4-2.5L9 11z" fill="currentColor"/><path d="M4 21a8 8 0 0 1 16 0" fill="none" stroke="currentColor" stroke-width="1.75"/><path d="M12 17v4" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round"/></svg>';
        button.addEventListener('click', () => {
            enabled = !enabled;
            updateMode(button);
        });
        toolbar.appendChild(button);
        globalThis.EDGIS_ARCHITECT_EXIT = () => {
            if (!enabled) return;
            enabled = false;
            updateMode(button);
        };
        bindArchitectKeyboardShortcuts();
        if (enabled) updateMode(button);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', addArchitectButton, { once: true });
    } else {
        addArchitectButton();
    }
}());
