(function () {
    'use strict';

    if (!globalThis.EDGIS_SYSMAP_CONFIG?.architectModeEnabled) return;

    const STORAGE_PREFIX = 'edgis:colony-builder:v1:';
    const PRE_REQUISITES = {
        satellite: ['hermes', 'angelia', 'eirene'],
        comms: ['pistis', 'soter', 'aletheia'],
        settlementAgr: ['consus', 'picumnus', 'annona', 'ceres', 'fornax'],
        installationAgr: ['demeter'],
        installationMil: ['vacuna', 'alastor'],
        outpostMining: ['euthenia', 'phorcys'],
        relay: ['enodia', 'ichnaea'],
        settlementBio: ['pheobe', 'asteria', 'caerus', 'chronos'],
        settlementTourist: ['aergia', 'comus', 'gelos', 'fufluns'],
        settlementMilitary: ['ioke', 'bellona', 'enyo', 'polemos', 'minerva'],
        settlementExtraction: ['ourea', 'mantus', 'orcus', 'aerecura', 'erebus'],
    };
    const PRE_REQUISITE_NAMES = {
        satellite: 'a satellite installation', comms: 'a communications installation',
        settlementAgr: 'an agricultural settlement', installationAgr: 'a space farm',
        installationMil: 'a military installation', outpostMining: 'a mining/industrial installation',
        relay: 'a relay installation', settlementBio: 'a bio settlement',
        settlementTourist: 'a tourist settlement', settlementMilitary: 'a military settlement',
        settlementExtraction: 'an extraction settlement',
    };
    const type = (name, subtypes, orbital, tier, needsTier, needsCount, givesTier, givesCount, preReq = null) => ({
        name, subtypes, orbital, tier, needs: [needsTier, needsCount], gives: [givesTier, givesCount], preReq,
    });

    // Raven Colonial site catalog, reduced to the placement and progression
    // fields used by this first Sysmap integration.  Keep this list aligned
    // with Raven's site-data.ts before expanding the economy/link simulation.
    const SITE_TYPES = [
        type('Coriolis Starport', ['no_truss', 'dual_truss', 'quad_truss'], true, 2, 2, 3, 3, 1),
        type('Asteroid Starport', ['asteroid'], true, 2, 2, 3, 3, 1),
        type('Dodec Starport', ['dodec', 'quint_truss', 'dec_truss'], true, 3, 3, 6, 0, 0),
        type('Ocellus Starport', ['ocellus'], true, 3, 3, 6, 0, 0),
        type('Orbis Starport', ['apollo', 'artemis'], true, 3, 3, 6, 0, 0),
        type('Commercial Outpost', ['plutus'], true, 1, 0, 0, 2, 1),
        type('Industrial Outpost', ['vulcan'], true, 1, 0, 0, 2, 1),
        type('Pirate Outpost', ['dysnomia'], true, 1, 0, 0, 2, 1),
        type('Civilian Outpost', ['vesta'], true, 1, 0, 0, 2, 1),
        type('Scientific Outpost', ['prometheus'], true, 1, 0, 0, 2, 1),
        type('Military Outpost', ['nemesis'], true, 1, 0, 0, 2, 1),
        type('Satellite Installation', ['angelia', 'eirene', 'hermes'], true, 1, 0, 0, 2, 1),
        type('Communication Installation', ['aletheia', 'pistis', 'soter'], true, 1, 0, 0, 2, 1),
        type('Space Farm', ['demeter'], true, 1, 0, 0, 2, 1),
        type('Pirate Base Installation', ['apate', 'laverna'], true, 1, 0, 0, 2, 1),
        type('Mining/Industrial Installation', ['euthenia', 'phorcys'], true, 1, 0, 0, 2, 1),
        type('Relay Installation', ['enodia', 'ichnaea'], true, 1, 0, 0, 2, 1),
        type('Military Installation', ['alastor', 'vacuna'], true, 2, 2, 1, 3, 1, 'settlementMilitary'),
        type('Security Installation', ['dicaeosyne', 'eunomia', 'nomos', 'poena'], true, 2, 2, 1, 3, 1, 'relay'),
        type('Government Installation', ['harmonia'], true, 2, 2, 1, 3, 1),
        type('Medical Installation', ['asclepius', 'eupraxia'], true, 2, 2, 1, 3, 1),
        type('Research Installation', ['astraeus', 'coeus', 'dione', 'dodona'], true, 2, 2, 1, 3, 1, 'settlementBio'),
        type('Tourist Installation', ['hedone', 'opora', 'pasithea'], true, 2, 2, 1, 3, 1, 'settlementTourist'),
        type('Space Bar Installation', ['bacchus', 'dionysus'], true, 2, 2, 1, 3, 1),
        type('Civilian Surface Outpost', ['atropos', 'clotho', 'decima', 'hestia', 'lachesis', 'nona'], false, 1, 0, 0, 2, 1),
        type('Industrial Surface Outpost', ['bia', 'hephaestus', 'mefitis', 'opis', 'ponos', 'tethys'], false, 1, 0, 0, 2, 1),
        type('Scientific Surface Outpost', ['ananke', 'antevorta', 'fauna', 'necessitas', 'porrima', 'providentia'], false, 1, 0, 0, 2, 1),
        type('Large Planetary Port', ['aphrodite', 'hera', 'poseidon', 'zeus'], false, 3, 3, 6, 0, 0),
        type('Agriculture Settlement: Small', ['consus'], false, 1, 0, 0, 2, 1),
        type('Agriculture Settlement: Medium', ['annona', 'picumnus'], false, 1, 0, 0, 2, 1),
        type('Agriculture Settlement: Large', ['ceres', 'fornax'], false, 2, 2, 1, 3, 2),
        type('Mining Settlement: Small', ['ourea'], false, 1, 0, 0, 2, 1),
        type('Mining Settlement: Medium', ['mantus', 'orcus'], false, 1, 0, 0, 2, 1),
        type('Mining Settlement: Large', ['aerecura', 'erebus'], false, 2, 2, 1, 3, 2),
        type('Industrial Settlement: Small', ['fontus'], false, 1, 0, 0, 2, 1),
        type('Industrial Settlement: Medium', ['meteope', 'minthe', 'palici'], false, 1, 0, 0, 2, 1),
        type('Industrial Settlement: Large', ['gaea'], false, 2, 2, 1, 3, 2),
        type('Military Settlement: Small', ['ioke'], false, 1, 0, 0, 2, 1),
        type('Military Settlement: Medium', ['bellona', 'enyo', 'polemos'], false, 1, 0, 0, 2, 1),
        type('Military Settlement: Large', ['minerva'], false, 2, 2, 1, 3, 2),
        type('Bio Settlement: Small', ['pheobe'], false, 2, 2, 1, 3, 1),
        type('Bio Settlement: Medium', ['asteria', 'caerus'], false, 2, 2, 1, 3, 1),
        type('Bio Settlement: Large', ['chronos'], false, 2, 2, 1, 3, 2),
        type('Tourist Settlement: Small', ['aergia'], false, 2, 2, 1, 3, 1, 'satellite'),
        type('Tourist Settlement: Medium', ['comus', 'gelos'], false, 2, 2, 1, 3, 1, 'satellite'),
        type('Tourist Settlement: Large', ['fufluns'], false, 2, 2, 1, 3, 2, 'satellite'),
        type('Extraction Hub', ['tartarus'], false, 2, 2, 1, 3, 1, 'settlementExtraction'),
        type('Civilian Hub', ['aegle'], false, 2, 2, 1, 3, 1, 'settlementAgr'),
        type('Exploration Hub', ['tellus_e'], false, 2, 2, 1, 3, 1, 'comms'),
        type('Outpost Hub', ['io'], false, 2, 2, 1, 3, 1, 'installationAgr'),
        type('Scientific Hub', ['athena', 'caelus'], false, 2, 2, 1, 3, 1),
        type('Military Hub', ['alala', 'ares'], false, 2, 2, 1, 3, 1, 'installationMil'),
        type('Refinery Hub', ['silenus'], false, 2, 2, 1, 3, 1),
        type('High Tech Hub', ['janus'], false, 2, 2, 1, 3, 1),
        type('Industrial Hub', ['eunostus', 'molae', 'tellus_i'], false, 2, 2, 1, 3, 1, 'outpostMining'),
    ];

    let active = false;
    let selectedBodyId = null;
    let button = null;

    const source = () => globalThis.EDGIS_ARCHITECT_SOURCE_SYSTEM;
    const planKey = () => `${STORAGE_PREFIX}${source()?.systemName || ''}`;
    const plans = () => {
        try {
            const value = globalThis.localStorage?.getItem(planKey());
            const parsed = value ? JSON.parse(value) : [];
            return Array.isArray(parsed) ? parsed : [];
        } catch (_) { return []; }
    };
    const savePlans = (value) => globalThis.localStorage?.setItem(planKey(), JSON.stringify(value));
    const bodyFor = (bodyId) => source()?.bodies?.find((body) => String(body.body_id ?? body.id) === String(bodyId));
    const siteTypeFor = (subtype) => SITE_TYPES.find((candidate) => candidate.subtypes.includes(subtype));
    const isLandable = (body) => Boolean(body?.is_landable ?? body?.landable ?? body?.isLandable);

    function projectedPoints(entries = plans()) {
        return entries.reduce((points, entry) => {
            const definition = siteTypeFor(entry.subtype);
            if (!definition || entry.status === 'demolish') return points;
            const [tier, count] = definition.gives;
            if (tier === 2) points.tier2 += count;
            if (tier === 3) points.tier3 += count;
            return points;
        }, { tier2: 0, tier3: 0 });
    }

    function portCost(definition, entries) {
        let count = -2;
        entries.forEach((entry) => {
            const existing = siteTypeFor(entry.subtype);
            if (existing?.name.includes('Starport') && existing.tier > 1) count += 1;
        });
        let cost = definition.needs[1];
        if (definition.name.includes('Starport') && definition.tier > 1 && count > 0) {
            cost += definition.needs[0] === 3 ? cost * count : Math.trunc(cost * .75 * count);
        }
        return cost;
    }

    function validate(definition, entries = plans()) {
        const points = projectedPoints(entries);
        const [needTier] = definition.needs;
        const needCount = portCost(definition, entries);
        if (needTier === 2 && points.tier2 < needCount) return `Needs ${needCount} Tier 2 point${needCount === 1 ? '' : 's'}`;
        if (needTier === 3 && points.tier3 < needCount) return `Needs ${needCount} Tier 3 point${needCount === 1 ? '' : 's'}`;
        if (definition.preReq) {
            const satisfied = entries.some((entry) => PRE_REQUISITES[definition.preReq].some((value) => entry.subtype.startsWith(value)));
            if (!satisfied) return `Requires ${PRE_REQUISITE_NAMES[definition.preReq]}`;
        }
        return '';
    }

    function renderMarkers() {
        document.querySelectorAll('.colony-plan-marker').forEach((marker) => marker.remove());
        if (!active) return;
        plans().forEach((entry) => {
            const host = [...document.querySelectorAll('#svg .node[data-body-id]')]
                .find((node) => node.dataset.bodyId === String(entry.bodyId));
            if (!host) return;
            const marker = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            marker.classList.add('colony-plan-marker');
            marker.setAttribute('transform', 'translate(10 -10)');
            marker.setAttribute('role', 'button');
            marker.setAttribute('tabindex', '0');
            marker.setAttribute('aria-label', `Planned ${entry.subtype}`);
            marker.innerHTML = '<rect x="-6" y="-6" width="12" height="12" rx="1"/><path d="M-3 4V-2h6v6M-1-5h2M0-6v2"/>';
            const select = (event) => { event.stopPropagation(); selectedBodyId = entry.bodyId; renderPanel(); };
            marker.addEventListener('click', select);
            marker.addEventListener('keydown', (event) => {
                if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); select(event); }
            });
            host.appendChild(marker);
        });
    }

    function optionFor(definition) {
        const option = document.createElement('option');
        option.value = definition.subtypes[0];
        option.textContent = `${definition.name} — ${definition.subtypes.join(', ')}`;
        return option;
    }

    function renderPanel() {
        const panel = document.getElementById('InfoPanel');
        if (!panel || !active) return;
        panel.replaceChildren();
        panel.style.display = 'block';
        const card = document.createElement('article');
        card.className = 'card colony-builder-card';
        const points = projectedPoints();
        const body = bodyFor(selectedBodyId);
        card.innerHTML = `<h1>COLONY BUILDER</h1><section><h2>RAVEN COLONIAL RULES</h2><p class="small">Design projection for ${source()?.systemName || 'this system'}.</p><p class="colony-points">T2 ${points.tier2} · T3 ${points.tier3}</p></section>`;
        if (!body) {
            const hint = document.createElement('p');
            hint.textContent = 'Select a body on the map to plan a colony site.';
            card.appendChild(hint);
            panel.appendChild(card);
            return;
        }
        const name = document.createElement('h2');
        name.textContent = body.body_name || body.name || `Body ${selectedBodyId}`;
        card.appendChild(name);
        const location = document.createElement('select');
        location.className = 'colony-field';
        location.innerHTML = '<option value="orbital">Orbital site</option><option value="surface">Surface site</option>';
        if (!isLandable(body)) location.querySelector('option[value="surface"]').disabled = true;
        const typeSelect = document.createElement('select');
        typeSelect.className = 'colony-field';
        const populate = () => {
            typeSelect.replaceChildren();
            const orbital = location.value === 'orbital';
            SITE_TYPES.filter((definition) => definition.orbital === orbital).forEach((definition) => typeSelect.appendChild(optionFor(definition)));
            updateRule();
        };
        const rule = document.createElement('p');
        rule.className = 'colony-rule';
        const updateRule = () => {
            const definition = siteTypeFor(typeSelect.value);
            const message = definition ? validate(definition) : '';
            rule.textContent = message || 'Eligible under the current projected plan.';
            rule.classList.toggle('blocked', Boolean(message));
        };
        location.addEventListener('change', populate);
        typeSelect.addEventListener('change', updateRule);
        populate();
        const add = document.createElement('button');
        add.type = 'button';
        add.className = 'colony-add-site';
        add.textContent = 'PLAN SITE';
        add.addEventListener('click', () => {
            const definition = siteTypeFor(typeSelect.value);
            if (!definition) return;
            const current = plans();
            current.push({ id: `colony-${Date.now()}`, bodyId: String(selectedBodyId), subtype: typeSelect.value, location: location.value, status: 'plan' });
            savePlans(current);
            renderMarkers();
            renderPanel();
        });
        const bodyPlans = plans().filter((entry) => String(entry.bodyId) === String(selectedBodyId));
        const list = document.createElement('section');
        list.innerHTML = '<h2>PLANNED SITES</h2>';
        bodyPlans.forEach((entry) => {
            const row = document.createElement('div');
            row.className = 'colony-site-row';
            const description = document.createElement('span');
            description.textContent = `${entry.subtype} · ${entry.location}`;
            const remove = document.createElement('button');
            remove.type = 'button'; remove.textContent = '×'; remove.title = `Remove ${entry.subtype}`;
            remove.addEventListener('click', () => { savePlans(plans().filter((candidate) => candidate.id !== entry.id)); renderMarkers(); renderPanel(); });
            row.append(description, remove); list.appendChild(row);
        });
        card.append(name, location, typeSelect, rule, add, list);
        panel.appendChild(card);
    }

    function activate() {
        if (!source()?.bodies?.length) return;
        globalThis.EDGIS_ARCHITECT_EXIT?.();
        active = !active;
        document.getElementById('svg')?.classList.toggle('colony-builder-active', active);
        button?.classList.toggle('active', active);
        button?.setAttribute('aria-pressed', String(active));
        button.title = active ? 'Exit Colony Builder' : 'Enter Colony Builder (current system)';
        if (!active) {
            selectedBodyId = null;
            document.querySelectorAll('.colony-plan-marker').forEach((marker) => marker.remove());
            document.getElementById('InfoPanel')?.replaceChildren();
            document.getElementById('InfoPanel')?.style.setProperty('display', 'none');
            return;
        }
        renderMarkers();
        renderPanel();
    }

    function addButton() {
        const toolbar = document.querySelector('.toolbar');
        if (!toolbar || document.getElementById('colonyBuilderButton')) return;
        const style = document.createElement('style');
        style.textContent = `
          #svg.colony-builder-active .node { cursor:crosshair; }
          #svg .colony-plan-marker rect { fill:#061c0b; stroke:#39db62; stroke-width:1; }
          #svg .colony-plan-marker path { fill:none; stroke:#39db62; stroke-width:1; stroke-linecap:round; }
          .colony-builder-card .colony-field { width:100%; box-sizing:border-box; margin:5px 0; padding:7px; border:1px solid rgba(57,219,98,.6); background:#07150a; color:#e6e9ff; }
          .colony-builder-card .colony-rule { color:#71df8c; font-size:.8rem; }
          .colony-builder-card .colony-rule.blocked { color:#f0ad4e; }
          .colony-points { color:#49d46b; font-variant-numeric:tabular-nums; }
          .colony-add-site { width:100%; margin:8px 0; padding:8px; border:1px solid #49d46b; background:#061c0b; color:#8aff9f; cursor:pointer; }
          .colony-site-row { display:flex; justify-content:space-between; gap:8px; padding:5px 0; border-bottom:1px solid rgba(57,219,98,.18); font-size:.82rem; }
          .colony-site-row button { color:#ff977f; border:0; background:transparent; cursor:pointer; }
        `;
        document.head.appendChild(style);
        button = document.createElement('button');
        button.id = 'colonyBuilderButton'; button.type = 'button';
        button.title = 'Enter Colony Builder (current system)'; button.setAttribute('aria-label', button.title); button.setAttribute('aria-pressed', 'false');
        button.innerHTML = '<svg width="26" height="26" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 21V9h7v12M13 21V3h7v18M2 21h20M6 12h2M6 16h2M15 7h2M15 11h2M15 15h2" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg>';
        button.addEventListener('click', activate);
        // Put the current-system tool next to (and before) the dream-system
        // builder so the two Architect paths are discoverable together.
        toolbar.insertBefore(button, document.getElementById('architectModeButton'));
        document.getElementById('svg')?.addEventListener('click', (event) => {
            if (!active) return;
            const node = event.target.closest?.('.node[data-body-id]');
            if (!node || !bodyFor(node.dataset.bodyId)) return;
            selectedBodyId = node.dataset.bodyId;
            renderPanel();
        });
        globalThis.addEventListener('edgis:sysmap-rendered', () => { if (active) { renderMarkers(); renderPanel(); } });
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', addButton, { once: true });
    else addButton();
}());
