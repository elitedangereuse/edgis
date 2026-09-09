'use strict';

// Headless render harness for the system map: drives the real sysmap.js +
// sysmap_roots.js against a fixture from this directory using a minimal DOM
// stub, then serializes the produced SVG to a file for visual inspection.
//
// Usage:
//   node api/tests/js/render_fixture.js "Kappa Cassiopeiae" "Kappa Cassiopeiae.json" [out.svg]
//
// The fixture path is resolved against api/tests/js when it does not exist as
// given. The output SVG embeds sysmap.css so it renders standalone; convert it
// to PNG with e.g. `inkscape out.svg -o out.png -w 1600` (dark background
// required: the map is drawn for a dark theme).

const fs = require('node:fs');
const path = require('node:path');

const REPO_ROOT = path.join(__dirname, '..', '..');
const SYSMAP_JS = path.join(REPO_ROOT, 'static', 'js', 'sysmap.js');
const SYSMAP_ROOTS_JS = path.join(REPO_ROOT, 'static', 'js', 'sysmap_roots.js');
const SYSMAP_CSS = path.join(REPO_ROOT, 'static', 'sysmap.css');

class El {
    constructor(tag){
        this.tagName = tag;
        this.attrs = {};
        this.children = [];
        this.style = {};
        this.dataset = {};
        this._listeners = {};
        this.textContent = '';
        this.parentNode = null;
        this.classList = {
            add: (...cs) => cs.forEach(c => this.attrs._class = `${this.attrs._class || ''} ${c}`.trim()),
            remove: () => {}
        };
    }
    get firstChild(){ return this.children[0] || null; }
    setAttribute(k, v){ this.attrs[k] = String(v); }
    setAttributeNS(ns, k, v){ this.attrs[k] = String(v); }
    getAttribute(k){ return this.attrs[k] ?? null; }
    getAttributeNS(k){ return this.attrs[k] ?? null; }
    appendChild(c){ c.parentNode = this; this.children.push(c); return c; }
    removeChild(c){ this.children = this.children.filter(x => x !== c); return c; }
    addEventListener(type, fn){ (this._listeners[type] ||= []).push(fn); }
    removeEventListener(){}
    querySelector(){ return null; }
    querySelectorAll(){ return []; }
    closest(){ return null; }
    cloneNode(){ return new El(this.tagName); }
    getBoundingClientRect(){ return { width: 0, height: 0, top: 0, left: 0 }; }
    getBBox(){ return { x: 0, y: 0, width: 0, height: 0 }; }
    focus(){} blur(){} select(){} contains(){ return false; }
}

function serialize(el, depth = 0){
    const pad = '  '.repeat(depth);
    const attrs = Object.entries(el.attrs)
        .filter(([k]) => k !== '_class')
        .map(([k, v]) => ` ${k}="${v}"`).join('');
    const cls = el.attrs._class ? ` class="${el.attrs._class}"` : '';
    const tag = el.tagName.toLowerCase().replace('svg:', '');
    if(el.children.length === 0 && !el.textContent){
        return `${pad}<${tag}${cls}${attrs}/>`;
    }
    const inner = el.children.map(c => serialize(c, depth + 1)).join('\n');
    const text = el.textContent ? `\n${'  '.repeat(depth + 1)}${el.textContent}` : '';
    return `${pad}<${tag}${cls}${attrs}>${text}\n${inner}\n${pad}</${tag}>`;
}

const ID_MAP = {
    svg: 'svg', InfoPanel: 'div', bodyInfoButton: 'button', controlsPanel: 'div',
    controlsToggleButton: 'button', downloadSvgButton: 'button', openGalaxyMapButton: 'button',
    openEdgisButton: 'button', copyEmbedButton: 'button', embedPanel: 'div',
    embedLinkInput: 'input', embedCodeOutput: 'textarea', embedCodeCopyButton: 'button',
    systemSuggestions: 'div', system: 'input', load: 'button', debugToggle: 'input'
};

function makeDocument(){
    const registry = {};
    const doc = new El('document');
    doc.getElementById = (id) => {
        if(!registry[id]) registry[id] = new El(ID_MAP[id] || 'div');
        return registry[id];
    };
    doc.createElement = (t) => new El(t);
    doc.createElementNS = (ns, t) => new El(t);
    doc.addEventListener = () => {};
    doc.removeEventListener = () => {};
    doc.body = new El('body');
    doc.execCommand = () => true;
    doc.activeElement = null;
    doc.registry = registry;
    return doc;
}

async function run(systemName, fixturePath, outPath){
    const bodies = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
    globalThis.SysmapRoots = require(SYSMAP_ROOTS_JS);
    globalThis.document = makeDocument();
    globalThis.navigator = {};
    globalThis.EDGIS_SYSMAP_CONFIG = {};
    globalThis.location = new URL(`https://edgis.test/static/sysmap.html?system=${encodeURIComponent(systemName)}`);
    globalThis.requestAnimationFrame = (fn) => setTimeout(fn, 0);
    globalThis.fetch = async (url) => {
        const u = String(url);
        const respond = (payload) => ({ ok: true, status: 200, json: async () => payload });
        if(u.includes('/bodies?')) return respond(bodies);
        if(u.includes('/systems/autocomplete')) return respond([]);
        return respond(null);
    };

    (0, eval)(fs.readFileSync(SYSMAP_JS, 'utf8'));
    await new Promise(r => setTimeout(r, 150));

    const svgEl = globalThis.document.registry.svg;
    let branchOriginStrokes = 0;
    const walk = (el) => {
        if(el.attrs.class === 'bary-branch-origin') branchOriginStrokes += 1;
        el.children.forEach(walk);
    };
    walk(svgEl);

    const css = fs.readFileSync(SYSMAP_CSS, 'utf8');
    const svgText = [
        `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${svgEl.attrs.viewBox}" width="1600" height="1000">`,
        `<style>${css}</style>`,
        `<rect x="0" y="0" width="100%" height="100%" fill="#000"/>`,
        ...svgEl.children.map(c => serialize(c, 1)),
        '</svg>'
    ].join('\n');
    fs.writeFileSync(outPath, svgText);

    const branchCount = branchOriginStrokes / 4;
    console.log(`${systemName}: rendered ${svgEl.attrs.viewBox}`);
    console.log(`  branch-origin crosses: ${branchCount} (${branchOriginStrokes} strokes)`);
    console.log(`  -> ${outPath}`);
}

const [,, systemName, fixtureArg, outArg] = process.argv;
if(!systemName || !fixtureArg){
    console.error('Usage: node render_fixture.js "SYSTEM NAME" "FIXTURE.json" [out.svg]');
    console.error('  fixture path is relative to api/tests/js unless it exists as given');
    process.exit(1);
}

const fixturePath = fs.existsSync(fixtureArg)
    ? fixtureArg
    : path.join(__dirname, fixtureArg);
const outPath = outArg || path.join('/tmp', `${path.basename(fixtureArg, '.json')}.render.svg`);

run(systemName, fixturePath, outPath).then(
    () => process.exit(0),
    (err) => { console.error('RENDER FAILED:', err && err.stack || err); process.exit(1); }
);
