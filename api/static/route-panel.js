(function () {
const $ = (id) => document.getElementById(id);
const KEY_LS = "edgis_route_key";
$("key").value = localStorage.getItem(KEY_LS) || "";
$("key").addEventListener("input", () => {
  localStorage.setItem(KEY_LS, $("key").value.trim());
  // Collapse the early-access banner once a key is entered; re-show (if there's
  // still a message) when it's cleared.
  const hasKey = !!$("key").value.trim();
  if (hasKey) $("banner").hidden = true;
  else if ($("bannerMsg").innerHTML.trim()) $("banner").hidden = false;
  if (typeof renderAccessIndicator === "function") renderAccessIndicator();
});
function headers() { const k = ($("key").value || "").trim(); return k ? { "x-route-key": k } : {}; }

// When embedded in the index, report busy state so the EDGIS hero logo animates
// during a plot/compare, just like the index's own queries.
let busyCount = 0;
function setBusy(on) {
  busyCount = Math.max(0, busyCount + (on ? 1 : -1));
  try { if (window.edgisSetRouteBusy) window.edgisSetRouteBusy(busyCount > 0); } catch (e) {}
}

// Remember the ship / supercharge choice between visits.
const SHIP_LS = "edgis_route_ship";
$("ship").value = localStorage.getItem(SHIP_LS) || "4";
$("ship").addEventListener("change", () => localStorage.setItem(SHIP_LS, $("ship").value));

// Remember the jump range too (rarely changes for a given ship build).
const RANGE_LS = "edgis_route_range";
const savedRange = localStorage.getItem(RANGE_LS);
if (savedRange) { $("range").value = savedRange; }
$("range").addEventListener("change", () => {
  const v = ($("range").value || "").trim();
  if (v) { localStorage.setItem(RANGE_LS, v); }
});

// Ship loadout (EDSY/SLEF) enables fuel-aware plotting. Stored in this browser.
const SLEF_LS = "edgis_route_loadout";
const FUELSAFE_LS = "edgis_route_fuelsafe";
let shipInfo = null;  // parsed ship summary, or null when no ship is imported
$("loadout").value = localStorage.getItem(SLEF_LS) || "";
setShipSummary();  // reflect a restored (saved, not-yet-imported) loadout at once
$("fuelSafe").checked = (localStorage.getItem(FUELSAFE_LS) ?? "1") === "1";
$("fuelSafe").addEventListener("change", () =>
  localStorage.setItem(FUELSAFE_LS, $("fuelSafe").checked ? "1" : "0"));

// Cargo tonnage (dynamic, not in the loadout): adds mass, shortens range.
const CARGO_LS = "edgis_route_cargo";
$("cargo").value = localStorage.getItem(CARGO_LS) || "0";
const cargoTons = () => Math.max(0, Number($("cargo").value) || 0);
$("cargo").addEventListener("change", () => {
  localStorage.setItem(CARGO_LS, String(cargoTons()));
  if (shipInfo) importShip();  // re-read the ship so the range reflects cargo
});

// Friendly ship label from a SLEF loadout, shown on the Ship & fuel summary so
// it's visible even when collapsed. Prefers the commander's ship name.
const SHIP_TYPES = {
  cobramkv: "Cobra Mk V", cobramkiii: "Cobra Mk III", cobramkiv: "Cobra Mk IV",
  anaconda: "Anaconda", asp: "Asp Explorer", asp_scout: "Asp Scout",
  diamondbackxl: "Diamondback Explorer", diamondback: "Diamondback Scout",
  krait_mkii: "Krait Mk II", krait_light: "Krait Phantom", mamba: "Mamba",
  python: "Python", python_nx: "Python Mk II", mandalay: "Mandalay",
  federation_corvette: "Federal Corvette", cutter: "Imperial Cutter",
  type9: "Type-9 Heavy", type6: "Type-6", type7: "Type-7", type8: "Type-8",
  sidewinder: "Sidewinder", dolphin: "Dolphin", orca: "Orca", corsair: "Corsair",
};
function shipLabel(loadout) {
  try {
    let d = JSON.parse(loadout);
    if (Array.isArray(d)) d = d[0];
    d = (d && d.data) || d;
    const name = String(d.ShipName || "").trim();
    const type = SHIP_TYPES[String(d.Ship || "").toLowerCase()]
              || String(d.Ship || "").replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
    if (name && type) return `${name} (${type})`;
    return name || type || "ship";
  } catch (e) { return "ship"; }
}
// The Ship & fuel section is collapsed by default, so its summary is the only
// always-visible signal. Make it state-explicit: a saved loadout that hasn't
// been imported (e.g. just after a page reload, before the auto-import lands)
// must not look the same as an active fuel-aware ship.
function setShipSummary() {
  const el = $("shipSummary");
  const hasText = !!($("loadout").value || "").trim();
  if (shipInfo) {
    el.innerHTML = `Ship &amp; fuel <span class="good">✓</span> · `
      + `${escapeHtml(shipLabel($("loadout").value))} · ${fmt(shipInfo.jump_range, 2)} LY`;
  } else if (hasText) {
    el.innerHTML = `Ship &amp; fuel · ${escapeHtml(shipLabel($("loadout").value))} `
      + `<span class="tiny">(saved — not imported)</span>`;
  } else {
    el.textContent = "Ship & fuel (optional)";
  }
}

async function importShip() {
  const loadout = ($("loadout").value || "").trim();
  if (!loadout) { shipInfo = null; localStorage.removeItem(SLEF_LS); $("shipInfo").textContent = ""; $("cmpGalaxy").hidden = true; setShipSummary(); return; }
  $("importShip").disabled = true;
  $("shipInfo").innerHTML = `<span class="tiny">Reading loadout…</span>`;
  $("shipSummary").innerHTML = `Ship &amp; fuel · <span class="tiny">reading loadout…</span>`;
  try {
    const res = await fetch("/route/ship", {
      method: "POST", credentials: "include",
      headers: { ...headers(), "Content-Type": "application/json" },
      body: JSON.stringify({ loadout, cargo: cargoTons() }),
    });
    if (!res.ok) {
      shipInfo = null;
      const d = (await res.json().catch(() => ({}))).detail;
      // detail can be a string (our 422), a dict (the early-access 403), or a
      // list (request validation) -- never render an object raw.
      let msg;
      if (res.status === 403) {
        msg = `${(d && d.message) || "This feature is in early access."} Add an access key below, or sign in as a patron.`;
      } else {
        msg = (typeof d === "string" ? d : (d && d.message)) || `Could not read that loadout (HTTP ${res.status}).`;
      }
      $("shipInfo").innerHTML = `<span class="bad">${escapeHtml(msg)}</span>`;
      return;
    }
    shipInfo = await res.json();
    localStorage.setItem(SLEF_LS, loadout);
    $("range").value = shipInfo.jump_range;
    localStorage.setItem(RANGE_LS, String(shipInfo.jump_range));
    // Plots at the full-tank range (always feasible); note the lighter max so
    // the lower-than-EDSY number isn't a surprise (range grows as fuel burns).
    const upTo = (shipInfo.max_range_light && shipInfo.max_range_light > shipInfo.jump_range + 0.1)
      ? ` <span class="tiny">(up to ${fmt(shipInfo.max_range_light, 1)} as fuel burns)</span>` : "";
    $("shipInfo").innerHTML = `<span class="good">Ship imported ✓</span> `
      + `${FUEL} range ${fmt(shipInfo.jump_range, 2)} LY${upTo} · tank ≈ ${shipInfo.tank_jumps} jumps · `
      + (shipInfo.has_scoop ? "fuel scoop fitted" : `<span class="bad">no fuel scoop</span>`);
  } catch (e) {
    shipInfo = null;
    $("shipInfo").innerHTML = `<span class="bad">${escapeHtml(e.message || String(e))}</span>`;
  } finally {
    $("importShip").disabled = false; $("cmpGalaxy").hidden = !shipInfo; setShipSummary();
    // A non-empty loadout that failed to import (bad JSON, access gate): open
    // the section so the reason in #shipInfo is visible, not hidden when collapsed.
    if (!shipInfo) { const dt = $("loadout").closest("details"); if (dt) dt.open = true; }
  }
}

// Bootstrap "bi-copy" icon + clipboard helper (matches the main EDGIS app).
const copyIconSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" fill="currentColor" viewBox="0 0 16 16"><path fill-rule="evenodd" d="M4 2a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2zm2-1a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V2a1 1 0 0 0-1-1zM2 5a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-1h1v1a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h1v1z"/></svg>`;
// Bootstrap "download" icon.
const downloadIconSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" fill="currentColor" viewBox="0 0 16 16"><path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5"/><path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708z"/></svg>`;
function copyText(text, btn) {
  navigator.clipboard.writeText(text).then(() => {
    const prev = btn.innerHTML; btn.innerHTML = '<span class="good" style="font-size:11px">Copied</span>';
    setTimeout(() => { btn.innerHTML = prev; }, 1200);
  }).catch(() => {});
}
// Bootstrap "fuel-pump" icon (https://icons.getbootstrap.com/icons/fuel-pump/).
const FUEL = `<svg class="fuel" width="12" height="12" viewBox="0 0 16 16" fill="currentColor"><title>Fuel-scoopable (KGBFOAM)</title><path d="M3 2.5a.5.5 0 0 1 .5-.5h5a.5.5 0 0 1 .5.5v5a.5.5 0 0 1-.5.5h-5a.5.5 0 0 1-.5-.5z"/><path d="M1 2a2 2 0 0 1 2-2h6a2 2 0 0 1 2 2v8a2 2 0 0 1 2 2v.5a.5.5 0 0 0 1 0V8h-.5a.5.5 0 0 1-.5-.5V4.375a.5.5 0 0 1 .5-.5h1.495c-.011-.476-.053-.894-.201-1.222a.97.97 0 0 0-.394-.458c-.184-.11-.464-.195-.9-.195a.5.5 0 0 1 0-1q.846-.002 1.412.336c.383.228.634.551.794.907.295.655.294 1.465.294 2.081v3.175a.5.5 0 0 1-.5.501H15v4.5a1.5 1.5 0 0 1-3 0V12a1 1 0 0 0-1-1v4h.5a.5.5 0 0 1 0 1H.5a.5.5 0 0 1 0-1H1zm9 0a1 1 0 0 0-1-1H3a1 1 0 0 0-1 1v13h8z"/></svg>`;

function escapeHtml(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function fmt(n, d = 0) { return Number(n).toLocaleString(undefined, { maximumFractionDigits: d }); }
function scoopable(s) { return /^[KGBFOAM]($| |\()/.test(String(s || "")); }
function specShort(s) { return String(s || "").split(/[\s(]/)[0] || ""; }
function spectralCell(j) {
  if (!j.spectral) return "";
  return `<span class="starcell">${scoopable(j.spectral) ? FUEL : ""}<span class="spec" title="${escapeHtml(j.spectral)}">${escapeHtml(specShort(j.spectral))}</span></span>`;
}
function legLabel(j, last) {
  if (last) return "arrive";
  if (j.type === "boost") return j.neutron_star ? "neutron ★" : "supercharged";
  return j.neutron_star ? "board ★" : "bridge";
}
function copyBtn(name) {
  return name ? ` <button class="copy-btn" data-copy="${escapeHtml(name)}" title="Copy system name">${copyIconSvg}</button>` : "";
}

// Per-row action icons, identical to the neighbors search results on the index.
const systemMapIconSvg = `<svg width="15" height="15" version="1.1" viewBox="0 0 24.77 24.77" xmlns="http://www.w3.org/2000/svg"><path d="m18.976 12.385a6.5907 6.5907 0 0 1-6.5907 6.5907 6.5907 6.5907 0 0 1-6.5907-6.5907 6.5907 6.5907 0 0 1 6.5907-6.5907 6.5907 6.5907 0 0 1 6.5907 6.5907zm5.2941 0a11.885 11.885 0 0 1-11.885 11.885 11.885 11.885 0 0 1-11.885-11.885 11.885 11.885 0 0 1 11.885-11.885 11.885 11.885 0 0 1 11.885 11.885z" fill="none" stroke="currentColor" style="paint-order:stroke markers fill"></path><path d="m14.495 12.305a1.898 1.898 0 0 1-1.898 1.898 1.898 1.898 0 0 1-1.898-1.898 1.898 1.898 0 0 1 1.898-1.898 1.898 1.898 0 0 1 1.898 1.898zm-10.089 6.7178a1.898 1.898 0 0 1-1.898 1.898 1.898 1.898 0 0 1-1.898-1.898 1.898 1.898 0 0 1 1.898-1.898 1.898 1.898 0 0 1 1.898 1.898zm11.794-0.30414a1.898 1.898 0 0 1-1.898 1.898 1.898 1.898 0 0 1-1.898-1.898 1.898 1.898 0 0 1 1.898-1.898 1.898 1.898 0 0 1 1.898 1.898zm-0.25378-12.724a1.898 1.898 0 0 1-1.898 1.898 1.898 1.898 0 0 1-1.898-1.898 1.898 1.898 0 0 1 1.898-1.898 1.898 1.898 0 0 1 1.898 1.898zm7.9976-0.53351a1.898 1.898 0 0 1-1.898 1.898 1.898 1.898 0 0 1-1.898-1.898 1.898 1.898 0 0 1 1.898-1.898 1.898 1.898 0 0 1 1.898 1.898z" fill="currentColor" style="paint-order:stroke markers fill"></path></svg>`;
const galaxyMapIconSvg = `<svg width="15" height="15" version="1.1" viewBox="0 0 6.6237 6.6237" xmlns="http://www.w3.org/2000/svg"><defs><clipPath id="clipPathGalaxyIcon"><path class="powerclip" d="m21.17 19.375h33.981v32.301h-33.981zm20.445 16.684a2.8145 2.8145 0 0 0-2.8145-2.8145 2.8145 2.8145 0 0 0-2.8145 2.8145 2.8145 2.8145 0 0 0 2.8145 2.8145 2.8145 2.8145 0 0 0 2.8145-2.8145z"/></clipPath></defs><path transform="matrix(.26458 0 0 .26458 -6.7848 -6.0877)" d="m38.926 34.569c0.8963 1.4788-2.3745 1.5434-2.5256 3.9958-0.11266 1.7681 0.50361 3.8201 2.3294 5.2908 1.8258 1.4707 8.1248 1.9127 8.1248 1.9127s-4.6011 1.0671-6.5214 0.8882c-1.9203-0.17895-3.6192-0.95569-5.1091-2.1638-1.4899-1.2081-2.6726-3.2119-2.9177-5.0384-0.24511-1.8265 0.16728-3.606 1.4423-4.6802 1.4115-1.2279 4.2811-1.6839 5.1774-0.20507zm0.74206 0.61547c-1.7189 0.18893-0.39785-2.804-2.5577-3.9753-1.5556-0.84792-3.6761-1.1547-5.7796-0.11955-2.1036 1.0351-5.1612 6.5599-5.1612 6.5599s0.97313-4.6219 1.9453-6.2876c0.97222-1.6656 2.3931-2.8784 4.1169-3.7197 1.7238-0.84134 4.0394-1.0685 5.7989-0.52035 1.7595 0.54816 3.199 1.6726 3.6352 3.2818 0.51799 1.7978-0.27898 4.5919-1.9978 4.7808zm-0.51038 0.87536c1.0091-1.4043 2.3507 1.5794 4.666 0.75702 1.6708-0.58947 3.3169-1.9608 3.9542-4.217s-1.4254-8.2243-1.4254-8.2243 2.7854 3.8145 3.3736 5.6513 0.53956 3.7041 0.01221 5.5484c-0.52735 1.8443-1.9071 3.7178-3.4913 4.6593s-3.3829 1.2597-4.871 0.50789c-1.683-0.81718-3.2273-3.2784-2.2183-4.6826z" clip-path="url(#clipPathGalaxyIcon)" fill="currentColor"/></svg>`;
const neighborsIconSvg = `<svg width="15" height="15" viewBox="0 0 39.902 39.902" xmlns="http://www.w3.org/2000/svg"><path d="m14.241 24.519 4.7463-5.6206-4.7463-5.6206 5.6206 4.7463 5.6206-4.7463-4.7463 5.6206 4.7463 5.6206-5.6206-4.7463zm-9.6642 2.7315-0.56969-2.653-2.653-0.56969 2.653-0.56969 0.56969-2.653 0.56969 2.653 2.653 0.56969-2.653 0.56969zm30.748-16.245-0.56969-2.653-2.653-0.56969 2.653-0.56969 0.56969-2.653 0.56969 2.653 2.653 0.56969-2.653 0.56969zm-2.4608 24.336-0.78092-3.6366-3.6366-0.78092 3.6366-0.78092 0.78092-3.6366 0.78092 3.6366 3.6366 0.78092-3.6366 0.78092zm-13.002-2.7759-2.416-11.251-11.251-2.416 11.251-2.416 2.416-11.251 2.416 11.251 11.251 2.416-11.251 2.416z" fill="currentColor" stroke-linecap="round" stroke-width=".25125"/></svg>`;

function actsCell(name, x, y, z) {
  const hasCoords = [x, y, z].every(v => typeof v === "number");
  const n = name ? escapeHtml(name) : "";
  return `<td class="acts">` +
    (n ? `<button class="act-btn" data-act="sysmap" data-name="${n}" title="View system map">${systemMapIconSvg}</button>` : "") +
    (hasCoords ? `<button class="act-btn" data-act="galmap" data-x="${x}" data-y="${y}" data-z="${z}" title="Open galaxy map">${galaxyMapIconSvg}</button>` +
                 `<button class="act-btn" data-act="neighbors" data-x="${x}" data-y="${y}" data-z="${z}" title="Search neighbors within 20 LY">${neighborsIconSvg}</button>` : "") +
    `</td>`;
}

let lastOurs = null, lastQuery = null;
let compactMode = localStorage.getItem("edgis_route_compact") !== "0";  // default compact (like spansh)
let exportFmt = localStorage.getItem("edgis_route_export_fmt") === "csv" ? "csv" : "json";

// Collapse to the systems you actually plot in-game: neutron supercharge stops
// + the destination, with the jump count to reach each (the game auto-plots the
// regular jumps between them).
function compactRows(r) {
  const out = []; let jumps = 0, ly = 0;
  r.jumps.forEach((j, i) => {
    jumps++; ly += j.distance_jumped;
    const last = i === r.jumps.length - 1;
    if (j.neutron_star || last) {
      out.push({ system: j.system ?? (last ? r.destination : "?"), spectral: j.spectral,
                 jumps, ly, left: j.distance_left, neutron: j.neutron_star, dest: last,
                 x: j.x, y: j.y, z: j.z });
      jumps = 0; ly = 0;
    }
  });
  return out;
}
function routeSystems() {
  if (!lastOurs) return [];
  if (compactMode) return compactRows(lastOurs).map(w => w.dest ? lastOurs.destination : w.system).filter(Boolean);
  return lastOurs.jumps.map((j, i) => j.system ?? (i === lastOurs.jumps.length - 1 ? lastOurs.destination : null)).filter(Boolean);
}

// Which access method is currently granting entry, mirroring the backend's
// precedence (public > key > patron). Temporary: returns null once public, so
// the indicator disappears at full release. Patron tier distinguishes the
// future follower / Galactic Hunter waves.
let lastAccess = { st: null, me: null };
function accessMethod() {
  const { st, me } = lastAccess;
  if (st && st.public) return null;            // fully released -> no indicator
  if (($("key").value || "").trim()) return "key";
  if (me && me.patron) {
    const t = String(me.tier || "").toLowerCase();
    if (t.includes("hunter")) return "Galactic Hunter";
    if (t.includes("follower")) return "follower";
    return "Patreon";
  }
  return null;
}
function renderAccessIndicator() {
  const m = accessMethod();
  const ind = $("accessIndicator");
  if (!ind) return;
  if (m) { ind.innerHTML = `<span class="dot"></span>access via <b>${escapeHtml(m)}</b>`; ind.hidden = false; }
  else { ind.hidden = true; }
}

// Plot button "warming up" state while the route index builds: spin the logo,
// disable the button, and poll /route/status until ready -- the poll is the
// "event" that reactivates the button.
let indexPollTimer = null;
function setPlotWarming(on) {
  const go = $("go");
  if (!go) return;
  go.classList.toggle("warming", on);
  go.disabled = on;
  const label = go.querySelector(".go-label");
  if (label) label.textContent = on ? "Warming up…" : "Plot route";
  if (on) pollIndexReady();
}
function pollIndexReady() {
  if (indexPollTimer) return;  // already polling
  const check = async () => {
    indexPollTimer = null;
    let ready = false;
    try { ready = !!(await fetch("/route/status").then(r => r.json())).index_ready; } catch (e) {}
    if (ready) { setPlotWarming(false); loadStatus(); } else indexPollTimer = setTimeout(check, 3000);
  };
  indexPollTimer = setTimeout(check, 3000);
}

async function loadStatus() {
  try {
    const [st, me] = await Promise.all([
      fetch("/route/status").then(r => r.json()),
      fetch("/auth/me", { credentials: "include" }).then(r => r.json()).catch(() => ({})),
    ]);
    lastAccess = { st, me };
    renderAccessIndicator();
    setPlotWarming(!st.index_ready);
    const pill = $("accessPill");   // removed from the UI; guard if absent
    if (pill) {
      if (st.public) { pill.textContent = "public"; pill.className = "pill ok"; }
      else if (me && me.patron) { pill.textContent = "patron ✓"; pill.className = "pill ok"; }
      else { pill.textContent = "early access"; pill.className = "pill warn"; }
    }
    const parts = [];
    if (!st.public) {
      parts.push(`<b>Early access, rolling out in waves:</b> key holders now · Patreon tier 1 from <b>7 Nov</b> · free followers and Galactic Hunters (5+ confirmed submissions) from <b>13 Nov</b> · everyone from <b>${st.public_after || "30 Nov"}</b>.`);
      if (!(me && me.patron)) parts.push(`Become a patron on <a href="${st.patreon}" target="_blank" rel="noopener">Patreon</a>, or use the access key below.`);
      else parts.push(`Signed in as <b>${escapeHtml(me.name || "patron")}</b>.`);
    }
    if (!st.index_ready) parts.push(`<span class="pill warn">index warming up…</span>`);
    $("bannerMsg").innerHTML = parts.join(" ");
    // Collapse the banner (with its key field) once a key is set or it's public.
    const hasKey = !!($("key").value || "").trim();
    $("banner").hidden = parts.length === 0 || hasKey;
  } catch (e) {}
}

let acTimer;
async function autocomplete(ev) {
  clearTimeout(acTimer);
  const term = ev.target.value.trim();
  if (term.length < 2) return;
  acTimer = setTimeout(async () => {
    try {
      const r = await fetch("/systems/autocomplete?q=" + encodeURIComponent(term));
      const data = await r.json();
      const names = Array.isArray(data?.suggestions) ? data.suggestions : [];
      $("sysopts").innerHTML = names.slice(0, 10).map(n => `<option value="${escapeHtml(n)}"></option>`).join("");
    } catch (e) {}
  }, 150);
}
$("from").addEventListener("input", autocomplete);
$("to").addEventListener("input", autocomplete);

function fuelVerdict(f) {
  const tank = `tank ≈ ${f.tank_jumps} jumps`;
  const added = f.scoop_stops ? ` · ${f.scoop_stops} scoop stop${f.scoop_stops === 1 ? "" : "s"} added` : "";
  if (f.stranded) {
    return `<span class="bad">⚠ Fuel: may strand near jump ${f.stranded_at + 1}</span> `
      + `<span class="tiny">no scoopable star within range (${tank})`
      + `${f.enforced ? " — couldn't insert a refuel detour there (sparse region)" : ""}.</span>`;
  }
  if (!f.safe) {
    return `<span class="bad">⚠ Fuel: tight</span> `
      + `<span class="tiny">${f.max_dry_jumps} jumps between scoopable stars, ${tank}${added} · min fuel ${fmt(f.min_fuel, 1)} t.</span>`;
  }
  return `<span class="good">✓ Fuel-safe</span> `
    + `<span class="tiny">longest dry run ${f.max_dry_jumps} jumps (${tank})${added} · min fuel ${fmt(f.min_fuel, 1)} t.</span>`;
}

// Convert our route to spansh's neutron-plotter result JSON so it imports into
// tools that read spansh routes. system_jumps = source first (jumps 0), then
// each neutron supercharge stop + the destination, with `jumps` = hops to reach
// it (same grouping as the compact view). total_jumps excludes the source.
function toSpanshRoute(r) {
  const r2 = (n) => Math.round(Number(n || 0) * 100) / 100;
  const sc = r.source_coords || {};
  const wps = [{
    system: r.source, id64: null,
    x: sc.x ?? null, y: sc.y ?? null, z: sc.z ?? null,
    distance_jumped: 0, distance_left: r2(r.straight_line_ly), jumps: 0, neutron_star: false,
  }];
  let jumps = 0, ly = 0;
  r.jumps.forEach((j, i) => {
    jumps++; ly += j.distance_jumped;
    const last = i === r.jumps.length - 1;
    if (j.neutron_star || last) {
      wps.push({
        system: j.system ?? (last ? r.destination : ""),
        id64: j.id64 ?? null, x: j.x, y: j.y, z: j.z,
        distance_jumped: r2(ly), distance_left: r2(j.distance_left),
        jumps, neutron_star: !!j.neutron_star,
      });
      jumps = 0; ly = 0;
    }
  });
  return { result: {
    source_system: r.source, destination_system: r.destination,
    distance: r2(r.straight_line_ly), range: r.jump_range, efficiency: 60,
    total_jumps: wps.length - 1, system_jumps: wps,
  } };
}

// Full per-jump route as CSV (every leg, source first), for spreadsheets. Not
// compacted like the view: gives all legs with a neutron/boost flag to filter.
function toCsvRoute(r) {
  const r2 = (n) => Math.round(Number(n || 0) * 100) / 100;
  const esc = (v) => {
    const s = String(v == null ? "" : v);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const sc = r.source_coords || {};
  const header = ["#", "system", "star", "neutron", "type", "jumped_ly", "left_ly", "x", "y", "z"];
  const rows = [[1, r.source, "", "no", "source", 0, r2(r.straight_line_ly), sc.x ?? "", sc.y ?? "", sc.z ?? ""]];
  r.jumps.forEach((j, i) => {
    const last = i === r.jumps.length - 1;
    const star = j.spectral || (j.neutron_star ? "Neutron Star" : "");
    rows.push([i + 2, j.system ?? (last ? r.destination : ""), star,
      j.neutron_star ? "yes" : "no", j.type || "regular",
      r2(j.distance_jumped), r2(j.distance_left), j.x ?? "", j.y ?? "", j.z ?? ""]);
  });
  return [header, ...rows].map((row) => row.map(esc).join(",")).join("\r\n");
}

function downloadRoute(fmt) {
  if (!lastOurs) return;
  const slug = (s) => String(s || "route").trim().replace(/[^\w.-]+/g, "_");
  const name = `route_${slug(lastOurs.source)}_to_${slug(lastOurs.destination)}`;
  const csv = fmt === "csv";
  const data = csv ? toCsvRoute(lastOurs) : JSON.stringify(toSpanshRoute(lastOurs), null, 2);
  const blob = new Blob([data], { type: csv ? "text/csv;charset=utf-8" : "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = `${name}.${csv ? "csv" : "json"}`;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function renderRoute(r) {
  lastOurs = r;
  let header, rows, legend;
  if (compactMode) {
    const wps = compactRows(r);
    header = `<th class="num">#</th><th>system</th><th>star</th><th class="num">jumps</th><th class="num">LY</th><th class="num">left</th><th class="acts-head">actions</th>`;
    rows = wps.map((w, i) => `<tr>
      <td class="num">${i + 1}</td>
      <td class="sys ${w.neutron ? "boost" : ""}">${escapeHtml(w.system)}${copyBtn(w.dest ? r.destination : w.system)}</td>
      <td>${spectralCell({ spectral: w.spectral, neutron_star: w.neutron })}</td>
      <td class="num">${fmt(w.jumps)}</td>
      <td class="num">${fmt(w.ly, 0)}</td>
      <td class="num">${fmt(w.left, 0)}</td>
      ${actsCell(w.dest ? r.destination : w.system, w.x, w.y, w.z)}
    </tr>`).join("");
    legend = "";
  } else {
    header = `<th class="num">#</th><th>system</th><th>star</th><th class="num">jumped</th><th class="num">left</th><th class="acts-head">actions</th>`;
    rows = r.jumps.map((j, i) => {
      const last = i === r.jumps.length - 1;
      const name = j.system ?? (last ? r.destination : "?");
      return `<tr>
      <td class="num">${i + 1}</td>
      <td class="sys">${escapeHtml(name)}${copyBtn(j.system ?? r.destination)}</td>
      <td>${spectralCell(j)}</td>
      <td class="num">${fmt(j.distance_jumped, 1)}</td>
      <td class="num">${fmt(j.distance_left, 0)}</td>
      ${actsCell(j.system ?? (last ? r.destination : null), j.x, j.y, j.z)}
    </tr>`;
    }).join("");
    legend = "";
  }
  $("out").innerHTML = `
    <div class="summary">
      <div class="stat"><b>${fmt(r.total_jumps)}</b><span>jumps</span></div>
      <div class="stat"><b>${fmt(r.neutron_jumps)}</b><span>neutron boosts</span></div>
      <div class="stat"><b>${fmt(r.bridge_jumps)}</b><span>regular jumps</span></div>
      <div class="stat"><b>${fmt(r.total_distance_ly)}</b><span>LY flown</span></div>
      <div class="stat"><b>${fmt(r.efficiency * 100, 1)}%</b><span>efficiency</span></div>
      <div class="stat"><b>${fmt(r.solve_ms, 0)} ms</b><span>solve</span></div>
    </div>
    ${r.fuel ? `<div class="fuelbar">${fuelVerdict(r.fuel)}</div>` : ""}
    <div id="cmpOut"></div>
    ${legend ? `<p class="legend">${legend}</p>` : ""}
    <div class="summary-actions">
      <label class="switch" title="Compact: only the waypoints you plot in-game. Off: every jump.">
        <input type="checkbox" id="compactToggle" ${compactMode ? "checked" : ""} />
        <span class="track"></span><span>compact</span>
      </label>
      <span class="export-group">
        <select id="exportFmt" title="Export format" aria-label="Export format">
          <option value="json"${exportFmt === "json" ? " selected" : ""}>spansh JSON</option>
          <option value="csv"${exportFmt === "csv" ? " selected" : ""}>CSV</option>
        </select>
        <button id="exportBtn" class="ghost" title="Download this route in the selected format">${downloadIconSvg} Export</button>
      </span>
      <button id="copyRoute" class="ghost" title="Copy the route systems, one per line">${copyIconSvg} Copy route</button>
    </div>
    <div class="tablewrap result-table">
      <table><thead><tr>${header}</tr></thead>
      <tbody>${rows}</tbody></table>
    </div>`;
  $("cmp").disabled = false; $("cmp").title = "Run the same query on spansh";
  if (shipInfo) { $("cmpGalaxy").disabled = false; }
  // Trigger the neighbors-style refresh glow on the freshly rendered table.
  const tbl = $("out").querySelector(".tablewrap.result-table");
  if (tbl) {
    tbl.classList.remove("is-refreshing");
    void tbl.offsetWidth;
    tbl.classList.add("is-refreshing");
    setTimeout(() => tbl.classList.remove("is-refreshing"), 500);
  }
}

// A dedicated line per spansh tool inside #cmpOut, so the Neutron and Galaxy
// comparisons can show at once without overwriting each other.
function cmpLine(id) {
  let host = document.getElementById("cmpOut");
  if (!host) { host = document.createElement("div"); host.id = "cmpOut"; $("out").appendChild(host); }
  let el = document.getElementById(id);
  if (!el) { el = document.createElement("div"); el.id = id; host.appendChild(el); }
  return el;
}

// Shared bits for a "spansh X: N jumps · ours: M · verdict · ⏱ …" line.
function cmpVerdict(spJumps) {
  const ours = lastOurs ? lastOurs.total_jumps : null;
  if (ours == null) return "";
  const d = spJumps - ours;
  const v = d > 0 ? `<span class="good">ours ${d} fewer ✓</span>`
          : d < 0 ? `<span class="bad">spansh ${-d} fewer</span>` : "tie";
  return ` · <b>ours:</b> ${fmt(ours)} jumps · ${v}`;
}
function cmpTime(durationS) {
  if (durationS == null || !lastOurs) return "";
  const spMs = durationS * 1000, ourMs = lastOurs.solve_ms, dMs = spMs - ourMs;
  const ft = (ms) => ms >= 1000 ? `${fmt(ms / 1000, 1)} s` : `${fmt(ms)} ms`;
  return ` · ⏱ ${ft(ourMs)} vs ${ft(spMs)} <span class="${dMs >= 0 ? "good" : "bad"}">(${ft(Math.abs(dMs))} ${dMs >= 0 ? "faster" : "slower"})</span>`;
}

async function compareSpansh() {
  if (!lastQuery) return;
  const { from, to, range, supercharge } = lastQuery;
  $("cmp").disabled = true; setBusy(true);
  const out = () => cmpLine("cmpNeutron");
  out().innerHTML = `<span class="tiny">Querying spansh… (can take ~10–30 s)</span>`;
  try {
    const res = await fetch(`/spansh/route?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&range=${encodeURIComponent(range)}&efficiency=60&supercharge=${encodeURIComponent(supercharge || 4)}`, { headers: headers(), credentials: "include" });
    if (!res.ok) { if (out()) out().innerHTML = `<span class="bad">spansh compare failed (HTTP ${res.status})</span>`; return; }
    const s = await res.json();
    const sDur = s.duration_s != null ? `, ${fmt(s.duration_s, 1)} s` : "";
    const ours = lastOurs ? lastOurs.total_jumps : null;
    let verdict = "";
    if (ours != null) { const d = s.total_jumps - ours;
      verdict = d > 0 ? `<span class="good">ours ${d} fewer ✓</span>` : d < 0 ? `<span class="bad">spansh ${-d} fewer</span>` : "tie"; }
    // Time comparison: spansh's job duration vs our solve time.
    let timeStr = "";
    if (s.duration_s != null && lastOurs) {
      const spMs = s.duration_s * 1000, ourMs = lastOurs.solve_ms, dMs = spMs - ourMs;
      const ft = (ms) => ms >= 1000 ? `${fmt(ms / 1000, 1)} s` : `${fmt(ms)} ms`;
      timeStr = ` · ⏱ ${ft(ourMs)} vs ${ft(spMs)} <span class="${dMs >= 0 ? "good" : "bad"}">(${ft(Math.abs(dMs))} ${dMs >= 0 ? "faster" : "slower"})</span>`;
    }
    const link = s.results_url ? ` · <a href="${escapeHtml(s.results_url)}" target="_blank" rel="noopener">open in spansh ↗</a>` : "";
    // Both sides use the selected supercharge; label it so the compare is
    // unambiguously apples-to-apples (e.g. Caspian 6x vs spansh at 6x).
    const sc = Number(supercharge || 4);
    const modeTag = sc !== 4 ? `<span class="tiny">supercharge ${sc}x</span> · ` : "";
    // Our solve time is already in the "solve" stat above; show only spansh's count here.
    if (out()) out().innerHTML = modeTag + `<b>spansh Neutron&nbsp;Plotter:</b> ${fmt(s.total_jumps)} jumps (eff 60${sDur})`
      + (ours != null ? ` · <b>ours:</b> ${fmt(ours)} jumps · ${verdict}` : "")
      + timeStr + link;
  } catch (e) { if (out()) out().innerHTML = `<span class="bad">${e.message || e}</span>`; }
  finally { $("cmp").disabled = false; setBusy(false); }
}

// Compare against spansh's fuel-aware Galaxy (exact) Plotter. Needs the ship
// build, so it's only offered once a loadout is imported.
async function compareGalaxy() {
  if (!lastQuery) return;
  const loadout = shipInfo ? ($("loadout").value || "").trim() : "";
  const out = () => cmpLine("cmpGalaxy_line");
  if (!loadout) { out().innerHTML = `<span class="bad">Import a ship first (the Galaxy Plotter is fuel-aware).</span>`; return; }
  const { from, to } = lastQuery;
  $("cmpGalaxy").disabled = true; setBusy(true);
  out().innerHTML = `<span class="tiny">Querying spansh Galaxy Plotter… (can take ~1 min)</span>`;
  try {
    const res = await fetch("/spansh/galaxy-route", {
      method: "POST", credentials: "include",
      headers: { ...headers(), "Content-Type": "application/json" },
      body: JSON.stringify({ from, to, loadout, cargo: cargoTons() }),
    });
    if (!res.ok) {
      const d = (await res.json().catch(() => ({}))).detail;
      out().innerHTML = `<span class="bad">Galaxy Plotter failed: ${escapeHtml(typeof d === "string" ? d : ("HTTP " + res.status))}</span>`;
      return;
    }
    const s = await res.json();
    const dur = s.duration_s != null ? ` (${fmt(s.duration_s, 1)} s)` : "";
    const link = s.results_url ? ` · <a href="${escapeHtml(s.results_url)}" target="_blank" rel="noopener">open in spansh ↗</a>` : "";
    out().innerHTML = `<b>spansh Galaxy&nbsp;Plotter:</b> ${fmt(s.total_jumps)} jumps${dur}`
      + cmpVerdict(s.total_jumps) + cmpTime(s.duration_s) + link;
  } catch (e) { out().innerHTML = `<span class="bad">${escapeHtml(e.message || String(e))}</span>`; }
  finally { $("cmpGalaxy").disabled = false; setBusy(false); }
}

async function plot() {
  const from = $("from").value.trim(), to = $("to").value.trim();
  if (!from || !to) { $("out").innerHTML = `<div class="msg warn">Enter both a start and destination system.</div>`; return; }
  const range = $("range").value || "50";
  const supercharge = $("ship").value || "4";
  // Fuel-aware plotting (POST with the loadout) kicks in once a ship is imported.
  const loadout = shipInfo ? ($("loadout").value || "").trim() : "";
  lastQuery = { from, to, range, supercharge };
  lastOurs = null;  // so a post-failure spansh compare doesn't show a stale "ours"
  $("go").disabled = true; setBusy(true); $("out").innerHTML = `<div class="msg">Plotting…</div>`;
  try {
    let res;
    if (loadout) {
      res = await fetch("/route", {
        method: "POST", credentials: "include",
        headers: { ...headers(), "Content-Type": "application/json" },
        body: JSON.stringify({
          from, to, range: Number(range), supercharge: Number(supercharge),
          loadout, cargo: cargoTons(), fuel_safe: $("fuelSafe").checked,
        }),
      });
    } else {
      const url = `/route?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&range=${encodeURIComponent(range)}&supercharge=${encodeURIComponent(supercharge)}`;
      res = await fetch(url, { headers: headers(), credentials: "include" });
    }
    if (res.status === 403) {
      const d = (await res.json()).detail || {};
      // If the backend recognises the caller and their wave just hasn't opened
      // yet, it returns a personalized date; otherwise show the full schedule.
      const schedule = d.your_access_after
        ? `Your access opens on <b>${d.your_access_after}</b>.`
        : `Rolling out in waves (Patreon tier 1 from 7 Nov, free followers and Galactic Hunters from 13 Nov, everyone from <b>${d.public_after || "30 Nov"}</b>).`;
      $("out").innerHTML = `<div class="msg warn"><b>Early access.</b> ${escapeHtml(d.message || "")} ${schedule} ${d.patreon ? `Join on <a href="${d.patreon}" target="_blank" rel="noopener">Patreon</a>` : ""} or add an access key above.</div>`;
      return;
    }
    if (res.status === 503) { $("out").innerHTML = `<div class="msg warn">Route index is warming up — the Plot button re-enables automatically when it's ready.</div>`; setPlotWarming(true); return; }
    if (res.status === 404) { $("out").innerHTML = `<div class="msg err">${escapeHtml((await res.json()).detail || "System not found.")}</div>`; return; }
    if (res.status === 422) { const d = (await res.json()).detail || {}; $("out").innerHTML = `<div class="msg err">No route: ${escapeHtml(d.error || "")} ${escapeHtml(d.message || "")}</div>`; return; }
    if (!res.ok) { $("out").innerHTML = `<div class="msg err">Error ${res.status}.</div>`; return; }
    renderRoute(await res.json());
  } catch (e) { $("out").innerHTML = `<div class="msg err">${escapeHtml(e.message || String(e))}</div>`; }
  finally {
    // Keep the button disabled if we entered the warming state (503).
    if (!$("go").classList.contains("warming")) $("go").disabled = false;
    setBusy(false);
    // Allow comparing against spansh after any attempt, including when EDGIS
    // couldn't find a route (spansh often can).
    if (lastQuery) { $("cmp").disabled = false; $("cmp").title = "Query spansh for this route";
      if (shipInfo) $("cmpGalaxy").disabled = false; }
    loadStatus();
  }
}

// Delegated clicks: per-row copy, row actions, copy-all, and the compact toggle.
$("out").addEventListener("click", (e) => {
  const cb = e.target.closest(".copy-btn");
  if (cb) { copyText(cb.dataset.copy, cb); return; }
  const act = e.target.closest(".act-btn");
  if (act) {
    const d = act.dataset;
    if (d.act === "sysmap") window.open(`/static/sysmap.html?system=${encodeURIComponent(d.name)}`, "_blank");
    else if (d.act === "galmap") window.open(`/static/galaxymap.html?x=${d.x}&y=${d.y}&z=${d.z}&radius=20`, "_blank");
    else if (d.act === "neighbors") window.open(`/?neighbors=${d.x},${d.y},${d.z},20`, "_blank");
    return;
  }
  const copyAll = e.target.closest("#copyRoute");
  if (copyAll && lastOurs) { copyText(routeSystems().join("\n"), copyAll); return; }
  const exportBtn = e.target.closest("#exportBtn");
  if (exportBtn && lastOurs) { downloadRoute(exportFmt); }
});
// The compact switch and export-format picker both change on input.
$("out").addEventListener("change", (e) => {
  const toggle = e.target.closest("#compactToggle");
  if (toggle && lastOurs) {
    compactMode = toggle.checked;
    localStorage.setItem("edgis_route_compact", compactMode ? "1" : "0");
    renderRoute(lastOurs);
    return;
  }
  const fmt = e.target.closest("#exportFmt");
  if (fmt) {
    exportFmt = fmt.value === "csv" ? "csv" : "json";
    localStorage.setItem("edgis_route_export_fmt", exportFmt);
  }
});

$("go").addEventListener("click", plot);
$("cmp").addEventListener("click", compareSpansh);
$("cmpGalaxy").addEventListener("click", compareGalaxy);
$("importShip").addEventListener("click", importShip);
[$("from"), $("to"), $("range")].forEach(el => el.addEventListener("keydown", e => { if (e.key === "Enter") plot(); }));
// Lazy init: defer the network calls (/route/status, /auth/me, remembered-ship
// /route/ship) and URL-param prefill until the Route tab is first opened, so
// they don't fire on every index page load. The index calls this when the route
// panel activates (and on load if ?tool=route).
let _routeInited = false;
window.edgisInitRoutePanel = function () {
  if (_routeInited) return;
  _routeInited = true;
  const q = new URLSearchParams(location.search);
  const from = q.get("from"); if (from) $("from").value = from;
  const to = q.get("to"); if (to) $("to").value = to;
  const cargo = q.get("cargo");
  if (cargo != null && cargo !== "") { $("cargo").value = cargo; localStorage.setItem(CARGO_LS, String(cargoTons())); }
  if ($("loadout").value.trim()) { importShip(); }
  loadStatus();
};
})();
