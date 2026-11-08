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

// Ship library: multiple stored SLEF loadouts the commander can switch between.
// Each entry is { name, loadout }; the active one's loadout also lives in
// SLEF_LS (the textarea) for the single-ship import flow.
const SHIPS_LS = "edgis_route_ships";
const ACTIVE_SHIP_LS = "edgis_route_active_ship";
function loadShips() { try { return JSON.parse(localStorage.getItem(SHIPS_LS)) || []; } catch (e) { return []; } }
function saveShips(a) { localStorage.setItem(SHIPS_LS, JSON.stringify(a)); }
function activeShip() { const i = Number(localStorage.getItem(ACTIVE_SHIP_LS)); return Number.isInteger(i) ? i : -1; }
function setActiveShip(i) { localStorage.setItem(ACTIVE_SHIP_LS, String(i)); }
// Upsert a loadout (by label or identical text), make it active, return index.
function rememberShip(loadout) {
  const ships = loadShips();
  const name = shipLabel(loadout);
  let i = ships.findIndex(s => s.loadout === loadout || s.name === name);
  if (i < 0) { ships.push({ name, loadout }); i = ships.length - 1; }
  else { ships[i] = { name, loadout }; }
  saveShips(ships); setActiveShip(i); renderShipLibrary();
  return i;
}
function renderShipLibrary() {
  const el = $("shipLibrary"); if (!el) return;
  const ships = loadShips(); const active = activeShip();
  if (!ships.length) { el.hidden = true; el.innerHTML = ""; return; }
  el.hidden = false;
  el.innerHTML = ships.map((s, i) =>
    `<span class="ship-item ${i === active ? "active" : ""}">`
    + `<button type="button" class="ship-pick" data-i="${i}" title="Switch to this ship">${escapeHtml(s.name)}</button>`
    + `<button type="button" class="ship-del" data-i="${i}" title="Remove this ship">&times;</button>`
    + `</span>`).join("");
}
// Remove every stored ship and return to fuel-agnostic plotting. Confirms first
// since it wipes the whole library and can't be undone.
function clearAllShips() {
  const n = loadShips().length;
  if (!n) return;  // nothing to clear
  if (!confirm(`Remove all ${n} stored ship${n > 1 ? "s" : ""}? This cannot be undone.`)) return;
  saveShips([]); setActiveShip(-1); clearShip();
}

setShipSummary();  // reflect a restored (saved, not-yet-imported) loadout at once
renderShipLibrary();
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

// Opt-in companion (secondary) neutrons within N Ls of arrival (0 = off).
const COMPANION_LS = "edgis_route_companion_ls";
$("companionLs").value = localStorage.getItem(COMPANION_LS) || "0";
const companionMaxLs = () =>
  Math.min(2000, Math.max(0, Number($("companionLs").value) || 0));
$("companionLs").addEventListener("change", () =>
  localStorage.setItem(COMPANION_LS, String(companionMaxLs())));

// Opt-in FSD synthesis injection, used "when required" to cross gaps a normal
// jump can't reach: 1 = off, 1.25/1.5/2 = basic/standard/premium.
const INJECT_MULT = "edgis_route_inject";
$("injectMult").value = localStorage.getItem(INJECT_MULT) || "1";
const injectMult = () => {
  const v = Number($("injectMult").value) || 1;
  return v >= 1 && v <= 2 ? v : 1;
};
$("injectMult").addEventListener("change", () =>
  localStorage.setItem(INJECT_MULT, String(injectMult())));

// Friendly ship label from a SLEF loadout, shown on the Ship & fuel summary so
// it's visible even when collapsed. Prefers the commander's ship name. The SLEF
// `Ship` field is the FDev internal symbol (e.g. "explorer_nx"); this maps it to
// the display name. Source: EDHM-UI Ship_List.json (ed_short -> name), Mk-numbers
// canonicalised to Roman. New ships need adding here as FDev ships them; the
// canonical upstream to sync from is EDCD FDevIDs ship.csv.
const SHIP_TYPES = {
  adder: "Adder", anaconda: "Anaconda", anacondamkii: "Anaconda Mk II",
  asp: "Asp Explorer", asp_scout: "Asp Scout", belugaliner: "Beluga Liner",
  cobramkiii: "Cobra Mk III", cobramkiv: "Cobra Mk IV", cobramkv: "Cobra Mk V",
  combat_multicrew_srv_01: "SRV Scorpion", corsair: "Corsair", cutter: "Imperial Cutter",
  diamondback: "Diamondback Scout", diamondbackxl: "Diamondback Explorer", dolphin: "Dolphin",
  eagle: "Eagle Mk II", empire_courier: "Imperial Courier", empire_eagle: "Imperial Eagle",
  empire_trader: "Imperial Clipper", explorer_nx: "Caspian Explorer", federation_corvette: "Federal Corvette",
  federation_dropship: "Federal Dropship", federation_dropship_mkii: "Federal Assault Ship", federation_gunship: "Federal Gunship",
  ferdelance: "Fer-de-lance", hauler: "Hauler", independant_trader: "Keelback",
  krait_light: "Krait Phantom", krait_mkii: "Krait Mk II", lakonminer: "Type 11 Prospector",
  mamba: "Mamba", mandalay: "Mandalay", mediumtransport01: "Lynx Highliner",
  mev_rhino: "SRV Rhino", orca: "Orca", panthermkii: "Panther Clipper Mk II",
  python: "Python", python_nx: "Python Mk II", sidewinder: "Sidewinder",
  smallcombat01_nx: "Kestrel Mk II", testbuggy: "SRV Scarab", type6: "Type 6 Transporter",
  type7: "Type 7 Transporter", type8: "Type 8 Transporter", type9: "Type 9 Heavy",
  type9_military: "Type 10 Defender", typex: "Alliance Chieftain", typex_2: "Alliance Crusader",
  typex_3: "Alliance Challenger", viper: "Viper Mk III", viper_mkiv: "Viper Mk IV",
  vulture: "Vulture",
};
// Neutron supercharge multiplier for a loadout: 6x only on the Caspian Explorer
// (explorer_nx) fitted with the Mk II overcharge-booster SCO drive; 4x for every
// other ship/FSD. The MKII drive currently exists only as 8A on the Caspian, but
// require BOTH so a future MKII variant on another hull does not wrongly get 6x.
function detectSupercharge(loadout) {
  try {
    let d = JSON.parse(loadout);
    if (Array.isArray(d)) d = d[0];
    d = (d && d.data) || d;
    const ship = String(d.Ship || "").toLowerCase();
    const fsd = (d.Modules || []).find(m => m.Slot === "FrameShiftDrive");
    const item = String((fsd && fsd.Item) || "").toLowerCase();
    return (ship === "explorer_nx" && item.includes("overchargebooster_mkii")) ? "6" : "4";
  } catch (e) { return "4"; }
}
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
    el.innerHTML = `Ship &amp; options <span class="good">✓</span> · `
      + `${escapeHtml(shipLabel($("loadout").value))} · ${fmt(shipInfo.jump_range, 2)} LY`;
  } else if (hasText) {
    el.innerHTML = `Ship &amp; options · ${escapeHtml(shipLabel($("loadout").value))} `
      + `<span class="tiny">(saved, not added)</span>`;
  } else {
    el.textContent = "Ship & options";
  }
}

async function importShip() {
  const loadout = ($("loadout").value || "").trim();
  if (!loadout) { shipInfo = null; localStorage.removeItem(SLEF_LS); $("shipInfo").textContent = ""; $("cmpGalaxy").hidden = true; setShipSummary(); return; }
  $("importShip").disabled = true;
  $("shipInfo").innerHTML = `<span class="tiny">Reading loadout…</span>`;
  $("shipSummary").innerHTML = `Ship &amp; options · <span class="tiny">reading loadout…</span>`;
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
    rememberShip(loadout);  // add/update this ship in the library + make it active
    $("range").value = shipInfo.jump_range;
    localStorage.setItem(RANGE_LS, String(shipInfo.jump_range));
    // Auto-set the neutron supercharge for the imported ship (6x only on the
    // Caspian Explorer's Mk II SCO drive, 4x otherwise), so swapping ships does
    // not silently keep a stale 6x (tester got caught by this with a Mandalay).
    const sc = detectSupercharge(loadout);
    $("ship").value = sc;
    localStorage.setItem(SHIP_LS, sc);
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

// Deselect the active ship: clears the loadout textarea and returns to
// fuel-agnostic plotting, but keeps the ships in the library. Also used by
// clearAllShips (after the list has been emptied) to reset the active state.
function clearShip() {
  shipInfo = null;
  $("loadout").value = "";
  localStorage.removeItem(SLEF_LS);
  $("shipInfo").textContent = "";
  $("cmpGalaxy").hidden = true;
  setActiveShip(-1);
  setShipSummary();
  renderShipLibrary();
}

// Bootstrap "bi-copy" icon + clipboard helper (matches the main EDGIS app).
const copyIconSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" fill="currentColor" viewBox="0 0 16 16"><path fill-rule="evenodd" d="M4 2a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2zm2-1a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V2a1 1 0 0 0-1-1zM2 5a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-1h1v1a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h1v1z"/></svg>`;
// Bootstrap "download" icon.
const downloadIconSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" fill="currentColor" viewBox="0 0 16 16"><path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5"/><path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708z"/></svg>`;
// Bootstrap "link-45deg" -- share: copies a deep link that reproduces the route.
const shareIconSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" fill="currentColor" viewBox="0 0 16 16"><path d="M4.715 6.542 3.343 7.914a3 3 0 1 0 4.243 4.243l1.828-1.829A3 3 0 0 0 8.586 5.5L8 6.086a1 1 0 0 0-.154.199 2 2 0 0 1 .861 3.337L6.88 11.45a2 2 0 1 1-2.83-2.83l.793-.792a4 4 0 0 1-.128-1.287z"/><path d="M6.586 4.672A3 3 0 0 0 7.414 9.5l.775-.776a2 2 0 0 1-.896-3.346L9.12 3.55a2 2 0 1 1 2.83 2.83l-.793.792c.112.42.155.855.128 1.287l1.372-1.372a3 3 0 1 0-4.243-4.243z"/></svg>`;
// Bootstrap "exclamation-triangle-fill" -- marks a leg that puts the CMDR at
// fuel risk (strand / over-range) directly in the results table.
const warnIconSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="currentColor" viewBox="0 0 16 16"><path d="M8.982 1.566a1.13 1.13 0 0 0-1.96 0L.165 13.233c-.457.778.091 1.767.98 1.767h13.713c.889 0 1.438-.99.98-1.767zM8 5c.535 0 .954.462.9.995l-.35 3.507a.552.552 0 0 1-1.1 0L7.1 5.995A.905.905 0 0 1 8 5m.002 6a1 1 0 1 1 0 2 1 1 0 0 1 0-2"/></svg>`;
function copyText(text, btn) {
  navigator.clipboard.writeText(text).then(() => {
    const prev = btn.innerHTML; btn.innerHTML = '<span class="good" style="font-size:11px">Copied</span>';
    setTimeout(() => { btn.innerHTML = prev; }, 1200);
  }).catch(() => {});
}
// Bootstrap "fuel-pump" icon (https://icons.getbootstrap.com/icons/fuel-pump/).
const FUEL = `<svg class="fuel" width="12" height="12" viewBox="0 0 16 16" fill="currentColor"><title>Fuel-scoopable (KGBFOAM)</title><path d="M3 2.5a.5.5 0 0 1 .5-.5h5a.5.5 0 0 1 .5.5v5a.5.5 0 0 1-.5.5h-5a.5.5 0 0 1-.5-.5z"/><path d="M1 2a2 2 0 0 1 2-2h6a2 2 0 0 1 2 2v8a2 2 0 0 1 2 2v.5a.5.5 0 0 0 1 0V8h-.5a.5.5 0 0 1-.5-.5V4.375a.5.5 0 0 1 .5-.5h1.495c-.011-.476-.053-.894-.201-1.222a.97.97 0 0 0-.394-.458c-.184-.11-.464-.195-.9-.195a.5.5 0 0 1 0-1q.846-.002 1.412.336c.383.228.634.551.794.907.295.655.294 1.465.294 2.081v3.175a.5.5 0 0 1-.5.501H15v4.5a1.5 1.5 0 0 1-3 0V12a1 1 0 0 0-1-1v4h.5a.5.5 0 0 1 0 1H.5a.5.5 0 0 1 0-1H1zm9 0a1 1 0 0 0-1-1H3a1 1 0 0 0-1 1v13h8z"/></svg>`;

// FSD synthesis injection grade icons: an upward delta with a dot inside and
// 1/2/3 horizontal lines below = basic/standard/premium (the in-game synthesis
// motif). currentColor so CSS tints them.
const _injSvg = (nLines) => {
  const ys = { 1: [12.2], 2: [11.4, 13.4], 3: [10.6, 12.4, 14.2] }[nLines];
  const lines = ys.map(y => `<path d="M4.5 ${y}h7" stroke-linecap="round"/>`).join("");
  return `<svg class="inject" width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linejoin="round"><path d="M8 1.6 13.8 9H2.2Z"/><circle cx="8" cy="6.2" r="1" fill="currentColor" stroke="none"/>${lines}</svg>`;
};
const INJECT_ICONS = { "1.25": _injSvg(1), "1.5": _injSvg(2), "2": _injSvg(3) };
const INJECT_NAMES = { "1.25": "Basic", "1.5": "Standard", "2": "Premium" };
// FSD Injection synthesis recipes (materials per craft; each craft = one jump).
// Confirmed against the ED synthesis tables (Inara / Fandom wiki). All current
// quantities are 1 each.
const INJECT_RECIPES = {
  "1.25": { Carbon: 1, Vanadium: 1, Germanium: 1 },
  "1.5": { Carbon: 1, Vanadium: 1, Germanium: 1, Cadmium: 1, Niobium: 1 },
  "2": { Carbon: 1, Germanium: 1, Arsenic: 1, Niobium: 1, Yttrium: 1, Polonium: 1 },
};
// A leg's grade is a float; snap to the nearest known key.
function injectKey(g) { return g >= 2 ? "2" : g >= 1.5 ? "1.5" : "1.25"; }
function injectBadge(g) {
  if (!g) return "";
  const k = injectKey(g);
  return ` <span class="inject-badge" title="FSD synthesis: ${INJECT_NAMES[k]} injection (+${Math.round((Number(k) - 1) * 100)}% range) needed for this jump">${INJECT_ICONS[k]}</span>`;
}
// Max grade needed across jumps [i0, i1) (0 = none), for the compact view.
function injectInRange(r, i0, i1) {
  let best = 0;
  for (let i = i0; i < i1 && i < r.jumps.length; i++) {
    const g = r.jumps[i] && r.jumps[i].inject_grade;
    if (g && g > best) best = g;
  }
  return best;
}
// "Synthesis: N injections needed: [icon] 2x Basic ..." built from the legs, so
// the commander knows what to synthesise and how much before the route.
function injectSummary(r) {
  const counts = {};
  for (const j of r.jumps || []) {
    if (j.inject_grade) { const k = injectKey(j.inject_grade); counts[k] = (counts[k] || 0) + 1; }
  }
  const parts = ["1.25", "1.5", "2"].filter(k => counts[k])
    .map(k => `${INJECT_ICONS[k]} <b>${counts[k]}x</b> ${INJECT_NAMES[k]}`);
  if (!parts.length) return "";
  const n = Object.values(counts).reduce((a, b) => a + b, 0);
  // Total raw materials consumed = sum of each grade's recipe x its count.
  const mats = {};
  for (const k of Object.keys(counts)) {
    for (const [m, q] of Object.entries(INJECT_RECIPES[k])) mats[m] = (mats[m] || 0) + q * counts[k];
  }
  const matParts = Object.entries(mats)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([m, q]) => `<b>${q}x</b> ${m}`);
  return `<div class="injectbar" title="Synthesise these FSD injections before the route; each is used on one jump where a normal jump cannot reach. The least sufficient grade is chosen per gap.">`
    + `<div><b>Synthesis:</b> ${n} FSD injection${n > 1 ? "s" : ""} needed: ${parts.join(" · ")}</div>`
    + `<div class="injectmats" title="Total materials consumed across the whole route (each injection is one craft / one jump).">Materials: ${matParts.join(", ")}</div>`
    + `</div>`;
}
function escapeHtml(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function fmt(n, d = 0) { return Number(n).toLocaleString(undefined, { maximumFractionDigits: d }); }
// Friendly star-type display. The route legs carry whatever is in
// systems_big.mainstar: raw FDev codes ("A_BlueWhiteSuperGiant"), spansh-style
// friendly names ("A (Blue-White super giant) Star"), or bare classes ("K").
// index.html exposes the galaxy/sysmap resolvers; fall back to a split if the
// panel is ever loaded standalone.
function starShort(s) {
  const fn = typeof window !== "undefined" && window.edgisStarShort;
  if (fn) { const v = fn(s); if (v && v !== "-") return v; }
  return String(s || "").split(/[\s(]/)[0] || "";
}
function starName(s) {
  // Drop the redundant trailing " Star" (e.g. "A (Blue-White super giant) Star"
  // -> "A (Blue-White super giant)", "Neutron Star" -> "Neutron").
  const strip = (v) => v.replace(/\s+Star$/, "");
  const fn = typeof window !== "undefined" && window.edgisStarName;
  if (fn) { const v = fn(s); if (v && v !== "Unknown Star") return strip(v); }
  return strip(String(s || ""));
}
// Scoopable = the resolved short class is exactly one of KGBFOAM (super/giants
// map to their base letter, e.g. A_BlueWhiteSuperGiant -> "A" -> scoopable;
// Ae/Be, DQ, N, etc. are not).
function scoopable(s) { return /^[KGBFOAM]$/.test(starShort(s)); }
// The same fuel-pump glyph, tinted, for a neutron whose system has a scoopable
// companion in supercruise range: you refuel there WITHOUT leaving the route.
const FUEL_INFLIGHT = FUEL
  .replace('class="fuel"', 'class="fuel inflight"')
  .replace('<title>Fuel-scoopable (KGBFOAM)</title>', '');
function inflightScoopMarker(ls) {
  const t = (ls != null)
    ? `In-system scoop: scoopable companion ~${fmt(ls, 0)} Ls (refuel without leaving the route)`
    : "In-system scoop (scoopable companion star)";
  return `<span class="inflight-scoop" title="${escapeHtml(t)}">${FUEL_INFLIGHT}</span>`;
}
function spectralCell(j) {
  if (!j.spectral) return "";
  const scoop = scoopable(j.spectral) ? FUEL
    : (j.inflight_scoop ? inflightScoopMarker(j.scoop_ls) : "");
  // Companion (secondary) neutron: show how far in-system you supercruise to it.
  const companion = (j.neutron_star && j.arrival_ls > 0)
    ? ` <span class="arrls" title="Companion neutron ${escapeHtml(fmt(j.arrival_ls, 0))} Ls from arrival (supercruise to it, then supercharge)">@${escapeHtml(fmt(j.arrival_ls, 0))} Ls</span>`
    : "";
  // In-system refuel companion: show the supercruise distance inline too, like
  // the neutron companion, so the scoop distance is visible without hovering.
  const scoopLs = (!scoopable(j.spectral) && j.inflight_scoop && j.scoop_ls != null)
    ? ` <span class="arrls scoop-ls" title="In-system scoop: scoopable companion ${escapeHtml(fmt(j.scoop_ls, 0))} Ls from arrival (refuel without leaving the route)">@${escapeHtml(fmt(j.scoop_ls, 0))} Ls</span>`
    : "";
  return `<span class="starcell">${scoop}<span class="spec" title="${escapeHtml(starName(j.spectral))}">${escapeHtml(starShort(j.spectral))}</span>${companion}${scoopLs}</span>`;
}
function legLabel(j, last) {
  if (last) return "arrive";
  if (j.type === "boost") return j.neutron_star ? "neutron ★" : "supercharged";
  const base = j.neutron_star ? "board ★" : "bridge";
  // An FSD-injected jump reaches past a normal gap; flag it so the commander
  // knows this hop needs synthesis materials.
  return j.inject_grade ? `${base} + inject` : base;
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
  const out = []; let jumps = 0, ly = 0, start = 0;
  r.jumps.forEach((j, i) => {
    jumps++; ly += j.distance_jumped;
    const last = i === r.jumps.length - 1;
    if (j.neutron_star || last) {
      out.push({ system: j.system ?? (last ? r.destination : "?"), spectral: j.spectral,
                 jumps, ly, left: j.distance_left, neutron: j.neutron_star, dest: last,
                 inflight_scoop: j.inflight_scoop, scoop_ls: j.scoop_ls, arrival_ls: j.arrival_ls,
                 x: j.x, y: j.y, z: j.z, i0: start, i1: i });  // i0..i1 = this leg group
      jumps = 0; ly = 0; start = i + 1;
    }
  });
  return out;
}
// A deep link that reproduces the plotted route (tool + from/to/range/
// supercharge/cargo). Uses the query that was actually plotted (lastQuery), not
// the live fields. The SLEF loadout is deliberately NOT shared (stored in the
// browser only); the range param already captures the ship's plot range.
function shareUrl() {
  const q = lastQuery || {};
  const p = new URLSearchParams({ tool: "route" });
  if (q.from) p.set("from", q.from);
  if (q.to) p.set("to", q.to);
  if (q.range) p.set("range", q.range);
  if (q.supercharge && q.supercharge !== "4") p.set("supercharge", q.supercharge);
  if (q.companionLs > 0) p.set("companion_max_ls", String(q.companionLs));
  if (q.inject > 1) p.set("inject_mult", String(q.inject));
  const cargo = cargoTons();
  if (cargo > 0) p.set("cargo", String(cargo));
  return `${location.origin}${location.pathname}?${p.toString()}`;
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

// Format an ISO wave date ("2026-11-11") as "11 Nov" for the schedule copy,
// falling back to the raw value. Wave dates ALWAYS come from the API payload
// (/route/status + the 403 body both carry `waves` and `public_after`), so the
// UI never hardcodes a date -- moving the backend waves updates this copy too.
function fmtWaveDate(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || ""));
  if (!m) return iso || "";
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return `${+m[3]} ${months[+m[2] - 1]}`;
}
// Build the "rolling out in waves" schedule from a status/403 payload's `waves`
// (array of {level, after}) + `public_after`. Follower and hunter usually share
// a date; show them together when equal.
function accessSchedule(obj) {
  const by = {};
  (Array.isArray(obj.waves) ? obj.waves : []).forEach(w => {
    if (w && w.level) by[w.level] = w.after;
  });
  const bits = ["key holders now"];
  if (by.supporter) bits.push(`Patreon tier 1 from <b>${fmtWaveDate(by.supporter)}</b>`);
  const f = by.follower, h = by.hunter;
  if (f && h && f === h) {
    bits.push(`free followers and Galactic Hunters (5+ confirmed submissions) from <b>${fmtWaveDate(f)}</b>`);
  } else {
    if (f) bits.push(`free followers from <b>${fmtWaveDate(f)}</b>`);
    if (h) bits.push(`Galactic Hunters (5+ confirmed submissions) from <b>${fmtWaveDate(h)}</b>`);
  }
  bits.push(`everyone from <b>${fmtWaveDate(obj.public_after)}</b>`);
  return bits.join(" · ");
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
      parts.push(`<b>Early access, rolling out in waves:</b> ${accessSchedule(st)}.`);
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

// Per-leg fuel risks for the results table. The fuel report's *_at indices
// point into r.jumps (the realized legs), so each maps to exactly one row.
// strand (dry tank, no scoopable star in reach) outranks over-range (jump too
// long at the current -- heavy -- fuel level) when they land on the same leg.
function fuelRisks(r) {
  const m = new Map();
  const f = r && r.fuel;
  if (!f) return m;
  const add = (idx, kind, label) => {
    if (idx == null || idx < 0) return;
    if (!m.has(idx) || kind === "strand") m.set(idx, { kind, label });
  };
  if (f.over_range) add(f.over_range_at, "over",
    "Jump too long at this fuel level: a full tank is heavy and jumps shorter (range grows as fuel burns). Likely unmakeable here.");
  if (f.stranded) add(f.stranded_at, "strand",
    "Out of fuel here: no scoopable star within reach to refuel. Risk of stranding.");
  return m;
}
function riskInRange(riskMap, i0, i1) {
  let found = null;  // a collapsed segment may hold several; surface the worst
  if (riskMap.size) for (const [idx, info] of riskMap) {
    if (idx < i0 || idx > i1) continue;
    if (!found || info.kind === "strand") found = info;
  }
  return found;
}
function riskBadge(info) {
  return info
    ? ` <span class="risk risk-${info.kind}" title="${escapeHtml(info.label)}" aria-label="fuel warning">${warnIconSvg}</span>`
    : "";
}

// Convert our route to spansh's neutron-plotter job JSON so it imports into
// tools that read spansh routes. system_jumps = source first (jumps 0), then
// each neutron supercharge stop + the destination; `jumps` = hops to reach it
// (same grouping as the compact view); total_jumps excludes the source. Mirrors
// spansh's exact shape: the job wrapper (created_at/job/parameters/state/
// status/updated_at), string-typed range/efficiency/supercharge_multiplier,
// and full-precision distances -- distance_jumped is the straight-line distance
// between consecutive WAYPOINTS (not summed flown LY), distance_left the
// straight-line to the destination, matching a real spansh result field-for-
// field. (id64 is null: our realized legs don't carry it yet -- backend gap.)
// spansh Exact (Galaxy) Plotter JSON: per-leg `result.jumps` (every jump, source
// first) with fuel detail + string id64s, matching what spansh's exact plotter
// exports so routes import into tools keyed on that shape. Per-leg fuel + the
// `parameters` block come from the backend when a ship loadout is used; without
// one the fuel fields are null and parameters is minimal.
function toSpanshExactRoute(r) {
  const sc = r.source_coords || { x: 0, y: 0, z: 0 };
  const sp = r.spansh_params || null;
  const tank = (sp && sp.tank_size != null) ? sp.tank_size
    : (r.fuel && r.fuel.tank_size != null ? r.fuel.tank_size : null);
  const idStr = (v) => (v === null || v === undefined) ? null : String(v);
  const legs = r.jumps || [];
  const destId = legs.length ? legs[legs.length - 1].id64 : null;
  const jumps = [{
    name: r.source, id64: idStr(r.source_id64),
    x: sc.x ?? null, y: sc.y ?? null, z: sc.z ?? null,
    distance: 0, distance_to_destination: r.straight_line_ly,
    has_neutron: false, is_scoopable: false,
    fuel_in_tank: tank, fuel_used: 0, must_inject: 0, must_refuel: false,
  }];
  legs.forEach((j, i) => {
    const last = i === legs.length - 1;
    jumps.push({
      name: j.system ?? (last ? r.destination : ""), id64: idStr(j.id64),
      x: j.x, y: j.y, z: j.z,
      distance: j.distance_jumped, distance_to_destination: j.distance_left,
      has_neutron: !!j.neutron_star,
      // A neutron with an in-system scoopable companion is scoopable here too.
      is_scoopable: scoopable(j.spectral) || !!j.inflight_scoop,
      fuel_in_tank: (j.fuel_in_tank ?? null), fuel_used: (j.fuel_used ?? null),
      must_inject: 0, must_refuel: !!j.must_refuel,
    });
  });
  const now = new Date().toISOString().slice(0, 19);  // "YYYY-MM-DDTHH:MM:SS"
  const job = (globalThis.crypto && crypto.randomUUID ? crypto.randomUUID() : "").toUpperCase();
  const scMul = r.boost_range && r.jump_range ? Math.round(r.boost_range / r.jump_range) : 4;
  const loadout = ($("loadout").value || "").trim();
  const parameters = {
    algorithm: "optimistic", max_time: "60",
    source_system: idStr(r.source_id64) ?? r.source,
    destination_system: idStr(destId) ?? r.destination,
    supercharge_multiplier: scMul,
    ...(sp || {}),
    ...(loadout ? { ship_build: loadout } : {}),
  };
  return {
    created_at: now, job, parameters,
    result: { jumps, refuel_every_scoopable: false },
    state: "completed", status: "ok", updated_at: now,
  };
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
  const data = csv ? toCsvRoute(lastOurs) : JSON.stringify(toSpanshExactRoute(lastOurs), null, 2);
  const blob = new Blob([data], { type: csv ? "text/csv;charset=utf-8" : "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = `${name}.${csv ? "csv" : "json"}`;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// Per-row "done" progress tracking (manual, like spansh). Keyed by system name
// within the plotted route (from|to|range|supercharge) and persisted in
// localStorage, so ticks survive reloads and the compact/full toggle (a neutron
// waypoint ticked in one mode stays ticked in the other). A future step is
// auto-ticking from live game state via the EDMC plugin / FDev cAPI (see memory).
const DONE_LS = "edgis_route_done";
let doneSet = new Set();
function routeDoneKey() {
  const q = lastQuery || {};
  return `${q.from || ""}|${q.to || ""}|${q.range || ""}|${q.supercharge || "4"}`;
}
function loadDone() {
  doneSet = new Set();
  try {
    const arr = JSON.parse(localStorage.getItem(DONE_LS) || "{}")[routeDoneKey()];
    if (Array.isArray(arr)) doneSet = new Set(arr);
  } catch (e) { /* ignore */ }
}
function saveDone() {
  try {
    const all = JSON.parse(localStorage.getItem(DONE_LS) || "{}");
    if (doneSet.size) all[routeDoneKey()] = [...doneSet];
    else delete all[routeDoneKey()];
    localStorage.setItem(DONE_LS, JSON.stringify(all));
  } catch (e) { /* ignore */ }
}
function doneBox(sys) {
  return `<td class="chk"><input type="checkbox" class="done-box" data-sys="${escapeHtml(sys)}"`
    + `${doneSet.has(sys) ? " checked" : ""} title="Mark this system done" /></td>`;
}

// The departure system as row #0 (like spansh): jumps/LY from source are 0,
// "left" is the straight-line distance to the destination. Compact has three
// numeric columns (jumps/LY/left), full has two (jumped/left).
function sourceRow(r, compact) {
  const sc = r.source_coords || {};
  const left = fmt(r.straight_line_ly, 0);
  const nums = compact
    ? `<td class="num">0</td><td class="num">0</td><td class="num">${left}</td>`
    : `<td class="num">0</td><td class="num">${left}</td>`;
  return `<tr class="src-row${doneSet.has(r.source) ? " done" : ""}">
      ${doneBox(r.source)}
      <td class="num">0</td>
      <td class="sys">${escapeHtml(r.source)}${copyBtn(r.source)}</td>
      <td></td>
      ${nums}
      ${actsCell(r.source, sc.x, sc.y, sc.z)}
    </tr>`;
}

function renderRoute(r) {
  lastOurs = r;
  loadDone();
  let header, rows;
  const riskMap = fuelRisks(r);
  if (compactMode) {
    const wps = compactRows(r);
    header = `<th class="chk-head" title="Done"></th><th class="num">#</th><th>system</th><th>star</th><th class="num">jumps</th><th class="num">LY</th><th class="num">left</th><th class="acts-head">actions</th>`;
    rows = wps.map((w, i) => {
      const risk = riskInRange(riskMap, w.i0, w.i1);
      const cname = w.dest ? r.destination : w.system;
      return `<tr class="${risk ? "risk-row " : ""}${doneSet.has(cname) ? "done" : ""}">
      ${doneBox(cname)}
      <td class="num">${i + 1}</td>
      <td class="sys ${w.neutron ? "boost" : ""}">${escapeHtml(w.system)}${injectBadge(injectInRange(r, w.i0, w.i1))}${riskBadge(risk)}${copyBtn(cname)}</td>
      <td>${spectralCell({ spectral: w.spectral, neutron_star: w.neutron, inflight_scoop: w.inflight_scoop, scoop_ls: w.scoop_ls, arrival_ls: w.arrival_ls })}</td>
      <td class="num">${fmt(w.jumps)}</td>
      <td class="num">${fmt(w.ly, 0)}</td>
      <td class="num">${fmt(w.left, 0)}</td>
      ${actsCell(w.dest ? r.destination : w.system, w.x, w.y, w.z)}
    </tr>`;
    }).join("");
    rows = sourceRow(r, true) + rows;
  } else {
    header = `<th class="chk-head" title="Done"></th><th class="num">#</th><th>system</th><th>star</th><th class="num">jumped</th><th class="num">left</th><th class="acts-head">actions</th>`;
    rows = r.jumps.map((j, i) => {
      const last = i === r.jumps.length - 1;
      const name = j.system ?? (last ? r.destination : "?");
      const risk = riskMap.get(i);
      return `<tr class="${risk ? "risk-row " : ""}${doneSet.has(name) ? "done" : ""}">
      ${doneBox(name)}
      <td class="num">${i + 1}</td>
      <td class="sys">${escapeHtml(name)}${injectBadge(j.inject_grade)}${riskBadge(risk)}${copyBtn(j.system ?? r.destination)}</td>
      <td>${spectralCell(j)}</td>
      <td class="num">${fmt(j.distance_jumped, 1)}</td>
      <td class="num">${fmt(j.distance_left, 0)}</td>
      ${actsCell(j.system ?? (last ? r.destination : null), j.x, j.y, j.z)}
    </tr>`;
    }).join("");
    rows = sourceRow(r, false) + rows;
  }
  $("out").innerHTML = `
    <div class="summary">
      <div class="stat"><b>${fmt(r.total_jumps)}</b><span>jumps</span></div>
      <div class="stat"><b>${fmt(r.neutron_jumps)}</b><span>neutron jumps</span></div>
      <div class="stat"><b>${fmt(r.bridge_jumps)}</b><span>regular jumps</span></div>
      <div class="stat"><b>${fmt(r.total_distance_ly)}</b><span>LY flown</span></div>
      <div class="stat"><b>${fmt(r.efficiency * 100, 1)}%</b><span>efficiency</span></div>
      <div class="stat"><b>${fmt(r.solve_ms, 0)} ms</b><span>solve</span></div>
    </div>
    ${r.fuel ? `<div class="fuelbar">${fuelVerdict(r.fuel)}</div>` : ""}
    ${injectSummary(r)}
    <div id="cmpOut"></div>
    <div class="summary-actions">
      <label class="switch" title="Compact: only the waypoints you plot in-game. Off: every jump.">
        <input type="checkbox" id="compactToggle" ${compactMode ? "checked" : ""} />
        <span class="track"></span><span>compact</span>
      </label>
      <span class="export-group">
        <select id="exportFmt" title="Export format" aria-label="Export format">
          <option value="json"${exportFmt === "json" ? " selected" : ""}>spansh (Galaxy) JSON</option>
          <option value="csv"${exportFmt === "csv" ? " selected" : ""}>CSV</option>
        </select>
        <button id="exportBtn" class="ghost" title="Download this route in the selected format">${downloadIconSvg} Export</button>
      </span>
      <button id="copyRoute" class="ghost" title="Copy the route systems, one per line">${copyIconSvg} Copy route</button>
      <button id="shareRoute" class="ghost" title="Copy a link that reproduces this route">${shareIconSvg} Share</button>
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
    const link = s.results_url ? ` · <a href="${escapeHtml(s.results_url)}" target="_blank" rel="noopener">open in spansh ↗</a>` : "";
    // Both sides use the selected supercharge; label it so the compare is
    // unambiguously apples-to-apples (e.g. Caspian 6x vs spansh at 6x).
    const sc = Number(supercharge || 4);
    const modeTag = sc !== 4 ? `<span class="tiny">supercharge ${sc}x</span> · ` : "";
    if (out()) out().innerHTML = modeTag + `<b>spansh Neutron&nbsp;Plotter:</b> ${fmt(s.total_jumps)} jumps (eff 60${sDur})`
      + cmpVerdict(s.total_jumps) + cmpTime(s.duration_s) + link;
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
      body: JSON.stringify({ from, to, loadout, cargo: cargoTons(), inject_mult: injectMult() }),
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
  const companionLs = companionMaxLs();
  // Fuel-aware plotting (POST with the loadout) kicks in once a ship is imported.
  const loadout = shipInfo ? ($("loadout").value || "").trim() : "";
  lastQuery = { from, to, range, supercharge, companionLs, inject: injectMult() };
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
          companion_max_ls: companionLs, inject_mult: injectMult(),
          loadout, cargo: cargoTons(), fuel_safe: $("fuelSafe").checked,
        }),
      });
    } else {
      let url = `/route?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&range=${encodeURIComponent(range)}&supercharge=${encodeURIComponent(supercharge)}`;
      if (companionLs > 0) url += `&companion_max_ls=${encodeURIComponent(companionLs)}`;
      if (injectMult() > 1) url += `&inject_mult=${encodeURIComponent(injectMult())}`;
      res = await fetch(url, { headers: headers(), credentials: "include" });
    }
    if (res.status === 403) {
      const d = (await res.json()).detail || {};
      // If the backend recognises the caller and their wave just hasn't opened
      // yet, it returns a personalized date; otherwise show the full schedule.
      const schedule = d.your_access_after
        ? `Your access opens on <b>${fmtWaveDate(d.your_access_after)}</b>.`
        : `Rolling out in waves: ${accessSchedule(d)}.`;
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
  const share = e.target.closest("#shareRoute");
  if (share && lastQuery) { copyText(shareUrl(), share); return; }
  const exportBtn = e.target.closest("#exportBtn");
  if (exportBtn && lastOurs) { downloadRoute(exportFmt); }
});
// The compact switch and export-format picker both change on input.
$("out").addEventListener("change", (e) => {
  const box = e.target.closest(".done-box");
  if (box) {
    // Cumulative progress: ticking a row marks everything above it done too (you
    // flew past them); unticking a row clears it and everything below. Operates
    // on the displayed rows in order, so it's correct in compact and full modes.
    const applyDone = (b, state) => {
      b.checked = state;
      if (state) doneSet.add(b.dataset.sys); else doneSet.delete(b.dataset.sys);
      b.closest("tr")?.classList.toggle("done", state);
    };
    const boxes = [...$("out").querySelectorAll(".done-box")];
    const idx = boxes.indexOf(box);
    if (box.checked) { for (let k = 0; k <= idx; k++) applyDone(boxes[k], true); }
    else { for (let k = idx; k < boxes.length; k++) applyDone(boxes[k], false); }
    saveDone();
    return;
  }
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
$("clearShip").addEventListener("click", clearShip);
$("clearAllShips").addEventListener("click", clearAllShips);
// Switch to or remove a stored ship (buttons are rendered dynamically).
$("shipLibrary").addEventListener("click", (ev) => {
  const pick = ev.target.closest(".ship-pick");
  const del = ev.target.closest(".ship-del");
  const ships = loadShips();
  if (pick) {
    const i = Number(pick.dataset.i);
    if (ships[i]) { $("loadout").value = ships[i].loadout; importShip(); }
  } else if (del) {
    const i = Number(del.dataset.i);
    const wasActive = i === activeShip();
    ships.splice(i, 1); saveShips(ships);
    if (wasActive) { clearShip(); }          // deselect + re-render
    else { const a = activeShip(); if (a > i) setActiveShip(a - 1); renderShipLibrary(); }
  }
});
// Swap departure and arrival, then re-plot if both are set (tester: "to go back
// I have to retype the systems").
$("swapFromTo").addEventListener("click", () => {
  const f = $("from").value;
  $("from").value = $("to").value;
  $("to").value = f;
  if ($("from").value.trim() && $("to").value.trim()) plot();
});
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
  const range = q.get("range"); if (range) $("range").value = range;
  const supercharge = q.get("supercharge");
  if (supercharge) { $("ship").value = supercharge; localStorage.setItem(SHIP_LS, supercharge); }
  const cargo = q.get("cargo");
  if (cargo != null && cargo !== "") { $("cargo").value = cargo; localStorage.setItem(CARGO_LS, String(cargoTons())); }
  const cml = q.get("companion_max_ls");
  if (cml != null && cml !== "") { $("companionLs").value = cml; localStorage.setItem(COMPANION_LS, String(companionMaxLs())); }
  const inj = q.get("inject_mult");
  if (inj != null && inj !== "") { $("injectMult").value = inj; localStorage.setItem(INJECT_MULT, String(injectMult())); }
  if ($("loadout").value.trim()) { importShip(); }
  loadStatus();
  // A shared link (from + to both present) plots straight away, at the shared
  // range. Deferred to the next tick so the synchronous field prefill is fully
  // applied before plot() reads it.
  if (from && to) setTimeout(plot, 0);
};

// The index's init is a deferred module that runs BEFORE this (deferred) script,
// so for a deep/shared link (?tool=route&from=...) it activates the panel but
// calls window.edgisInitRoutePanel before it exists -> no prefill. Self-init
// here if the URL already requests the route tool (idempotent via _routeInited).
if (new URLSearchParams(location.search).get("tool") === "route") {
  window.edgisInitRoutePanel();
}
})();
