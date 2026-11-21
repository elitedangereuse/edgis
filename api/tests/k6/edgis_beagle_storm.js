// k6: reproduce the 2026-11-20 braben outage and prove the mitigation holds.
//
// THE INCIDENT: a commander range-swept Sol -> Beagle Point (the galaxy's
// longest route) 9x in ~90s. Each distinct range is a distinct cache key, so
// there was NO coalescing -- 9 full-galaxy corridor realizations ran in
// parallel and drove the swapless box to OOM/thrash.
//
// THIS TEST multiplies that ~10x: many VUs each plot a galaxy-spanning route
// with a UNIQUE range every iteration -> guaranteed COLD solves -> the same
// concurrent-realization pressure, an order of magnitude harder. The server's
// defenses (heavy-solve serialization, memory-pressure 429 shed, ROUTE_MAX_QUEUE
// shed, swap, worker recycling) should SHED the excess gracefully while the box
// stays alive and normal users stay fast.
//
// WHAT "PASS" LOOKS LIKE (mitigation works):
//   - lots of 429 (graceful shedding), FEW/NO 5xx, no timeouts,
//   - the PROD CANARY (a cached Sol->Colonia plot) stays fast throughout,
//   - braben survives (watch the live monitor: swap used stays modest, Avail > 0).
// "FAIL" (do steps 2/3/4): canary latency balloons, 5xx/timeouts appear, or the
//   live monitor shows Avail -> 0 / heavy swap / load spike = box in distress.
//
// The STORM hits STAGING (its own app process + its own per-process defenses),
// the CANARY hits PROD -- so this measures whether a 10x cold storm on the
// SHARED box/Postgres degrades real prod users (the exact question the incident
// raised). Both share braben's DB, which is where the memory pressure lives.
//
// Run (RAMP GRADUALLY, watch the braben monitor, abort k6 if Avail -> 0):
//   k6 run -e ROUTE_KEY=<key> edgis_beagle_storm.js
//   k6 run -e ROUTE_KEY=<key> -e PEAK_VUS=200 edgis_beagle_storm.js   // "even more"
//   k6 run -e ROUTE_KEY=<key> -e STORM_URL=https://edgis-staging.elitedangereuse.fr \
//          -e CANARY_URL=https://edgis.elitedangereuse.fr edgis_beagle_storm.js

import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';

const STORM_URL = __ENV.STORM_URL || 'https://edgis-staging.elitedangereuse.fr';
const CANARY_URL = __ENV.CANARY_URL || 'https://edgis.elitedangereuse.fr';
// Staging and prod have different entitlement keys; STORM_KEY/CANARY_KEY
// override ROUTE_KEY per target.
const ROUTE_KEY = __ENV.ROUTE_KEY || '';
const STORM_KEY = __ENV.STORM_KEY || ROUTE_KEY;
const CANARY_KEY = __ENV.CANARY_KEY || ROUTE_KEY;
const PEAK_VUS = Number(__ENV.PEAK_VUS || 100);     // ~10x the incident's ~10 concurrent
const HOLD = __ENV.HOLD || '90s';                   // hold at peak
// RUN_SALT gives each run its OWN 7.5 ly range band (base 40 + SALT*7.5) so a
// repeat run never reuses a prior run's cached ranges. Use a DISTINCT salt per
// run (0,1,2,...). Keeps every storm solve genuinely COLD across runs.
const RUN_SALT = Number(__ENV.RUN_SALT || 0);
const stormHeaders = { 'x-route-key': STORM_KEY };
const canaryHeaders = { 'x-route-key': CANARY_KEY };

// Galaxy-spanning routes (all > 20 kly => all take the heavy-solve semaphore).
// Sol -> Beagle Point is THE incident route; the others add realistic variety.
const ROUTES = [
  { from: 'Sol', to: 'Beagle Point' },
  { from: 'Sol', to: 'Beagle Point' },      // weight Beagle (the incident)
  { from: 'Sol', to: 'Sagittarius A*' },
  { from: 'Colonia', to: 'Beagle Point' },
];



// Metrics
const timeToRoute = new Trend('time_to_route', true);   // TRUE user wait to a 200, incl. 429 retries
const solveLatency = new Trend('solve_latency', true);  // latency of a single successful solve
const served = new Counter('served_200');
const shed = new Counter('shed_429');
const serverErr = new Counter('errors_5xx');
const timeouts = new Counter('timeouts');
const canaryLatency = new Trend('canary_latency', true);
const canaryFail = new Rate('canary_failed');

export const options = {
  scenarios: {
    storm: {
      executor: 'ramping-vus',
      exec: 'storm',
      startVUs: 0,
      stages: [
        { duration: '30s', target: Math.max(1, Math.round(PEAK_VUS * 0.1)) },  // ~incident level first
        { duration: '45s', target: Math.max(1, Math.round(PEAK_VUS * 0.1)) },  // hold, confirm shedding
        { duration: '30s', target: Math.round(PEAK_VUS * 0.5) },               // ~5x
        { duration: '30s', target: PEAK_VUS },                                 // ~10x
        { duration: HOLD, target: PEAK_VUS },                                  // hold the 10x
        { duration: '20s', target: 0 },
      ],
      gracefulRampDown: '15s',
    },
    canary: {
      executor: 'constant-arrival-rate',
      exec: 'canary',
      // CANARY_DUR should cover the whole storm (ramp + HOLD + ramp-down); for a
      // long soak pass e.g. CANARY_DUR=13m so prod-canary coverage never drops.
      rate: 1, timeUnit: '1s', duration: __ENV.CANARY_DUR || '4m',
      preAllocatedVUs: 3, maxVUs: 8,
    },
  },
  thresholds: {
    // The box must NEVER 5xx/timeout (crash) and normal users must stay fast.
    // 429 shedding is EXPECTED and is NOT a failure.
    'errors_5xx': ['count==0'],
    'timeouts': ['count==0'],
    'canary_latency': ['p(95)<1500'],   // cached prod plot stays snappy under the storm
    'canary_failed': ['rate<0.01'],
  },
};

function stormUrl() {
  const r = ROUTES[(__VU + __ITER) % ROUTES.length];
  // GUARANTEED-COLD range: iterationInTest is a GLOBAL monotonic counter across
  // all VUs, so every logical solve gets a distinct range (0.005 ly steps in a
  // realistic 40-ly-and-up band) -> a distinct cache key -> a true cold solve.
  // The old (__VU*1000+__ITER)%700 formula collided every 7th VU and wrapped at
  // 700 values, so big storms silently reused cached ranges (NOT a cold test).
  // retries within one iteration reuse the same range (same user, same plot).
  const uid = exec.scenario.iterationInTest;
  const range = (40 + (RUN_SALT * 1500 + uid) * 0.005).toFixed(3);
  const p = `from=${encodeURIComponent(r.from)}&to=${encodeURIComponent(r.to)}&range=${range}`;
  return `${STORM_URL}/route?${p}`;
}

// One "user" plotting a galaxy route: attempt, and on a 429 honor Retry-After
// and retry (the real frontend auto-retries behind its plotting queue). We
// record the TOTAL time until a 200 -- the actual wait a user experiences.
export function storm() {
  const t0 = Date.now();
  let res, attempts = 0;
  while (attempts < 8) {
    attempts++;
    res = http.get(stormUrl(), { headers: stormHeaders, timeout: '200s', tags: { kind: 'storm' } });
    if (res.status === 200) {
      served.add(1);
      solveLatency.add(res.timings.duration);
      break;
    }
    if (res.status === 429) {
      shed.add(1);
      const ra = Number(res.headers['Retry-After'] || 5);
      sleep(Math.min(Math.max(ra, 1), 15));
      continue;
    }
    if (res.status === 0) { timeouts.add(1); break; }          // client/network timeout
    if (res.status >= 500) { serverErr.add(1); break; }        // box in distress
    break;                                                     // 4xx other than 429: stop
  }
  if (res && res.status === 200) timeToRoute.add(Date.now() - t0);
  sleep(0.5);
}

// A normal user whose plot is a cache hit (Sol->Colonia is a hot route). If the
// box is thrashing from the storm, THIS slows down -- that's the user impact.
export function canary() {
  const res = http.get(`${CANARY_URL}/route?from=Sol&to=Colonia&range=50`,
    { headers: canaryHeaders, timeout: '10s', tags: { kind: 'canary' } });
  canaryLatency.add(res.timings.duration);
  canaryFail.add(res.status !== 200);
  check(res, { 'canary 200': (r) => r.status === 200 });
}
