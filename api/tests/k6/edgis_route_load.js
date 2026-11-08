// k6 load test for the EDGIS route plotter (/route).
//
// The plotter is unlike the other endpoints: it needs an access key, a fresh
// solve costs seconds (bounded by a server-side concurrency semaphore), and
// results are Redis-cached. So this script, by default, exercises a SMALL pool
// of popular routes -- after warmup they are cache hits, which is what real
// traffic looks like (a few hot routes served from cache). It measures endpoint
// + cache throughput, NOT the cold solver.
//
// Run:
//   k6 run -e ROUTE_KEY=<key> edgis_route_load.js
//   k6 run -e ROUTE_KEY=<key> -e BASE_URL=https://edgis-staging.elitedangereuse.fr edgis_route_load.js
//   k6 run -e ROUTE_KEY=<key> -e ROUTE_FRESH=1 -e VUS=2 edgis_route_load.js   // solver stress (cold)
//
// ROUTE_FRESH=1 appends a per-iteration beam_width so every request busts the
// cache and forces a real solve -- use only with low VUS, it will be slow and
// queue behind the ROUTE_CONCURRENCY semaphore.

import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'https://edgis.elitedangereuse.fr';
const ROUTE_KEY = __ENV.ROUTE_KEY || '';
const FRESH = __ENV.ROUTE_FRESH === '1';
const VUS = Number(__ENV.VUS || (FRESH ? 2 : 10));

// Popular, mixed-length routes. Kept small so the cache-hit path dominates.
const ROUTES = [
  { from: 'Sol', to: 'Colonia', range: 50 },
  { from: 'Lave', to: 'Maia', range: 50 },
  { from: 'Sol', to: 'Maia', range: 50 },
  { from: 'Colonia', to: 'Sagittarius A*', range: 50 },
  { from: 'Cubeo', to: 'Col 173 Sector LJ-F c12-0', range: 50 },
  { from: 'Celaeno', to: 'Colonia', range: 80 },
];

export const options = {
  stages: [
    { duration: '20s', target: VUS },
    { duration: '1m', target: VUS },
    { duration: '20s', target: 0 },
  ],
  thresholds: {
    // Cache-dominant traffic should be quick; fresh solves are intentionally
    // slow, so loosen the bound when ROUTE_FRESH is set.
    http_req_duration: [FRESH ? 'p(95)<120000' : 'p(95)<1500'],
    http_req_failed: ['rate<0.02'],
    checks: ['rate>0.98'],
  },
};

function routeUrl(r, nonce) {
  const p = `from=${encodeURIComponent(r.from)}&to=${encodeURIComponent(r.to)}&range=${r.range}`;
  return `${BASE_URL}/route?${p}${nonce != null ? `&beam_width=${nonce}` : ''}`;
}

const params = ROUTE_KEY ? { headers: { 'x-route-key': ROUTE_KEY } } : {};

// Warm the cache once so the load phase is cache-dominant (skipped for FRESH).
export function setup() {
  if (FRESH || !ROUTE_KEY) return;
  for (const r of ROUTES) {
    http.get(routeUrl(r), Object.assign({ timeout: '120s' }, params));
  }
}

export default function () {
  const r = ROUTES[Math.floor(Math.random() * ROUTES.length)];
  const nonce = FRESH ? (__VU * 100000 + __ITER) : null;
  const res = http.get(routeUrl(r, nonce), Object.assign({ timeout: '120s' }, params));
  check(res, {
    // 200 = solved/cached; 503 = index still warming; 403 = missing/invalid key.
    'route: ok (200/503)': (x) => x.status === 200 || x.status === 503,
    'route: has jumps when 200': (x) =>
      x.status !== 200 || (JSON.parse(x.body || '{}').total_jumps || 0) > 0,
  });
  if (!ROUTE_KEY && res.status === 403) {
    // Surface the misconfig loudly: without a key the test is meaningless.
    check(res, { 'route: ROUTE_KEY provided': () => false });
  }
  sleep(1);
}
