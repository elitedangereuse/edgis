// Saturation test: push the public stack (proxy + TLS + uvicorn + Redis) to its
// cache-hit ceiling with NO think-time, ramping arrival rate until latency or
// errors break. Cache-dominant (pre-warmed popular routes) -- this finds the
// request-rate the box sustains for the common case, NOT the cold solver.
//
//   k6 run -e ROUTE_KEY=<key> -e BASE_URL=https://edgis-staging.elitedangereuse.fr edgis_route_saturation.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'https://edgis.elitedangereuse.fr';
const ROUTE_KEY = __ENV.ROUTE_KEY || '';
const ROUTES = [
  { from: 'Sol', to: 'Colonia', range: 50 },
  { from: 'Lave', to: 'Maia', range: 50 },
  { from: 'Sol', to: 'Maia', range: 50 },
  { from: 'Colonia', to: 'Sagittarius A*', range: 50 },
  { from: 'Celaeno', to: 'Colonia', range: 80 },
];
const params = { headers: { 'x-route-key': ROUTE_KEY } };

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 50, timeUnit: '1s',
      preAllocatedVUs: 100, maxVUs: 800,
      stages: [
        { target: 100, duration: '15s' },
        { target: 300, duration: '20s' },
        { target: 600, duration: '20s' },
        { target: 1000, duration: '20s' },
        { target: 1000, duration: '15s' },
      ],
    },
  },
  thresholds: { http_req_failed: ['rate<0.05'] },
};

function url(r) {
  return `${BASE_URL}/route?from=${encodeURIComponent(r.from)}&to=${encodeURIComponent(r.to)}&range=${r.range}`;
}
export function setup() {
  for (const r of ROUTES) http.get(url(r), Object.assign({ timeout: '120s' }, params));
}
export default function () {
  const r = ROUTES[Math.floor(Math.random() * ROUTES.length)];
  const res = http.get(url(r), params);
  check(res, { 'ok': (x) => x.status === 200 });
}
