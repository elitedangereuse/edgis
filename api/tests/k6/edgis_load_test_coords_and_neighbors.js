import http from 'k6/http';
import { check, sleep } from 'k6';

// Test configuration
export const options = {
  stages: [
    { duration: '30s', target: 20 },
    { duration: '1m', target: 20 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<800'],
    http_req_failed: ['rate<0.01'],
  },
};

const BASE_URL = 'https://edgis.elitedangereuse.fr';

const DEFAULT_COORDS = { x: 0, y: 0, z: 0, radius: 20 };
const FALLBACK_SYSTEM = 'Eoch Flyuae VL-F a44-0';

function pickRandomNeighbor(neighbors) {
  if (!Array.isArray(neighbors) || neighbors.length === 0) {
    return null;
  }
  const idx = Math.floor(Math.random() * neighbors.length);
  return neighbors[idx];
}

function getRandomCoords(base) {
  return {
    x: base.x + (Math.random() - 0.5) * 2000 - 1000,
    y: base.y + (Math.random() - 0.5) * 2000 - 1000,
    z: base.z + (Math.random() - 0.5) * 2000 - 1000,
    radius: base.radius,
  };
}

export default function () {
  const coords = getRandomCoords(DEFAULT_COORDS);
  const neighborsUrl = `${BASE_URL}/neighbors?x=${coords.x.toFixed(2)}&y=${coords.y.toFixed(2)}&z=${coords.z.toFixed(2)}&radius=${coords.radius}`;

  const neighborsRes = http.get(neighborsUrl);
  let neighborsPayload = null;
  check(neighborsRes, {
    'neighbors: status 200': (r) => r.status === 200,
    'neighbors: valid JSON': (r) => {
      try {
        neighborsPayload = JSON.parse(r.body);
        return true;
      } catch (_) {
        return false;
      }
    },
  });

  const chosenNeighbor = pickRandomNeighbor(neighborsPayload);
  const systemName = chosenNeighbor?.name || FALLBACK_SYSTEM;
  const coordsUrl = `${BASE_URL}/coords?q=${encodeURIComponent(systemName)}`;

  const coordsRes = http.get(coordsUrl);
  check(coordsRes, {
    'coords: status 200': (r) => r.status === 200,
    'coords: valid JSON': (r) => {
      try {
        JSON.parse(r.body);
        return true;
      } catch (_) {
        return false;
      }
    },
  });

  const followup = chosenNeighbor?.coords || coords;
  const followupX = Number(followup.x ?? coords.x).toFixed(2);
  const followupY = Number(followup.y ?? coords.y).toFixed(2);
  const followupZ = Number(followup.z ?? coords.z).toFixed(2);
  const secondNeighborsUrl = `${BASE_URL}/neighbors?x=${followupX}&y=${followupY}&z=${followupZ}&radius=${coords.radius}`;
  const secondNeighborsRes = http.get(secondNeighborsUrl);
  check(secondNeighborsRes, {
    'follow-up neighbors: status 200': (r) => r.status === 200,
  });

  const bodiesUrl = `${BASE_URL}/bodies?name_or_id=${encodeURIComponent(systemName)}`;
  const bodiesRes = http.get(bodiesUrl);
  check(bodiesRes, {
    'bodies: status ok/404': (r) => r.status === 200 || r.status === 404,
  });

  sleep(1);
}
