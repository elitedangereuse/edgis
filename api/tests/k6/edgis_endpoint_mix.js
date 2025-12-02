import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 15 },
    { duration: '1m', target: 15 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<900'],
    http_req_failed: ['rate<0.02'],
  },
};

const BASE_URL = 'https://edgis.elitedangereuse.fr';
const DEFAULT_COORDS = { x: 0, y: 0, z: 0, radius: 30 };
const KNOWN_SYSTEMS = [
  'Sol',
  'Lave',
  'Eoch Flyuae VL-F b27-33',
  'Kamadhenu',
  'Colonia',
  'Shinrarta Dezhra',
  'HIP 87621',
];

function pickRandomSystem() {
  const idx = Math.floor(Math.random() * KNOWN_SYSTEMS.length);
  return KNOWN_SYSTEMS[idx];
}

function rememberSystems(neighbors) {
  if (!Array.isArray(neighbors)) return;
  for (const item of neighbors) {
    if (!item?.name) continue;
    if (KNOWN_SYSTEMS.length > 300) {
      KNOWN_SYSTEMS.shift();
    }
    KNOWN_SYSTEMS.push(item.name);
  }
}

function pickRandomNeighbor(neighbors) {
  if (!Array.isArray(neighbors) || neighbors.length === 0) {
    return null;
  }
  const idx = Math.floor(Math.random() * neighbors.length);
  return neighbors[idx];
}

function getRandomCoords(base) {
  return {
    x: base.x + (Math.random() - 0.5) * 200,
    y: base.y + (Math.random() - 0.5) * 200,
    z: base.z + (Math.random() - 0.5) * 200,
    radius: base.radius,
  };
}

function toFixed(value) {
  return Number(value).toFixed(2);
}

function safeJsonParse(body) {
  try {
    return JSON.parse(body);
  } catch (err) {
    return null;
  }
}

export default function () {
  const coords = getRandomCoords(DEFAULT_COORDS);
  const neighborsUrl = `${BASE_URL}/neighbors?x=${toFixed(coords.x)}&y=${toFixed(coords.y)}&z=${toFixed(coords.z)}&radius=${coords.radius}`;

  let neighborsPayload = null;
  const neighborsRes = http.get(neighborsUrl);
  check(neighborsRes, {
    'neighbors: status 200': (r) => r.status === 200,
    'neighbors: valid JSON': (r) => {
      neighborsPayload = safeJsonParse(r.body);
      return Array.isArray(neighborsPayload);
    },
  });

  rememberSystems(neighborsPayload);
  const chosenNeighbor = pickRandomNeighbor(neighborsPayload);
  const systemName = chosenNeighbor?.name || pickRandomSystem();
  const followupCoords = chosenNeighbor?.coords || coords;

  const endpoints = [
    {
      label: 'coords',
      url: `${BASE_URL}/coords?q=${encodeURIComponent(systemName)}`,
      validator: (r) => r.status === 200 && Boolean(safeJsonParse(r.body)),
    },
    {
      label: 'coords/predict',
      url: `${BASE_URL}/coords/predict?q=${encodeURIComponent(systemName)}`,
      validator: (r) => r.status === 200 || r.status === 404,
    },
    {
      label: 'bodies',
      url: `${BASE_URL}/bodies?name_or_id=${encodeURIComponent(systemName)}`,
      validator: (r) => r.status === 200 || r.status === 404,
    },
    {
      label: 'nearest-neutron-star',
      url: `${BASE_URL}/nearest-neutron-star?system_name=${encodeURIComponent(systemName)}`,
      validator: (r) => r.status === 200 || r.status === 404,
    },
    {
      label: 'nearest-neutron-star/coords',
      url: `${BASE_URL}/nearest-neutron-star/coords?x=${toFixed(followupCoords.x ?? coords.x)}&y=${toFixed(followupCoords.y ?? coords.y)}&z=${toFixed(followupCoords.z ?? coords.z)}`,
      validator: (r) => r.status === 200 || r.status === 404,
    },
    {
      label: 'stats/total-systems',
      url: `${BASE_URL}/stats/total-systems`,
      validator: (r) => r.status === 200 && Boolean(safeJsonParse(r.body)?.total_systems),
    },
  ];

  for (const step of endpoints) {
    const res = http.get(step.url);
    check(res, {
      [`${step.label}: ok`]: (r) => step.validator(r),
    });
  }

  sleep(1);
}
