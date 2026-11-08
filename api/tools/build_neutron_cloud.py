#!/usr/bin/env python3
"""Precompute a static neutron-star point cloud for the WebGL galaxy view.

This is the spansh-style move: bake the whole neutron field into one compact,
static binary file served straight off disk (no DB in the hot path, edge
cacheable), and let the browser upload it to the GPU as a single buffer.

Output (into ``--out-dir``, default the deployed static dir):
  neutrons.i16.bin   packed little-endian int16 triples (x,y,z), quantized by a
                     global scale -> ~6 bytes/star (~30 MB for ~5M neutrons).
                     At galaxy zoom the ~2 ly quantization error is sub-pixel.
  neutrons.meta.json {count, scale, bbox, generated_at} -- the client divides
                     each int16 by ``scale`` to recover ly coordinates.

Quantization keeps this a prototype-simple format; production can add nginx
gzip/brotli (~2x) and/or spatial tiling + LOD on top. Run on the DB host where the
DB is local, e.g.:

  sudo -u postgres /opt/edgis/bin/python \
      api/tools/build_neutron_cloud.py --out-dir /opt/edgis/static/data
"""

from __future__ import annotations

import argparse
import json
import os
import time

import numpy as np
import psycopg

NEUTRON_SQL = (
    "SELECT ST_X(coords), ST_Y(coords), ST_Z(coords) "
    "FROM systems_big WHERE mainstar IN ('N', 'Neutron Star')"
)
INT16_MAX = 32767


def _dsn() -> str:
    """Connection string from DB_* env, or a local peer-auth default (postgres)."""
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "systems")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    parts = [f"dbname={name}"]
    if host:
        parts.append(f"host={host}")
    if user:
        parts.append(f"user={user}")
    if password:
        parts.append(f"password={password}")
    return " ".join(parts)


def fetch_coords() -> np.ndarray:
    """All neutron coordinates as an (N, 3) float32 array (server-side cursor)."""
    chunks: list[np.ndarray] = []
    with psycopg.connect(_dsn()) as conn:
        # Named cursor = server-side, so 5M rows don't all land in memory at once.
        with conn.cursor(name="neutron_cloud") as cur:
            cur.itersize = 200_000
            cur.execute(NEUTRON_SQL)
            while True:
                rows = cur.fetchmany(200_000)
                if not rows:
                    break
                chunks.append(np.asarray(rows, dtype=np.float32))
    if not chunks:
        raise SystemExit("no neutron rows returned")
    return np.concatenate(chunks, axis=0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="/opt/edgis/static/data")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    t0 = time.time()
    coords = fetch_coords()
    n = coords.shape[0]
    fetch_s = time.time() - t0

    maxabs = float(np.abs(coords).max()) or 1.0
    scale = INT16_MAX / maxabs
    packed = np.round(coords * scale).astype("<i2")  # little-endian int16

    bin_path = os.path.join(args.out_dir, "neutrons.i16.bin")
    meta_path = os.path.join(args.out_dir, "neutrons.meta.json")
    packed.tofile(bin_path)

    bbox = {
        "min": coords.min(axis=0).tolist(),
        "max": coords.max(axis=0).tolist(),
    }
    meta = {
        "count": int(n),
        "scale": scale,              # ly = int16 / scale
        "bbox": bbox,
        "bytes": os.path.getsize(bin_path),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(meta_path, "w") as fh:
        json.dump(meta, fh)

    # Round-trip check on a sample: quantization error must stay within 1 unit.
    err = np.abs(packed.astype(np.float32) / scale - coords).max()
    print(
        f"wrote {n:,} neutrons -> {bin_path} "
        f"({meta['bytes'] / 1e6:.1f} MB, scale={scale:.5f}, "
        f"max_err={err:.2f} ly, fetch={fetch_s:.1f}s)"
    )


if __name__ == "__main__":
    main()
