#!/usr/bin/env python3
"""Build the permit-locked node mask for the experimental fullgraph engine.

uint8 array aligned 1:1 with the neutron index (1 = usable, 0 = permit-locked),
saved next to the sparse graph as neutron_permit_mask.npy. Folded into the
fullgraph search's `allowed` mask so it never routes through permit zones. Doing
this offline (not per-process at runtime) keeps it off the request path: ~5M
is_locked calls take ~9 s here but would stall the event loop under load.

Rebuild whenever the index snapshot or the permit definitions change.

Usage:
    build_permit_mask.py --index <neutron_index.npz> --out <neutron_permit_mask.npy>
"""
import argparse
import sys
import time

import numpy as np


def build(index_path, out_path):
    # permit_zones lives next to the app modules; import by running from there
    # or with the app dir on sys.path.
    import permit_zones

    z = np.load(index_path, allow_pickle=True)
    names = z["names_blob"].tobytes().decode("utf-8").split("\n")
    id64 = z["id64"]
    coords = z["coords"]
    n = len(coords)
    if len(names) != n:
        raise ValueError(f"names {len(names)} != coords {n}")
    t0 = time.time()
    mask = np.ones(n, dtype=np.uint8)
    locked = 0
    for i in range(n):
        if permit_zones.is_locked(names[i], id64=int(id64[i]), coords=coords[i]):
            mask[i] = 0
            locked += 1
    np.save(out_path, mask)
    print(f"built in {time.time() - t0:.1f}s: {locked} locked "
          f"({100 * locked / n:.2f}%) -> {out_path}", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--index", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--app-dir", default=".",
                    help="dir containing permit_zones.py (default cwd)")
    a = ap.parse_args()
    sys.path.insert(0, a.app_dir)
    build(a.index, a.out)
