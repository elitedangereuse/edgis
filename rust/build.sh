#!/bin/bash
# Build the native route kernel and drop the extension next to routing.py.
#
# Runtime needs ONLY the resulting api/edgis_route.abi3.so (no Rust toolchain).
# The .so is abi3 (Python 3.8+) but still glibc/arch-specific: build it on the
# TARGET platform (or a matching container) so it loads on the prod box. deploy.sh
# rsyncs the .so from the working tree, so just run this before deploying.
set -euo pipefail
cd "$(dirname "$0")/edgis_route"

cargo build --release
cp target/release/libedgis_route.so ../../api/edgis_route.abi3.so
echo "built -> api/edgis_route.abi3.so"

# Smoke test the extension imports and exposes the entry points.
python3 - <<'PY'
import sys
sys.path.insert(0, "../../api")
import edgis_route
assert hasattr(edgis_route, "graph_search_range")
print("ok: edgis_route", edgis_route.__version__)
PY
