"""Staging entrypoint for the EDGIS FastAPI app."""

import importlib
import os

os.environ.setdefault("UVICORN_PORT", "8384")
os.environ.setdefault("INDEX_HTML_FILENAME", "index-staging.html")

try:
    from .systems import app as _app  # type: ignore[attr-defined]
    from .systems import UVICORN_PORT as _PORT  # type: ignore[attr-defined]
except ImportError:  # Fallback for script-style execution (python systems_staging.py).
    systems_module = importlib.import_module("systems")
    _app = systems_module.app
    _PORT = systems_module.UVICORN_PORT

app = _app
UVICORN_PORT = _PORT

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=UVICORN_PORT)
