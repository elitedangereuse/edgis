# EDGIS
[![Support EDGIS on Patreon](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Fshieldsio-patreon.vercel.app%2Fapi%3Fusername%3Delitedangereuse%26type%3Dpatrons&style=flat)](https://patreon.com/elitedangereuse)

## API
### Development Setup
#### EDTS module
Fetch the EDTS submodule once after cloning:

```bash
git submodule update --init --recursive
```

Then set up the virtual environment and dependencies:
#### Python dependancies
```bash
cd api
python3 -m venv .direnv
. .direnv/bin/activate
pip install -r requirements.txt
```

#### CORS
Populate your `.env` with database credentials and add a comma-separated `CORS_ORIGINS` entry for allowed browsers.
```
CORS_ORIGINS = https://example.com,https://example.fr
```
#### API port
Expose the development server on a predictable port by setting `UVICORN_PORT` (defaults to `8383`).
```
UVICORN_PORT=8383
```
#### Autocomplete tuning
Cap autocomplete query duration with `AUTOCOMPLETE_STATEMENT_TIMEOUT_MS` (first attempt) and optionally fall back to a slower retry with `AUTOCOMPLETE_TIMEOUT_RETRY_MS`.
```
AUTOCOMPLETE_STATEMENT_TIMEOUT_MS=300
AUTOCOMPLETE_TIMEOUT_RETRY_MS=1200
```
#### Run locally

```bash
edgis/api$ uvicorn systems:app --reload --host 0.0.0.0 --port $UVICORN_PORT
```

## Frontend
