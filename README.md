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


## Frontend
