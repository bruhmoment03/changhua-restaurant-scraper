# Setup On Another Device

Use this when moving the Changhua restaurant scraper/dashboard to a second laptop or workstation.

## Prerequisites

- Python `3.10+`
- Node.js `18+` with `npm`
- Google Chrome
- A Google Places API key
- Google Maps cookie values if you want to scrape with `google_maps_auth_mode: cookie`

## Fast Path

```bash
git clone <your-fork-url>
cd google-reviews-scraper-pro
./setup
```

`./setup` does the local bootstrap work:

- creates `.venv/` if missing
- installs Python dependencies from `requirements.txt`
- installs dashboard dependencies in `dashboard/`
- copies `config.sample.yaml` to `config.yaml` when missing
- copies `.env.example` to `.env` when missing
- copies `.env.google_maps.cookies.example` to `.env.google_maps.cookies` when missing

## Files To Edit

1. `.env`
   Add `GOOGLE_PLACES_API_KEY=...` or `GOOGLE_MAPS_API_KEY=...`
2. `.env.google_maps.cookies`
   Add `GOOGLE_MAPS_COOKIE_1PSID` and `GOOGLE_MAPS_COOKIE_1PSIDTS` if you will scrape with cookie auth
3. `config.yaml` or `batch/config.top50.yaml`
   Point the runtime at the config you want to use on that device

The Changhua school workflow is already wired around [`batch/config.top50.yaml`](/Users/speedo/Documents/project/google-reviews-scraper-pro/batch/config.top50.yaml), which uses cookie-mode scraping.

## Start Everything

```bash
./dev
```

That starts:

- API server: `http://127.0.0.1:8000`
- Dashboard: `http://127.0.0.1:3000`
- API docs: `http://127.0.0.1:8000/docs`

`./dev` auto-loads `.env` and `.env.google_maps.cookies`.

## Quick Verification

```bash
./.venv/bin/python -m pytest -q
./.venv/bin/python start.py progress --config batch/config.top50.yaml
```

If the dashboard opens but discovery is disabled, the API key is missing. If scraping fails immediately in cookie mode, the Google Maps cookie values are missing or expired.
