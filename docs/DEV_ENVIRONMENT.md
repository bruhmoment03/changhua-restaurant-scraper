# Dev Environment Notes

## SentinelOne (Endpoint Security)

This project uses **SeleniumBase UC Mode** for browser automation, which may trigger
alerts from endpoint security software like SentinelOne.

### Known Behaviors

- SentinelOne may flag Chrome/Chromium processes spawned by SeleniumBase as suspicious.
- Automated browser sessions with unusual navigation patterns can trigger behavioral
  detection alerts.
- IT security teams (e.g. via Lark notifications) may receive alerts when the scraper
  is running.

### Recommendations

1. **Notify your IT team** before running the scraper on a managed device. Let them
   know this is an academic project using browser automation for data collection.
2. **Run in headless mode** (`headless: true` in `config.yaml`) to reduce the chance
   of triggering UI-based detection heuristics.
3. If alerts persist, consider running the scraper on a **personal/unmanaged device**
   or a **cloud VM** instead.
4. The `.chrome_profile/` directory is gitignored, but be aware it may contain
   session data that SentinelOne could inspect.

## Sensitive Files

The following files contain secrets and are **gitignored** — never commit them:

| File | Contents |
|------|----------|
| `.env` | `GOOGLE_PLACES_API_KEY` |
| `config.yaml` | Runtime config (may contain business-specific data) |
| `*.db` | SQLite databases with scraped review data |
| `.chrome_profile/` | Browser session data |
