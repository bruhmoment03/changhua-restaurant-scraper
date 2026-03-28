# Google Reviews Scraper Pro - Complete Project Documentation

> School Project: Changhua City Restaurant Review Collection & Analysis System
> Built on top of [google-reviews-scraper-pro](https://github.com/georgekhananaev/google-reviews-scraper-pro) with major extensions for discovery, dashboard, concurrency, and export.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [How Scraping Works (Step-by-Step)](#3-how-scraping-works-step-by-step)
4. [SeleniumBase UC Mode & Anti-Detection](#4-seleniumbase-uc-mode--anti-detection)
5. [Review Extraction from Google Maps DOM](#5-review-extraction-from-google-maps-dom)
6. [Deduplication & Change Detection](#6-deduplication--change-detection)
7. [Smart Stop Conditions](#7-smart-stop-conditions)
8. [Database Schema & Persistence](#8-database-schema--persistence)
9. [Concurrent Scraping System](#9-concurrent-scraping-system)
10. [Restaurant Discovery Pipeline](#10-restaurant-discovery-pipeline)
11. [Job Management System](#11-job-management-system)
12. [Export System](#12-export-system)
13. [Dashboard (Next.js)](#13-dashboard-nextjs)
14. [REST API Endpoints](#14-rest-api-endpoints)
15. [Configuration System](#15-configuration-system)
16. [CLI Commands Reference](#16-cli-commands-reference)
17. [Tech Stack & Dependencies](#17-tech-stack--dependencies)
18. [Development Environment Notes](#18-development-environment-notes)

---

## 1. Project Overview

### What This Project Does

This system automatically collects Google Maps restaurant reviews for Changhua City (彰化市). It:

1. **Discovers** restaurants via the Google Places API
2. **Scrapes** their reviews from Google Maps using browser automation
3. **Stores** everything in a local SQLite database with full audit history
4. **Exports** the data as Excel/CSV/JSON for analysis
5. **Monitors** everything through a real-time web dashboard

### Why We Built It

For academic research on restaurant review patterns in Changhua City, we need a large dataset of reviews with ratings, text, dates, and author information. Manually copying reviews is impractical — many restaurants have 100+ reviews, and we're tracking 80+ restaurants.

### Key Numbers

- **Target**: 80-100+ restaurants in Changhua City
- **Minimum reviews per restaurant**: 100 (our quality threshold)
- **Concurrent scrapers**: Up to 3 running simultaneously
- **Database**: SQLite with WAL mode for safe concurrent access
- **Dashboard**: Real-time Next.js web interface

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interface Layer                       │
│                                                               │
│  ┌──────────────────┐         ┌──────────────────────────┐   │
│  │   CLI (start.py) │         │  Dashboard (Next.js)     │   │
│  │   python start.py│         │  http://localhost:3000    │   │
│  │   scrape -j 3    │         │  Real-time monitoring    │   │
│  └────────┬─────────┘         └───────────┬──────────────┘   │
└───────────┼───────────────────────────────┼──────────────────┘
            │                               │
            │         HTTP REST API         │
            │                               │
┌───────────┴───────────────────────────────┴──────────────────┐
│                FastAPI Server (api_server.py)                  │
│                http://localhost:8000                           │
│                                                               │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐ │
│  │ Job Router  │  │ Places Router│  │ Discovery Router    │ │
│  │ /jobs/*     │  │ /places/*    │  │ /ops/discovery/*    │ │
│  └──────┬──────┘  └──────┬───────┘  └──────────┬──────────┘ │
│         │                │                      │             │
│  ┌──────┴──────┐  ┌──────┴───────┐  ┌──────────┴──────────┐ │
│  │ Export      │  │ Reviews      │  │ Validation          │ │
│  │ /exports/* │  │ /reviews/*   │  │ /ops/places/*       │ │
│  └─────────────┘  └──────────────┘  └─────────────────────┘ │
│                                                               │
│  Authentication: X-API-Key header (SHA256 hashed in DB)       │
│  Audit: All requests logged with timestamps & response times  │
│  CORS: Configurable allowed origins                           │
└──────────────────────────┬───────────────────────────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
│  Job Manager │  │   Scraper    │  │ Google Places    │
│  (Thread     │  │ (SeleniumBase│  │ API Service      │
│   Pool)      │  │  UC Mode)    │  │                  │
│              │  │              │  │ - Text Search    │
│ 3 workers    │  │ Anti-detect  │  │ - Place Details  │
│ Job dedup    │  │ Chrome auto  │  │ - Validation     │
│ Auto-promote │  │ Scroll loop  │  │                  │
└──────┬───────┘  └──────┬───────┘  └──────────────────┘
       │                 │
       └─────────────────┤
                         │
                         ▼
          ┌────────────────────────┐
          │   SQLite Database      │
          │   (WAL mode)           │
          │                        │
          │ Tables:                │
          │  - places              │
          │  - reviews             │
          │  - scrape_sessions     │
          │  - review_history      │
          │  - place_aliases       │
          │  - discovery_candidates│
          │  - api_keys            │
          │  - api_audit_log       │
          └────────────────────────┘
```

### How `./dev` Starts Everything

The `./dev` script (calls `tools/dev_dashboard.sh`) starts two services:

1. **API Server** (port 8000): `python3 -m uvicorn api_server:app --host 127.0.0.1 --port 8000 --reload`
2. **Dashboard** (port 3000): `npm run dev` inside `dashboard/`

It auto-detects if either service is already running and reuses it. Environment variables are loaded from `.env` and `.env.google_maps.cookies` if present.

---

## 3. How Scraping Works (Step-by-Step)

This is the complete flow from "user clicks scrape" to "reviews in database":

### Step 1: Job Creation

```
User action: Click "Queue RPA Scrape" on dashboard
    → POST /ops/scrape-all
    → API reads config.yaml for restaurant URLs
    → For each restaurant below the review threshold:
        → JobManager.create_job(url, config_overrides)
        → Dedup check: skip if same restaurant already queued/running
        → Job enters PENDING state
```

### Step 2: Job Execution

```
JobManager auto-promotes PENDING → RUNNING (up to 3 concurrent)
    → ThreadPoolExecutor.submit(_run_scraping_job)
    → Each job runs in its own thread with its own Chrome browser
```

### Step 3: Browser Setup

```python
# Inside GoogleReviewsScraper.scrape():

1. setup_driver(headless=True/False)
   → SeleniumBase Driver(uc=True, incognito=True)
   → Automatic Chrome/ChromeDriver version matching
   → Anti-detection patches applied at protocol level
   → Optional: inject Google auth cookies for authenticated browsing

2. Browser window created (~300-500MB RAM per instance)
```

### Step 4: Navigation to Restaurant Page

```python
3. navigate_to_place(driver, url)

   Strategy A: Direct navigation
   → driver.get("https://www.google.com/maps/place/...")
   → Wait for reviews tab to appear (20s timeout)
   → If success: proceed to Step 5

   Strategy B: Search-based fallback (if direct nav hits search results)
   → Detect search results page via DOM markers
   → Find matching restaurant in search results
   → Click to open restaurant detail
   → Wait for reviews tab

   Strategy C: Limited-view detection
   → Google sometimes shows a restricted view
   → If fail_on_limited_view=true: abort with error
   → If false: attempt to proceed anyway
   → Optional: capture debug screenshots for analysis
```

### Step 5: Reviews Tab & Sort

```python
4. click_reviews_tab(driver)
   → Find reviews tab using multi-language CSS selectors
   → Supports: English, Chinese, Thai, Hebrew, German, etc.
   → Wait for review cards to appear in DOM

5. set_sort(driver, "newest")
   → Click sort dropdown button
   → Select "Newest" from menu
   → Wait for reviews to re-render with new sort order
   → Confirm sort was applied (check aria-selected attribute)
```

### Step 6: The Scroll Loop (Core of Scraping)

This is where reviews are actually collected:

```python
while attempts < max_scroll_attempts:
    # 1. Find all visible review cards in the scrollable pane
    cards = pane.find_elements(By.CSS_SELECTOR, "div[data-review-id]")

    # 2. For each card, extract review data
    for card in cards:
        raw = RawReview.from_card(card)
        # Extracts: author, rating, text, date, likes, photos, profile URL

        # 3. Generate SHA1 fingerprint for deduplication
        fingerprint = SHA1(author + rating + text + date + profile)

        # 4. Skip if already processed in this session
        if fingerprint in processed_fingerprints:
            continue

        # 5. Check against database
        if review_id in seen_ids:
            batch_seen_count += 1  # Already in DB
        else:
            fresh_raws.append(raw)  # New review!

    # 6. Store new reviews in SQLite
    for raw in fresh_raws:
        result = review_db.upsert_review(place_id, review_dict, session_id)
        # result = "new" | "updated" | "restored" | "unchanged"

    # 7. Check stop conditions
    if consecutive_matched_batches >= stop_threshold:
        break  # All reviews in last N batches were already in DB
    if idle >= scroll_idle_limit:
        break  # No new reviews found for N scrolls
    if max_reviews > 0 and new_count >= max_reviews:
        break  # Hit the per-session review limit

    # 8. Scroll down to load more reviews
    scroll_reviews_forward(driver, scroll_target)

    # 9. Smart wait: poll DOM for new cards instead of fixed sleep
    WebDriverWait(driver, 1.5).until(
        lambda d: len(cards_now) > len(cards_before)
    )
    # Falls back to timeout if no new cards (same end behavior, faster on avg)
```

### Step 7: Post-Scrape Cleanup

```python
# After scroll loop exits:

1. Record scrape session results in scrape_sessions table
   → reviews_found, reviews_new, reviews_updated, status

2. Reconcile stale reviews
   → If Google says restaurant has 150 reviews but DB has 200
   → Some may have been deleted by authors
   → Mark missing reviews as potentially stale

3. Update place metadata
   → total_reviews count
   → last_scraped timestamp
   → reviews_exhausted flag (if all reviews collected)

4. Close browser
   → driver.quit()
   → Chrome process terminated
   → Temp profile directory cleaned up

5. Job status → COMPLETED or FAILED
   → JobManager auto-promotes next PENDING job
```

---

## 4. SeleniumBase UC Mode & Anti-Detection

### What is UC Mode?

UC (Undetectable Chrome) Mode is SeleniumBase's built-in anti-bot-detection system. Google Maps actively tries to detect and block automated browsers. UC Mode counters this through:

### Anti-Detection Techniques

| Technique | What It Does |
|-----------|-------------|
| **WebDriver property masking** | Removes `navigator.webdriver = true` flag that Chrome normally sets |
| **Chrome DevTools Protocol patches** | Hides automation indicators at the CDP level |
| **Automatic version matching** | Downloads the exact ChromeDriver version for your Chrome — prevents version mismatch detection |
| **JavaScript context modification** | Patches `window.chrome.runtime` and other fingerprinting vectors |
| **Incognito mode** | Each session uses a fresh profile with no history/cookies that could identify it as a bot |
| **Optional custom User-Agent** | Can override the default UA string to match a specific browser version |
| **Optional stealth_undetectable** | Extra aggressive anti-detection for stricter sites |

### Authentication Modes

**Anonymous Mode** (default):
- No Google account cookies
- Google Maps shows public data
- Lower risk of detection
- Some features may be limited (e.g., "limited view")

**Cookie Mode**:
- Injects real Google account cookies from environment variables
- Required cookies: `__Secure-1PSID`, `__Secure-1PSIDTS`
- Optional: `SID`, `HSID`, `SSID`, `__Secure-1PAPISID`
- Gets full access to all reviews
- Higher risk: Google can track session patterns
- `fail_on_limited_view` auto-set to `true` in cookie mode

### Driver Configuration

```python
driver = Driver(
    uc=True,           # Enable UC Mode
    incognito=True,    # Fresh profile per session
    headless=True,     # No visible window (faster)
    # Chrome binary auto-detected for macOS/Windows
    # ChromeDriver auto-downloaded and version-matched
)
```

---

## 5. Review Extraction from Google Maps DOM

### How Reviews Appear in HTML

Google Maps renders reviews as DOM elements inside a scrollable panel. Each review card has a `data-review-id` attribute:

```html
<div data-review-id="ChdDSU...">
  <div class="d4r55">Author Name</div>
  <span role="img" aria-label="4 stars">★★★★☆</span>
  <span class="rsqaWe">2 weeks ago</span>
  <span class="wiI7pd">The food was amazing! Great service...</span>
  <button class="...">👍 3</button>
  <!-- Optional: photos, owner response -->
</div>
```

### CSS Selectors Used

| Data | Selector | Notes |
|------|----------|-------|
| **Review container** | `div[data-review-id]` or `div.jftiEf` | Primary identifier for each review |
| **Author name** | `div[class*="d4r55"]` | Author display name |
| **Star rating** | `span[role="img"][aria-label*="star"]` | Parsed from aria-label (e.g., "4 stars" → 4.0) |
| **Date** | `span[class*="rsqaWe"]` | Relative date ("2 weeks ago", "一個月前") |
| **Review text** | `span[jsname="bN97Pc"]`, `span[jsname="fbQN7e"]`, `div.MyEned span.wiI7pd` | Multiple fallback selectors |
| **Likes count** | `button[jsaction*="toggleThumbsUp"]` | Parsed from aria-label |
| **Photos** | `button.Tya61d` + `style="background-image:url(...)"` | Review photo URLs |
| **Profile URL** | `button[data-review-id][data-href]` | Link to reviewer's profile |
| **Profile avatar** | `button[data-review-id] img[src]` | Avatar image URL |
| **Owner response** | `div.CDe7pd` | Restaurant owner's reply text |
| **"More" button** | `button.kyuRq` | Expands truncated review text |

### Multi-Language Support

The scraper handles reviews in 25+ languages. Key challenges:

- **Date parsing**: "2 weeks ago" (English), "2 週前" (Chinese), "vor 2 Wochen" (German) — all parsed to ISO dates
- **Tab labels**: "Reviews" tab may be labeled differently per locale — scraper tries multiple selectors
- **Text encoding**: UTF-8 throughout, `ensure_ascii=False` in JSON serialization

### The "More" Button Problem

Google truncates long reviews with a "More" button. The scraper:
1. Finds all `button.kyuRq` elements on the page
2. Clicks each one to expand the full text
3. Re-extracts the text after expansion
4. This happens non-blockingly (errors are caught and skipped)

---

## 6. Deduplication & Change Detection

### Why Deduplication Matters

When scrolling through reviews, the same cards appear multiple times:
- Google lazy-loads reviews — previous cards stay visible as you scroll
- Re-scraping a restaurant shows reviews already in the database
- Multiple scrape sessions may overlap

### SHA1 Fingerprinting

Each review gets a **semantic fingerprint** — a SHA1 hash of its core identity:

```python
fingerprint = SHA1(json.dumps({
    "author": normalize(author),
    "text": normalize(review_text),
    "rating": rating,
    "date": normalize(date),
    "profile": profile_url,
}, sort_keys=True))
```

This fingerprint stays stable even if:
- Google changes the review's internal ID
- The review card's DOM structure changes
- Whitespace or formatting varies slightly

### Content Hash (for Change Detection)

Separate from the fingerprint, each review also has a **content hash** for detecting edits:

```python
content_hash = SHA256(json.dumps({
    "text": review_text,
    "rating": rating,
    "author": author,
}, sort_keys=True))
```

When re-scraping:
- Same content_hash → "unchanged" (no DB write needed)
- Different content_hash → "updated" (merge new data)
- Review was soft-deleted but reappears → "restored"

### Optimistic Locking

When multiple scraper threads write to the same DB, race conditions are possible. The `row_version` column prevents lost updates:

```sql
UPDATE reviews
SET review_text = ?, rating = ?, row_version = row_version + 1
WHERE review_id = ? AND place_id = ? AND row_version = ?
-- If rowcount = 0: another thread modified it first → retry
```

---

## 7. Smart Stop Conditions

The scraper doesn't scroll forever. It uses multiple strategies to know when to stop:

### 1. Batch Matching (Early Stop)

```
Scroll iteration 1: Found 10 reviews, 2 new, 8 already in DB
Scroll iteration 2: Found 10 reviews, 0 new, 10 already in DB ← matched batch #1
Scroll iteration 3: Found 10 reviews, 0 new, 10 already in DB ← matched batch #2
Scroll iteration 4: Found 10 reviews, 0 new, 10 already in DB ← matched batch #3
→ STOP: 3 consecutive fully-matched batches (stop_threshold=3)
```

This is the fastest stop — it means we've reached reviews already collected in a previous session.

### 2. Scroll Idle Limit

If we keep scrolling but find zero new review cards (not even ones already in DB), we've probably reached the end:

```
Scroll 1: 0 new cards (idle=1)
Scroll 2: 0 new cards (idle=2)
...
Scroll 15: 0 new cards (idle=15)
→ STOP: scroll_idle_limit reached (default: 15)
```

### 3. Max Reviews Limit

Hard cap on reviews per session:

```
max_reviews=300 in config
After collecting 300 new reviews → STOP
```

### 4. Max Scroll Attempts

Hard cap on scroll iterations:

```
max_scroll_attempts=50 in config
After 50 scroll iterations regardless of results → STOP
```

### 5. Exhaustion Detection

If the scroll position stops changing (stuck):

```
scroll_position = 4500px
scroll_position = 4500px (stuck_count=1)
scroll_position = 4500px (stuck_count=2)
...
→ Alternative scroll methods attempted
→ If still stuck after 5+ iterations → STOP
→ Mark restaurant as "reviews_exhausted" in DB
```

### 6. Update Mode Early Exit

In `scrape_mode=update` (default), if we haven't found any new reviews after several scrolls:

```
zero_new_idle_limit reached AND session_new_ids is empty
→ STOP: all reviews are already up to date
```

---

## 8. Database Schema & Persistence

### SQLite with WAL Mode

We use SQLite in **WAL (Write-Ahead Logging)** mode for safe concurrent access:

```sql
PRAGMA journal_mode=WAL;     -- Allows concurrent readers + one writer
PRAGMA busy_timeout=30000;   -- Wait up to 30s for write lock
PRAGMA foreign_keys=ON;      -- Enforce referential integrity
```

### Core Tables

#### `places` — Restaurant Registry

| Column | Type | Description |
|--------|------|-------------|
| `place_id` | TEXT PK | Google Maps place identifier |
| `place_name` | TEXT | Restaurant name |
| `original_url` | TEXT | Google Maps URL from config |
| `resolved_url` | TEXT | Final URL after redirects |
| `latitude` | REAL | GPS latitude |
| `longitude` | REAL | GPS longitude |
| `first_seen` | TEXT | When first added to DB |
| `last_scraped` | TEXT | Last successful scrape timestamp |
| `total_reviews` | INTEGER | Count of active (non-deleted) reviews |
| `reviews_exhausted` | INTEGER | 1 if all available reviews collected |
| `exhausted_at` | TEXT | When exhaustion was detected |
| `validation_status` | TEXT | "valid", "invalid_closed", "unknown", etc. |
| `validation_checked_at` | TEXT | Last validation timestamp |
| `validation_reason` | TEXT | Why validation failed (if applicable) |

#### `reviews` — Individual Review Data

| Column | Type | Description |
|--------|------|-------------|
| `review_id` | TEXT | Google's review identifier |
| `place_id` | TEXT FK | Which restaurant this review belongs to |
| `author` | TEXT | Reviewer's display name |
| `rating` | REAL | Star rating (1.0 - 5.0) |
| `review_text` | TEXT | JSON dict of text by language |
| `review_date` | TEXT | Parsed ISO date |
| `raw_date` | TEXT | Original date string ("2 weeks ago") |
| `likes` | INTEGER | Thumbs-up count |
| `user_images` | TEXT | JSON list of photo URLs |
| `s3_images` | TEXT | JSON list of S3-uploaded URLs |
| `profile_url` | TEXT | Link to reviewer's Google profile |
| `profile_picture` | TEXT | Avatar image URL |
| `owner_responses` | TEXT | JSON of restaurant owner's reply |
| `created_date` | TEXT | When first scraped |
| `last_modified` | TEXT | Last change timestamp |
| `last_seen_session` | INTEGER | Last scrape session that saw this review |
| `last_changed_session` | INTEGER | Last session that modified this review |
| `is_deleted` | INTEGER | Soft delete flag (0=active, 1=deleted) |
| `content_hash` | TEXT | SHA256 for change detection |
| `row_version` | INTEGER | Optimistic locking version counter |

**Primary Key**: `(review_id, place_id)` — composite, since the same review ID could theoretically appear across places.

#### `scrape_sessions` — Audit Trail

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | INTEGER PK | Auto-increment session ID |
| `place_id` | TEXT FK | Target restaurant |
| `action` | TEXT | "scrape" (default) |
| `started_at` | TEXT | Session start timestamp |
| `completed_at` | TEXT | Session end timestamp |
| `status` | TEXT | "running", "completed", "failed" |
| `reviews_found` | INTEGER | Total cards processed |
| `reviews_new` | INTEGER | New reviews inserted |
| `reviews_updated` | INTEGER | Existing reviews updated |
| `sort_by` | TEXT | Sort order used |
| `error_message` | TEXT | Error details (if failed) |

#### `review_history` — Change Log

Every modification to a review is logged:

| Column | Type | Description |
|--------|------|-------------|
| `history_id` | INTEGER PK | Auto-increment |
| `review_id` | TEXT | Which review changed |
| `place_id` | TEXT | Which restaurant |
| `event_type` | TEXT | "created", "updated", "restored", "deleted" |
| `old_values` | TEXT | JSON snapshot of previous state |
| `new_values` | TEXT | JSON snapshot of new state |
| `changed_at` | TEXT | Timestamp |
| `session_id` | INTEGER | Which scrape session caused it |

#### `discovery_candidates` — Staged Discoveries

| Column | Type | Description |
|--------|------|-------------|
| `candidate_id` | INTEGER PK | Auto-increment |
| `google_place_id` | TEXT | Google's place identifier |
| `name` | TEXT | Restaurant name from API |
| `formatted_address` | TEXT | Full address |
| `rating` | REAL | Google rating |
| `user_ratings_total` | INTEGER | Total ratings count on Google |
| `status` | TEXT | "staged", "approved", "rejected", "duplicate_config", "duplicate_db" |
| `duplicate_source` | TEXT | Where the duplicate was found |
| `updated_at` | TEXT | Last status change |

#### `place_aliases` — URL Deduplication

Maps different URL formats to the same canonical place:

```
alias: "maps/place/Restaurant+Name/..."  →  canonical: "ChIJ..."
alias: "maps?cid=12345"                  →  canonical: "ChIJ..."
```

### Soft Deletes & Resurrection

Reviews are never physically deleted. Instead:

```sql
-- Soft delete (hide):
UPDATE reviews SET is_deleted = 1 WHERE review_id = ? AND place_id = ?

-- Restore:
UPDATE reviews SET is_deleted = 0 WHERE review_id = ? AND place_id = ?

-- Normal queries exclude deleted:
SELECT * FROM reviews WHERE is_deleted = 0 AND place_id = ?
```

If a deleted review reappears in a later scrape, it's automatically **restored** (status = "restored").

---

## 9. Concurrent Scraping System

### Why Concurrency?

Sequential scraping of 83 restaurants at 1-5 minutes each = **1.5 to 7 hours**. With 3 concurrent scrapers, this drops to **30 minutes to 2.5 hours**.

### How It Works

```
┌─────────────────────────────────────────────────┐
│              ThreadPoolExecutor                   │
│              max_workers = 3                      │
│                                                   │
│  Thread 1          Thread 2          Thread 3     │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐ │
│  │ Chrome   │     │ Chrome   │     │ Chrome   │ │
│  │ Instance │     │ Instance │     │ Instance │ │
│  │          │     │          │     │          │ │
│  │ Scraping │     │ Scraping │     │ Scraping │ │
│  │ 麥味鄉    │     │ 阿璋肉圓  │     │ 貓鼠麵   │ │
│  └────┬─────┘     └────┬─────┘     └────┬─────┘ │
│       │                │                │        │
│       └────────────────┼────────────────┘        │
│                        │                          │
│                        ▼                          │
│               SQLite DB (WAL mode)                │
│               Concurrent reads OK                 │
│               Writes serialized with              │
│               30s busy_timeout                    │
└───────────────────────────────────────────────────┘
```

### Isolation Mechanisms

1. **Separate Chrome processes**: Each thread spawns its own Chrome browser with an isolated incognito profile (no shared cookies/cache)
2. **No shared user-data-dir**: When concurrency > 1, `chrome_user_data_dir` is forced to `None`
3. **Staggered starts**: Jobs start 5 seconds apart to avoid simultaneous Google hits
4. **Thread-safe job dict**: All job state access protected by `threading.Lock`
5. **SQLite WAL mode**: Multiple readers can run alongside one writer, `busy_timeout=30000ms`

### CLI Usage

```bash
# Default: 3 concurrent scrapers
python3 start.py scrape

# Explicit concurrency
python3 start.py scrape -j 3

# Sequential mode (one at a time)
python3 start.py scrape -j 1

# Maximum (4 concurrent)
python3 start.py scrape -j 4
```

### Resource Usage

| Concurrent Jobs | RAM Usage | CPU Usage | Notes |
|----------------|-----------|-----------|-------|
| 1 | ~300-500MB | Low | Safe, slowest |
| 2 | ~600MB-1GB | Medium | Good balance |
| 3 (default) | ~900MB-1.5GB | Medium-High | Recommended |
| 4 (max) | ~1.2-2GB | High | May trigger rate limiting |

---

## 10. Restaurant Discovery Pipeline

### Overview

Before scraping, we need to know *which* restaurants to scrape. The discovery pipeline finds restaurants in Changhua City via the Google Places API.

### Workflow

```
Step 1: Search                    Step 2: Review                    Step 3: Approve
┌────────────────────┐           ┌────────────────────┐           ┌────────────────────┐
│ User enters query: │           │ Dashboard shows    │           │ User selects       │
│ "restaurants in    │  ──API──▶ │ candidates with:   │  ──UI──▶  │ restaurants and    │
│  Changhua City"    │           │ • Name             │           │ clicks "Approve"   │
│                    │           │ • Rating           │           │                    │
│ Filters:           │           │ • Total ratings    │           │ Approved ones get  │
│ • Min 100 ratings  │           │ • Address          │           │ added to config    │
│ • Any rating       │           │ • Status (staged/  │           │ and become         │
│ • Up to 200 places │           │   duplicate/etc)   │           │ scrape targets     │
└────────────────────┘           └────────────────────┘           └────────────────────┘
```

### Google Places API Integration

**Text Search endpoint**: `https://maps.googleapis.com/maps/api/place/textsearch/json`

```python
# Search request:
{
    "query": "restaurants in Changhua City",
    "key": GOOGLE_PLACES_API_KEY,
    "language": "zh-TW",
    "region": "tw"
}

# Each result contains:
{
    "place_id": "ChIJ...",
    "name": "麥味鄉",
    "formatted_address": "彰化縣彰化市...",
    "rating": 4.2,
    "user_ratings_total": 347,
    "geometry": { "location": { "lat": 24.08, "lng": 120.54 } }
}
```

**Pagination**: API returns ~20 results per page. The system follows `next_page_token` (with 2s delay between pages) until the limit is reached.

### Deduplication Logic

When new candidates are discovered, they're checked against:

1. **Existing config**: Is this `google_place_id` already in our YAML config?
   → Status: `duplicate_config`
2. **Existing database**: Is this place already in our `places` table?
   → Status: `duplicate_db`
3. **Other candidates**: Is there another candidate with the same normalized name?
   → Keep the one with higher `user_ratings_total`
4. **None of the above**: New discovery!
   → Status: `staged` (ready for review)

### Candidate Statuses

| Status | Meaning |
|--------|---------|
| `staged` | New discovery, awaiting user review |
| `approved` | User approved, added to config for scraping |
| `rejected` | User rejected, won't be scraped |
| `duplicate_config` | Already exists in config.yaml |
| `duplicate_db` | Already exists in database |
| `with_reviews` | Already has reviews in DB |

### Place Validation

After discovery, places can be validated via the Google Places **Details API**:

```python
# Checks:
1. Place still exists (not removed from Google Maps)
2. Not permanently closed (business_status != "CLOSED_PERMANENTLY")
3. Name matches expectations (no place_id redirect)

# Results:
- "valid": All checks passed
- "invalid_not_found": Place doesn't exist
- "invalid_closed": Permanently closed
- "invalid_mismatch": Name or ID changed
- "error": API error
```

---

## 11. Job Management System

### Job Lifecycle

```
                    create_job()
                        │
                        ▼
                   ┌─────────┐
                   │ PENDING  │ ◄─── Waiting for worker slot
                   └────┬────┘
                        │ start_job() or auto-promote
                        ▼
                   ┌─────────┐
                   │ RUNNING  │ ◄─── Chrome is scraping
                   └────┬────┘
                        │
              ┌─────────┼─────────┐
              │         │         │
              ▼         ▼         ▼
         ┌─────────┐ ┌──────┐ ┌──────────┐
         │COMPLETED│ │FAILED│ │CANCELLED │
         └─────────┘ └──────┘ └──────────┘
```

### Job Deduplication

The system prevents scraping the same restaurant twice simultaneously:

```python
# Target key extraction (priority order):
1. query_place_id from URL parameter → "qpid:ChIJ..."
2. /maps/place/{name} from URL path  → "place:restaurant_name"
3. Full normalized URL                → "url:https://..."

# When creating a job:
if existing_job_with_same_target_key in (PENDING, RUNNING):
    return existing_job.job_id  # Skip, reuse existing
```

### Retry Logic

Transient failures (network issues, browser crashes) are automatically retried:

```
Attempt 1: Scrape → FAILED (err_internet_disconnected)
    → Wait 5 seconds
Attempt 2: Scrape → FAILED (timeout)
    → Wait 15 seconds
Attempt 3: Scrape → SUCCESS ✓
```

**Transient error markers**: `err_internet_disconnected`, `invalid session id`, `timeout`, `chrome not reachable`, `no such window`

**Non-transient (no retry)**: `limited view` errors — these indicate Google blocking, not a fixable error.

### Auto-Promotion

When a running job finishes, the JobManager automatically starts the next pending job:

```python
def _promote_pending_jobs():
    if running_count < max_concurrent_jobs:
        oldest_pending_job = sorted(pending_jobs, key=created_at)[0]
        if not duplicate_already_running(oldest_pending_job):
            start_job(oldest_pending_job)
```

---

## 12. Export System

### Supported Formats

| Format | Best For | Structure |
|--------|----------|-----------|
| **XLSX** | Opening in Excel, sharing | Multi-sheet workbook: summary + reviews |
| **CSV** | Data analysis tools, Python/R | Flat file, all columns |
| **JSON** | API consumption, programming | Nested structure with metadata |

### Export Dialog (Dashboard)

The dashboard provides a popup dialog with options:

- **Format**: Excel (.xlsx) / JSON / CSV
- **Sheet Name**: Custom name for the XLSX sheet (max 31 chars)
- **Exclude star-only reviews**: Filter out reviews with ratings but no text content
- **Include deleted reviews**: Include soft-deleted reviews in export

### XLSX Structure

**Single Place Export**:
- Sheet 1: `summary` — place metadata (name, ID, last scraped, total reviews, etc.)
- Sheet 2: `{place_name}` or custom name — all reviews as rows

**All Places Export**:
- Sheet 1: `index` — list of all places with place_id, name, and review count
- Sheet 2+: One sheet per place, named after the restaurant

### CSV Column Mapping (30 columns)

```
place_id, place_name, review_id, author, rating,
review_text_primary, review_text_all_json,
review_date, raw_date, likes, profile_url,
is_deleted, created_date, last_modified,
last_seen_session, last_changed_session,
owner_responses_json, user_images_json, s3_images_json,
source_url, resolved_place_url,
scrape_session_id, scrape_started_at, scrape_completed_at,
scrape_mode, google_maps_auth_mode,
sort_order_requested, sort_order_confirmed,
extraction_confidence, source_locale
```

### Star-Only Review Filtering

Some reviews only have a star rating (1-5 stars) but no text content. These can be excluded:

```python
# Filter logic:
def _filter_empty_text(rows):
    return [r for r in rows if r.get("review_text_primary", "").strip()]
```

In the dashboard review list, star-only reviews are visually flagged with a yellow "star-only" badge.

---

## 13. Dashboard (Next.js)

### Technology

- **Framework**: Next.js 15 (React 19, App Router)
- **Styling**: Tailwind CSS with custom dark theme
- **Language**: TypeScript
- **API Client**: Custom fetch wrapper in `lib/api.ts`

### Pages

#### Home (`/`) — Operations Dashboard

The main control center with:

- **Stat Cards** (top row): In Config, In Database, Goal Met, Still Scraping, Exhausted, Awaiting Approval
- **Scrape Settings**: Min reviews goal, reviews per run, only-below-threshold toggle
- **Discovery Staging**: Search Google Places API, review/approve/reject candidates, queue for scraping
- **Validation Results**: Results from Google Places API validation
- **Data Health**: Stale totals, conflict groups, invalid archive
- **Job Monitor**: Real-time job status (pending/running/completed/failed)
- **Queue-Eligible Targets**: Restaurants that still need more reviews
- **Exhausted Under Threshold**: Restaurants where Google has no more reviews available
- **Recent Errors**: Last 20 error log entries

#### Places (`/places`) — Restaurant List

Table view of all restaurants with:
- Search and filter by name, place_id, validation status
- Status filters: all, active, out-of-scope, under-threshold, exhausted, invalid
- Per-place actions: Validate, Archive, Scrape, Restore
- Bulk export button

#### Place Detail (`/places/[placeId]`) — Single Restaurant

Detailed view with:
- Summary: last scraped, review counts, validation status
- Paginated review list (20 per page) with "star-only" badges
- Review Inspector: selected review's full details and raw JSON
- Export button (opens ExportDialog)

#### Logs (`/logs`) — Log Viewer

Real-time structured log viewer with level filtering.

### Dashboard Features

- **Auto-refresh**: Polls API every 5 seconds for job status updates
- **Persistent settings**: Scrape settings saved to localStorage
- **Responsive**: Works on desktop and tablet screens
- **Color-coded badges**: Green (good), yellow (warning), red (error)

---

## 14. REST API Endpoints

### Grouped by Router

#### System Routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/db-stats` | Database statistics (places, reviews, sessions, size) |
| GET | `/system/log-tail` | Recent log entries (with level filter) |
| GET | `/system/data-health/summary` | Data health metrics (stale totals, conflicts) |
| GET | `/system/data-quality/conflicts` | Review deduplication conflict details |

#### Job Routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/jobs` | List all jobs (filter by status, limit) |
| GET | `/jobs/{job_id}` | Get job details and progress |
| POST | `/jobs/{job_id}/cancel` | Cancel a running/pending job |

#### Operations Routes

| Method | Path | Description |
|--------|------|-------------|
| POST | `/ops/scrape-all` | Queue scrape jobs for all config targets |
| POST | `/ops/scrape-target` | Queue single target by google_place_id |
| POST | `/ops/scrape-targets` | Queue multiple targets by google_place_id list |
| POST | `/ops/places/validate` | Validate places via Google Places API |
| POST | `/ops/places/archive-invalid` | Move invalid places to archive |
| GET | `/ops/places/invalid-archive` | List archived invalid places |
| POST | `/ops/discovery/search` | Search Google Places for new restaurants |
| GET | `/ops/discovery/candidates` | List discovery candidates |
| POST | `/ops/discovery/approve` | Approve selected candidates |
| POST | `/ops/discovery/reject` | Reject selected candidates |
| POST | `/ops/maintenance/rebuild-place-totals` | Recalculate cached review counts |

#### Places & Reviews Routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/places` | List all places in database |
| GET | `/places/{place_id}` | Get single place details |
| GET | `/reviews/{place_id}` | Paginated reviews for a place |
| GET | `/reviews/{place_id}/{review_id}` | Single review details |
| GET | `/reviews/{place_id}/{review_id}/history` | Review change history |

#### Export Routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/exports/places/{place_id}` | Export single place (xlsx/json/csv) |
| GET | `/exports/all` | Export all places |

Query parameters: `format`, `include_deleted`, `exclude_empty_text`, `sheet_name`

#### Audit Routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/audit-log` | Query API request audit trail |

### Authentication

Optional API key authentication via `X-API-Key` header:

```bash
# Create key:
python3 start.py api-key-create "my-key-name"
# Output: grs_a1b2c3d4e5f6... (shown once, store securely)

# Use in requests:
curl -H "X-API-Key: grs_a1b2c3d4e5f6..." http://localhost:8000/places
```

Keys are SHA256-hashed in the database. All API requests are logged in the audit trail.

---

## 15. Configuration System

### Config File (`config.yaml`)

```yaml
# ── Scraper Behavior ──
headless: true                    # Run Chrome without visible window
sort_by: newest                   # Review sort order
scrape_mode: update               # "new_only" | "update" | "full"
stop_threshold: 3                 # Consecutive matched batches before stopping
max_reviews: 25                   # Reviews per scrape session (0=unlimited)
max_scroll_attempts: 50           # Max scroll iterations
scroll_idle_limit: 15             # Max idle scrolls before stopping

# ── Anti-Detection ──
google_maps_auth_mode: anonymous  # "anonymous" | "cookie"
fail_on_limited_view: false       # Abort on limited view detection
stealth_undetectable: false       # Extra anti-detection features

# ── Database ──
db_path: reviews.db               # SQLite file path

# ── Date Processing ──
convert_dates: true               # Parse "2 weeks ago" → ISO dates

# ── Image Handling ──
download_images: false            # Download review/profile images
image_dir: review_images          # Local storage directory
download_threads: 4               # Parallel download threads

# ── JSON Backup ──
backup_to_json: true              # Export after each scrape
json_path: google_reviews.json    # Backup file path

# ── Target Restaurants ──
businesses:
  - url: https://www.google.com/maps/place/...
    custom_params:
      company: "麥味鄉"
      address: "彰化縣彰化市..."
      source: Google Maps
      google_place_id: "ChIJ..."

# ── API Server ──
api:
  allowed_origins: "*"            # CORS configuration
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `GOOGLE_PLACES_API_KEY` or `GOOGLE_MAPS_API_KEY` | Google Places API key for discovery |
| `SCRAPER_MAX_CONCURRENT_JOBS` | Override concurrent job limit (default: 1) |
| `ALLOWED_ORIGINS` | Override CORS origins |
| `GOOGLE_MAPS_COOKIE_1PSID` | Google auth cookie (for cookie mode) |
| `GOOGLE_MAPS_COOKIE_1PSIDTS` | Google auth cookie timestamp |

---

## 16. CLI Commands Reference

```bash
# ── Scraping ──
python3 start.py scrape                      # Scrape all configured restaurants
python3 start.py scrape -j 3                 # 3 concurrent scrapers
python3 start.py scrape -q                   # Headless mode
python3 start.py scrape --url "https://..."  # Scrape single URL
python3 start.py scrape --only-missing       # Only scrape restaurants not yet in DB
python3 start.py scrape --max-businesses 10  # Limit to first 10 restaurants

# ── Progress & Stats ──
python3 start.py progress                    # Show scraping progress
python3 start.py progress --json             # Machine-readable output
python3 start.py db-stats                    # Database statistics

# ── Export ──
python3 start.py export --format json        # Export all as JSON
python3 start.py export --format csv --place-id "ChIJ..."  # Single place CSV
python3 start.py export -o output/           # Custom output path

# ── Review Management ──
python3 start.py hide REVIEW_ID PLACE_ID     # Soft-delete a review
python3 start.py restore REVIEW_ID PLACE_ID  # Restore soft-deleted review
python3 start.py clear --place-id "ChIJ..."  # Clear all reviews for a place

# ── API Key Management ──
python3 start.py api-key-create "key-name"   # Create new API key
python3 start.py api-key-list                # List all keys
python3 start.py api-key-revoke 1            # Revoke key #1

# ── Maintenance ──
python3 start.py prune-history --older-than 90  # Clean old audit entries
python3 start.py prune-audit --older-than-days 90
python3 start.py migrate --source json --json-path data.json
python3 start.py logs -f --level ERROR       # Follow error logs
```

---

## 17. Tech Stack & Dependencies

### Backend (Python 3.10+)

| Package | Purpose |
|---------|---------|
| **SeleniumBase** | Browser automation with UC (undetectable) mode |
| **FastAPI** | REST API framework (async-capable) |
| **Uvicorn** | ASGI server for FastAPI |
| **Pydantic** | Request/response validation |
| **openpyxl** | XLSX file generation |
| **Pillow** | Image processing and resizing |
| **PyYAML** | Config file parsing |
| **requests** | Google Places API calls |
| **rich** | CLI colored output and progress bars |
| **pymongo** | MongoDB sync (optional) |
| **boto3** | S3/R2/MinIO upload (optional) |

### Frontend

| Package | Purpose |
|---------|---------|
| **Next.js 15** | React framework with App Router |
| **React 19** | UI library |
| **TypeScript** | Type safety |
| **Tailwind CSS** | Utility-first styling |

### Infrastructure

| Technology | Purpose |
|------------|---------|
| **SQLite** | Primary database (WAL mode) |
| **Chrome/Chromium** | Browser for scraping |
| **Git/GitHub** | Version control |

---

## 18. Development Environment Notes

### Starting the Development Server

```bash
cd google-reviews-scraper-pro
./dev
# Starts:
#   API server → http://localhost:8000
#   Dashboard  → http://localhost:3000
```

### SentinelOne (Endpoint Security)

This project uses browser automation which may trigger alerts on managed devices:

- SentinelOne may flag Chrome processes spawned by SeleniumBase as suspicious
- IT security teams may receive alerts via Lark when the scraper is running
- **Recommendation**: Notify IT team that this is an academic project using browser automation
- **Alternative**: Run in headless mode (`headless: true`) to reduce detection
- **Alternative**: Use a personal/unmanaged device or cloud VM

### Sensitive Files (Never Commit)

| File | Contains |
|------|----------|
| `config.yaml` | May contain business URLs and settings |
| `.env` | API keys and secrets |
| `.env.google_maps.cookies` | Google authentication cookies |
| `secrets.json` | Additional credentials |
| `*.db` | Local database with scraped data |
| `.chrome_profile/` | Chrome session data |

All of these are in `.gitignore`.

### File Structure

```
google-reviews-scraper-pro/
├── api_server.py              # FastAPI REST server (~2000 lines)
├── start.py                   # CLI entry point
├── config.yaml                # Active config (gitignored)
├── config.sample.yaml         # Config template
├── dev                        # Dev server launcher
├── requirements.txt           # Python dependencies
│
├── modules/
│   ├── scraper.py             # Core scraping logic (~3700 lines)
│   ├── review_db.py           # SQLite database (~2400 lines)
│   ├── job_manager.py         # Concurrent job management
│   ├── export_service.py      # JSON/CSV/XLSX export
│   ├── google_places_service.py  # Google Places API
│   ├── progress.py            # Progress tracking
│   ├── config.py              # Config loading/validation
│   ├── cli.py                 # CLI argument parsing
│   ├── api_keys.py            # API key management
│   └── log_manager.py         # Structured logging
│
├── dashboard/
│   ├── src/app/
│   │   ├── page.tsx           # Operations dashboard
│   │   ├── places/page.tsx    # Places list
│   │   └── places/[placeId]/page.tsx  # Place detail
│   ├── src/components/
│   │   ├── ExportDialog.tsx   # Export popup
│   │   ├── Card.tsx           # Card component
│   │   ├── Badge.tsx          # Status badges
│   │   └── Sidebar.tsx        # Navigation
│   └── src/lib/api.ts         # API client
│
├── batch/                     # Batch scraping configs
├── tools/                     # Utility scripts
├── tests/                     # Test suite
└── docs/                      # Documentation
```
