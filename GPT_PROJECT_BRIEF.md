# GPT Project Brief

## 1. Project Context

This repository is a school project for collecting and analyzing Google Maps restaurant reviews in Changhua City (彰化市).

It is built on top of the upstream `google-reviews-scraper-pro` project, but this fork has already expanded into a larger system with:

- Google Places discovery
- Google Maps review scraping
- SQLite-based storage with history/audit tracking
- FastAPI backend
- Next.js dashboard
- CSV / JSON / XLSX export
- batch configs for Changhua restaurant targets

The current local goal is not to redesign the whole product. The priority is:

- `資料收集` = data collection
- `資料前處理` = data preprocessing

That means the most important improvements should focus on dataset quality, completeness, reproducibility, and research usefulness.

## 2. What Already Exists

### Main collection flow

1. Discover candidate restaurants with Google Places Text Search
2. Convert discovered places into scraper-ready Google Maps URLs
3. Queue or run scraping jobs against those URLs
4. Store places, scrape sessions, reviews, and review history in SQLite
5. Export the collected data for analysis

### Main components

- `start.py`
  CLI entrypoint for scrape + management commands
- `api_server.py`
  FastAPI service for queueing, progress, discovery, validation, export, and dashboard data
- `modules/scraper.py`
  SeleniumBase-based Google Maps review scraper
- `modules/review_db.py`
  SQLite schema and persistence layer
- `modules/google_places_service.py`
  Google Places discovery + validation helpers
- `tools/places_textsearch_to_config.py`
  Batch discovery script that generates scraper input config
- `modules/pipeline.py`
  Current post-scrape processing pipeline
- `modules/progress.py`
  Coverage / threshold progress logic
- `modules/export_service.py`
  Research-facing export builders
- `dashboard/`
  Next.js monitoring and operations UI
- `batch/config.top50.yaml`
  Main Changhua restaurant batch config currently used by the dashboard

### Current strengths

- The project is already operational end-to-end
- There is a strong SQLite schema with places, reviews, scrape sessions, history, aliases, discovery candidates, and validation/archive tables
- Data collection has retry logic, concurrency, limited-view handling, and debugging artifacts
- The dashboard already exposes progress, jobs, discovery candidates, exports, and data-health views
- The test suite is large and currently passing

## 3. Current Collection/Preprocessing Reality

This repo already does some preprocessing, but it is still mostly **operational preprocessing**, not yet a full **research-grade preprocessing pipeline**.

### Current preprocessing that exists

- relative date conversion to ISO-like dates
- image download / optional S3 handling
- custom metadata injection
- export filtering such as excluding empty-text reviews
- dedupe / merge logic based on IDs, fingerprints, and DB reconciliation
- per-place progress counting and threshold tracking

### What is still missing or underdeveloped

This is the most important part for future improvement.

#### A. Research-grade cleaned dataset layer is missing

Right now the system is very good at collecting and storing data, but it does **not** yet clearly separate:

- raw scraped data
- normalized intermediate data
- analysis-ready final dataset

It needs a clearer `raw -> cleaned -> analysis-ready` pipeline.

#### B. Text preprocessing is still shallow

There is no dedicated preprocessing stage for:

- Chinese text normalization policy
- multilingual normalization policy
- punctuation / emoji / whitespace cleanup rules
- duplicate or near-duplicate text detection beyond scrape identity
- author-name normalization rules
- owner-response normalization
- explicit handling of empty / templated / low-information reviews

#### C. Dataset QA and completeness controls are still weak

The system tracks scraping progress, but it still needs stronger dataset quality checks such as:

- collection coverage by query / district / restaurant type
- detection of restaurants with too few reviews for analysis
- scrape freshness / staleness reports
- anomaly detection for sudden review count drops
- audit reports for failed or low-confidence targets
- sampling workflow for manual review

#### D. Restaurant metadata enrichment is still limited

The config stores `company`, `address`, and `google_place_id`, but a more analysis-ready restaurant table should probably include:

- district / area
- cuisine category
- latitude / longitude consistency checks
- rating and ratings_total snapshots from Places API
- chain vs non-chain labeling
- source query lineage

#### E. Discovery quality control can improve

Discovery already exists, but it is still missing a stronger selection policy for research collection:

- query coverage strategy
- ranking strategy review
- false-positive filtering for non-restaurant results
- duplicate branch handling
- reproducible discovery snapshots
- explicit acceptance / rejection reasons

#### F. Reproducibility and lineage can be much better

For a school project, the final dataset should be reproducible.

The repo would benefit from:

- dataset version numbers
- export schema versioning
- run manifests
- config snapshot attached to each dataset export
- clearer raw-data lineage from discovery to scrape to cleaned export

## 4. Important Repo Observations

These are concrete observations from the current codebase and docs:

- `batch/config.top50.yaml` is clearly tuned for the school dataset:
  - `google_maps_auth_mode: cookie`
  - `fail_on_limited_view: true`
  - `max_reviews: 100`
  - `download_images: false`
  - `use_mongodb: false`
- The dashboard homepage also targets `batch/config.top50.yaml` and defaults to `100` reviews as the threshold
- The project overview mentions a target of roughly `80-100+` restaurants in Changhua City, but the currently wired config is still a top-50 style batch
- The current preprocessing pipeline in `modules/pipeline.py` is mostly:
  - date conversion
  - image handling
  - optional S3
  - cleanup
  - custom params
  - MongoDB/JSON writing
- That means the repo still lacks a dedicated academic/research preprocessing module
- There is at least one docs/config mismatch:
  - the README still describes higher scroll defaults in places
  - historically, the actual config logic clamped `max_scroll_attempts` and `scroll_idle_limit` to `10`

## 5. What GPT Should Focus On

GPT should not start by proposing random UI changes or a full rewrite.

It should focus on these questions first:

1. How can this repo produce a higher-quality restaurant review dataset for Changhua City?
2. What collection gaps still exist between discovery, scrape coverage, and final export?
3. What preprocessing steps are missing if the final goal is analysis or modeling?
4. How should the repo separate raw data, normalized data, and analysis-ready data?
5. What new tables, exports, scripts, or tests would make the dataset trustworthy and reproducible?

## 6. Preferred Improvement Direction

If GPT proposes a roadmap, it should prioritize the following order:

### Priority 1: collection quality

- improve target discovery coverage
- reduce false positives
- improve scrape completeness and retry visibility
- identify which restaurants are below quality threshold

### Priority 2: preprocessing quality

- define a canonical cleaned review schema
- create explicit normalization rules for text/date/metadata
- separate raw and cleaned outputs
- add quality flags and preprocessing metadata

### Priority 3: export and research readiness

- build analysis-ready exports
- include schema metadata / dataset version / run manifest
- produce documentation that explains the final dataset format

### Priority 4: only then improve UI or workflow polish

- dashboard polish
- convenience APIs
- non-critical UX improvements

## 7. Good Concrete Ideas to Explore

GPT should consider proposing some of these:

- a `clean_reviews.py` or `modules/preprocessing.py` stage
- a canonical cleaned export such as:
  - `reviews_raw`
  - `reviews_cleaned`
  - `restaurants_cleaned`
- preprocessing rules for:
  - empty-text reviews
  - duplicated content
  - multilingual text
  - malformed dates
  - owner replies
- a data dictionary markdown file
- a dataset manifest JSON for each export
- automatic QA reports:
  - missing restaurants
  - under-threshold restaurants
  - stale targets
  - duplicate targets
  - invalid discovery candidates
- a more explicit discovery-to-scrape approval flow
- research-specific derived features:
  - text length
  - language
  - has_owner_reply
  - image_count
  - review_age_days
  - restaurant category

## 8. Guardrails for GPT

GPT should follow these rules when suggesting or implementing changes:

- do not break the existing scraper flow without a clear reason
- do not remove the current SQLite-first design
- do not over-prioritize MongoDB or S3 for this school project
- do not replace working batch configs without preserving current behavior
- do not spend most effort on frontend styling
- do not assume preprocessing is already “done”
- prefer incremental, testable, research-oriented improvements

## 9. Requested Output From GPT

When GPT analyzes this repo, the ideal output should be:

1. A clear assessment of what is already strong
2. A gap analysis focused on data collection and preprocessing
3. A prioritized roadmap
4. A proposed cleaned-data architecture
5. Specific file/module changes
6. Suggested tests and validation steps

## 10. Starter Prompt

You are helping improve a school project repository for collecting and preprocessing Google Maps restaurant review data in Changhua City.

Before proposing changes, inspect the current codebase structure and understand the existing system:

- discovery via Google Places
- scraping via SeleniumBase
- storage via SQLite
- queueing via FastAPI job manager
- exports via CSV/JSON/XLSX
- dashboard via Next.js

My responsibility is specifically:

- 資料收集 (data collection)
- 資料前處理 (data preprocessing)

Your task is to:

1. analyze what already exists
2. identify what is still missing for high-quality collection and research-grade preprocessing
3. avoid random full-stack redesign suggestions
4. prioritize practical improvements for dataset quality, coverage, normalization, QA, and reproducibility
5. propose a concrete roadmap with files, modules, schemas, and tests

Focus especially on:

- discovery quality
- scrape completeness
- cleaned dataset design
- normalization rules
- export reproducibility
- QA / validation workflow

Do not assume the current preprocessing pipeline is sufficient just because the scraper already works.
