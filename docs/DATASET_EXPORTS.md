# Dataset Exports

The `dataset-export` command creates a reproducible, config-scoped dataset bundle for research work.

All bundle files are derived artifacts built from the current config plus the current SQLite database state. The raw SQLite DB remains the source of truth.

## Command

```bash
python start.py --config batch/config.top50.yaml dataset-export --output-dir dataset_export
```

Optional flags:

- `--min-reviews 100`: threshold used for QA completeness checks.
- `--include-deleted`: include soft-deleted reviews in `reviews_raw.csv` and `reviews_cleaned.csv`.

## API And Dashboard

The dashboard/API now exposes one canonical latest dataset bundle only.

- Dashboard route: `/dataset-export`
- Generate endpoint: `POST /exports/dataset-bundle/generate`
- Latest summary endpoint: `GET /exports/dataset-bundle/latest`
- Artifact download endpoint: `GET /exports/dataset-bundle/latest/artifacts/{artifact_name}`
- Artifact preview endpoint: `GET /exports/dataset-bundle/latest/artifacts/{artifact_name}/preview`

This API surface is intentionally separate from arbitrary historical CLI output directories.

- The dashboard reads and downloads only the canonical latest bundle managed by the backend.
- The dashboard can inspect manifest-listed CSV artifacts in place before download.
- The backend does not attempt to discover past `dataset-export --output-dir ...` runs.
- CLI compatibility is unchanged. Manual CLI runs still work exactly as before.

By default, the canonical latest bundle is written to the repo's `dataset_export/` directory.

- Optional env override: `DATASET_EXPORT_LATEST_DIR`
- This override affects the API/dashboard canonical latest bundle location only.
- It does not change the `dataset-export` CLI contract unless you also pass that path explicitly via `--output-dir`.

CSV artifacts are written and downloaded with a UTF-8 BOM for spreadsheet compatibility.

- This is intentional so Excel/Numbers recognize Chinese and other non-ASCII text correctly on open.
- The preview endpoint is CSV-only in this first pass and returns the first `25` rows from the actual artifact file.

## Output Artifacts

- `reviews_raw.csv`: current raw flat export rows for configured places only.
- `reviews_cleaned.csv`: deterministic derived review rows with normalization and QA flags. This is a derived artifact, not a replacement for raw stored review rows.
- `restaurants_cleaned.csv`: one derived row per configured target, including missing targets.
- `qa_samples_reviews_low_information.csv`: deterministic manual-QA sample for empty or very low-information review text. Row cap: `100`.
- `qa_samples_reviews_duplicate_text.csv`: deterministic manual-QA sample for exact duplicate normalized text within the same place. Row cap: `100`.
- `qa_samples_reviews_format_anomalies.csv`: deterministic manual-QA sample for format-heavy reviews such as repeated punctuation or very high emoji/punctuation density. Row cap: `100`.
- `qa_samples_targets_followup.csv`: deterministic manual-QA sample for targets that need follow-up because of coverage, validation, or lineage gaps. Row cap: `200`.
- `qa_report.json`: derived config-scoped quality report for completeness, conflicts, stale totals, and discovery audit data.
- `dataset_manifest.json`: derived bundle metadata, preprocessing version, artifact inventory, and summary counts.
- `config_snapshot.yaml`: derived config snapshot captured for reproducibility of the bundle.

## Preprocessing Rules

The cleaned review export is intentionally conservative:

- Unicode NFKC normalization
- bidi-control removal
- line-break to space normalization
- whitespace collapsing and trim
- no translation
- no sentiment scoring
- no fuzzy duplicate matching
- no destructive filtering of empty or low-information reviews

Derived flags include:

- `has_text`
- `review_text_language_count`
- `review_text_word_count`
- `review_text_meaningful_char_count`
- `is_empty_text`
- `is_low_information_text`
- `review_text_has_cjk`
- `emoji_count`
- `emoji_density`
- `punctuation_count`
- `punctuation_density`
- `has_repeated_punctuation_run`
- `possible_format_anomaly`
- `possible_duplicate_text_within_place`
- `duplicate_text_group_size`
- `qa_flags`
- `owner_response_language_count`
- `owner_response_meaningful_char_count`

- `is_low_information_text` is a conservative heuristic based on very short normalized alphanumeric/CJK content. It is not a semantic quality label.
- `possible_format_anomaly` is a deterministic formatting heuristic based on repeated punctuation runs and unusually high emoji/punctuation density. It is not sentiment or spam classification.
- `possible_duplicate_text_within_place` is exact normalized-hash matching within the same `place_id` only. It is not fuzzy matching and it does not compare across places.
- Owner response selection is deterministic first-pass extraction from the stored owner-response payload. It is not language-priority inference.
- `qa_flags` is a stable pipe-delimited aggregate of conservative review QA flags in this order: `empty_text`, `low_information_text`, `duplicate_text_within_place`, `format_anomaly`.

Duplicate detection is exact normalized-text grouping within the same `place_id`.

## Restaurant Metadata

`restaurants_cleaned.csv` remains config-scoped and now exposes additional lineage-oriented fields already available from existing tables:

- config metadata: `config_source`
- place presence / scrape lineage: `has_place_record`, `has_last_scraped`
- validation lineage: `has_validation_lineage`, `validation_log_status`, `validation_log_checked_at`, `validation_log_reason`, `validation_expected_name`, `validation_api_name`, `validation_api_address`, `validation_business_status`
- discovery lineage: `has_discovery_lineage`, `discovery_candidate_status`, `discovery_name_snapshot`, `discovery_address_snapshot`, `discovery_rating_snapshot`, `discovery_user_ratings_total_snapshot`, `discovery_discovered_at`, `discovery_updated_at`
- target QA routing: `missing_lineage_flag_count`, `lineage_flags`, `followup_priority_rank`, `followup_reasons`

`lineage_flags` is a stable pipe-delimited aggregate of missing target-level lineage components in this order:

- `missing_google_place_id`
- `missing_place_record`
- `missing_last_scraped`
- `missing_validation_lineage`
- `missing_discovery_lineage`
- `missing_coordinates`

## QA Report Semantics

`qa_report.json` summarizes the current config scope using existing SQLite state and audit tables.

Sections include:

- missing configured targets
- present targets with zero reviews
- exhausted targets still below threshold
- validation issues
- duplicate config targets by `google_place_id` and normalized URL
- stale cached totals
- cross-place review ID conflicts
- staged discovery candidates
- archived invalid targets
- review QA flag summary
- target/review lineage completeness summary
- deterministic follow-up target summary and ordered target list

`lineage_completeness` exposes current gaps instead of rewriting historical provenance semantics. In particular, current raw exports do not backfill historical `google_maps_auth_mode` or `sort_order_confirmed`, and `scrape_mode` still reflects the stored session action field.

## QA Sample Pack

The manual QA sample pack is deterministic and always emitted, even when a file has `0` rows.

- `qa_samples_reviews_low_information.csv`: rows where `is_empty_text` or `is_low_information_text`; ordered by `is_empty_text desc`, `review_text_meaningful_char_count asc`, `place_id asc`, `review_id asc`.
- `qa_samples_reviews_duplicate_text.csv`: rows where `possible_duplicate_text_within_place` is true; ordered by `duplicate_text_group_size desc`, `place_id asc`, `review_text_normalized_hash asc`, `review_id asc`.
- `qa_samples_reviews_format_anomalies.csv`: rows where `possible_format_anomaly` is true; ordered by `has_repeated_punctuation_run desc`, `punctuation_density desc`, `emoji_density desc`, `place_id asc`, `review_id asc`.
- `qa_samples_targets_followup.csv`: rows where `followup_reasons` is non-empty; ordered by `followup_priority_rank asc`, `reviews_needed desc`, `missing_lineage_flag_count desc`, `config_order asc`.

These sample CSVs are for manual inspection only. They do not modify raw storage and they do not imply semantic classification.

## Raw vs Cleaned

Raw SQLite data remains the source of truth.

The dataset bundle does not rewrite or replace raw storage. `reviews_cleaned.csv`, `restaurants_cleaned.csv`, `qa_report.json`, `dataset_manifest.json`, `config_snapshot.yaml`, and the `qa_samples_*.csv` files are deterministic derived artifacts meant for analysis, QA, and final report preparation.
