"""
Dataset bundle export helpers for reproducible research outputs.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

from modules.dataset_quality import (
    build_dataset_quality_report,
    build_dataset_scope,
    build_followup_targets,
    summarize_followup_targets,
    summarize_lineage_completeness,
    summarize_review_flag_summary,
)
from modules.dataset_samples import SAMPLE_ARTIFACT_SPECS, build_dataset_sample_artifacts
from modules.export_service import CSV_COLUMNS
from modules.preprocessing import (
    PREPROCESSING_VERSION,
    build_cleaned_review_rows,
    normalize_place_name,
)


BUNDLE_VERSION = "dataset-quality-provenance-v2"

REVIEWS_CLEANED_COLUMNS: List[str] = [
    "place_id",
    "place_name",
    "review_id",
    "author_raw",
    "author_normalized",
    "rating",
    "review_date",
    "raw_date",
    "likes",
    "source_locale",
    "extraction_confidence",
    "review_text_raw",
    "review_text_clean",
    "review_text_normalized_hash",
    "review_text_char_count",
    "has_text",
    "review_text_language_count",
    "review_text_word_count",
    "review_text_meaningful_char_count",
    "review_text_has_cjk",
    "emoji_count",
    "emoji_density",
    "punctuation_count",
    "punctuation_density",
    "has_repeated_punctuation_run",
    "is_empty_text",
    "is_low_information_text",
    "possible_format_anomaly",
    "qa_flags",
    "owner_response_raw",
    "owner_response_clean",
    "has_owner_response",
    "owner_response_char_count",
    "owner_response_language_count",
    "owner_response_meaningful_char_count",
    "possible_duplicate_text_within_place",
    "duplicate_text_group_size",
    "is_deleted",
    "scrape_session_id",
    "scrape_started_at",
    "scrape_completed_at",
    "scrape_mode",
    "source_url",
    "resolved_place_url",
]

RESTAURANTS_CLEANED_COLUMNS: List[str] = [
    "config_order",
    "company",
    "config_source",
    "google_place_id",
    "target_url",
    "config_address",
    "place_id",
    "has_place_record",
    "place_name_db",
    "place_name_normalized",
    "original_url",
    "resolved_url",
    "latitude",
    "longitude",
    "has_coordinates",
    "first_seen",
    "last_scraped",
    "has_last_scraped",
    "db_review_count",
    "cached_total_reviews",
    "reviews_exhausted",
    "exhausted_at",
    "validation_status",
    "validation_checked_at",
    "validation_reason",
    "has_validation_lineage",
    "validation_issue_present",
    "validation_log_status",
    "validation_log_checked_at",
    "validation_log_reason",
    "validation_expected_name",
    "validation_api_name",
    "validation_api_address",
    "validation_business_status",
    "target_status",
    "meets_min_reviews",
    "reviews_needed",
    "has_discovery_lineage",
    "discovery_candidate_status",
    "discovery_query_count",
    "discovery_queries_json",
    "discovery_name_snapshot",
    "discovery_address_snapshot",
    "discovery_rating_snapshot",
    "discovery_user_ratings_total_snapshot",
    "discovery_discovered_at",
    "discovery_updated_at",
    "missing_lineage_flag_count",
    "lineage_flags",
    "followup_priority_rank",
    "followup_reasons",
]

_TARGET_LINEAGE_FLAG_ORDER = (
    "missing_google_place_id",
    "missing_place_record",
    "missing_last_scraped",
    "missing_validation_lineage",
    "missing_discovery_lineage",
    "missing_coordinates",
)

_TARGET_FOLLOWUP_REASON_ORDER = (
    "missing_from_db",
    "present_zero_reviews",
    "exhausted_under_threshold",
    "under_min_reviews",
    "validation_issue",
    "missing_discovery_lineage",
    "missing_validation_lineage",
    "missing_last_scraped",
    "missing_coordinates",
)

_PROVENANCE_CAVEATS = [
    "scrape_mode in raw export provenance reflects scrape_sessions.action, not the scraper's higher-level historical mode.",
    "google_maps_auth_mode is not historically stored in SQLite review provenance and therefore remains incomplete in derived exports.",
    "sort_order_confirmed is not historically stored in SQLite review provenance and therefore remains incomplete in derived exports.",
    "discovery_candidates keeps one latest row per (config_path, google_place_id), so historical multi-query discovery lineage is partial.",
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if value == "":
        return None
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return value


def _write_csv(path: Path, rows: List[Dict[str, Any]], columns: List[str]) -> Dict[str, Any]:
    # Write CSV bundles with UTF-8 BOM so Excel/Numbers detect Unicode content
    # correctly for Chinese and other non-ASCII text fields.
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return {
        "filename": path.name,
        "format": "csv",
        "row_count": len(rows),
        "sha256": _sha256(path),
        "columns": columns,
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> Dict[str, Any]:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "filename": path.name,
        "format": "json",
        "row_count": None,
        "sha256": _sha256(path),
        "columns": [],
    }


def _write_yaml(path: Path, payload: Dict[str, Any]) -> Dict[str, Any]:
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return {
        "filename": path.name,
        "format": "yaml",
        "row_count": None,
        "sha256": _sha256(path),
        "columns": [],
    }


def _artifact_by_name(artifacts: List[Dict[str, Any]], filename: str) -> Dict[str, Any]:
    for artifact in artifacts:
        if artifact.get("filename") == filename:
            return artifact
    return {}


def _ordered_pipe_join(active_values: set[str], order: tuple[str, ...]) -> str:
    return "|".join(value for value in order if value in active_values)


def _followup_priority_rank(followup_reasons: str) -> int:
    active_values = {
        value.strip()
        for value in str(followup_reasons or "").split("|")
        if value.strip()
    }
    for index, reason in enumerate(_TARGET_FOLLOWUP_REASON_ORDER, start=1):
        if reason in active_values:
            return index
    return 0


def _discovery_query_map(review_db, config_path: str) -> Dict[str, List[str]]:
    rows = review_db.backend.fetchall(
        "SELECT google_place_id, query FROM discovery_candidates WHERE config_path = ? "
        "ORDER BY updated_at DESC, candidate_id DESC",
        (config_path,),
    )
    queries_by_place: Dict[str, List[str]] = {}
    for row in rows:
        google_place_id = str(row.get("google_place_id") or "").strip()
        query = str(row.get("query") or "").strip()
        if not google_place_id or not query:
            continue
        queries_by_place.setdefault(google_place_id, [])
        if query not in queries_by_place[google_place_id]:
            queries_by_place[google_place_id].append(query)
    return queries_by_place


def _load_discovery_candidate_map(review_db, config_path: str) -> Dict[str, Dict[str, Any]]:
    rows = review_db.backend.fetchall(
        "SELECT * FROM discovery_candidates WHERE config_path = ? "
        "ORDER BY updated_at DESC, candidate_id DESC",
        (config_path,),
    )
    by_google_place_id: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        google_place_id = str(row.get("google_place_id") or "").strip()
        if not google_place_id or google_place_id in by_google_place_id:
            continue
        entry = dict(row)
        entry["source_payload"] = _json_loads(entry.get("source_payload"))
        by_google_place_id[google_place_id] = entry
    return by_google_place_id


def _load_validation_maps(review_db, config_path: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    rows = review_db.backend.fetchall(
        "SELECT * FROM place_validation_log WHERE config_path = ? "
        "ORDER BY checked_at DESC, validation_id DESC",
        (config_path,),
    )
    by_place_id: Dict[str, Dict[str, Any]] = {}
    by_google_place_id: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        entry = dict(row)
        entry["response_payload"] = _json_loads(entry.get("response_payload"))
        place_id = str(entry.get("place_id") or "").strip()
        google_place_id = str(entry.get("google_place_id") or "").strip()
        if place_id and place_id not in by_place_id:
            by_place_id[place_id] = entry
        if google_place_id and google_place_id not in by_google_place_id:
            by_google_place_id[google_place_id] = entry
    return {
        "by_place_id": by_place_id,
        "by_google_place_id": by_google_place_id,
    }


def _build_restaurants_cleaned_rows(
    config: Dict[str, Any],
    review_db,
    *,
    config_path: str,
    min_reviews: int,
) -> List[Dict[str, Any]]:
    scope = build_dataset_scope(config, review_db, min_reviews=min_reviews)
    businesses = scope["businesses"]
    report = scope["report"]
    discovery_queries = _discovery_query_map(review_db, config_path)
    discovery_rows = _load_discovery_candidate_map(review_db, config_path)
    validation_maps = _load_validation_maps(review_db, config_path)
    place_cache: Dict[str, Dict[str, Any] | None] = {}
    rows: List[Dict[str, Any]] = []

    for index, (business, target) in enumerate(zip(businesses, report.get("targets", [])), start=1):
        custom_params = business.get("custom_params", {}) if isinstance(business, dict) else {}
        google_place_id = str(target.get("google_place_id") or "").strip()
        place_id = str(target.get("place_id") or "").strip()
        if place_id not in place_cache:
            place_cache[place_id] = review_db.get_place(place_id) if place_id else None
        place = place_cache.get(place_id) or {}
        queries = discovery_queries.get(google_place_id, [])
        discovery_row = discovery_rows.get(google_place_id) or {}
        validation_row = (
            validation_maps["by_place_id"].get(place_id)
            or validation_maps["by_google_place_id"].get(google_place_id)
            or {}
        )

        latitude = place.get("latitude")
        longitude = place.get("longitude")
        place_name = place.get("place_name") or target.get("place_name") or target.get("company") or ""
        has_place_record = bool(place_id)
        last_scraped = place.get("last_scraped") or target.get("last_scraped")
        has_last_scraped = bool(last_scraped)
        has_coordinates = latitude is not None and longitude is not None
        has_validation_lineage = bool(validation_row)
        has_discovery_lineage = bool(discovery_row)
        validation_status = target.get("validation_status")
        validation_status_value = str(validation_status or "unknown")
        validation_issue_present = validation_status_value not in {"unknown", "valid"}
        validation_log_status = str(validation_row.get("status") or "")
        if validation_log_status and validation_log_status not in {"unknown", "valid"}:
            validation_issue_present = True

        active_lineage_flags = set()
        if not google_place_id:
            active_lineage_flags.add("missing_google_place_id")
        if not has_place_record:
            active_lineage_flags.add("missing_place_record")
        if has_place_record and not has_last_scraped:
            active_lineage_flags.add("missing_last_scraped")
        if not has_validation_lineage:
            active_lineage_flags.add("missing_validation_lineage")
        if not has_discovery_lineage:
            active_lineage_flags.add("missing_discovery_lineage")
        if has_place_record and not has_coordinates:
            active_lineage_flags.add("missing_coordinates")
        lineage_flags = _ordered_pipe_join(active_lineage_flags, _TARGET_LINEAGE_FLAG_ORDER)

        target_status = str(target.get("status") or "")
        active_followup_reasons = set()
        if target_status == "missing_from_db":
            active_followup_reasons.add("missing_from_db")
        elif target_status == "present_zero_reviews":
            active_followup_reasons.add("present_zero_reviews")
        elif target_status == "exhausted_under_threshold":
            active_followup_reasons.add("exhausted_under_threshold")
        elif not bool(target.get("meets_min_reviews")) and int(target.get("review_count", 0) or 0) > 0:
            active_followup_reasons.add("under_min_reviews")
        if validation_issue_present:
            active_followup_reasons.add("validation_issue")
        if not has_discovery_lineage:
            active_followup_reasons.add("missing_discovery_lineage")
        if not has_validation_lineage:
            active_followup_reasons.add("missing_validation_lineage")
        if has_place_record and not has_last_scraped:
            active_followup_reasons.add("missing_last_scraped")
        if has_place_record and not has_coordinates:
            active_followup_reasons.add("missing_coordinates")
        followup_reasons = _ordered_pipe_join(active_followup_reasons, _TARGET_FOLLOWUP_REASON_ORDER)

        rows.append(
            {
                "config_order": index,
                "company": target.get("company") or str(custom_params.get("company") or ""),
                "config_source": str(custom_params.get("source") or ""),
                "google_place_id": google_place_id,
                "target_url": target.get("url") or business.get("url", ""),
                "config_address": str(custom_params.get("address") or ""),
                "place_id": place_id,
                "has_place_record": has_place_record,
                "place_name_db": place.get("place_name") or "",
                "place_name_normalized": normalize_place_name(place_name),
                "original_url": place.get("original_url") or "",
                "resolved_url": place.get("resolved_url") or "",
                "latitude": latitude,
                "longitude": longitude,
                "has_coordinates": has_coordinates,
                "first_seen": place.get("first_seen"),
                "last_scraped": last_scraped,
                "has_last_scraped": has_last_scraped,
                "db_review_count": int(place.get("total_reviews", target.get("review_count", 0)) or 0),
                "cached_total_reviews": int(
                    place.get("cached_total_reviews", target.get("cached_total_reviews", 0)) or 0
                ),
                "reviews_exhausted": bool(place.get("reviews_exhausted", target.get("reviews_exhausted", False))),
                "exhausted_at": place.get("exhausted_at"),
                "validation_status": validation_status,
                "validation_checked_at": target.get("validation_checked_at"),
                "validation_reason": target.get("validation_reason"),
                "has_validation_lineage": has_validation_lineage,
                "validation_issue_present": validation_issue_present,
                "validation_log_status": validation_log_status,
                "validation_log_checked_at": validation_row.get("checked_at"),
                "validation_log_reason": validation_row.get("reason"),
                "validation_expected_name": validation_row.get("expected_name"),
                "validation_api_name": validation_row.get("api_name"),
                "validation_api_address": validation_row.get("api_address"),
                "validation_business_status": validation_row.get("business_status"),
                "target_status": target_status,
                "meets_min_reviews": bool(target.get("meets_min_reviews")),
                "reviews_needed": int(target.get("reviews_needed", 0) or 0),
                "has_discovery_lineage": has_discovery_lineage,
                "discovery_candidate_status": discovery_row.get("status"),
                "discovery_query_count": len(queries),
                "discovery_queries_json": json.dumps(queries, ensure_ascii=False, separators=(",", ":")),
                "discovery_name_snapshot": discovery_row.get("name"),
                "discovery_address_snapshot": discovery_row.get("formatted_address"),
                "discovery_rating_snapshot": discovery_row.get("rating"),
                "discovery_user_ratings_total_snapshot": discovery_row.get("user_ratings_total"),
                "discovery_discovered_at": discovery_row.get("discovered_at"),
                "discovery_updated_at": discovery_row.get("updated_at"),
                "missing_lineage_flag_count": len(active_lineage_flags),
                "lineage_flags": lineage_flags,
                "followup_priority_rank": _followup_priority_rank(followup_reasons),
                "followup_reasons": followup_reasons,
            }
        )

    return rows


def export_dataset_bundle(
    review_db,
    config: Dict[str, Any],
    *,
    config_path: str,
    output_dir: str | Path,
    min_reviews: int = 100,
    include_deleted: bool = False,
) -> Dict[str, Any]:
    """
    Export a deterministic dataset bundle for configured targets.

    Raw data remains in SQLite; the bundle is a reproducible derived artifact.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    scope = build_dataset_scope(config, review_db, min_reviews=min_reviews)
    raw_rows: List[Dict[str, Any]] = []
    for place_id in scope["unique_place_ids"]:
        raw_rows.extend(review_db.export_place_flat_rows(place_id, include_deleted=include_deleted))

    cleaned_rows = build_cleaned_review_rows(raw_rows)
    restaurants_rows = _build_restaurants_cleaned_rows(
        config,
        review_db,
        config_path=config_path,
        min_reviews=min_reviews,
    )
    qa_report = build_dataset_quality_report(
        config,
        review_db,
        config_path=config_path,
        min_reviews=min_reviews,
        include_deleted=include_deleted,
    )
    review_flag_summary = summarize_review_flag_summary(cleaned_rows)
    lineage_completeness = summarize_lineage_completeness(restaurants_rows, raw_rows)
    followup_targets_summary = summarize_followup_targets(restaurants_rows)
    qa_report["review_flag_summary"] = review_flag_summary
    qa_report["lineage_completeness"] = lineage_completeness
    qa_report["followup_targets_summary"] = followup_targets_summary
    qa_report["followup_targets"] = build_followup_targets(restaurants_rows)
    qa_report["summary"].update(
        {
            "review_with_any_qa_flag_count": int(review_flag_summary["with_any_qa_flag_count"]),
            "lineage_gap_target_count": int(lineage_completeness["targets"]["with_any_lineage_gap"]),
            "review_provenance_gap_count": int(lineage_completeness["reviews"]["with_any_provenance_gap"]),
            "followup_target_count": int(followup_targets_summary["total"]),
            "missing_discovery_lineage_count": int(
                lineage_completeness["targets"]["total"] - lineage_completeness["targets"]["with_discovery_lineage"]
            ),
            "missing_validation_lineage_count": int(
                lineage_completeness["targets"]["total"] - lineage_completeness["targets"]["with_validation_lineage"]
            ),
        }
    )

    sample_artifacts = build_dataset_sample_artifacts(cleaned_rows, restaurants_rows)

    artifacts = [
        _write_csv(output_path / "reviews_raw.csv", raw_rows, CSV_COLUMNS),
        _write_csv(output_path / "reviews_cleaned.csv", cleaned_rows, REVIEWS_CLEANED_COLUMNS),
        _write_csv(output_path / "restaurants_cleaned.csv", restaurants_rows, RESTAURANTS_CLEANED_COLUMNS),
    ]
    for sample_artifact in sample_artifacts:
        artifacts.append(
            _write_csv(
                output_path / sample_artifact["filename"],
                sample_artifact["rows"],
                sample_artifact["columns"],
            )
        )
    artifacts.extend(
        [
            _write_json(output_path / "qa_report.json", qa_report),
            _write_yaml(output_path / "config_snapshot.yaml", config),
        ]
    )

    config_snapshot_artifact = _artifact_by_name(artifacts, "config_snapshot.yaml")
    manifest = {
        "generated_at": qa_report.get("generated_at"),
        "bundle_version": BUNDLE_VERSION,
        "preprocessing_version": PREPROCESSING_VERSION,
        "config_path": config_path,
        "config_snapshot_sha256": config_snapshot_artifact.get("sha256"),
        "db_path_basename": Path(str(review_db.backend.db_path)).name,
        "db_schema_version": int(review_db.get_schema_version()),
        "scope": "config_targets",
        "min_reviews": int(min_reviews),
        "include_deleted": bool(include_deleted),
        "raw_sqlite_authoritative": True,
        "derived_artifacts_only": True,
        "artifact_count": len(artifacts) + 1,
        "artifacts": [
            *artifacts,
            {
                "filename": "dataset_manifest.json",
                "format": "json",
                "row_count": None,
                "sha256": None,
                "columns": [],
            },
        ],
        "summary": qa_report.get("summary", {}),
        "lineage_completeness": lineage_completeness,
        "provenance_caveats": _PROVENANCE_CAVEATS,
        "qa_sample_pack": {
            "selection_version": "dataset-samples-v1",
            "artifacts": [
                {
                    "filename": spec["filename"],
                    "row_cap": int(spec["row_cap"]),
                    "selection_rule": spec["selection_rule"],
                }
                for spec in SAMPLE_ARTIFACT_SPECS
            ],
        },
    }

    manifest_path = output_path / "dataset_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
