"""
Config-scoped dataset quality reporting helpers.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from modules.progress import (
    business_identity,
    compute_progress_report,
    normalize_url_for_match,
    resolve_businesses,
)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def build_dataset_scope(
    config: Dict[str, Any],
    review_db,
    *,
    min_reviews: int = 100,
) -> Dict[str, Any]:
    """Resolve configured targets and their current DB scope."""
    businesses = resolve_businesses(config)
    report = compute_progress_report(businesses, review_db, min_reviews=min_reviews)
    unique_place_ids: List[str] = []
    seen_place_ids: set[str] = set()
    for target in report.get("targets", []):
        place_id = str(target.get("place_id") or "").strip()
        if place_id and place_id not in seen_place_ids:
            seen_place_ids.add(place_id)
            unique_place_ids.append(place_id)
    return {
        "businesses": businesses,
        "report": report,
        "unique_place_ids": unique_place_ids,
    }


def _duplicate_groups(
    businesses: Iterable[Dict[str, Any]],
    *,
    kind: str,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for index, business in enumerate(businesses, start=1):
        ident = business_identity(business)
        company = ident.get("company") or ""
        url = ident.get("url") or ""
        if kind == "google_place_id":
            key = str(ident.get("google_place_id") or "").strip()
            if not key:
                continue
            grouped[key].append(
                {
                    "config_order": index,
                    "company": company,
                    "target_url": url,
                    "google_place_id": key,
                }
            )
        elif kind == "url":
            key = normalize_url_for_match(url)
            if not key:
                continue
            grouped[key].append(
                {
                    "config_order": index,
                    "company": company,
                    "target_url": url,
                    "normalized_url": key,
                }
            )

    rows: List[Dict[str, Any]] = []
    for key in sorted(grouped.keys()):
        targets = grouped[key]
        if len(targets) < 2:
            continue
        field_name = "google_place_id" if kind == "google_place_id" else "normalized_url"
        rows.append(
            {
                field_name: key,
                "count": len(targets),
                "targets": targets,
            }
        )
    return rows


def _load_discovery_candidates(review_db, config_path: str) -> List[Dict[str, Any]]:
    rows = review_db.backend.fetchall(
        "SELECT * FROM discovery_candidates WHERE config_path = ? "
        "ORDER BY updated_at DESC, candidate_id DESC",
        (config_path,),
    )
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        entry = dict(row)
        entry["source_payload"] = _json_loads(entry.get("source_payload"))
        parsed.append(entry)
    return parsed


def _load_invalid_archives(review_db, config_path: str) -> List[Dict[str, Any]]:
    rows = review_db.backend.fetchall(
        "SELECT * FROM invalid_place_archive WHERE config_path = ? "
        "ORDER BY archived_at DESC, archive_id DESC",
        (config_path,),
    )
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        entry = dict(row)
        for key in ("validation_snapshot", "config_entry", "deleted_counts"):
            entry[key] = _json_loads(entry.get(key))
        parsed.append(entry)
    return parsed


def build_dataset_quality_report(
    config: Dict[str, Any],
    review_db,
    *,
    config_path: str,
    min_reviews: int = 100,
    include_deleted: bool = False,
) -> Dict[str, Any]:
    """Build a config-scoped QA report for dataset exports."""
    scope = build_dataset_scope(config, review_db, min_reviews=min_reviews)
    businesses = scope["businesses"]
    report = scope["report"]
    scope_place_ids = set(scope["unique_place_ids"])

    duplicate_google_place_ids = _duplicate_groups(businesses, kind="google_place_id")
    duplicate_urls = _duplicate_groups(businesses, kind="url")

    stale_place_totals = [
        dict(row)
        for row in review_db.list_stale_place_totals(limit=max(50, len(scope_place_ids) or 1))
        if str(row.get("place_id") or "") in scope_place_ids
    ]
    cross_place_review_conflicts = [
        conflict
        for conflict in review_db.get_cross_place_conflicts(include_hash_only=False)
        if scope_place_ids.intersection(set(str(pid) for pid in conflict.get("place_ids", [])))
    ]

    discovery_candidates = _load_discovery_candidates(review_db, config_path)
    staged_discovery_candidates = [
        row for row in discovery_candidates if str(row.get("status") or "") == "staged"
    ]
    discovery_candidate_counts = Counter(str(row.get("status") or "unknown") for row in discovery_candidates)

    invalid_archives = _load_invalid_archives(review_db, config_path)

    missing_from_db = [
        dict(target)
        for target in report.get("targets", [])
        if str(target.get("status") or "") == "missing_from_db"
    ]
    present_zero_reviews = [
        dict(target)
        for target in report.get("targets", [])
        if str(target.get("status") or "") == "present_zero_reviews"
    ]
    under_min_reviews = [
        dict(target)
        for target in report.get("targets", [])
        if (
            int(target.get("review_count", 0) or 0) > 0
            and not bool(target.get("meets_min_reviews"))
            and str(target.get("status") or "") != "exhausted_under_threshold"
        )
    ]
    exhausted_under_threshold = [
        dict(target)
        for target in report.get("targets", [])
        if str(target.get("status") or "") == "exhausted_under_threshold"
    ]
    validation_issues = [
        dict(target)
        for target in report.get("targets", [])
        if str(target.get("validation_status") or "unknown") not in {"unknown", "valid"}
    ]

    return {
        "generated_at": _now_utc(),
        "config_path": config_path,
        "min_reviews": int(min_reviews),
        "include_deleted": bool(include_deleted),
        "summary": {
            "targets_total": int(report.get("targets_total", 0) or 0),
            "active_db_place_count": len(scope["unique_place_ids"]),
            "with_reviews": int(report.get("with_reviews", 0) or 0),
            "present_zero_reviews": int(report.get("present_zero_reviews", 0) or 0),
            "missing_from_db": int(report.get("missing_from_db", 0) or 0),
            "meeting_min_reviews": int(report.get("meeting_min_reviews", 0) or 0),
            "under_min_reviews": int(report.get("under_min_reviews", 0) or 0),
            "exhausted_under_threshold_count": int(report.get("exhausted_under_threshold_count", 0) or 0),
            "duplicate_config_google_place_id_groups": len(duplicate_google_place_ids),
            "duplicate_config_url_groups": len(duplicate_urls),
            "stale_place_total_count": len(stale_place_totals),
            "cross_place_conflict_count": len(cross_place_review_conflicts),
            "staged_candidate_count": len(staged_discovery_candidates),
            "invalid_archive_count": len(invalid_archives),
        },
        "missing_from_db": missing_from_db,
        "present_zero_reviews": present_zero_reviews,
        "under_min_reviews": under_min_reviews,
        "exhausted_under_threshold": exhausted_under_threshold,
        "validation_issues": validation_issues,
        "duplicate_config_google_place_ids": duplicate_google_place_ids,
        "duplicate_config_urls": duplicate_urls,
        "stale_place_totals": stale_place_totals,
        "cross_place_review_conflicts": cross_place_review_conflicts,
        "staged_discovery_candidates": staged_discovery_candidates,
        "recent_invalid_place_archives": invalid_archives,
        "discovery_candidate_counts_by_status": {
            key: int(discovery_candidate_counts[key])
            for key in sorted(discovery_candidate_counts.keys())
        },
    }


def summarize_review_flag_summary(cleaned_review_rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    rows = list(cleaned_review_rows)
    return {
        "reviews_total": len(rows),
        "empty_text_count": sum(1 for row in rows if _as_bool(row.get("is_empty_text"))),
        "low_information_text_count": sum(1 for row in rows if _as_bool(row.get("is_low_information_text"))),
        "duplicate_text_within_place_count": sum(
            1 for row in rows if _as_bool(row.get("possible_duplicate_text_within_place"))
        ),
        "format_anomaly_count": sum(1 for row in rows if _as_bool(row.get("possible_format_anomaly"))),
        "cjk_text_count": sum(1 for row in rows if _as_bool(row.get("review_text_has_cjk"))),
        "owner_response_present_count": sum(1 for row in rows if _as_bool(row.get("has_owner_response"))),
        "with_any_qa_flag_count": sum(1 for row in rows if str(row.get("qa_flags") or "").strip()),
    }


def summarize_lineage_completeness(
    restaurants_rows: Iterable[Dict[str, Any]],
    raw_review_rows: Iterable[Dict[str, Any]],
) -> Dict[str, Dict[str, int]]:
    restaurants = list(restaurants_rows)
    raw_reviews = list(raw_review_rows)

    target_total = len(restaurants)
    review_total = len(raw_reviews)

    def _target_count(field: str) -> int:
        return sum(1 for row in restaurants if _as_bool(row.get(field)))

    def _review_has(field: str) -> int:
        return sum(1 for row in raw_reviews if row.get(field) not in (None, ""))

    return {
        "targets": {
            "total": target_total,
            "with_google_place_id": sum(1 for row in restaurants if str(row.get("google_place_id") or "").strip()),
            "with_place_record": _target_count("has_place_record"),
            "with_last_scraped": _target_count("has_last_scraped"),
            "with_coordinates": _target_count("has_coordinates"),
            "with_validation_lineage": _target_count("has_validation_lineage"),
            "with_discovery_lineage": _target_count("has_discovery_lineage"),
            "with_any_lineage_gap": sum(1 for row in restaurants if _as_int(row.get("missing_lineage_flag_count")) > 0),
        },
        "reviews": {
            "total": review_total,
            "with_scrape_session_id": _review_has("scrape_session_id"),
            "with_scrape_started_at": _review_has("scrape_started_at"),
            "with_scrape_completed_at": _review_has("scrape_completed_at"),
            "with_scrape_mode": _review_has("scrape_mode"),
            "with_source_url": _review_has("source_url"),
            "with_resolved_place_url": _review_has("resolved_place_url"),
            "with_sort_order_requested": _review_has("sort_order_requested"),
            "with_source_locale": _review_has("source_locale"),
            "missing_google_maps_auth_mode": sum(
                1 for row in raw_reviews if row.get("google_maps_auth_mode") in (None, "")
            ),
            "missing_sort_order_confirmed": sum(
                1 for row in raw_reviews if row.get("sort_order_confirmed") in (None, "")
            ),
            "with_any_provenance_gap": sum(
                1
                for row in raw_reviews
                if any(
                    row.get(field) in (None, "")
                    for field in (
                        "scrape_session_id",
                        "scrape_started_at",
                        "scrape_completed_at",
                        "scrape_mode",
                        "source_url",
                        "resolved_place_url",
                        "sort_order_requested",
                        "source_locale",
                        "google_maps_auth_mode",
                        "sort_order_confirmed",
                    )
                )
            ),
        },
    }


def build_followup_targets(restaurants_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = [dict(row) for row in restaurants_rows if str(row.get("followup_reasons") or "").strip()]
    rows.sort(
        key=lambda row: (
            _as_int(row.get("followup_priority_rank")) or 999,
            -_as_int(row.get("reviews_needed")),
            -_as_int(row.get("missing_lineage_flag_count")),
            _as_int(row.get("config_order")),
            str(row.get("google_place_id") or ""),
        )
    )
    return [
        {
            "config_order": row.get("config_order"),
            "company": row.get("company"),
            "config_source": row.get("config_source"),
            "google_place_id": row.get("google_place_id"),
            "place_id": row.get("place_id"),
            "target_status": row.get("target_status"),
            "db_review_count": row.get("db_review_count"),
            "reviews_needed": row.get("reviews_needed"),
            "validation_status": row.get("validation_status"),
            "has_validation_lineage": row.get("has_validation_lineage"),
            "has_discovery_lineage": row.get("has_discovery_lineage"),
            "missing_lineage_flag_count": row.get("missing_lineage_flag_count"),
            "lineage_flags": row.get("lineage_flags"),
            "followup_priority_rank": row.get("followup_priority_rank"),
            "followup_reasons": row.get("followup_reasons"),
        }
        for row in rows
    ]


def summarize_followup_targets(restaurants_rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    targets = build_followup_targets(restaurants_rows)
    counts = Counter()
    for row in targets:
        for reason in str(row.get("followup_reasons") or "").split("|"):
            reason = reason.strip()
            if reason:
                counts[reason] += 1
    return {
        "total": len(targets),
        "counts_by_reason": {
            key: int(counts[key])
            for key in sorted(counts.keys())
        },
    }
