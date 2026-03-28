"""
Deterministic manual-QA sample pack builders for dataset exports.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


LOW_INFORMATION_SAMPLE_COLUMNS = [
    "place_id",
    "place_name",
    "review_id",
    "rating",
    "review_date",
    "review_text_raw",
    "review_text_clean",
    "review_text_meaningful_char_count",
    "is_empty_text",
    "is_low_information_text",
    "qa_flags",
]

DUPLICATE_TEXT_SAMPLE_COLUMNS = [
    "place_id",
    "place_name",
    "review_id",
    "author_normalized",
    "rating",
    "review_date",
    "review_text_clean",
    "review_text_normalized_hash",
    "duplicate_text_group_size",
    "qa_flags",
]

FORMAT_ANOMALY_SAMPLE_COLUMNS = [
    "place_id",
    "place_name",
    "review_id",
    "rating",
    "review_date",
    "review_text_clean",
    "emoji_count",
    "emoji_density",
    "punctuation_count",
    "punctuation_density",
    "has_repeated_punctuation_run",
    "possible_format_anomaly",
    "qa_flags",
]

FOLLOWUP_TARGET_SAMPLE_COLUMNS = [
    "config_order",
    "company",
    "config_source",
    "google_place_id",
    "target_url",
    "place_id",
    "target_status",
    "db_review_count",
    "reviews_needed",
    "validation_status",
    "discovery_query_count",
    "missing_lineage_flag_count",
    "lineage_flags",
    "followup_priority_rank",
    "followup_reasons",
]

SAMPLE_ARTIFACT_SPECS: List[Dict[str, Any]] = [
    {
        "filename": "qa_samples_reviews_low_information.csv",
        "row_cap": 100,
        "selection_rule": (
            "Rows where is_empty_text OR is_low_information_text; ordered by "
            "is_empty_text desc, review_text_meaningful_char_count asc, place_id asc, review_id asc."
        ),
        "columns": LOW_INFORMATION_SAMPLE_COLUMNS,
    },
    {
        "filename": "qa_samples_reviews_duplicate_text.csv",
        "row_cap": 100,
        "selection_rule": (
            "Rows where possible_duplicate_text_within_place is true; ordered by "
            "duplicate_text_group_size desc, place_id asc, review_text_normalized_hash asc, review_id asc."
        ),
        "columns": DUPLICATE_TEXT_SAMPLE_COLUMNS,
    },
    {
        "filename": "qa_samples_reviews_format_anomalies.csv",
        "row_cap": 100,
        "selection_rule": (
            "Rows where possible_format_anomaly is true; ordered by "
            "has_repeated_punctuation_run desc, punctuation_density desc, emoji_density desc, place_id asc, review_id asc."
        ),
        "columns": FORMAT_ANOMALY_SAMPLE_COLUMNS,
    },
    {
        "filename": "qa_samples_targets_followup.csv",
        "row_cap": 200,
        "selection_rule": (
            "Rows where followup_reasons is non-empty; ordered by "
            "followup_priority_rank asc, reviews_needed desc, missing_lineage_flag_count desc, config_order asc."
        ),
        "columns": FOLLOWUP_TARGET_SAMPLE_COLUMNS,
    },
]


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


def _as_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _project_rows(rows: Iterable[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
    return [{column: row.get(column) for column in columns} for row in rows]


def _build_low_information_rows(cleaned_review_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected = [
        row
        for row in cleaned_review_rows
        if _as_bool(row.get("is_empty_text")) or _as_bool(row.get("is_low_information_text"))
    ]
    selected.sort(
        key=lambda row: (
            -int(_as_bool(row.get("is_empty_text"))),
            _as_int(row.get("review_text_meaningful_char_count")),
            str(row.get("place_id") or ""),
            str(row.get("review_id") or ""),
        )
    )
    return selected


def _build_duplicate_text_rows(cleaned_review_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected = [
        row
        for row in cleaned_review_rows
        if _as_bool(row.get("possible_duplicate_text_within_place"))
    ]
    selected.sort(
        key=lambda row: (
            -_as_int(row.get("duplicate_text_group_size")),
            str(row.get("place_id") or ""),
            str(row.get("review_text_normalized_hash") or ""),
            str(row.get("review_id") or ""),
        )
    )
    return selected


def _build_format_anomaly_rows(cleaned_review_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected = [
        row
        for row in cleaned_review_rows
        if _as_bool(row.get("possible_format_anomaly"))
    ]
    selected.sort(
        key=lambda row: (
            -int(_as_bool(row.get("has_repeated_punctuation_run"))),
            -_as_float(row.get("punctuation_density")),
            -_as_float(row.get("emoji_density")),
            str(row.get("place_id") or ""),
            str(row.get("review_id") or ""),
        )
    )
    return selected


def _build_followup_target_rows(restaurants_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected = [row for row in restaurants_rows if str(row.get("followup_reasons") or "").strip()]
    selected.sort(
        key=lambda row: (
            _as_int(row.get("followup_priority_rank")) or 999,
            -_as_int(row.get("reviews_needed")),
            -_as_int(row.get("missing_lineage_flag_count")),
            _as_int(row.get("config_order")),
            str(row.get("google_place_id") or ""),
        )
    )
    return selected


def build_dataset_sample_artifacts(
    cleaned_review_rows: Iterable[Dict[str, Any]],
    restaurants_rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    cleaned_review_rows = list(cleaned_review_rows)
    restaurants_rows = list(restaurants_rows)

    low_information_rows = _build_low_information_rows(cleaned_review_rows)
    duplicate_text_rows = _build_duplicate_text_rows(cleaned_review_rows)
    format_anomaly_rows = _build_format_anomaly_rows(cleaned_review_rows)
    followup_target_rows = _build_followup_target_rows(restaurants_rows)

    samples = [
        {
            **SAMPLE_ARTIFACT_SPECS[0],
            "rows": _project_rows(
                low_information_rows[: SAMPLE_ARTIFACT_SPECS[0]["row_cap"]],
                LOW_INFORMATION_SAMPLE_COLUMNS,
            ),
        },
        {
            **SAMPLE_ARTIFACT_SPECS[1],
            "rows": _project_rows(
                duplicate_text_rows[: SAMPLE_ARTIFACT_SPECS[1]["row_cap"]],
                DUPLICATE_TEXT_SAMPLE_COLUMNS,
            ),
        },
        {
            **SAMPLE_ARTIFACT_SPECS[2],
            "rows": _project_rows(
                format_anomaly_rows[: SAMPLE_ARTIFACT_SPECS[2]["row_cap"]],
                FORMAT_ANOMALY_SAMPLE_COLUMNS,
            ),
        },
        {
            **SAMPLE_ARTIFACT_SPECS[3],
            "rows": _project_rows(
                followup_target_rows[: SAMPLE_ARTIFACT_SPECS[3]["row_cap"]],
                FOLLOWUP_TARGET_SAMPLE_COLUMNS,
            ),
        },
    ]
    return samples
