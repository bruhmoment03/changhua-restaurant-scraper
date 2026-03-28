"""Tests for config-scoped dataset bundle exports."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import yaml

from modules.config import load_config
from modules.dataset_export import export_dataset_bundle
from modules.review_db import ReviewDB


def _make_review(rid: str, text: str, *, rating: float = 5.0, author: str = "Tester", owner_text: str = ""):
    return {
        "review_id": rid,
        "text": text,
        "rating": rating,
        "likes": 1,
        "lang": "en",
        "date": "3 months ago",
        "review_date": "2025-06-15",
        "author": author,
        "profile": "",
        "avatar": "",
        "owner_text": owner_text,
        "photos": [],
    }


def _write_config(path: Path) -> None:
    payload = {
        "db_path": str(path.parent / "reviews.db"),
        "businesses": [
            {
                "url": "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
                "custom_params": {
                    "company": "Alpha",
                    "address": "A Street",
                    "google_place_id": "PID_A",
                },
            },
            {
                "url": "https://www.google.com/maps/search/?api=1&query=Beta&query_place_id=PID_B",
                "custom_params": {
                    "company": "Beta",
                    "address": "B Street",
                    "google_place_id": "PID_B",
                },
            },
            {
                "url": "https://www.google.com/maps/search/?api=1&query=Missing&query_place_id=PID_MISSING",
                "custom_params": {
                    "company": "Missing",
                    "address": "Missing Street",
                    "google_place_id": "PID_MISSING",
                },
            },
            {
                "url": "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
                "custom_params": {
                    "company": "Alpha Duplicate",
                    "address": "A2 Street",
                    "google_place_id": "PID_A",
                },
            },
        ],
    }
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def test_export_dataset_bundle_writes_cleaned_outputs_and_qa(tmp_path):
    cfg_path = tmp_path / "config.top50.yaml"
    _write_config(cfg_path)

    db_path = tmp_path / "reviews.db"
    db = ReviewDB(str(db_path))
    try:
        db.upsert_place(
            "place_a",
            "Alpha Place",
            "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
            resolved_url="https://www.google.com/maps/place/Alpha/data=!1",
            lat=24.08,
            lng=120.54,
        )
        db.upsert_review("place_a", _make_review("r1", " Great\nfood ", author=" Alice  ", owner_text=" Thanks!  "))
        db.upsert_review("place_a", _make_review("r2", "Great food", author="ALICE"))
        db.upsert_review("place_a", _make_review("shared", "Signature dish", author="Carol"))
        db.upsert_review("place_a", _make_review("r_empty", "", rating=4.0, author="Bob"))

        db.upsert_place(
            "place_b",
            "Beta Place",
            "https://www.google.com/maps/search/?api=1&query=Beta&query_place_id=PID_B",
        )

        db.upsert_place(
            "place_conflict",
            "Conflict Place",
            "https://www.google.com/maps/search/?api=1&query=Conflict&query_place_id=PID_CONFLICT",
        )
        db.upsert_review("place_conflict", _make_review("shared", "Signature dish", author="Carol"))

        db.backend.execute(
            "UPDATE places SET total_reviews = ?, validation_status = ?, validation_checked_at = ?, validation_reason = ? "
            "WHERE place_id = ?",
            (99, "invalid_not_found", "2026-03-22T00:00:00+00:00", "Place Details returned NOT_FOUND", "place_b"),
        )
        db.backend.execute(
            "UPDATE places SET total_reviews = ? WHERE place_id = ?",
            (4, "place_a"),
        )
        db.backend.commit()
        db.record_place_validation(
            place_id="place_b",
            google_place_id="PID_B",
            config_path=str(cfg_path),
            expected_name="Beta",
            status="invalid_not_found",
            reason="Place Details returned NOT_FOUND",
            api_name="Beta Place",
            api_address="B Street",
            business_status="CLOSED_PERMANENTLY",
            checked_at="2026-03-22T00:00:00+00:00",
            response_payload={"status": "NOT_FOUND"},
        )

        db.upsert_discovery_candidates(
            config_path=str(cfg_path),
            query="best restaurants in Changhua",
            candidates=[
                {
                    "google_place_id": "PID_A",
                    "name": "Alpha",
                    "formatted_address": "A Street",
                    "rating": 4.8,
                    "user_ratings_total": 300,
                    "maps_url": "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
                    "status": "approved",
                    "duplicate_source": None,
                    "source_payload": {"place_id": "PID_A"},
                },
                {
                    "google_place_id": "PID_STAGE",
                    "name": "Stage Candidate",
                    "formatted_address": "Stage Street",
                    "rating": 4.2,
                    "user_ratings_total": 50,
                    "maps_url": "https://www.google.com/maps/search/?api=1&query=Stage&query_place_id=PID_STAGE",
                    "status": "staged",
                    "duplicate_source": None,
                    "source_payload": {"place_id": "PID_STAGE"},
                },
            ],
        )

        db.archive_invalid_place_record(
            config_path=str(cfg_path),
            place={
                "place_id": "place_old",
                "place_name": "Old Place",
                "original_url": "https://example.com/old",
                "resolved_url": "https://example.com/old?resolved=1",
                "total_reviews": 0,
                "cached_total_reviews": 0,
            },
            google_place_id="PID_OLD",
            validation_row={
                "status": "invalid_not_found",
                "checked_at": "2026-03-22T00:00:00+00:00",
                "reason": "Place Details returned NOT_FOUND",
            },
            config_entry={"custom_params": {"company": "Old Place"}},
            deleted_counts={"reviews": 0},
        )

        config = load_config(cfg_path)
        output_dir = tmp_path / "dataset_bundle"
        manifest = export_dataset_bundle(
            review_db=db,
            config=config,
            config_path=str(cfg_path),
            output_dir=output_dir,
            min_reviews=10,
            include_deleted=False,
        )
    finally:
        db.close()

    assert manifest["preprocessing_version"] == "dataset-preprocessing-v2"
    assert manifest["bundle_version"] == "dataset-quality-provenance-v2"
    assert manifest["raw_sqlite_authoritative"] is True
    assert manifest["derived_artifacts_only"] is True
    assert manifest["db_schema_version"] == 3

    artifact_names = {artifact["filename"] for artifact in manifest["artifacts"]}
    assert artifact_names == {
        "reviews_raw.csv",
        "reviews_cleaned.csv",
        "restaurants_cleaned.csv",
        "qa_samples_reviews_low_information.csv",
        "qa_samples_reviews_duplicate_text.csv",
        "qa_samples_reviews_format_anomalies.csv",
        "qa_samples_targets_followup.csv",
        "qa_report.json",
        "config_snapshot.yaml",
        "dataset_manifest.json",
    }

    assert (output_dir / "reviews_raw.csv").read_bytes().startswith(b"\xef\xbb\xbf")
    assert (output_dir / "reviews_cleaned.csv").read_bytes().startswith(b"\xef\xbb\xbf")
    assert (output_dir / "restaurants_cleaned.csv").read_bytes().startswith(b"\xef\xbb\xbf")
    assert (output_dir / "qa_samples_reviews_low_information.csv").read_bytes().startswith(b"\xef\xbb\xbf")

    with open(output_dir / "reviews_raw.csv", newline="", encoding="utf-8-sig") as handle:
        raw_rows = list(csv.DictReader(handle))
    assert len(raw_rows) == 4

    with open(output_dir / "reviews_cleaned.csv", newline="", encoding="utf-8-sig") as handle:
        cleaned_rows = list(csv.DictReader(handle))
    assert len(cleaned_rows) == 4
    by_review_id = {row["review_id"]: row for row in cleaned_rows}
    assert by_review_id["r1"]["review_text_clean"] == "Great food"
    assert by_review_id["r1"]["has_text"] == "True"
    assert by_review_id["r1"]["review_text_language_count"] == "1"
    assert by_review_id["r1"]["review_text_word_count"] == "2"
    assert by_review_id["r1"]["review_text_meaningful_char_count"] == "9"
    assert by_review_id["r1"]["duplicate_text_group_size"] == "2"
    assert by_review_id["r1"]["possible_duplicate_text_within_place"] == "True"
    assert by_review_id["r1"]["qa_flags"] == "duplicate_text_within_place"
    assert by_review_id["r_empty"]["is_empty_text"] == "True"
    assert by_review_id["r_empty"]["qa_flags"] == "empty_text"

    with open(output_dir / "restaurants_cleaned.csv", newline="", encoding="utf-8-sig") as handle:
        restaurant_rows = list(csv.DictReader(handle))
    assert len(restaurant_rows) == 4
    by_google_place_id = {}
    for row in restaurant_rows:
        by_google_place_id.setdefault(row["google_place_id"], []).append(row)

    assert by_google_place_id["PID_A"][0]["discovery_query_count"] == "1"
    assert by_google_place_id["PID_A"][0]["config_source"] == ""
    assert by_google_place_id["PID_B"][0]["target_status"] == "present_zero_reviews"
    assert by_google_place_id["PID_B"][0]["has_validation_lineage"] == "True"
    assert by_google_place_id["PID_B"][0]["validation_log_status"] == "invalid_not_found"
    assert by_google_place_id["PID_B"][0]["validation_api_name"] == "Beta Place"
    assert by_google_place_id["PID_B"][0]["followup_reasons"] == (
        "present_zero_reviews|validation_issue|missing_discovery_lineage|missing_coordinates"
    )
    assert by_google_place_id["PID_MISSING"][0]["target_status"] == "missing_from_db"
    assert by_google_place_id["PID_MISSING"][0]["has_place_record"] == "False"
    assert by_google_place_id["PID_MISSING"][0]["lineage_flags"] == (
        "missing_place_record|missing_validation_lineage|missing_discovery_lineage"
    )

    for filename in (
        "qa_samples_reviews_low_information.csv",
        "qa_samples_reviews_duplicate_text.csv",
        "qa_samples_reviews_format_anomalies.csv",
        "qa_samples_targets_followup.csv",
    ):
        assert (output_dir / filename).exists()

    qa_report = json.loads((output_dir / "qa_report.json").read_text(encoding="utf-8"))
    assert qa_report["summary"]["missing_from_db"] == 1
    assert qa_report["summary"]["present_zero_reviews"] == 1
    assert qa_report["summary"]["duplicate_config_google_place_id_groups"] == 1
    assert qa_report["summary"]["duplicate_config_url_groups"] == 1
    assert qa_report["summary"]["stale_place_total_count"] == 2
    assert qa_report["summary"]["cross_place_conflict_count"] == 1
    assert qa_report["summary"]["staged_candidate_count"] == 1
    assert qa_report["summary"]["invalid_archive_count"] == 1
    assert qa_report["summary"]["review_with_any_qa_flag_count"] == 3
    assert qa_report["summary"]["lineage_gap_target_count"] == 4
    assert qa_report["summary"]["followup_target_count"] == 4
    assert qa_report["validation_issues"][0]["google_place_id"] == "PID_B"
    assert qa_report["review_flag_summary"]["duplicate_text_within_place_count"] == 2
    assert qa_report["review_flag_summary"]["with_any_qa_flag_count"] == 3
    assert qa_report["lineage_completeness"]["targets"]["with_validation_lineage"] == 1
    assert qa_report["lineage_completeness"]["targets"]["with_discovery_lineage"] == 2
    assert qa_report["lineage_completeness"]["reviews"]["missing_google_maps_auth_mode"] == 4
    assert qa_report["followup_targets_summary"]["counts_by_reason"]["missing_discovery_lineage"] == 2
    assert qa_report["followup_targets"][0]["google_place_id"] == "PID_MISSING"

    manifest_payload = json.loads((output_dir / "dataset_manifest.json").read_text(encoding="utf-8"))
    assert manifest_payload["artifact_count"] == 10
    assert manifest_payload["summary"]["targets_total"] == 4
    assert manifest_payload["summary"]["active_db_place_count"] == 2
    assert manifest_payload["config_snapshot_sha256"]
    assert len(manifest_payload["provenance_caveats"]) == 4
    assert manifest_payload["qa_sample_pack"]["artifacts"][0]["filename"] == "qa_samples_reviews_low_information.csv"
