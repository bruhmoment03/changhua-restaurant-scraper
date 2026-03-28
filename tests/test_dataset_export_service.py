"""Tests for the canonical latest dataset export service wrapper."""

from __future__ import annotations

import pytest

from modules.dataset_export_service import (
    generate_latest_dataset_bundle,
    load_latest_dataset_bundle_summary,
    preview_latest_dataset_bundle_artifact,
    read_latest_dataset_bundle_artifact,
)
from modules.review_db import ReviewDB


def _make_review(rid: str = "r1", text: str = "Great!"):
    return {
        "review_id": rid,
        "text": text,
        "rating": 5.0,
        "likes": 1,
        "lang": "en",
        "date": "3 months ago",
        "review_date": "2025-06-15",
        "author": "Tester",
        "profile": "",
        "avatar": "",
        "owner_text": "",
        "photos": [],
    }


def _config_payload():
    return {
        "db_path": "ignored.db",
        "businesses": [
            {
                "url": "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
                "custom_params": {
                    "company": "Alpha",
                    "google_place_id": "PID_A",
                },
            }
        ],
    }


def test_generate_and_load_latest_dataset_bundle_summary(tmp_path, monkeypatch):
    output_dir = tmp_path / "latest_bundle"
    monkeypatch.setenv("DATASET_EXPORT_LATEST_DIR", str(output_dir))

    db = ReviewDB(str(tmp_path / "reviews.db"))
    try:
        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
        )
        db.upsert_review("place_a", _make_review("r1", " Great\nfood "))

        payload = generate_latest_dataset_bundle(
            review_db=db,
            config=_config_payload(),
            config_path=str(tmp_path / "config.top50.yaml"),
            min_reviews=25,
            include_deleted=False,
        )
    finally:
        db.close()

    assert payload["output_dir"] == str(output_dir.resolve())
    assert payload["manifest"]["bundle_version"] == "dataset-quality-provenance-v2"
    assert payload["manifest"]["min_reviews"] == 25
    assert payload["qa_report_excerpt"]["summary"]["targets_total"] == 1

    artifacts = {artifact["filename"]: artifact for artifact in payload["artifacts"]}
    assert "reviews_cleaned.csv" in artifacts
    assert artifacts["reviews_cleaned.csv"]["exists"] is True
    assert artifacts["reviews_cleaned.csv"]["previewable"] is True
    assert artifacts["reviews_cleaned.csv"]["download_path"].endswith("/reviews_cleaned.csv")
    assert artifacts["reviews_cleaned.csv"]["preview_path"].endswith("/reviews_cleaned.csv/preview")
    assert artifacts["qa_report.json"]["previewable"] is False
    assert artifacts["qa_report.json"]["preview_path"] is None

    latest = load_latest_dataset_bundle_summary()
    assert latest["output_dir"] == str(output_dir.resolve())
    assert latest["manifest"]["config_path"] == str(tmp_path / "config.top50.yaml")


def test_load_latest_dataset_bundle_summary_raises_when_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("DATASET_EXPORT_LATEST_DIR", str(tmp_path / "missing_bundle"))

    with pytest.raises(FileNotFoundError):
        load_latest_dataset_bundle_summary()


def test_read_latest_dataset_bundle_artifact_is_manifest_based_only(tmp_path, monkeypatch):
    output_dir = tmp_path / "latest_bundle"
    monkeypatch.setenv("DATASET_EXPORT_LATEST_DIR", str(output_dir))

    db = ReviewDB(str(tmp_path / "reviews.db"))
    try:
        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
        )
        db.upsert_review("place_a", _make_review("r1", "Great food"))
        generate_latest_dataset_bundle(
            review_db=db,
            config=_config_payload(),
            config_path=str(tmp_path / "config.top50.yaml"),
            min_reviews=10,
            include_deleted=False,
        )
    finally:
        db.close()

    rogue_path = output_dir / "rogue.csv"
    rogue_path.write_text("not,listed\n", encoding="utf-8")

    payload, media_type, filename = read_latest_dataset_bundle_artifact("reviews_cleaned.csv")
    assert media_type.startswith("text/csv")
    assert filename == "reviews_cleaned.csv"
    assert payload.startswith(b"\xef\xbb\xbf")
    assert b"review_id" in payload

    with pytest.raises(FileNotFoundError):
        read_latest_dataset_bundle_artifact("rogue.csv")

    with pytest.raises(FileNotFoundError):
        read_latest_dataset_bundle_artifact("../qa_report.json")


def test_preview_latest_dataset_bundle_artifact_returns_csv_sample_and_rejects_non_csv(tmp_path, monkeypatch):
    output_dir = tmp_path / "latest_bundle"
    monkeypatch.setenv("DATASET_EXPORT_LATEST_DIR", str(output_dir))

    db = ReviewDB(str(tmp_path / "reviews.db"))
    try:
        db.upsert_place(
            "place_a",
            "曹環",
            "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
        )
        for index in range(30):
            db.upsert_review("place_a", _make_review(f"r{index}", f"測試評論 {index}"))
        generate_latest_dataset_bundle(
            review_db=db,
            config=_config_payload(),
            config_path=str(tmp_path / "config.top50.yaml"),
            min_reviews=10,
            include_deleted=False,
        )
    finally:
        db.close()

    preview = preview_latest_dataset_bundle_artifact("reviews_cleaned.csv")
    assert preview["artifact"]["filename"] == "reviews_cleaned.csv"
    assert preview["artifact"]["previewable"] is True
    assert preview["preview"]["kind"] == "csv"
    assert preview["preview"]["columns"][0] == "place_id"
    assert preview["preview"]["sample_row_count"] == 25
    assert preview["preview"]["total_row_count"] == 30
    assert preview["preview"]["truncated"] is True
    assert {row["review_text_raw"] for row in preview["preview"]["rows"]} == {
        f"測試評論 {index}" for index in range(5, 30)
    }

    with pytest.raises(ValueError):
        preview_latest_dataset_bundle_artifact("qa_report.json")

    with pytest.raises(FileNotFoundError):
        preview_latest_dataset_bundle_artifact("rogue.csv")

    with pytest.raises(FileNotFoundError):
        preview_latest_dataset_bundle_artifact("../dataset_manifest.json")
