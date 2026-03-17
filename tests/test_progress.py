"""Unit tests for shared progress computation helpers."""

from modules.progress import compute_progress_report
from modules.review_db import ReviewDB


def _make_review(rid="r1"):
    return {
        "review_id": rid,
        "text": "Great!",
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


def test_query_place_id_match_precedes_url_fallback(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        businesses = [
            {
                "url": "https://www.google.com/maps/search/?api=1&query=Target&query_place_id=PID_X",
                "custom_params": {"company": "Target", "google_place_id": "PID_X"},
            }
        ]

        # query_place_id match row (no reviews)
        db.upsert_place(
            "place_qpid",
            "QP Place",
            "https://www.google.com/maps/search/?api=1&query=Other&query_place_id=PID_X",
        )

        # URL match row (has reviews) -> should NOT be selected when query_place_id is present
        db.upsert_place(
            "place_url",
            "URL Place",
            "https://www.google.com/maps/search/?api=1&query=Target&query_place_id=PID_OTHER",
        )
        db.upsert_review("place_url", _make_review("r_url"))

        report = compute_progress_report(businesses, db)
        target = report["targets"][0]
        assert target["place_id"] == "place_qpid"
        assert target["status"] == "present_zero_reviews"
    finally:
        db.close()


def test_url_fallback_works_when_query_place_id_missing(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        url = "https://www.google.com/maps/place/Foo/@1,2,3z"
        businesses = [{"url": url, "custom_params": {"company": "Foo"}}]

        db.upsert_place("place1", "Foo Place", url)
        db.upsert_review("place1", _make_review("r1"))

        report = compute_progress_report(businesses, db)
        target = report["targets"][0]
        assert target["place_id"] == "place1"
        assert target["status"] == "with_reviews"
    finally:
        db.close()


def test_missing_vs_present_zero_classification(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        businesses = [
            {
                "url": "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
                "custom_params": {"company": "B", "google_place_id": "PID_B"},
            },
            {
                "url": "https://www.google.com/maps/search/?api=1&query=C&query_place_id=PID_C",
                "custom_params": {"company": "C", "google_place_id": "PID_C"},
            },
        ]

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )

        report = compute_progress_report(businesses, db)
        assert report["present_zero_reviews"] == 1
        assert report["missing_from_db"] == 1
        assert report["with_reviews"] == 0
    finally:
        db.close()


def test_threshold_fields_are_derived_from_min_reviews(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        businesses = [
            {
                "url": "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
                "custom_params": {"company": "A", "google_place_id": "PID_A"},
            },
            {
                "url": "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
                "custom_params": {"company": "B", "google_place_id": "PID_B"},
            },
            {
                "url": "https://www.google.com/maps/search/?api=1&query=C&query_place_id=PID_C",
                "custom_params": {"company": "C", "google_place_id": "PID_C"},
            },
        ]

        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
        )
        for i in range(60):
            db.upsert_review("place_a", _make_review(f"a_{i}"))

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        for i in range(12):
            db.upsert_review("place_b", _make_review(f"b_{i}"))

        report = compute_progress_report(businesses, db, min_reviews=50)
        assert report["min_reviews"] == 50
        assert report["meeting_min_reviews"] == 1
        assert report["under_min_reviews"] == 2

        by_qpid = {t["google_place_id"]: t for t in report["targets"]}
        assert by_qpid["PID_A"]["meets_min_reviews"] is True
        assert by_qpid["PID_A"]["reviews_needed"] == 0
        assert by_qpid["PID_B"]["meets_min_reviews"] is False
        assert by_qpid["PID_B"]["reviews_needed"] == 38
        assert by_qpid["PID_C"]["meets_min_reviews"] is False
        assert by_qpid["PID_C"]["reviews_needed"] == 50
    finally:
        db.close()
