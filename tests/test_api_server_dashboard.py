"""Tests for dashboard-facing API endpoint handlers."""

import asyncio
import json
import os
from pathlib import Path
import re

import pytest
import yaml
from fastapi import HTTPException

import api_server
from modules.google_places_service import PlaceHit
from modules.review_db import ReviewDB


class _FakeJobManager:
    def __init__(self):
        self.created = []
        self._idx = 0
        self.max_concurrent_jobs = 1
        self.updated_limits = []

    def create_job(self, url, config_overrides=None):
        self._idx += 1
        job_id = f"job_{self._idx}"
        self.created.append({"job_id": job_id, "url": url, "config_overrides": config_overrides or {}})
        return job_id

    def start_job(self, job_id):
        return True

    def set_max_concurrent_jobs(self, limit):
        self.max_concurrent_jobs = int(limit)
        self.updated_limits.append(int(limit))
        return self.max_concurrent_jobs


def _make_review(rid="r1", text="Great!"):
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


def _write_config(path: Path):
    path.write_text(
        yaml.safe_dump(
            {
                "headless": True,
                "scrape_mode": "update",
                "businesses": [
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
                ],
            }
        ),
        encoding="utf-8",
    )


def test_progress_endpoint_reports_threshold_fields(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
        )
        for i in range(52):
            db.upsert_review("place_a", _make_review(f"ra_{i}"))

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        for i in range(10):
            db.upsert_review("place_b", _make_review(f"rb_{i}"))

        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        report = asyncio.run(api_server.get_progress(config_path=str(cfg_path), min_reviews=50, review_db=db))
        assert report.targets_total == 3
        assert report.meeting_min_reviews == 1
        assert report.under_min_reviews == 2

        by_qpid = {t.google_place_id: t for t in report.targets}
        assert by_qpid["PID_A"].meets_min_reviews is True
        assert by_qpid["PID_B"].reviews_needed == 40
        assert by_qpid["PID_C"].reviews_needed == 50
    finally:
        db.close()


def test_scrape_concurrency_limit_defaults_to_isolated_mode(monkeypatch):
    monkeypatch.delenv("SCRAPER_MAX_CONCURRENT_JOBS", raising=False)
    assert api_server._scrape_concurrency_limit() == 1


def test_scrape_concurrency_limit_honors_valid_env(monkeypatch):
    monkeypatch.setenv("SCRAPER_MAX_CONCURRENT_JOBS", "2")
    assert api_server._scrape_concurrency_limit() == 2


def test_scrape_concurrency_limit_rejects_bad_env(monkeypatch):
    monkeypatch.setenv("SCRAPER_MAX_CONCURRENT_JOBS", "not-an-int")
    assert api_server._scrape_concurrency_limit() == 1


def test_get_scrape_settings_reports_current_job_manager_limit(monkeypatch):
    fake = _FakeJobManager()
    fake.max_concurrent_jobs = 4
    monkeypatch.setattr(api_server, "job_manager", fake)

    result = asyncio.run(api_server.ops_get_scrape_settings())

    assert result.max_concurrent_jobs == 4


def test_update_scrape_settings_applies_new_limit(monkeypatch):
    fake = _FakeJobManager()
    monkeypatch.setattr(api_server, "job_manager", fake)

    result = asyncio.run(
        api_server.ops_update_scrape_settings(
            api_server.UpdateScrapeSettingsRequest(max_concurrent_jobs=10)
        )
    )

    assert result.max_concurrent_jobs == 10
    assert fake.max_concurrent_jobs == 10
    assert fake.updated_limits == [10]


def test_progress_counts_exhausted_under_threshold_separately(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        for i in range(5):
            db.upsert_review("place_b", _make_review(f"rb_{i}"))
        db.backend.execute(
            "UPDATE places SET reviews_exhausted = 1 WHERE place_id = ?",
            ("place_b",),
        )
        db.backend.commit()

        report = asyncio.run(api_server.get_progress(config_path=str(cfg_path), min_reviews=50, review_db=db))
        by_qpid = {t.google_place_id: t for t in report.targets}
        assert by_qpid["PID_B"].meets_min_reviews is False
        assert by_qpid["PID_B"].reviews_exhausted is True
        assert by_qpid["PID_B"].reviews_needed == 0
        assert report.meeting_min_reviews == 0
        assert report.under_min_reviews == 2
        assert report.exhausted_under_threshold_count == 1
    finally:
        db.close()


def test_ops_reset_exhausted_targets_restores_under_threshold_place(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        for i in range(5):
            db.upsert_review("place_b", _make_review(f"rb_{i}"))
        db.backend.execute(
            "UPDATE places SET reviews_exhausted = 1 WHERE place_id = ?",
            ("place_b",),
        )
        db.backend.commit()

        request = api_server.ResetExhaustedTargetsRequest(
            config_path=str(cfg_path),
            min_reviews=50,
        )
        result = asyncio.run(api_server.ops_reset_exhausted_targets(request, review_db=db))

        assert result["reset_count"] == 1
        row = db.backend.fetchone(
            "SELECT reviews_exhausted FROM places WHERE place_id = ?",
            ("place_b",),
        )
        assert row["reviews_exhausted"] == 0

        report = asyncio.run(api_server.get_progress(config_path=str(cfg_path), min_reviews=50, review_db=db))
        by_qpid = {t.google_place_id: t for t in report.targets}
        assert by_qpid["PID_B"].meets_min_reviews is False
        assert by_qpid["PID_B"].reviews_exhausted is False
        assert by_qpid["PID_B"].reviews_needed == 45
    finally:
        db.close()


def test_ops_reset_exhausted_targets_can_scope_to_single_place(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        for place_id, query_place_id in (("place_a", "PID_A"), ("place_b", "PID_B")):
            db.upsert_place(
                place_id,
                f"Place {place_id}",
                f"https://www.google.com/maps/search/?api=1&query={place_id}&query_place_id={query_place_id}",
            )
            for i in range(5):
                db.upsert_review(place_id, _make_review(f"{place_id}_{i}"))
            db.backend.execute(
                "UPDATE places SET reviews_exhausted = 1 WHERE place_id = ?",
                (place_id,),
            )
        db.backend.commit()

        request = api_server.ResetExhaustedTargetsRequest(
            config_path=str(cfg_path),
            min_reviews=50,
            place_id="place_b",
        )
        result = asyncio.run(api_server.ops_reset_exhausted_targets(request, review_db=db))

        assert result["reset_count"] == 1
        rows = db.backend.fetchall(
            "SELECT place_id, reviews_exhausted FROM places ORDER BY place_id"
        )
        assert rows[0]["place_id"] == "place_a" and rows[0]["reviews_exhausted"] == 1
        assert rows[1]["place_id"] == "place_b" and rows[1]["reviews_exhausted"] == 0
    finally:
        db.close()


def test_progress_endpoint_404_for_missing_config(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        missing = tmp_path / "does-not-exist.yaml"
        with pytest.raises(HTTPException) as exc:
            asyncio.run(api_server.get_progress(config_path=str(missing), review_db=db))
        assert exc.value.status_code == 404
    finally:
        db.close()


def test_system_log_tail_filters_and_limits(tmp_path):
    log_path = Path(tmp_path) / "scraper.log"
    lines = [
        {"ts": "2026-03-02T10:00:00", "level": "INFO", "logger": "scraper", "msg": "ok"},
        {"ts": "2026-03-02T10:00:01", "level": "ERROR", "logger": "scraper", "msg": "boom1"},
        {"ts": "2026-03-02T10:00:02", "level": "ERROR", "logger": "scraper", "msg": "boom2"},
    ]
    log_path.write_text("\n".join(json.dumps(l) for l in lines) + "\n", encoding="utf-8")

    old_dir = api_server._config.get("log_dir")
    old_file = api_server._config.get("log_file")
    api_server._config["log_dir"] = str(tmp_path)
    api_server._config["log_file"] = "scraper.log"

    try:
        rows = asyncio.run(api_server.get_log_tail(level="ERROR", limit=1))
        assert len(rows) == 1
        assert rows[0].level == "ERROR"
        assert rows[0].msg == "boom2"
    finally:
        api_server._config["log_dir"] = old_dir
        api_server._config["log_file"] = old_file


def test_load_env_exports_parses_export_syntax(tmp_path, monkeypatch):
    env_file = tmp_path / ".env.google_maps.cookies"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "export GOOGLE_MAPS_COOKIE_1PSID=alpha",
                'GOOGLE_MAPS_COOKIE_1PSIDTS="beta"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("GOOGLE_MAPS_COOKIE_1PSID", raising=False)
    monkeypatch.delenv("GOOGLE_MAPS_COOKIE_1PSIDTS", raising=False)

    loaded = api_server._load_env_exports(env_file)

    assert loaded == 2
    assert os.environ["GOOGLE_MAPS_COOKIE_1PSID"] == "alpha"
    assert os.environ["GOOGLE_MAPS_COOKIE_1PSIDTS"] == "beta"


def test_ops_scrape_all_selects_under_threshold_targets(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    fake = _FakeJobManager()
    old_job_manager = api_server.job_manager
    api_server.job_manager = fake
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
        )
        for i in range(55):
            db.upsert_review("place_a", _make_review(f"ra_{i}"))

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        for i in range(5):
            db.upsert_review("place_b", _make_review(f"rb_{i}"))

        req = api_server.ScrapeAllRequest(config_path=str(cfg_path), min_reviews=50)
        res = asyncio.run(api_server.ops_scrape_all(req, review_db=db))

        assert res["selected_targets"] == 2  # B below threshold + C missing
        assert res["created_count"] == 2
        assert len(fake.created) == 2
    finally:
        api_server.job_manager = old_job_manager
        db.close()


def test_ops_scrape_all_uses_text_reviews_for_threshold(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    fake = _FakeJobManager()
    old_job_manager = api_server.job_manager
    api_server.job_manager = fake
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
        )
        for i in range(60):
            db.upsert_review("place_a", _make_review(f"ra_{i}", text=""))

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        for i in range(5):
            db.upsert_review("place_b", _make_review(f"rb_{i}"))

        req = api_server.ScrapeAllRequest(config_path=str(cfg_path), min_reviews=50)
        res = asyncio.run(api_server.ops_scrape_all(req, review_db=db))

        created_qpids = {entry["google_place_id"] for entry in res["created_jobs"]}
        assert "PID_A" in created_qpids
        assert "PID_B" in created_qpids
        assert "PID_C" in created_qpids
    finally:
        api_server.job_manager = old_job_manager
        db.close()


def test_ops_scrape_all_raises_target_max_reviews_floor(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    fake = _FakeJobManager()
    old_job_manager = api_server.job_manager
    api_server.job_manager = fake
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        raw["businesses"][0]["max_reviews"] = 120
        cfg_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

        req = api_server.ScrapeAllRequest(
            config_path=str(cfg_path),
            min_reviews=100,
            default_max_reviews=300,
            only_below_threshold=False,
        )
        res = asyncio.run(api_server.ops_scrape_all(req, review_db=db))

        assert res["created_count"] == 3
        assert len(fake.created) == 3
        for row in fake.created:
            assert int(row["config_overrides"].get("max_reviews", 0)) >= 300
    finally:
        api_server.job_manager = old_job_manager
        db.close()


def test_ops_scrape_all_can_skip_known_totals_below_goal(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    fake = _FakeJobManager()
    old_job_manager = api_server.job_manager
    api_server.job_manager = fake
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
        )
        for i in range(55):
            db.upsert_review("place_a", _make_review(f"ra_{i}"))

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        for i in range(5):
            db.upsert_review("place_b", _make_review(f"rb_{i}"))
        db.backend.execute(
            "UPDATE places SET total_reviews = ? WHERE place_id = ?",
            (39, "place_b"),
        )
        db.backend.commit()

        req = api_server.ScrapeAllRequest(
            config_path=str(cfg_path),
            min_reviews=50,
            exclude_known_below_goal=True,
        )
        res = asyncio.run(api_server.ops_scrape_all(req, review_db=db))

        assert res["selected_targets"] == 1  # C missing remains unknown and queueable
        assert res["created_count"] == 1
        assert len(fake.created) == 1
        assert res["skipped_count"] == 1
        assert res["skipped_targets"][0]["google_place_id"] == "PID_B"
        assert res["skipped_targets"][0]["reason"] == "known_total_below_goal"
        assert res["skipped_targets"][0]["cached_total_reviews"] == 39
    finally:
        api_server.job_manager = old_job_manager
        db.close()


def test_ops_scrape_target_enqueues_one_by_place_id(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    fake = _FakeJobManager()
    old_job_manager = api_server.job_manager
    api_server.job_manager = fake
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )

        req = api_server.ScrapeTargetRequest(
            config_path=str(cfg_path),
            place_id="place_b",
            max_reviews=250,
        )
        res = asyncio.run(api_server.ops_scrape_target(req, review_db=db))

        assert str(res["job_id"]).startswith("job_")
        assert res["effective_max_reviews"] == 250
        assert len(fake.created) == 1
    finally:
        api_server.job_manager = old_job_manager
        db.close()


def test_ops_set_target_max_reviews_persists_config(tmp_path):
    cfg_path = tmp_path / "config.top50.yaml"
    _write_config(cfg_path)

    req = api_server.TargetMaxReviewsRequest(
        config_path=str(cfg_path),
        google_place_id="PID_B",
        max_reviews=333,
    )
    res = asyncio.run(api_server.ops_set_target_max_reviews(req))
    assert res["max_reviews"] == 333

    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    found = None
    for b in raw.get("businesses", []):
        qpid = str((b.get("custom_params", {}) or {}).get("google_place_id", "")).strip()
        if qpid == "PID_B":
            found = b
            break
    assert found is not None
    assert int(found.get("max_reviews")) == 333


def test_export_place_json_endpoint_returns_download(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("place_a", "Place A", "https://example.com/a")
        db.upsert_review("place_a", _make_review("r1"))

        resp = asyncio.run(
            api_server.export_place(
                place_id="place_a",
                format="json",
                include_deleted=False,
                review_db=db,
            )
        )
        assert resp.status_code == 200
        assert "attachment; filename=" in resp.headers.get("Content-Disposition", "")

        payload = json.loads(resp.body.decode("utf-8"))
        assert payload["place"]["place_id"] == "place_a"
        assert payload["export_meta"]["scope"] == "place"
        assert len(payload["reviews"]) == 1
    finally:
        db.close()


def test_export_all_csv_endpoint_returns_flat_header(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("place_a", "Place A", "https://example.com/a")
        db.upsert_review("place_a", _make_review("r1"))

        resp = asyncio.run(
            api_server.export_all(
                format="csv",
                include_deleted=False,
                review_db=db,
            )
        )
        assert resp.status_code == 200
        assert resp.media_type.startswith("text/csv")
        assert resp.body.startswith(b"\xef\xbb\xbf")
        text = resp.body.decode("utf-8-sig")
        assert "place_id,place_name,review_id" in text
        assert "place_a,Place A,r1" in text
    finally:
        db.close()


def test_export_place_xlsx_endpoint_returns_binary(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("place_a", "Place A", "https://example.com/a")
        db.upsert_review("place_a", _make_review("r1"))

        resp = asyncio.run(
            api_server.export_place(
                place_id="place_a",
                format="xlsx",
                include_deleted=False,
                review_db=db,
            )
        )
        assert resp.status_code == 200
        # XLSX is a zip container.
        assert resp.body[:2] == b"PK"
        assert resp.headers.get("Content-Disposition", "").endswith(".xlsx\"")
    finally:
        db.close()


def test_export_place_csv_endpoint_returns_download(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("place_a", "Place A", "https://example.com/a")
        db.upsert_review("place_a", _make_review("r1"))

        resp = asyncio.run(
            api_server.export_place(
                place_id="place_a",
                format="csv",
                include_deleted=False,
                review_db=db,
            )
        )
        assert resp.status_code == 200
        assert resp.body.startswith(b"\xef\xbb\xbf")
        assert "place_id,place_name,review_id" in resp.body.decode("utf-8-sig")
        assert "attachment; filename=" in resp.headers.get("Content-Disposition", "")
    finally:
        db.close()


def test_dataset_bundle_generate_and_latest_endpoints(tmp_path, monkeypatch):
    db = ReviewDB(str(tmp_path / "test.db"))
    monkeypatch.setenv("DATASET_EXPORT_LATEST_DIR", str(tmp_path / "latest_bundle"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
        )
        db.upsert_review("place_a", _make_review("r1"))

        request = api_server.DatasetBundleGenerateRequest(
            config_path=str(cfg_path),
            min_reviews=25,
            include_deleted=False,
        )
        generated = asyncio.run(api_server.generate_dataset_bundle(request, review_db=db))

        assert generated.output_dir == str((tmp_path / "latest_bundle").resolve())
        assert generated.manifest["bundle_version"] == "dataset-quality-provenance-v2"
        assert generated.qa_report_excerpt["summary"]["targets_total"] == 3
        artifacts = {artifact.filename: artifact for artifact in generated.artifacts}
        assert "reviews_cleaned.csv" in artifacts
        assert "qa_report.json" in artifacts
        assert artifacts["reviews_cleaned.csv"].previewable is True
        assert artifacts["reviews_cleaned.csv"].preview_path.endswith("/reviews_cleaned.csv/preview")
        assert artifacts["qa_report.json"].previewable is False

        latest = asyncio.run(api_server.get_latest_dataset_bundle())
        assert latest.output_dir == generated.output_dir
        assert latest.manifest["config_path"] == str(cfg_path)
    finally:
        db.close()


def test_dataset_bundle_latest_endpoint_404_when_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("DATASET_EXPORT_LATEST_DIR", str(tmp_path / "missing_bundle"))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_server.get_latest_dataset_bundle())
    assert exc.value.status_code == 404


def test_dataset_bundle_artifact_download_endpoint_is_manifest_validated(tmp_path, monkeypatch):
    db = ReviewDB(str(tmp_path / "test.db"))
    monkeypatch.setenv("DATASET_EXPORT_LATEST_DIR", str(tmp_path / "latest_bundle"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_a",
            "Place A",
            "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
        )
        db.upsert_review("place_a", _make_review("r1"))

        request = api_server.DatasetBundleGenerateRequest(config_path=str(cfg_path))
        asyncio.run(api_server.generate_dataset_bundle(request, review_db=db))

        rogue_path = tmp_path / "latest_bundle" / "rogue.csv"
        rogue_path.parent.mkdir(parents=True, exist_ok=True)
        rogue_path.write_text("rogue\n", encoding="utf-8")

        resp = asyncio.run(
            api_server.download_latest_dataset_bundle_artifact("reviews_cleaned.csv")
        )
        assert resp.status_code == 200
        assert resp.media_type.startswith("text/csv")
        assert resp.headers.get("Content-Disposition", "").endswith('"reviews_cleaned.csv"')
        assert resp.body.startswith(b"\xef\xbb\xbf")
        assert b"review_id" in resp.body

        with pytest.raises(HTTPException) as exc:
            asyncio.run(api_server.download_latest_dataset_bundle_artifact("rogue.csv"))
        assert exc.value.status_code == 404
    finally:
        db.close()


def test_dataset_bundle_artifact_preview_endpoint_returns_csv_rows_and_rejects_json(tmp_path, monkeypatch):
    db = ReviewDB(str(tmp_path / "test.db"))
    monkeypatch.setenv("DATASET_EXPORT_LATEST_DIR", str(tmp_path / "latest_bundle"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        db.upsert_place(
            "place_a",
            "曹環",
            "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
        )
        for index in range(30):
            db.upsert_review("place_a", _make_review(f"r{index}"))
            db.backend.execute(
                "UPDATE reviews SET review_text = ? WHERE review_id = ? AND place_id = ?",
                (json.dumps({"zh-Hant": f"測試評論 {index}"}, ensure_ascii=False), f"r{index}", "place_a"),
            )
        db.backend.commit()

        request = api_server.DatasetBundleGenerateRequest(config_path=str(cfg_path))
        asyncio.run(api_server.generate_dataset_bundle(request, review_db=db))

        preview = asyncio.run(
            api_server.preview_latest_dataset_bundle_artifact("reviews_cleaned.csv")
        )
        assert preview.artifact.filename == "reviews_cleaned.csv"
        assert preview.preview.kind == "csv"
        assert preview.preview.columns[0] == "place_id"
        assert preview.preview.sample_row_count == 25
        assert preview.preview.total_row_count == 30
        assert preview.preview.truncated is True
        assert {row["review_text_raw"] for row in preview.preview.rows} == {
            f"測試評論 {index}" for index in range(5, 30)
        }

        with pytest.raises(HTTPException) as exc:
            asyncio.run(api_server.preview_latest_dataset_bundle_artifact("qa_report.json"))
        assert exc.value.status_code == 400

        with pytest.raises(HTTPException) as exc:
            asyncio.run(api_server.preview_latest_dataset_bundle_artifact("rogue.csv"))
        assert exc.value.status_code == 404
    finally:
        db.close()


@pytest.mark.parametrize("fmt", ["json", "csv", "xlsx"])
def test_export_all_endpoint_supports_all_formats(tmp_path, fmt):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("place_a", "Place A", "https://example.com/a")
        db.upsert_review("place_a", _make_review("r1"))

        resp = asyncio.run(
            api_server.export_all(
                format=fmt,
                include_deleted=False,
                review_db=db,
            )
        )
        assert resp.status_code == 200
        assert "attachment; filename=" in resp.headers.get("Content-Disposition", "")
        if fmt == "json":
            payload = json.loads(resp.body.decode("utf-8"))
            assert payload["export_meta"]["scope"] == "all"
        elif fmt == "csv":
            assert "place_id,place_name,review_id" in resp.body.decode("utf-8")
        else:
            assert resp.body[:2] == b"PK"
    finally:
        db.close()


def test_export_place_direct_call_honors_columns_and_exclude_empty_text(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("place_a", "Place A", "https://example.com/a")
        db.upsert_review("place_a", _make_review("r1"))
        empty_text_review = _make_review("r2")
        empty_text_review["text"] = ""
        db.upsert_review("place_a", empty_text_review)

        resp = asyncio.run(
            api_server.export_place(
                place_id="place_a",
                format="csv",
                include_deleted=False,
                exclude_empty_text=True,
                columns="place_id,review_id,review_text_primary",
                review_db=db,
            )
        )

        text = resp.body.decode("utf-8-sig").splitlines()
        assert text[0] == "place_id,review_id,review_text_primary"
        assert text[1] == "place_a,r1,Great!"
        assert len(text) == 2
    finally:
        db.close()


def test_export_all_direct_call_honors_columns_and_exclude_empty_text(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("place_a", "Place A", "https://example.com/a")
        db.upsert_review("place_a", _make_review("r1"))
        empty_text_review = _make_review("r2")
        empty_text_review["text"] = ""
        db.upsert_review("place_a", empty_text_review)

        resp = asyncio.run(
            api_server.export_all(
                format="csv",
                include_deleted=False,
                exclude_empty_text=True,
                columns="place_id,review_id,review_text_primary",
                review_db=db,
            )
        )

        text = resp.body.decode("utf-8-sig").splitlines()
        assert text[0] == "place_id,review_id,review_text_primary"
        assert text[1] == "place_a,r1,Great!"
        assert len(text) == 2
    finally:
        db.close()


def test_export_all_direct_call_honors_min_review_count(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("place_a", "Place A", "https://example.com/a")
        db.upsert_place("place_b", "Place B", "https://example.com/b")
        db.upsert_review("place_a", _make_review("r1"))
        db.upsert_review("place_b", _make_review("r2"))
        db.upsert_review("place_b", _make_review("r3"))

        resp = asyncio.run(
            api_server.export_all(
                format="csv",
                include_deleted=False,
                min_review_count=2,
                columns="place_id,review_id",
                review_db=db,
            )
        )

        text = resp.body.decode("utf-8-sig").splitlines()
        assert text[0] == "place_id,review_id"
        assert set(text[1:]) == {"place_b,r2", "place_b,r3"}
        assert len(text) == 3
    finally:
        db.close()


def test_validate_places_endpoint_persists_google_places_results(tmp_path, monkeypatch):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)
        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )

        monkeypatch.setattr(
            api_server,
            "validate_place",
            lambda **_: {
                "google_place_id": "PID_B",
                "status": "invalid_mismatch",
                "reason": "Expected Place B but got Wrong Place",
                "api_name": "Wrong Place",
                "api_address": "Addr",
                "business_status": "OPERATIONAL",
                "checked_at": "2026-03-09T00:00:00+00:00",
            },
        )

        request = api_server.ValidatePlacesRequest(
            config_path=str(cfg_path),
            google_place_ids=["PID_B"],
        )
        result = asyncio.run(api_server.ops_validate_places(request, review_db=db))

        assert result.invalid_count == 1
        assert result.results[0].status == "invalid_mismatch"
        place = db.get_place("place_b")
        assert place["validation_status"] == "invalid_mismatch"
        latest = db.get_latest_place_validation(place_id="place_b")
        assert latest is not None
        assert latest["api_name"] == "Wrong Place"
    finally:
        db.close()


def test_archive_invalid_place_removes_config_and_db_and_writes_archive(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)
        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        db.upsert_review("place_b", _make_review("r1"))
        db.record_place_validation(
            place_id="place_b",
            google_place_id="PID_B",
            config_path=str(cfg_path),
            expected_name="Place B",
            status="invalid_closed",
            reason="Place is permanently closed",
            api_name="Place B",
            api_address="Addr",
            business_status="CLOSED_PERMANENTLY",
            checked_at="2026-03-09T00:00:00+00:00",
            response_payload={"status": "invalid_closed"},
        )

        request = api_server.ArchiveInvalidPlaceRequest(
            config_path=str(cfg_path),
            place_id="place_b",
        )
        result = asyncio.run(api_server.ops_archive_invalid_place(request, review_db=db))

        assert result.archived.google_place_id == "PID_B"
        assert db.get_place("place_b") is None
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        google_place_ids = [
            str((business.get("custom_params", {}) or {}).get("google_place_id", "")).strip()
            for business in raw.get("businesses", [])
        ]
        assert "PID_B" not in google_place_ids
        rows = db.list_invalid_place_archives(limit=10)
        assert len(rows) == 1
    finally:
        db.close()


def test_archive_invalid_place_rejects_active_jobs(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    old_job_manager = api_server.job_manager

    class _ActiveJobs:
        def list_jobs(self, limit=1000):
            return [
                type(
                    "Job",
                    (),
                    {
                        "status": api_server.JobStatus.RUNNING,
                        "url": "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
                        "to_dict": lambda self: {"job_id": "job_1"},
                    },
                )()
            ]

    api_server.job_manager = _ActiveJobs()
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)
        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        db.record_place_validation(
            place_id="place_b",
            google_place_id="PID_B",
            config_path=str(cfg_path),
            expected_name="Place B",
            status="invalid_not_found",
            reason="Place Details returned NOT_FOUND",
            api_name=None,
            api_address=None,
            business_status=None,
            checked_at="2026-03-09T00:00:00+00:00",
            response_payload={"status": "invalid_not_found"},
        )

        request = api_server.ArchiveInvalidPlaceRequest(config_path=str(cfg_path), place_id="place_b")
        with pytest.raises(HTTPException) as exc:
            asyncio.run(api_server.ops_archive_invalid_place(request, review_db=db))
        assert exc.value.status_code == 409
    finally:
        api_server.job_manager = old_job_manager
        db.close()


def test_discovery_search_marks_config_and_db_duplicates(tmp_path, monkeypatch):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)
        db.upsert_place(
            "place_d",
            "Place D",
            "https://www.google.com/maps/search/?api=1&query=D&query_place_id=PID_D",
        )

        monkeypatch.setattr(api_server, "get_google_places_api_key", lambda: "test-key")
        monkeypatch.setattr(
            api_server,
            "fetch_places_textsearch",
            lambda **_: [
                PlaceHit("A", "Addr A", "PID_A", 4.9, 300),
                PlaceHit("D", "Addr D", "PID_D", 4.8, 200),
                PlaceHit("E", "Addr E", "PID_E", 4.7, 150),
            ],
        )

        request = api_server.DiscoverySearchRequest(config_path=str(cfg_path), query="restaurants")
        result = asyncio.run(api_server.ops_discovery_search(request, review_db=db))
        by_id = {row.google_place_id: row for row in result.candidates}
        assert by_id["PID_A"].status == "duplicate_config"
        assert by_id["PID_D"].status == "duplicate_db"
        assert by_id["PID_E"].status == "staged"
    finally:
        db.close()


def test_approve_discovery_candidates_appends_businesses(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)
        rows = db.upsert_discovery_candidates(
            config_path=str(cfg_path),
            query="restaurants",
            candidates=[
                {
                    "google_place_id": "PID_D",
                    "name": "Place D",
                    "formatted_address": "Addr D",
                    "rating": 4.9,
                    "user_ratings_total": 111,
                    "maps_url": "https://www.google.com/maps/search/?api=1&query=D&query_place_id=PID_D",
                    "status": "staged",
                    "duplicate_source": None,
                    "source_payload": {"place_id": "PID_D"},
                }
            ],
        )
        request = api_server.CandidateSelectionRequest(
            config_path=str(cfg_path),
            candidate_ids=[rows[0]["candidate_id"]],
        )
        result = asyncio.run(api_server.ops_approve_discovery_candidates(request, review_db=db))

        assert result.approved_count == 1
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        assert any(
            str((business.get("custom_params", {}) or {}).get("google_place_id", "")).strip() == "PID_D"
            for business in raw.get("businesses", [])
        )
        refreshed = db.list_discovery_candidates(candidate_ids=[rows[0]["candidate_id"]])
        assert refreshed[0]["status"] == "approved"
    finally:
        db.close()


def test_dashboard_discovery_panel_does_not_truncate_candidates_to_40():
    page_path = Path(__file__).resolve().parents[1] / "dashboard" / "src" / "app" / "page.tsx"
    source = page_path.read_text(encoding="utf-8")

    assert "const DISCOVERY_CANDIDATE_LIST_LIMIT = 200;" in source
    assert re.search(
        r"getDiscoveryCandidates\(\{\s*configPath:\s*CONFIG_PATH,\s*limit:\s*DISCOVERY_CANDIDATE_LIST_LIMIT\s*\}\)",
        source,
    )
    assert "getDiscoveryCandidates({ configPath: CONFIG_PATH, limit: 40 })" not in source


def test_ops_scrape_targets_queues_selected_google_place_ids(tmp_path):
    fake = _FakeJobManager()
    old_job_manager = api_server.job_manager
    api_server.job_manager = fake
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)

        request = api_server.ScrapeTargetsRequest(
            config_path=str(cfg_path),
            google_place_ids=["PID_A", "PID_C"],
            max_reviews=250,
        )
        result = asyncio.run(api_server.ops_scrape_targets(request))

        assert result.created_count == 2
        assert len(fake.created) == 2
        assert all(int(row["config_overrides"].get("max_reviews", 0)) == 250 for row in fake.created)
    finally:
        api_server.job_manager = old_job_manager


def test_data_health_summary_reports_stale_totals_and_archives(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        cfg_path = tmp_path / "config.top50.yaml"
        _write_config(cfg_path)
        db.upsert_place(
            "place_b",
            "Place B",
            "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
        )
        db.upsert_review("place_b", _make_review("r1"))
        db.backend.execute("UPDATE places SET total_reviews = 0 WHERE place_id = ?", ("place_b",))
        db.backend.commit()
        db.archive_invalid_place_record(
            config_path=str(cfg_path),
            place=db.get_place("place_b"),
            google_place_id="PID_B",
            validation_row={
                "status": "invalid_closed",
                "checked_at": "2026-03-09T00:00:00+00:00",
                "reason": "closed",
                "api_name": "Place B",
                "api_address": "Addr",
                "business_status": "CLOSED_PERMANENTLY",
            },
            config_entry={"url": "https://example.com", "custom_params": {"google_place_id": "PID_B"}},
            deleted_counts={"reviews": 1},
        )

        summary = asyncio.run(
            api_server.get_data_health_summary(
                config_path=str(cfg_path),
                min_reviews=100,
                review_db=db,
            )
        )

        assert summary.stale_total_count == 1
        assert summary.invalid_archive_count == 1
        assert len(summary.recent_invalid_places) == 1
    finally:
        db.close()


def test_data_quality_conflicts_endpoint_reports_active_conflicts(tmp_path):
    db = ReviewDB(str(tmp_path / "test.db"))
    try:
        db.upsert_place("p1", "Place 1", "http://1")
        db.upsert_place("p2", "Place 2", "http://2")
        db.upsert_review("p1", _make_review("shared"))
        db.upsert_review("p2", _make_review("shared"))

        report = asyncio.run(api_server.get_data_quality_conflicts(review_db=db))
        assert report.total_conflicts == 1
        assert report.conflicts[0].review_id == "shared"
        assert report.conflicts[0].has_multiple_real_places is True
    finally:
        db.close()
