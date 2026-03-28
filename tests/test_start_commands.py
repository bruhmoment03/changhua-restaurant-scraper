"""Tests for start.py command dispatch and management commands."""

import json
import pytest
from pathlib import Path
from unittest.mock import patch
import yaml

from modules.review_db import ReviewDB


def _make_db(tmp_path, reviews=None):
    """Create a test DB and optionally populate it."""
    db_path = str(tmp_path / "test.db")
    db = ReviewDB(db_path)
    if reviews:
        db.upsert_place("place1", "Test Place", "http://test")
        for r in reviews:
            db.upsert_review("place1", r)
    return db, db_path


def _make_review(rid="r1", text="Great!", rating=5.0):
    return {
        "review_id": rid, "text": text, "rating": rating,
        "likes": 1, "lang": "en", "date": "3 months ago",
        "review_date": "2025-06-15", "author": "Test",
        "profile": "", "avatar": "", "owner_text": "", "photos": [],
    }


class TestExportCommand:
    """Tests for the export command."""

    def test_export_json(self, tmp_path):
        db, db_path = _make_db(tmp_path, [_make_review("r1"), _make_review("r2")])
        db.close()

        output_path = str(tmp_path / "export.json")
        from start import _run_export, _get_db_path
        from types import SimpleNamespace

        args = SimpleNamespace(
            db_path=db_path, config=None,
            format="json", place_id="place1",
            output=output_path, include_deleted=False,
        )
        _run_export({}, args)

        data = json.loads(Path(output_path).read_text())
        assert len(data) == 2

    def test_export_csv(self, tmp_path):
        db, db_path = _make_db(tmp_path, [_make_review("r1")])
        db.close()

        output_path = str(tmp_path / "export.csv")
        from start import _run_export
        from types import SimpleNamespace

        args = SimpleNamespace(
            db_path=db_path, config=None,
            format="csv", place_id="place1",
            output=output_path, include_deleted=False,
        )
        _run_export({}, args)
        assert Path(output_path).exists()


class TestDatasetExportCommand:
    def test_dataset_export_writes_bundle(self, tmp_path):
        from modules.config import load_config
        from start import _run_dataset_export
        from types import SimpleNamespace

        cfg_path = tmp_path / "config.top50.yaml"
        cfg_path.write_text(
            yaml.safe_dump(
                {
                    "db_path": str(tmp_path / "test.db"),
                    "businesses": [
                        {
                            "url": "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
                            "custom_params": {"company": "Alpha", "google_place_id": "PID_A"},
                        }
                    ],
                },
                allow_unicode=True,
                sort_keys=False,
            ),
            encoding="utf-8",
        )

        db = ReviewDB(str(tmp_path / "test.db"))
        try:
            db.upsert_place(
                "place_a",
                "Place A",
                "https://www.google.com/maps/search/?api=1&query=Alpha&query_place_id=PID_A",
            )
            db.upsert_review("place_a", _make_review("r1"))
        finally:
            db.close()

        args = SimpleNamespace(
            db_path=str(tmp_path / "test.db"),
            config=cfg_path,
            output_dir=str(tmp_path / "bundle"),
            min_reviews=5,
            include_deleted=False,
        )
        cfg = load_config(cfg_path)
        _run_dataset_export(cfg, args)

        assert (tmp_path / "bundle" / "reviews_raw.csv").exists()
        assert (tmp_path / "bundle" / "reviews_cleaned.csv").exists()
        assert (tmp_path / "bundle" / "restaurants_cleaned.csv").exists()
        assert (tmp_path / "bundle" / "qa_samples_reviews_low_information.csv").exists()
        assert (tmp_path / "bundle" / "qa_samples_reviews_duplicate_text.csv").exists()
        assert (tmp_path / "bundle" / "qa_samples_reviews_format_anomalies.csv").exists()
        assert (tmp_path / "bundle" / "qa_samples_targets_followup.csv").exists()
        assert (tmp_path / "bundle" / "qa_report.json").exists()
        assert (tmp_path / "bundle" / "dataset_manifest.json").exists()


class TestDbStatsCommand:
    """Tests for the db-stats command."""

    def test_shows_stats(self, tmp_path, capsys):
        db, db_path = _make_db(tmp_path, [_make_review("r1")])
        db.close()

        from start import _run_db_stats
        from types import SimpleNamespace
        args = SimpleNamespace(db_path=db_path, config=None)
        _run_db_stats({}, args)

        output = capsys.readouterr().out
        assert "Reviews:" in output
        assert "Places:" in output


class TestClearCommand:
    """Tests for the clear command."""

    def test_clear_place(self, tmp_path):
        db, db_path = _make_db(tmp_path, [_make_review("r1")])
        db.close()

        from start import _run_clear
        from types import SimpleNamespace
        args = SimpleNamespace(
            db_path=db_path, config=None,
            place_id="place1", confirm=True,
        )
        _run_clear({}, args)

        db = ReviewDB(db_path)
        try:
            assert db.get_reviews("place1") == []
        finally:
            db.close()


class TestHideRestoreCommands:
    """Tests for hide and restore commands."""

    def test_hide_and_restore(self, tmp_path, capsys):
        db, db_path = _make_db(tmp_path, [_make_review("r1")])
        db.close()

        from start import _run_hide, _run_restore
        from types import SimpleNamespace

        args = SimpleNamespace(
            db_path=db_path, config=None,
            review_id="r1", place_id="place1",
        )

        _run_hide({}, args)
        output = capsys.readouterr().out
        assert "hidden" in output

        _run_restore({}, args)
        output = capsys.readouterr().out
        assert "restored" in output


class TestPruneHistoryCommand:
    """Tests for prune-history command."""

    def test_prune_dry_run(self, tmp_path, capsys):
        db, db_path = _make_db(tmp_path, [_make_review("r1")])
        db.close()

        from start import _run_prune_history
        from types import SimpleNamespace
        args = SimpleNamespace(
            db_path=db_path, config=None,
            older_than=0, dry_run=True,
        )
        _run_prune_history({}, args)
        output = capsys.readouterr().out
        assert "Would prune" in output


class TestSyncStatusCommand:
    """Tests for sync-status command."""

    def test_no_checkpoints(self, tmp_path, capsys):
        db, db_path = _make_db(tmp_path)
        db.close()

        from start import _run_sync_status
        from types import SimpleNamespace
        args = SimpleNamespace(db_path=db_path, config=None)
        _run_sync_status({}, args)
        output = capsys.readouterr().out
        assert "No sync checkpoints" in output


class TestProgressCommand:
    def _config_with_businesses(self):
        return {
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
            ]
        }

    def test_progress_summary_and_selection_helpers(self, tmp_path):
        from start import _compute_progress_report, _select_businesses_for_scrape

        db, db_path = _make_db(tmp_path)
        try:
            # A has reviews
            db.upsert_place(
                "place_a", "Place A",
                "https://www.google.com/maps/search/?api=1&query=A&query_place_id=PID_A",
            )
            db.upsert_review("place_a", _make_review("ra1"))

            # B exists in DB but has zero reviews
            db.upsert_place(
                "place_b", "Place B",
                "https://www.google.com/maps/search/?api=1&query=B&query_place_id=PID_B",
            )

            cfg = self._config_with_businesses()
            businesses = cfg["businesses"]
            report = _compute_progress_report(businesses, db)
            assert report["targets_total"] == 3
            assert report["with_reviews"] == 1
            assert report["present_zero_reviews"] == 1
            assert report["missing_from_db"] == 1

            selected = _select_businesses_for_scrape(
                businesses, report, only_missing=True, max_businesses=1
            )
            assert len(selected) == 1
            assert selected[0]["custom_params"]["google_place_id"] in {"PID_B", "PID_C"}
        finally:
            db.close()

    def test_progress_fail_if_incomplete_exits_nonzero(self, tmp_path):
        from start import _run_progress
        from types import SimpleNamespace

        db, db_path = _make_db(tmp_path)
        db.close()

        cfg = self._config_with_businesses()
        args = SimpleNamespace(
            db_path=db_path,
            config=None,
            json=False,
            fail_if_incomplete=True,
        )
        with pytest.raises(SystemExit) as exc:
            _run_progress(cfg, args)
        assert exc.value.code == 2


class TestScrapeOverrides:
    def test_new_limited_view_overrides_are_applied(self):
        from start import _apply_scrape_overrides
        from types import SimpleNamespace

        config = {}
        args = SimpleNamespace(
            headless=False,
            sort_by=None,
            google_maps_auth_mode="cookie",
            fail_on_limited_view=True,
            debug_on_limited_view=True,
            debug_artifacts_dir="debug_artifacts_custom",
            stealth_undetectable=True,
            stealth_user_agent="UA/1.0",
            scrape_mode=None,
            stop_threshold=None,
            max_reviews=None,
            max_scroll_attempts=None,
            scroll_idle_limit=None,
            url=None,
            use_mongodb=None,
            convert_dates=None,
            download_images=None,
            image_dir=None,
            download_threads=None,
            store_local_paths=None,
            replace_urls=None,
            custom_url_base=None,
            custom_url_profiles=None,
            custom_url_reviews=None,
            preserve_original_urls=None,
            overwrite_existing=False,
            stop_on_match=False,
            db_path=None,
            custom_params=None,
        )

        _apply_scrape_overrides(config, args)
        assert config["google_maps_auth_mode"] == "cookie"
        assert config["fail_on_limited_view"] is True
        assert config["debug_on_limited_view"] is True
        assert config["debug_artifacts_dir"] == "debug_artifacts_custom"
        assert config["stealth_undetectable"] is True
        assert config["stealth_user_agent"] == "UA/1.0"

    def test_headed_forces_headless_false(self):
        from start import _apply_scrape_overrides
        from types import SimpleNamespace

        config = {"headless": True}
        args = SimpleNamespace(
            headless=False,
            headed=True,
            sort_by=None,
            google_maps_auth_mode=None,
            fail_on_limited_view=None,
            debug_on_limited_view=None,
            debug_artifacts_dir=None,
            stealth_undetectable=None,
            stealth_user_agent=None,
            scrape_mode=None,
            stop_threshold=None,
            max_reviews=None,
            max_scroll_attempts=None,
            scroll_idle_limit=None,
            url=None,
            use_mongodb=None,
            convert_dates=None,
            download_images=None,
            image_dir=None,
            download_threads=None,
            store_local_paths=None,
            replace_urls=None,
            custom_url_base=None,
            custom_url_profiles=None,
            custom_url_reviews=None,
            preserve_original_urls=None,
            overwrite_existing=False,
            stop_on_match=False,
            db_path=None,
            custom_params=None,
        )
        _apply_scrape_overrides(config, args)
        assert config["headless"] is False
