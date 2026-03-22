"""Focused tests for limited-view and auth behavior in scraper."""

import inspect
import threading
from unittest.mock import MagicMock

import pytest
from selenium.common.exceptions import StaleElementReferenceException

from modules.scraper import CARD_SEL, GoogleReviewsScraper, LimitedViewError


def _minimal_config(tmp_path, **extra):
    config = {
        "url": "https://maps.app.goo.gl/test",
        "headless": True,
        "use_mongodb": False,
        "backup_to_json": False,
        "db_path": str(tmp_path / "reviews.db"),
    }
    config.update(extra)
    return config


def test_cookie_mode_requires_env_vars(tmp_path, monkeypatch):
    monkeypatch.delenv("GOOGLE_MAPS_COOKIE_1PSID", raising=False)
    monkeypatch.delenv("GOOGLE_MAPS_COOKIE_1PSIDTS", raising=False)
    with pytest.raises(ValueError, match="GOOGLE_MAPS_COOKIE_1PSID"):
        GoogleReviewsScraper(_minimal_config(tmp_path, google_maps_auth_mode="cookie"))


def test_anonymous_mode_does_not_require_cookie_env(tmp_path, monkeypatch):
    monkeypatch.delenv("GOOGLE_MAPS_COOKIE_1PSID", raising=False)
    monkeypatch.delenv("GOOGLE_MAPS_COOKIE_1PSIDTS", raising=False)
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path, google_maps_auth_mode="anonymous"))
    scraper.review_db.close()


def test_cookie_env_values_expand_to_secure_aliases(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_COOKIE_1PSID", "value_a")
    monkeypatch.setenv("GOOGLE_MAPS_COOKIE_1PSIDTS", "value_b")
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path, google_maps_auth_mode="cookie"))
    try:
        cookies = scraper._read_cookie_env_values()
        assert cookies["1PSID"] == "value_a"
        assert cookies["__Secure-1PSID"] == "value_a"
        assert cookies["1PSIDTS"] == "value_b"
        assert cookies["__Secure-1PSIDTS"] == "value_b"
    finally:
        scraper.review_db.close()


def test_limited_view_detector_positive(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        driver = MagicMock()
        body = MagicMock()
        body.text = "You're seeing a limited view of Google Maps. Sign in."
        driver.find_element.return_value = body
        driver.current_url = "https://www.google.com/maps/place/test"
        scraper._collect_review_surface_counts = MagicMock(
            return_value={
                "cards_data_review_id": 0,
                "cards_jftiEf": 0,
                "sort_buttons": 0,
                "review_tabs": 0,
                "review_url_hint": 0,
            }
        )

        limited, details = scraper._is_limited_view(driver, stage="unit")
        assert limited is True
        assert details["explicit_limited"] is True
    finally:
        scraper.review_db.close()


def test_limited_view_detector_negative_with_review_surface(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        driver = MagicMock()
        body = MagicMock()
        body.text = "Normal page"
        driver.find_element.return_value = body
        driver.current_url = "https://www.google.com/maps/place/test/reviews"
        scraper._collect_review_surface_counts = MagicMock(
            return_value={
                "cards_data_review_id": 1,
                "cards_jftiEf": 0,
                "sort_buttons": 1,
                "review_tabs": 1,
                "review_url_hint": 1,
            }
        )

        limited, _ = scraper._is_limited_view(driver, stage="unit")
        assert limited is False
    finally:
        scraper.review_db.close()


def test_limited_view_detector_prefers_review_surface_over_signed_out_copy(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        driver = MagicMock()
        body = MagicMock()
        body.text = (
            "You're seeing a limited view of Google Maps. Sign in. "
            "But the reviews UI is already visible."
        )
        driver.find_element.return_value = body
        driver.current_url = "https://www.google.com/maps/place/test"
        scraper._collect_review_surface_counts = MagicMock(
            return_value={
                "cards_data_review_id": 0,
                "cards_jftiEf": 0,
                "sort_buttons": 1,
                "review_tabs": 1,
                "review_url_hint": 0,
            }
        )

        limited, _ = scraper._is_limited_view(driver, stage="unit")
        assert limited is False
    finally:
        scraper.review_db.close()


def test_limited_view_detector_handles_dead_current_url_property(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        class _DeadDriver:
            @property
            def current_url(self):
                raise RuntimeError("invalid session id")

            def find_element(self, *_args, **_kwargs):
                class _Body:
                    text = "Normal body"
                return _Body()

        scraper._collect_review_surface_counts = MagicMock(
            return_value={
                "cards_data_review_id": 0,
                "cards_jftiEf": 0,
                "sort_buttons": 0,
                "review_tabs": 0,
                "review_url_hint": 0,
            }
        )

        limited, details = scraper._is_limited_view(_DeadDriver(), stage="unit")
        assert limited is False
        assert details["url"] == ""
    finally:
        scraper.review_db.close()


def test_extract_query_place_id_from_url(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        url = (
            "https://www.google.com/maps/search/?api=1&query=test"
            "&query_place_id=ChIJf7JvHgA5aTQRuhp1CSmPez8"
        )
        assert scraper._extract_query_place_id(url) == "ChIJf7JvHgA5aTQRuhp1CSmPez8"
        assert scraper._extract_query_place_id("https://www.google.com/maps/search/?api=1&query=test") == ""
    finally:
        scraper.review_db.close()


def test_extract_place_name_ignores_generic_maps_title(tmp_path, monkeypatch):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        monkeypatch.setattr("modules.scraper.time.sleep", lambda *_args, **_kwargs: None)
        driver = MagicMock()
        driver.title = "Google 地圖"
        assert scraper._extract_place_name(driver, "https://www.google.com/maps/place/") == ""
    finally:
        scraper.review_db.close()


def test_extract_place_name_prefers_query_param_without_navigation(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        driver = MagicMock()
        name = scraper._extract_place_name(
            driver,
            "https://www.google.com/maps/search/?api=1&query=%E7%94%BA%E5%91%B3%E9%A3%9F%E5%A0%82&query_place_id=PID_X",
        )
        assert name == "町味食堂"
        driver.get.assert_not_called()
    finally:
        scraper.review_db.close()


def test_is_expected_place_context_rejects_wrong_place(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        driver = MagicMock()
        driver.current_url = (
            "https://www.google.com/maps/place/%E5%85%AD%E5%85%AD%E6%85%A2%E6%BC%AB%E9%A2%A8%E5%91%B3/"
            "@24.066874,120.5361601,17z"
        )
        driver.title = "六六慢漫風味 - Google 地圖"
        driver.find_elements.return_value = []
        assert (
            scraper._is_expected_place_context(
                driver,
                "町味食堂",
                "ChIJERwh5-Q5aTQRHtA3h9khc-4",
            )
            is False
        )
    finally:
        scraper.review_db.close()


def test_is_expected_place_context_accepts_query_place_id_from_page_source(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        expected_qpid = "ChIJC68kDuo4aTQRVglq5Ud3voM"
        driver = MagicMock()
        driver.current_url = "https://www.google.com/maps/place/%E8%81%9E%E9%A6%99%E7%89%9B%E8%82%89%E9%BA%B5/"
        driver.title = "聞香牛肉麵 - Google 地圖"
        driver.find_elements.return_value = []
        driver.page_source = f"<html><body>...query_place_id={expected_qpid}...</body></html>"
        assert (
            scraper._is_expected_place_context(
                driver,
                "Wenxiang Beef Noodle Restaurant",
                expected_qpid,
            )
            is True
        )
    finally:
        scraper.review_db.close()


def test_extract_total_reviews_hint_parses_multilingual_strings(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        driver = MagicMock()
        body = MagicMock()
        body.text = "4.6 28 篇評論"
        driver.find_element.return_value = body
        assert scraper._extract_total_reviews_hint(driver) == 28

        body.text = "Rated 4.4 with 1,234 reviews"
        assert scraper._extract_total_reviews_hint(driver) == 1234
    finally:
        scraper.review_db.close()


def test_scrape_treats_dead_browser_error_as_cancellation_noise(tmp_path, monkeypatch):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path), cancel_event=threading.Event())
    scraper.cancel_event.set()
    try:
        monkeypatch.setattr(
            scraper,
            "setup_driver",
            lambda _headless: (_ for _ in ()).throw(
                RuntimeError(
                    "HTTPConnectionPool(host='localhost', port=52574): Max retries exceeded "
                    "with url: /session/test/url (Caused by NewConnectionError('connection refused'))"
                )
            ),
        )

        assert scraper.scrape() is False
        assert scraper.last_error_message == "Scrape cancelled while browser session was closing"
        assert scraper.last_error_transient is False
    finally:
        scraper.review_db.close()


def test_looks_like_place_page_false_for_search_results_shell(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        driver = MagicMock()
        body = MagicMock()
        body.text = "你已看完所有搜尋結果。充分運用 Google 地圖 登入"
        driver.find_element.return_value = body
        driver.current_url = "https://www.google.com/maps/search/%E9%A4%8A%E9%8D%8B+Yang+Guo/"
        driver.title = "養鍋 Yang Guo 石頭涮涮鍋 (彰化旗艦店) - Google 地圖"
        driver.find_elements.return_value = []
        assert scraper._looks_like_place_page(driver) is False
    finally:
        scraper.review_db.close()


def test_navigate_to_place_raises_when_search_list_never_resolves(tmp_path, monkeypatch):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        monkeypatch.setattr("modules.scraper.time.sleep", lambda *_args, **_kwargs: None)
        scraper.dismiss_cookies = MagicMock(return_value=False)
        scraper._extract_place_name = MagicMock(return_value="Test Place")
        scraper._looks_like_place_page = MagicMock(return_value=False)
        scraper._open_search_result_from_list = MagicMock(return_value=False)
        scraper._handle_limited_view = MagicMock(return_value=False)

        driver = MagicMock()
        driver.current_url = "https://www.google.com/maps/search/?api=1&query=Test&query_place_id=PID_X"
        driver.find_element.return_value = MagicMock(text="normal body")
        driver.find_elements.return_value = []
        wait = MagicMock()
        wait.until.return_value = True

        with pytest.raises(LimitedViewError, match="Unable to open a place detail page"):
            scraper.navigate_to_place(
                driver,
                "https://www.google.com/maps/search/?api=1&query=Test&query_place_id=PID_X",
                wait,
            )
    finally:
        scraper.review_db.close()


def test_navigate_to_place_ignores_stale_tabs_during_search_resolution(tmp_path, monkeypatch):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        monkeypatch.setattr("modules.scraper.time.sleep", lambda *_args, **_kwargs: None)
        scraper.dismiss_cookies = MagicMock(return_value=False)
        scraper._extract_place_name = MagicMock(return_value="Test Place")
        scraper._extract_place_coords = MagicMock(return_value=(None, None))
        scraper._open_search_result_from_list = MagicMock(return_value=False)
        scraper._handle_limited_view = MagicMock(return_value=False)
        scraper._is_expected_place_context = MagicMock(return_value=True)

        class _StaleTab:
            @property
            def text(self):
                raise StaleElementReferenceException("stale")

        stale_tab = _StaleTab()

        reviews_tab = MagicMock()
        reviews_tab.text = "Reviews"

        driver = MagicMock()
        driver.current_url = "https://www.google.com/maps/search/?api=1&query=Test"
        driver.find_elements.return_value = [stale_tab, reviews_tab]

        wait = MagicMock()

        assert (
            scraper.navigate_to_place(
                driver,
                "https://www.google.com/maps/search/?api=1&query=Test",
                wait,
            )
            is True
        )
    finally:
        scraper.review_db.close()


def test_cookie_mode_fail_fast_on_limited_view(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_COOKIE_1PSID", "v1")
    monkeypatch.setenv("GOOGLE_MAPS_COOKIE_1PSIDTS", "v2")
    scraper = GoogleReviewsScraper(
        _minimal_config(
            tmp_path,
            google_maps_auth_mode="cookie",
            fail_on_limited_view=True,
        )
    )
    try:
        scraper._is_limited_view = MagicMock(return_value=(True, {}))
        scraper._write_debug_artifacts = MagicMock()
        with pytest.raises(LimitedViewError):
            scraper._handle_limited_view(MagicMock(), "post_reviews_click", "place", strict=True)
    finally:
        scraper.review_db.close()


def test_anonymous_mode_warns_without_raising_on_limited_view(tmp_path):
    scraper = GoogleReviewsScraper(
        _minimal_config(
            tmp_path,
            google_maps_auth_mode="anonymous",
            fail_on_limited_view=True,
        )
    )
    try:
        scraper._is_limited_view = MagicMock(return_value=(True, {}))
        scraper._write_debug_artifacts = MagicMock()
        assert scraper._handle_limited_view(MagicMock(), "post_reviews_click", "place", strict=True) is True
    finally:
        scraper.review_db.close()


def test_is_reviews_tab_does_not_accept_plain_tab_index_1(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        tab = MagicMock()
        tab.text = ""

        def _attr(name):
            mapping = {
                "data-tab-index": "1",
                "role": "tab",
                "aria-selected": "false",
                "aria-label": "",
                "innerHTML": "",
                "textContent": "",
                "href": "",
                "data-href": "",
                "data-url": "",
                "data-target": "",
                "class": "",
            }
            return mapping.get(name, "")

        tab.get_attribute.side_effect = _attr
        tab.find_elements.return_value = []

        assert scraper.is_reviews_tab(tab) is False
    finally:
        scraper.review_db.close()


def test_click_reviews_tab_noop_when_surface_already_present(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        scraper._has_active_reviews_surface = MagicMock(return_value=True)
        driver = MagicMock()
        assert scraper.click_reviews_tab(driver) is True
        driver.find_elements.assert_not_called()
    finally:
        scraper.review_db.close()


def test_active_reviews_surface_false_when_reviews_tab_not_selected(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        review_tab = MagicMock()

        def _tab_attr(name):
            mapping = {
                "data-tab-index": "",
                "role": "tab",
                "aria-selected": "false",
                "aria-label": "評論",
                "innerHTML": "評論",
                "textContent": "評論",
                "href": "",
                "data-href": "",
                "data-url": "",
                "data-target": "",
                "class": "",
            }
            return mapping.get(name, "")

        review_tab.text = "評論"
        review_tab.get_attribute.side_effect = _tab_attr
        review_tab.find_elements.return_value = []

        hidden_card = MagicMock()
        hidden_card.is_displayed.return_value = False

        driver = MagicMock()

        def _find_elements(by, selector):
            if selector == '[role="tab"]':
                return [review_tab]
            if selector == CARD_SEL:
                return [hidden_card]
            if selector == 'button[aria-label*="Sort" i], button.HQzyZ[aria-haspopup="true"]':
                return []
            return []

        driver.find_elements.side_effect = _find_elements
        driver.current_url = "https://www.google.com/maps/place/test"

        assert scraper._has_active_reviews_surface(driver) is False
    finally:
        scraper.review_db.close()


def test_review_fingerprint_matches_raw_and_db_shapes(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        raw_fp = scraper._review_fingerprint(
            author="Alice",
            rating=5,
            text="Great place",
            review_date="2026-03-04",
            raw_date="today",
            profile="https://maps.google.com/user",
        )
        db_fp = scraper._review_fingerprint(
            author="Alice",
            rating=5,
            text={"en": "Great place"},
            review_date="2026-03-04",
            raw_date="today",
            profile="https://maps.google.com/user",
        )

        assert raw_fp
        assert raw_fp == db_fp
    finally:
        scraper.review_db.close()


def test_find_reviews_pane_prefers_review_card_ancestor(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        card = MagicMock()
        card.is_displayed.return_value = True
        anchored_pane = MagicMock()
        driver = MagicMock()
        wait = MagicMock()

        def _find_elements(by, selector):
            if selector == CARD_SEL:
                return [card]
            return []

        driver.find_elements.side_effect = _find_elements
        driver.execute_script.return_value = anchored_pane

        pane = scraper._find_reviews_pane(driver, wait)

        assert pane is anchored_pane
        wait.until.assert_not_called()
    finally:
        scraper.review_db.close()


def test_find_reviews_scroll_target_prefers_scrollable_ancestor(tmp_path):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        card = MagicMock()
        card.is_displayed.return_value = True
        pane = MagicMock()
        pane.find_elements.return_value = [card]
        scroll_target = MagicMock()
        driver = MagicMock()
        driver.execute_script.return_value = scroll_target

        target = scraper._find_reviews_scroll_target(driver, pane)

        assert target is scroll_target
    finally:
        scraper.review_db.close()


def test_scroll_reviews_forward_uses_stronger_card_fallback_when_pane_stalls():
    driver = MagicMock()
    scroll_target = MagicMock()
    last_card = MagicMock()
    driver.execute_script.side_effect = ([120, 120], [120, 260])

    GoogleReviewsScraper._scroll_reviews_forward(driver, scroll_target, last_card)

    assert driver.execute_script.call_count == 2
    first_script, first_target = driver.execute_script.call_args_list[0][0]
    second_script, second_card, second_target = driver.execute_script.call_args_list[1][0]
    assert "node.scrollBy(0, step)" in first_script
    assert first_target is scroll_target
    assert "card.scrollIntoView" in second_script
    assert "node.scrollTo(0, node.scrollHeight)" in second_script
    assert second_card is last_card
    assert second_target is scroll_target


def test_scrape_no_stale_fresh_cards_reference():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "fresh_cards" not in source


def test_max_reviews_limit_not_based_on_preloaded_seen_set():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "len(seen) >= max_reviews" not in source
    assert "len(processed_ids) >= max_reviews" not in source
    assert "len(session_new_ids) >= max_reviews" in source


def test_backfill_disables_stop_threshold_when_seen_below_max_reviews():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "existing_seen_count < max_reviews" in source
    assert "Disabling early stop (stop_threshold=%d) for backfill" in source


def test_scrape_fail_fast_on_dead_browser_session_errors():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "invalid session id" in source
    assert "no such window" in source
    assert "Browser session died during review loop" in source


def test_scrape_emits_plain_scraped_progress_logs():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "Scraped %d reviews so far" in source


def test_scrape_uses_logical_existing_review_count_for_progress_display():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "displayed_review_count = logical_existing_count" in source
    assert "logical_existing_count + len(session_new_ids)" in source


def test_scrape_runs_confirmation_scrolls_before_end_stop():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "Confirmation scroll attempt %d/3" in source
    assert "running %d confirmation scroll attempts before stopping" in source


def test_scrape_binds_to_dedicated_scroll_target():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "scroll_target = self._find_reviews_scroll_target(driver, pane)" in source
    assert "return arguments[0].scrollTop;" in source


def test_scrape_does_not_fail_fast_on_post_click_limited_view_probe():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "post_reviews_click" not in source


def test_scrape_does_not_fail_fast_when_quick_review_surface_probe_is_empty():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "Review surface not detected after navigation in cookie mode." not in source
    assert "continuing to pane lookup" in source


def test_set_sort_avoids_risky_generic_container_fallbacks():
    source = inspect.getsource(GoogleReviewsScraper.set_sort)
    assert "Found sort button through container element" not in source
    assert "Found potential sort button via fallback dropdown detection" not in source
    assert "has_sort_keyword or has_sort_class" in source
