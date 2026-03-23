"""Focused tests for limited-view and auth behavior in scraper."""

import inspect
import threading
from unittest.mock import MagicMock

import pytest
from selenium.common.exceptions import StaleElementReferenceException

from modules.scraper import (
    CARD_SEL,
    STUCK_SCROLL_RECOVERY_THRESHOLD,
    GoogleReviewsScraper,
    LimitedViewError,
    _is_transient_browser_error,
)


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


def test_scroll_reviews_forward_scroll_by_returns_true_when_target_moves():
    driver = MagicMock()
    scroll_target = MagicMock()
    driver.execute_script.side_effect = (120, 260)

    moved = GoogleReviewsScraper._scroll_reviews_forward(driver, scroll_target)

    assert moved is True
    assert driver.execute_script.call_count == 2
    assert "scrollTop || 0" in driver.execute_script.call_args_list[0][0][0]
    assert "node.scrollBy(0, step)" in driver.execute_script.call_args_list[1][0][0]


def test_scroll_reviews_forward_card_end_uses_last_card():
    driver = MagicMock()
    scroll_target = MagicMock()
    last_card = MagicMock()
    driver.execute_script.side_effect = (120, 260)

    moved = GoogleReviewsScraper._scroll_reviews_forward(
        driver,
        scroll_target,
        last_card,
        strategy="card_end",
    )

    assert moved is True
    assert driver.execute_script.call_count == 2
    second_script, second_card, second_target = driver.execute_script.call_args_list[1][0]
    assert "card.scrollIntoView" in second_script
    assert second_card is last_card
    assert second_target is scroll_target


def test_scroll_reviews_forward_window_fallback_returns_false():
    driver = MagicMock()
    scroll_target = MagicMock()
    driver.execute_script.side_effect = (120, None)

    moved = GoogleReviewsScraper._scroll_reviews_forward(
        driver,
        scroll_target,
        strategy="window_fallback",
    )

    assert moved is False
    assert driver.execute_script.call_count == 2
    assert driver.execute_script.call_args_list[1][0] == ("window.scrollBy(0, 500);",)


def test_scroll_progressed_treats_changed_visible_tail_as_progress():
    previous = {
        "scroll_top": 11507,
        "visible_count": 10,
        "first_card_id": "review-1",
        "last_card_id": "review-10",
    }
    current = {
        "scroll_top": 11507,
        "visible_count": 10,
        "first_card_id": "review-1",
        "last_card_id": "review-11",
    }

    assert GoogleReviewsScraper._scroll_progressed(previous, current) is True


def test_scroll_progressed_treats_new_card_ids_as_progress():
    previous = {"scroll_top": 11507, "visible_count": 10, "first_card_id": "review-1", "last_card_id": "review-10"}
    current = {"scroll_top": 11507, "visible_count": 10, "first_card_id": "review-1", "last_card_id": "review-10"}

    assert (
        GoogleReviewsScraper._scroll_progressed(
            previous,
            current,
            discovered_card_ids=1,
        )
        is True
    )


def test_transient_browser_error_includes_connection_refused():
    message = (
        "HTTPConnectionPool(host='localhost', port=53931): Max retries exceeded with url: "
        "/session/test/url (Caused by NewConnectionError('connection refused'))"
    )
    assert _is_transient_browser_error(message) is True


def test_should_fail_for_review_gap_requires_material_gap_and_no_end_evidence():
    assert (
        GoogleReviewsScraper._should_fail_for_review_gap(
            200,
            133,
            positive_end_of_list_evidence=False,
        )
        is True
    )
    assert (
        GoogleReviewsScraper._should_fail_for_review_gap(
            140,
            133,
            positive_end_of_list_evidence=False,
        )
        is False
    )
    assert (
        GoogleReviewsScraper._should_fail_for_review_gap(
            200,
            133,
            positive_end_of_list_evidence=True,
        )
        is False
    )


def test_scrape_marks_transport_failure_inside_card_parse_as_transient(tmp_path, monkeypatch):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        monkeypatch.setattr("modules.scraper.time.sleep", lambda *_args, **_kwargs: None)

        class _ImmediateWait:
            def __init__(self, *_args, **_kwargs):
                pass

            def until(self, _condition):
                return True

        card = MagicMock()
        pane = MagicMock()
        pane.find_elements.return_value = [card]
        scroll_target = MagicMock()
        scroll_target.find_elements.return_value = [card]

        driver = MagicMock()
        driver.current_url = "https://www.google.com/maps/place/test/reviews"
        driver.title = "Test Place - Google Maps"

        def _execute_script(script, *args):
            if "document.readyState" in script:
                return "complete"
            if "tagName" in script and "className" in script:
                return {
                    "scrollTop": 0,
                    "clientHeight": 720,
                    "scrollHeight": 2400,
                    "tagName": "div",
                    "className": "review-pane",
                }
            if "scrollTop || 0" in script:
                return 0
            return None

        driver.execute_script.side_effect = _execute_script
        driver.find_elements.return_value = []

        monkeypatch.setattr("modules.scraper.WebDriverWait", _ImmediateWait)
        monkeypatch.setattr("modules.scraper.extract_place_id", lambda *_args, **_kwargs: "place_1")
        monkeypatch.setattr(scraper, "setup_driver", lambda _headless: driver)
        monkeypatch.setattr(scraper, "navigate_to_place", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(scraper, "_extract_place_name", lambda *_args, **_kwargs: "Test Place")
        monkeypatch.setattr(scraper, "dismiss_cookies", lambda *_args, **_kwargs: False)
        monkeypatch.setattr(scraper, "click_reviews_tab", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(scraper, "set_sort", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(scraper, "_has_reviews_surface", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(scraper, "_find_reviews_pane", lambda *_args, **_kwargs: pane)
        monkeypatch.setattr(scraper, "_find_reviews_scroll_target", lambda *_args, **_kwargs: scroll_target)
        monkeypatch.setattr(scraper, "_extract_total_reviews_hint", lambda *_args, **_kwargs: 90)
        monkeypatch.setattr(scraper, "_has_explicit_end_of_list_marker", lambda *_args, **_kwargs: False)
        monkeypatch.setattr(scraper, "_write_debug_artifacts", lambda *_args, **_kwargs: None)

        def _raise_transport(_card):
            raise RuntimeError(
                "HTTPConnectionPool(host='localhost', port=53931): Max retries exceeded with url: "
                "/session/test/url (Caused by NewConnectionError('connection refused'))"
            )

        monkeypatch.setattr("modules.scraper.RawReview.from_card", _raise_transport)

        scraper.review_db.upsert_place = MagicMock(return_value="place_1")
        scraper.review_db.start_session = MagicMock(return_value="session_1")
        scraper.review_db.get_review_ids = MagicMock(return_value=set())
        scraper.review_db.get_reviews = MagicMock(return_value=[])
        scraper.review_db.end_session = MagicMock()

        assert scraper.scrape() is False
        assert scraper.last_error_transient is True
        assert "connection refused" in scraper.last_error_message.lower()
        assert scraper.review_db.end_session.call_args[0][1] == "failed"
    finally:
        scraper.review_db.close()


def test_scrape_fails_transiently_when_review_hint_materially_exceeds_seen_count(tmp_path, monkeypatch):
    scraper = GoogleReviewsScraper(
        _minimal_config(
            tmp_path,
            scrape_mode="update",
            max_scroll_attempts=1,
            scroll_idle_limit=1,
        )
    )
    try:
        monkeypatch.setattr("modules.scraper.time.sleep", lambda *_args, **_kwargs: None)

        class _ImmediateWait:
            def __init__(self, *_args, **_kwargs):
                pass

            def until(self, _condition):
                return True

        class _Raw:
            id = "existing_1"
            text = "Already stored"
            rating = 4.0
            likes = 0
            lang = "en"
            date = "today"
            review_date = "2026-03-01"
            author = "Alice"
            profile = "https://maps.google.com/user/alice"
            avatar = ""
            owner_text = ""
            photos = []

        card = MagicMock()
        card.is_displayed.return_value = True
        card.text = "Already stored"
        card.get_attribute.side_effect = lambda name: "existing_1" if name == "data-review-id" else ""

        pane = MagicMock()
        pane.find_elements.return_value = [card]
        scroll_target = MagicMock()
        scroll_target.find_elements.return_value = [card]

        driver = MagicMock()
        driver.current_url = "https://www.google.com/maps/place/test/reviews"
        driver.title = "Test Place - Google Maps"

        def _execute_script(script, *args):
            if "document.readyState" in script:
                return "complete"
            if "tagName" in script and "className" in script:
                return {
                    "scrollTop": 120,
                    "clientHeight": 720,
                    "scrollHeight": 2400,
                    "tagName": "div",
                    "className": "review-pane",
                }
            if "scrollTop || 0" in script:
                return 120
            return None

        driver.execute_script.side_effect = _execute_script
        driver.find_elements.return_value = []

        monkeypatch.setattr("modules.scraper.WebDriverWait", _ImmediateWait)
        monkeypatch.setattr("modules.scraper.extract_place_id", lambda *_args, **_kwargs: "place_1")
        monkeypatch.setattr(scraper, "setup_driver", lambda _headless: driver)
        monkeypatch.setattr(scraper, "navigate_to_place", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(scraper, "_extract_place_name", lambda *_args, **_kwargs: "Test Place")
        monkeypatch.setattr(scraper, "dismiss_cookies", lambda *_args, **_kwargs: False)
        monkeypatch.setattr(scraper, "click_reviews_tab", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(scraper, "set_sort", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(scraper, "_has_reviews_surface", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(scraper, "_find_reviews_pane", lambda *_args, **_kwargs: pane)
        monkeypatch.setattr(scraper, "_find_reviews_scroll_target", lambda *_args, **_kwargs: scroll_target)
        monkeypatch.setattr(scraper, "_extract_total_reviews_hint", lambda *_args, **_kwargs: 20)
        monkeypatch.setattr(scraper, "_has_explicit_end_of_list_marker", lambda *_args, **_kwargs: False)
        monkeypatch.setattr(scraper, "_write_debug_artifacts", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("modules.scraper.RawReview.from_card", lambda _card: _Raw())

        scraper.review_db.upsert_place = MagicMock(return_value="place_1")
        scraper.review_db.start_session = MagicMock(return_value="session_1")
        scraper.review_db.get_review_ids = MagicMock(return_value={"existing_1"})
        scraper.review_db.get_reviews = MagicMock(return_value=[])
        scraper.review_db.end_session = MagicMock()

        assert scraper.scrape() is False
        assert scraper.last_error_transient is True
        assert "page hinted 20 reviews but scraper only confirmed 1" in scraper.last_error_message.lower()
        assert scraper.review_db.end_session.call_args[0][1] == "failed"
    finally:
        scraper.review_db.close()


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
    assert "_transport_failure_is_retryable" in source
    assert "raise TransientScrapeError(str(e)) from e" in source


def test_scrape_emits_plain_scraped_progress_logs():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "Scraped %d reviews so far" in source


def test_progress_description_includes_short_job_tag_and_place_name(tmp_path):
    scraper = GoogleReviewsScraper(
        _minimal_config(
            tmp_path,
            job_id="12345678-90ab-cdef-1234-567890abcdef",
            custom_params={"company": "Fallback Company"},
        )
    )
    try:
        assert (
            scraper._progress_description(place_name="Do Nothing Day", place_id="place_123")
            == "[job:12345678] Do Nothing Day"
        )
    finally:
        scraper.review_db.close()


def test_progress_description_falls_back_to_company_and_truncates(tmp_path):
    scraper = GoogleReviewsScraper(
        _minimal_config(
            tmp_path,
            job_id="abcdef12-3456-7890-abcd-ef1234567890",
            custom_params={
                "company": "A very long restaurant name that should be shortened for progress output"
            },
        )
    )
    try:
        assert (
            scraper._progress_description(place_name="", place_id="place_456")
            == "[job:abcdef12] A very long restaurant name that..."
        )
    finally:
        scraper.review_db.close()


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
    assert "self._capture_scroll_progress(" in source


def test_scrape_rebinds_scroll_target_after_two_no_progress_iterations():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "generation_no_progress_iterations >= 2" in source
    assert "_rebind_scroll_target(" in source


def test_scrape_uses_material_review_gap_to_fail_transiently():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "_maybe_raise_coverage_gap_transient(" in source
    assert "no scroll progress after" in source
    assert "still 0 new reviews after" in source


def test_scrape_hard_stall_threshold_requires_four_no_progress_iterations():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "no_progress_iterations > STUCK_SCROLL_RECOVERY_THRESHOLD" in source


def test_scrape_loop_keeps_confirmation_paths_reachable_at_attempt_limit():
    source = inspect.getsource(GoogleReviewsScraper.scrape)
    assert "while attempts < max_attempts or idle >= max_idle or consecutive_no_cards > 5:" in source


def test_setup_driver_forces_isolated_remote_debugging_port(tmp_path, monkeypatch):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        captured = []

        def _fake_driver(**kwargs):
            captured.append(kwargs)
            driver = MagicMock()
            driver.set_page_load_timeout = MagicMock()
            driver.set_window_size = MagicMock()
            driver.execute_cdp_cmd = MagicMock()
            return driver

        monkeypatch.setattr(scraper, "_allocate_remote_debugging_port", lambda: 45555)
        monkeypatch.setattr("modules.scraper.Driver", _fake_driver)

        scraper.setup_driver(headless=True)

        assert captured[0]["chromium_arg"] == ["remote-debugging-port=45555"]
    finally:
        scraper.review_db.close()


def test_setup_driver_uses_fresh_remote_debugging_port_per_browser(tmp_path, monkeypatch):
    scraper = GoogleReviewsScraper(_minimal_config(tmp_path))
    try:
        ports = iter([45555, 45556])
        captured = []

        def _fake_driver(**kwargs):
            captured.append(kwargs)
            driver = MagicMock()
            driver.set_page_load_timeout = MagicMock()
            driver.set_window_size = MagicMock()
            driver.execute_cdp_cmd = MagicMock()
            return driver

        monkeypatch.setattr(scraper, "_allocate_remote_debugging_port", lambda: next(ports))
        monkeypatch.setattr("modules.scraper.Driver", _fake_driver)

        scraper.setup_driver(headless=True)
        scraper.setup_driver(headless=True)

        assert captured[0]["chromium_arg"] == ["remote-debugging-port=45555"]
        assert captured[1]["chromium_arg"] == ["remote-debugging-port=45556"]
    finally:
        scraper.review_db.close()


def test_stuck_scroll_recovery_threshold_is_conservative_but_early():
    assert STUCK_SCROLL_RECOVERY_THRESHOLD == 3


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
