"""Tests for deterministic dataset preprocessing helpers."""

from modules.preprocessing import build_cleaned_review_rows, normalize_text


def test_normalize_text_removes_bidi_controls_and_collapses_whitespace():
    value = "  A\u200eＢ \n  C  "
    assert normalize_text(value) == "AB C"


def test_build_cleaned_review_rows_sets_flags_and_duplicate_groups():
    rows = [
        {
            "place_id": "place_a",
            "place_name": "Place A",
            "review_id": "r1",
            "author": " Alice  ",
            "rating": 5.0,
            "review_date": "2025-06-15",
            "raw_date": "3 months ago",
            "likes": 1,
            "source_locale": "en",
            "extraction_confidence": "good",
            "review_text_primary": " Great\nfood ",
            "review_text_all_json": '{"en":{"text":" Great\\nfood "}}',
            "owner_responses_json": '{"en":{"text":" Thanks!  "}}',
            "is_deleted": 0,
            "scrape_session_id": 1,
            "scrape_started_at": "2026-03-22T00:00:00+00:00",
            "scrape_completed_at": "2026-03-22T00:01:00+00:00",
            "scrape_mode": "scrape",
            "source_url": "https://example.com/a",
            "resolved_place_url": "https://example.com/a?resolved=1",
        },
        {
            "place_id": "place_a",
            "place_name": "Place A",
            "review_id": "r2",
            "author": "ALICE",
            "rating": 5.0,
            "review_date": "2025-06-16",
            "raw_date": "2 months ago",
            "likes": 0,
            "source_locale": "en",
            "extraction_confidence": "good",
            "review_text_primary": "Great food",
            "review_text_all_json": '{"en":{"text":"Great food"}}',
            "owner_responses_json": "{}",
            "is_deleted": 0,
            "scrape_session_id": 1,
            "scrape_started_at": "2026-03-22T00:00:00+00:00",
            "scrape_completed_at": "2026-03-22T00:01:00+00:00",
            "scrape_mode": "scrape",
            "source_url": "https://example.com/a",
            "resolved_place_url": "https://example.com/a?resolved=1",
        },
        {
            "place_id": "place_a",
            "place_name": "Place A",
            "review_id": "r3",
            "author": "Bob",
            "rating": 4.0,
            "review_date": "2025-06-17",
            "raw_date": "1 month ago",
            "likes": 0,
            "source_locale": "zh-Hant",
            "extraction_confidence": "good",
            "review_text_primary": "讚",
            "review_text_all_json": '{"zh-Hant":{"text":"讚"}}',
            "owner_responses_json": "{}",
            "is_deleted": 0,
            "scrape_session_id": 2,
            "scrape_started_at": "2026-03-22T00:00:00+00:00",
            "scrape_completed_at": "2026-03-22T00:01:00+00:00",
            "scrape_mode": "scrape",
            "source_url": "https://example.com/a",
            "resolved_place_url": "https://example.com/a?resolved=1",
        },
        {
            "place_id": "place_a",
            "place_name": "Place A",
            "review_id": "r4",
            "author": "Charlie",
            "rating": 3.0,
            "review_date": "2025-06-18",
            "raw_date": "3 weeks ago",
            "likes": 0,
            "source_locale": "en",
            "extraction_confidence": "good",
            "review_text_primary": "",
            "review_text_all_json": "{}",
            "owner_responses_json": "{}",
            "is_deleted": 0,
            "scrape_session_id": 2,
            "scrape_started_at": "2026-03-22T00:00:00+00:00",
            "scrape_completed_at": "2026-03-22T00:01:00+00:00",
            "scrape_mode": "scrape",
            "source_url": "https://example.com/a",
            "resolved_place_url": "https://example.com/a?resolved=1",
        },
        {
            "place_id": "place_a",
            "place_name": "Place A",
            "review_id": "r5",
            "author": "Dana",
            "rating": 2.0,
            "review_date": "2025-06-19",
            "raw_date": "2 weeks ago",
            "likes": 0,
            "source_locale": "en",
            "extraction_confidence": "good",
            "review_text_primary": "!!!🔥🔥🔥",
            "review_text_all_json": '{"en":{"text":"!!!🔥🔥🔥"}}',
            "owner_responses_json": "{}",
            "is_deleted": 0,
            "scrape_session_id": 3,
            "scrape_started_at": "2026-03-22T00:00:00+00:00",
            "scrape_completed_at": "2026-03-22T00:01:00+00:00",
            "scrape_mode": "scrape",
            "source_url": "https://example.com/a",
            "resolved_place_url": "https://example.com/a?resolved=1",
        },
    ]

    cleaned = build_cleaned_review_rows(rows)
    by_id = {row["review_id"]: row for row in cleaned}

    assert by_id["r1"]["review_text_clean"] == "Great food"
    assert by_id["r1"]["author_normalized"] == "Alice"
    assert by_id["r1"]["owner_response_clean"] == "Thanks!"
    assert by_id["r1"]["has_text"] is True
    assert by_id["r1"]["review_text_language_count"] == 1
    assert by_id["r1"]["review_text_word_count"] == 2
    assert by_id["r1"]["review_text_meaningful_char_count"] == 9
    assert by_id["r1"]["owner_response_language_count"] == 1
    assert by_id["r1"]["owner_response_meaningful_char_count"] == 6
    assert by_id["r1"]["duplicate_text_group_size"] == 2
    assert by_id["r1"]["possible_duplicate_text_within_place"] is True
    assert by_id["r1"]["qa_flags"] == "duplicate_text_within_place"

    assert by_id["r2"]["duplicate_text_group_size"] == 2
    assert by_id["r2"]["possible_duplicate_text_within_place"] is True

    assert by_id["r3"]["review_text_has_cjk"] is True
    assert by_id["r3"]["is_low_information_text"] is True
    assert by_id["r3"]["qa_flags"] == "low_information_text"

    assert by_id["r4"]["is_empty_text"] is True
    assert by_id["r4"]["has_text"] is False
    assert by_id["r4"]["review_text_normalized_hash"] == ""
    assert by_id["r4"]["qa_flags"] == "empty_text"

    assert by_id["r5"]["emoji_count"] == 3
    assert by_id["r5"]["emoji_density"] == 0.5
    assert by_id["r5"]["punctuation_count"] == 3
    assert by_id["r5"]["punctuation_density"] == 0.5
    assert by_id["r5"]["has_repeated_punctuation_run"] is True
    assert by_id["r5"]["possible_format_anomaly"] is True
    assert by_id["r5"]["qa_flags"] == "low_information_text|format_anomaly"
