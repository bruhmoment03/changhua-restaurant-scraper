"""
Deterministic preprocessing helpers for dataset exports.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import Counter
from typing import Any, Dict, Iterable, List


_BIDI_CONTROL_RE = re.compile(r"[\u200e\u200f\u202a-\u202e]")
_WHITESPACE_RE = re.compile(r"\s+")
_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_MEANINGFUL_CHAR_RE = re.compile(r"[0-9A-Za-z\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\u2600-\u26FF"
    "\u2700-\u27BF"
    "]"
)
_REPEATED_PUNCTUATION_RE = re.compile(r"([!?.,;:~*#。，、！？…])\1{2,}")
_QA_FLAG_ORDER = (
    "empty_text",
    "low_information_text",
    "duplicate_text_within_place",
    "format_anomaly",
)

PREPROCESSING_VERSION = "dataset-preprocessing-v2"


def normalize_text(value: Any) -> str:
    """Normalize text conservatively for reproducible exports."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = _BIDI_CONTROL_RE.sub("", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", " ")
    return _WHITESPACE_RE.sub(" ", text).strip()


def normalize_author(value: Any) -> str:
    """Normalize author names conservatively for cleaned outputs."""
    return normalize_text(value)


def normalize_place_name(value: Any) -> str:
    """Normalize place names for lightweight comparison fields."""
    normalized = normalize_text(value).lower()
    normalized = normalized.split("|", 1)[0].split("｜", 1)[0].strip()
    return _WHITESPACE_RE.sub(" ", normalized).strip()


def has_cjk(text: str) -> bool:
    return bool(_CJK_RE.search(text or ""))


def is_low_information_text(text: str) -> bool:
    """Flag very short text with limited alnum/CJK signal."""
    if not text:
        return False
    meaningful_chars = _MEANINGFUL_CHAR_RE.findall(text)
    return len(meaningful_chars) < 4


def normalized_text_hash(text: str) -> str:
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _json_loads(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _first_text_value(value: Any) -> str:
    payload = _json_loads(value)
    if isinstance(payload, dict):
        for lang in sorted(payload.keys()):
            item = payload.get(lang)
            if isinstance(item, dict):
                text = item.get("text")
            else:
                text = item
            if isinstance(text, str) and text.strip():
                return text
    return ""


def _language_text_count(value: Any, *, fallback_text: str = "") -> int:
    payload = _json_loads(value)
    count = 0
    if isinstance(payload, dict):
        for item in payload.values():
            text = item.get("text") if isinstance(item, dict) else item
            if normalize_text(text):
                count += 1
        if count:
            return count
    return 1 if normalize_text(fallback_text) else 0


def _meaningful_char_count(text: str) -> int:
    return len(_MEANINGFUL_CHAR_RE.findall(text or ""))


def _word_count(text: str) -> int:
    if not text:
        return 0
    return len([part for part in text.split(" ") if part])


def _emoji_count(text: str) -> int:
    return len(_EMOJI_RE.findall(text or ""))


def _punctuation_count(text: str) -> int:
    return sum(1 for char in text or "" if unicodedata.category(char).startswith("P"))


def _density(count: int, text: str) -> float:
    if not text:
        return 0.0
    return round(float(count) / float(len(text)), 4)


def _has_repeated_punctuation_run(text: str) -> bool:
    return bool(_REPEATED_PUNCTUATION_RE.search(text or ""))


def _possible_format_anomaly(
    text: str,
    *,
    meaningful_char_count: int,
    emoji_count: int,
    punctuation_count: int,
    emoji_density: float,
    punctuation_density: float,
    has_repeated_punctuation_run: bool,
) -> bool:
    if not text:
        return False
    if has_repeated_punctuation_run:
        return True
    if emoji_density >= 0.35:
        return True
    if punctuation_density >= 0.45 and meaningful_char_count <= 10:
        return True
    if meaningful_char_count == 0 and (emoji_count > 0 or punctuation_count > 0):
        return True
    return False


def _qa_flags(row: Dict[str, Any]) -> str:
    active = []
    if row.get("is_empty_text"):
        active.append("empty_text")
    if row.get("is_low_information_text"):
        active.append("low_information_text")
    if row.get("possible_duplicate_text_within_place"):
        active.append("duplicate_text_within_place")
    if row.get("possible_format_anomaly"):
        active.append("format_anomaly")
    return "|".join(flag for flag in _QA_FLAG_ORDER if flag in active)


def build_cleaned_review_rows(raw_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build cleaned review rows from raw flat export rows.

    The output is intentionally conservative: raw content remains untouched in
    storage, while cleaned rows expose deterministic derived fields and QA flags.
    """
    cleaned_rows: List[Dict[str, Any]] = []

    for raw in raw_rows:
        review_text_raw = str(raw.get("review_text_primary") or "")
        review_text_clean = normalize_text(review_text_raw)
        review_text_all_json = raw.get("review_text_all_json")
        owner_response_raw = _first_text_value(raw.get("owner_responses_json"))
        owner_response_clean = normalize_text(owner_response_raw)
        author_raw = str(raw.get("author") or "")
        author_normalized = normalize_author(author_raw)
        text_hash = normalized_text_hash(review_text_clean)
        review_text_language_count = _language_text_count(
            review_text_all_json,
            fallback_text=review_text_clean,
        )
        review_text_meaningful_char_count = _meaningful_char_count(review_text_clean)
        review_text_word_count = _word_count(review_text_clean)
        emoji_count = _emoji_count(review_text_clean)
        punctuation_count = _punctuation_count(review_text_clean)
        emoji_density = _density(emoji_count, review_text_clean)
        punctuation_density = _density(punctuation_count, review_text_clean)
        has_repeated_punctuation_run = _has_repeated_punctuation_run(review_text_clean)
        owner_response_language_count = _language_text_count(
            raw.get("owner_responses_json"),
            fallback_text=owner_response_clean,
        )
        owner_response_meaningful_char_count = _meaningful_char_count(owner_response_clean)
        possible_format_anomaly = _possible_format_anomaly(
            review_text_clean,
            meaningful_char_count=review_text_meaningful_char_count,
            emoji_count=emoji_count,
            punctuation_count=punctuation_count,
            emoji_density=emoji_density,
            punctuation_density=punctuation_density,
            has_repeated_punctuation_run=has_repeated_punctuation_run,
        )

        cleaned_rows.append(
            {
                "place_id": raw.get("place_id"),
                "place_name": raw.get("place_name"),
                "review_id": raw.get("review_id"),
                "author_raw": author_raw,
                "author_normalized": author_normalized,
                "rating": raw.get("rating"),
                "review_date": raw.get("review_date"),
                "raw_date": raw.get("raw_date"),
                "likes": raw.get("likes"),
                "source_locale": raw.get("source_locale"),
                "extraction_confidence": raw.get("extraction_confidence"),
                "review_text_raw": review_text_raw,
                "review_text_clean": review_text_clean,
                "review_text_normalized_hash": text_hash,
                "review_text_char_count": len(review_text_clean),
                "has_text": bool(review_text_clean),
                "review_text_language_count": review_text_language_count,
                "review_text_word_count": review_text_word_count,
                "review_text_meaningful_char_count": review_text_meaningful_char_count,
                "review_text_has_cjk": has_cjk(review_text_clean),
                "emoji_count": emoji_count,
                "emoji_density": emoji_density,
                "punctuation_count": punctuation_count,
                "punctuation_density": punctuation_density,
                "has_repeated_punctuation_run": has_repeated_punctuation_run,
                "is_empty_text": not bool(review_text_clean),
                "is_low_information_text": is_low_information_text(review_text_clean),
                "possible_format_anomaly": possible_format_anomaly,
                "qa_flags": "",
                "owner_response_raw": owner_response_raw,
                "owner_response_clean": owner_response_clean,
                "has_owner_response": bool(owner_response_clean),
                "owner_response_char_count": len(owner_response_clean),
                "owner_response_language_count": owner_response_language_count,
                "owner_response_meaningful_char_count": owner_response_meaningful_char_count,
                "possible_duplicate_text_within_place": False,
                "duplicate_text_group_size": 0,
                "is_deleted": raw.get("is_deleted"),
                "scrape_session_id": raw.get("scrape_session_id"),
                "scrape_started_at": raw.get("scrape_started_at"),
                "scrape_completed_at": raw.get("scrape_completed_at"),
                "scrape_mode": raw.get("scrape_mode"),
                "source_url": raw.get("source_url"),
                "resolved_place_url": raw.get("resolved_place_url"),
            }
        )

    group_sizes = Counter(
        (str(row.get("place_id") or ""), row["review_text_normalized_hash"])
        for row in cleaned_rows
        if row["review_text_normalized_hash"]
    )
    for row in cleaned_rows:
        key = (str(row.get("place_id") or ""), row["review_text_normalized_hash"])
        group_size = group_sizes.get(key, 0) if row["review_text_normalized_hash"] else 0
        row["duplicate_text_group_size"] = group_size
        row["possible_duplicate_text_within_place"] = group_size > 1
        row["qa_flags"] = _qa_flags(row)

    return cleaned_rows
