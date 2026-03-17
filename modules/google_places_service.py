"""
Shared Google Places discovery and validation helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import time
from typing import Any, Iterable, Optional
import unicodedata
import urllib.parse

import requests


TEXTSEARCH_ENDPOINT = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_ENDPOINT = "https://maps.googleapis.com/maps/api/place/details/json"


class GooglePlacesError(RuntimeError):
    """Base error for Google Places API helpers."""


class GooglePlacesConfigError(GooglePlacesError):
    """Raised when Google Places credentials are unavailable."""


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class PlaceHit:
    name: str
    formatted_address: str
    place_id: str
    rating: float | None
    user_ratings_total: int | None

    def to_maps_url(self) -> str:
        params = {
            "api": "1",
            "query": self.name,
            "query_place_id": self.place_id,
        }
        return "https://www.google.com/maps/search/?" + urllib.parse.urlencode(params)


def get_api_key() -> str:
    key = (os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key:
        raise GooglePlacesConfigError(
            "Missing API key. Set GOOGLE_PLACES_API_KEY (or GOOGLE_MAPS_API_KEY) in your environment."
        )
    return key


def http_get_json(
    url: str,
    *,
    timeout_s: int = 30,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    client = session or requests
    resp = client.get(
        url,
        timeout=timeout_s,
        headers={
            "User-Agent": (
                "google-places-service/1.0 "
                "(+https://github.com/georgekhananaev/google-reviews-scraper-pro)"
            ),
        },
    )
    resp.raise_for_status()
    payload = resp.json()
    return payload if isinstance(payload, dict) else {}


def parse_location(s: str) -> str:
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 2:
        raise ValueError("location must be 'lat,lng'")
    lat = float(parts[0])
    lng = float(parts[1])
    return f"{lat},{lng}"


def read_query_file(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"query file not found: {path}")
    queries: list[str] = []
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        queries.append(line)
    return queries


def dedupe_places(places: Iterable[PlaceHit]) -> list[PlaceHit]:
    seen_place_ids: set[str] = set()
    result: list[PlaceHit] = []
    for place in places:
        if place.place_id in seen_place_ids:
            continue
        seen_place_ids.add(place.place_id)
        result.append(place)
    return result


def normalize_place_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKC", (name or "")).lower().strip()
    normalized = normalized.split("|", 1)[0].split("｜", 1)[0].strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _is_better_name_duplicate(candidate: PlaceHit, incumbent: PlaceHit) -> bool:
    candidate_total = candidate.user_ratings_total if candidate.user_ratings_total is not None else -1
    incumbent_total = incumbent.user_ratings_total if incumbent.user_ratings_total is not None else -1
    if candidate_total != incumbent_total:
        return candidate_total > incumbent_total

    candidate_rating = candidate.rating if candidate.rating is not None else -1.0
    incumbent_rating = incumbent.rating if incumbent.rating is not None else -1.0
    if candidate_rating != incumbent_rating:
        return candidate_rating > incumbent_rating

    return candidate.place_id < incumbent.place_id


def dedupe_places_by_name_highest_ratings_total(places: Iterable[PlaceHit]) -> list[PlaceHit]:
    chosen: dict[str, PlaceHit] = {}
    order: list[str] = []
    for place in places:
        name_key = normalize_place_name(place.name) or place.place_id
        if name_key not in chosen:
            chosen[name_key] = place
            order.append(name_key)
            continue
        if _is_better_name_duplicate(place, chosen[name_key]):
            chosen[name_key] = place
    return [chosen[k] for k in order]


def rank_and_select_places(
    places: list[PlaceHit],
    *,
    rank_by: str,
    min_rating: float,
    min_ratings_total: int,
    limit: int,
) -> list[PlaceHit]:
    filtered: list[PlaceHit] = []
    for place in places:
        rating = place.rating if place.rating is not None else 0.0
        ratings_total = place.user_ratings_total if place.user_ratings_total is not None else 0
        if rating < min_rating:
            continue
        if ratings_total < min_ratings_total:
            continue
        filtered.append(place)

    if rank_by == "composite":
        filtered.sort(
            key=lambda p: (
                -(p.rating if p.rating is not None else -1.0),
                -(p.user_ratings_total if p.user_ratings_total is not None else -1),
                p.name.lower(),
                p.place_id,
            )
        )
    return filtered[:limit]


def fetch_places_textsearch(
    *,
    api_key: str,
    query: str,
    location: str | None,
    radius_m: int | None,
    region: str | None,
    language: str | None,
    limit: int,
    timeout_s: int,
    session: Optional[requests.Session] = None,
) -> list[PlaceHit]:
    hits: list[PlaceHit] = []
    page_token: str | None = None

    while len(hits) < limit:
        params: dict[str, Any] = {"query": query, "key": api_key}
        if location:
            params["location"] = location
        if radius_m:
            params["radius"] = int(radius_m)
        if region:
            params["region"] = region
        if language:
            params["language"] = language
        if page_token:
            params["pagetoken"] = page_token

        url = TEXTSEARCH_ENDPOINT + "?" + urllib.parse.urlencode(params)
        payload = http_get_json(url, timeout_s=timeout_s, session=session)

        status = str(payload.get("status") or "").strip()
        if status not in ("OK", "ZERO_RESULTS"):
            err = payload.get("error_message") or payload
            raise GooglePlacesError(f"Places Text Search failed: status={status} error={err}")
        if status == "ZERO_RESULTS":
            break

        for item in payload.get("results", []):
            if len(hits) >= limit:
                break
            place_id = str(item.get("place_id") or "").strip()
            name = str(item.get("name") or "").strip()
            addr = str(item.get("formatted_address") or "").strip()
            if not (place_id and name):
                continue
            rating = item.get("rating")
            try:
                rating_f = float(rating) if rating is not None else None
            except Exception:
                rating_f = None
            urt = item.get("user_ratings_total")
            try:
                urt_i = int(urt) if urt is not None else None
            except Exception:
                urt_i = None
            hits.append(
                PlaceHit(
                    name=name,
                    formatted_address=addr,
                    place_id=place_id,
                    rating=rating_f,
                    user_ratings_total=urt_i,
                )
            )

        page_token = payload.get("next_page_token")
        if not page_token:
            break

        time.sleep(2.0)

    return hits


def get_place_details(
    *,
    api_key: str,
    google_place_id: str,
    language: str | None = None,
    timeout_s: int = 30,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "place_id": google_place_id,
        "fields": "place_id,name,formatted_address,business_status",
        "key": api_key,
    }
    if language:
        params["language"] = language
    url = DETAILS_ENDPOINT + "?" + urllib.parse.urlencode(params)
    payload = http_get_json(url, timeout_s=timeout_s, session=session)
    return payload


def _normalize_name_for_match(value: str) -> str:
    cleaned = re.sub(r"[\u200e\u200f\u202a-\u202e]", "", (value or "")).lower().strip()
    return re.sub(r"[\W_]+", "", cleaned, flags=re.UNICODE)


def names_match(expected_name: str, actual_name: str) -> bool:
    expected_norm = _normalize_name_for_match(expected_name)
    actual_norm = _normalize_name_for_match(actual_name)
    if not expected_norm or not actual_norm:
        return False
    return expected_norm in actual_norm or actual_norm in expected_norm


def validate_place(
    *,
    google_place_id: str,
    expected_name: str = "",
    language: str | None = None,
    timeout_s: int = 30,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    checked_at = now_utc_iso()
    try:
        api_key = get_api_key()
    except GooglePlacesConfigError as exc:
        return {
            "google_place_id": google_place_id,
            "status": "error",
            "reason": str(exc),
            "api_name": None,
            "api_address": None,
            "business_status": None,
            "checked_at": checked_at,
        }

    try:
        payload = get_place_details(
            api_key=api_key,
            google_place_id=google_place_id,
            language=language,
            timeout_s=timeout_s,
            session=session,
        )
    except Exception as exc:
        return {
            "google_place_id": google_place_id,
            "status": "error",
            "reason": str(exc),
            "api_name": None,
            "api_address": None,
            "business_status": None,
            "checked_at": checked_at,
        }

    status = str(payload.get("status") or "").strip()
    if status in {"NOT_FOUND", "ZERO_RESULTS"}:
        return {
            "google_place_id": google_place_id,
            "status": "invalid_not_found",
            "reason": f"Place Details returned {status}",
            "api_name": None,
            "api_address": None,
            "business_status": None,
            "checked_at": checked_at,
        }
    if status != "OK":
        return {
            "google_place_id": google_place_id,
            "status": "error",
            "reason": str(payload.get("error_message") or f"Place Details returned {status}"),
            "api_name": None,
            "api_address": None,
            "business_status": None,
            "checked_at": checked_at,
        }

    result = payload.get("result") or {}
    api_name = str(result.get("name") or "").strip() or None
    api_address = str(result.get("formatted_address") or "").strip() or None
    business_status = str(result.get("business_status") or "").strip() or None
    returned_place_id = str(result.get("place_id") or "").strip()

    if returned_place_id and returned_place_id != google_place_id:
        return {
            "google_place_id": google_place_id,
            "status": "invalid_mismatch",
            "reason": f"Place Details returned different place_id {returned_place_id}",
            "api_name": api_name,
            "api_address": api_address,
            "business_status": business_status,
            "checked_at": checked_at,
        }

    if business_status == "CLOSED_PERMANENTLY":
        return {
            "google_place_id": google_place_id,
            "status": "invalid_closed",
            "reason": "Place is permanently closed",
            "api_name": api_name,
            "api_address": api_address,
            "business_status": business_status,
            "checked_at": checked_at,
        }

    if expected_name and api_name and not names_match(expected_name, api_name):
        return {
            "google_place_id": google_place_id,
            "status": "invalid_mismatch",
            "reason": f"Expected '{expected_name}' but Google Places returned '{api_name}'",
            "api_name": api_name,
            "api_address": api_address,
            "business_status": business_status,
            "checked_at": checked_at,
        }

    return {
        "google_place_id": google_place_id,
        "status": "valid",
        "reason": "Validated by Google Places API",
        "api_name": api_name,
        "api_address": api_address,
        "business_status": business_status,
        "checked_at": checked_at,
    }
