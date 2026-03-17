#!/usr/bin/env python3
"""
Generate a google-reviews-scraper-pro config YAML from Google Places Text Search.

Why:
- Google Places/Maps APIs can help you identify the correct listing (place_id, name, address),
  but they do NOT provide full review export. This script is meant to produce stable input
  URLs for the scraper to collect reviews via browser automation.

Auth:
- Reads API key from env var GOOGLE_PLACES_API_KEY (or GOOGLE_MAPS_API_KEY).
  Do not hardcode keys in files or commit them to git.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import requests
import yaml


TEXTSEARCH_ENDPOINT = "https://maps.googleapis.com/maps/api/place/textsearch/json"


@dataclass(frozen=True)
class PlaceHit:
    name: str
    formatted_address: str
    place_id: str
    rating: float | None
    user_ratings_total: int | None

    def to_maps_url(self) -> str:
        # This URL should resolve to a place page in the browser; the scraper extracts a stable id
        # from the resolved URL after navigation.
        params = {
            "api": "1",
            "query": self.name,
            "query_place_id": self.place_id,
        }
        return "https://www.google.com/maps/search/?" + urllib.parse.urlencode(params)


def _get_api_key() -> str:
    key = (os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key:
        raise SystemExit(
            "Missing API key. Set GOOGLE_PLACES_API_KEY (or GOOGLE_MAPS_API_KEY) in your environment."
        )
    return key


def _http_get_json(url: str, timeout_s: int = 30) -> dict[str, Any]:
    resp = requests.get(
        url,
        timeout=timeout_s,
        headers={
            # Keep it simple; some environments require a UA.
            "User-Agent": "places_textsearch_to_config/1.0 (+https://github.com/georgekhananaev/google-reviews-scraper-pro)",
        },
    )
    resp.raise_for_status()
    return resp.json()


def _parse_location(s: str) -> str:
    # Returns "lat,lng" suitable for the API.
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("location must be 'lat,lng'")
    try:
        lat = float(parts[0])
        lng = float(parts[1])
    except ValueError as e:
        raise argparse.ArgumentTypeError("location must be numeric 'lat,lng'") from e
    return f"{lat},{lng}"


def _read_query_file(path: str) -> list[str]:
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


def _dedupe_places(places: Iterable[PlaceHit]) -> list[PlaceHit]:
    seen_place_ids: set[str] = set()
    result: list[PlaceHit] = []
    for place in places:
        if place.place_id in seen_place_ids:
            continue
        seen_place_ids.add(place.place_id)
        result.append(place)
    return result


def _normalize_place_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKC", (name or "")).lower().strip()
    # Drop common descriptor suffix after separators (e.g. "Foo | Downtown Branch").
    normalized = normalized.split("|", 1)[0].split("｜", 1)[0].strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _is_better_name_duplicate(candidate: PlaceHit, incumbent: PlaceHit) -> bool:
    # Primary key: highest review-count/engagement volume.
    candidate_total = candidate.user_ratings_total if candidate.user_ratings_total is not None else -1
    incumbent_total = incumbent.user_ratings_total if incumbent.user_ratings_total is not None else -1
    if candidate_total != incumbent_total:
        return candidate_total > incumbent_total

    # Secondary key: higher rating.
    candidate_rating = candidate.rating if candidate.rating is not None else -1.0
    incumbent_rating = incumbent.rating if incumbent.rating is not None else -1.0
    if candidate_rating != incumbent_rating:
        return candidate_rating > incumbent_rating

    # Deterministic tie-breaker.
    return candidate.place_id < incumbent.place_id


def _dedupe_places_by_name_highest_ratings_total(places: Iterable[PlaceHit]) -> list[PlaceHit]:
    chosen: dict[str, PlaceHit] = {}
    order: list[str] = []
    for place in places:
        name_key = _normalize_place_name(place.name)
        if not name_key:
            name_key = place.place_id
        if name_key not in chosen:
            chosen[name_key] = place
            order.append(name_key)
            continue
        if _is_better_name_duplicate(place, chosen[name_key]):
            chosen[name_key] = place
    return [chosen[k] for k in order]


def _rank_and_select_places(
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


def _fetch_places_textsearch(
    *,
    api_key: str,
    query: str,
    location: str | None,
    radius_m: int | None,
    region: str | None,
    language: str | None,
    limit: int,
    timeout_s: int,
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
        payload = _http_get_json(url, timeout_s=timeout_s)

        status = payload.get("status")
        if status not in ("OK", "ZERO_RESULTS"):
            # INVALID_REQUEST is commonly returned if next_page_token isn't "ready" yet.
            err = payload.get("error_message") or payload
            raise RuntimeError(f"Places Text Search failed: status={status} error={err}")
        if status == "ZERO_RESULTS":
            break

        for item in payload.get("results", []):
            if len(hits) >= limit:
                break
            place_id = (item.get("place_id") or "").strip()
            name = (item.get("name") or "").strip()
            addr = (item.get("formatted_address") or "").strip()
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

        # Per Google docs, next_page_token can take a couple seconds to activate.
        time.sleep(2.0)

    return hits


def _write_config(
    *,
    out_path: str,
    places: list[PlaceHit],
    max_reviews: int,
    sort_by: str,
    headless: bool,
    download_images: bool,
    use_mongodb: bool,
    google_maps_auth_mode: str,
    fail_on_limited_view: bool,
    debug_on_limited_view: bool,
    max_scroll_attempts: int,
    scroll_idle_limit: int,
) -> None:
    cfg: dict[str, Any] = {
        "headless": bool(headless),
        "sort_by": sort_by,
        "google_maps_auth_mode": google_maps_auth_mode,
        "fail_on_limited_view": bool(fail_on_limited_view),
        "debug_on_limited_view": bool(debug_on_limited_view),
        "debug_artifacts_dir": "debug_artifacts",
        "scrape_mode": "update",
        "stop_threshold": 3,
        "max_reviews": int(max_reviews),
        "max_scroll_attempts": int(max_scroll_attempts),
        "scroll_idle_limit": int(scroll_idle_limit),
        "db_path": "reviews.db",
        "use_mongodb": bool(use_mongodb),
        "convert_dates": True,
        "download_images": bool(download_images),
        "image_dir": "review_images",
        "download_threads": 4,
        "backup_to_json": True,
        "json_path": "google_reviews.json",
        "businesses": [],
    }

    for p in places:
        cfg["businesses"].append(
            {
                "url": p.to_maps_url(),
                "custom_params": {
                    "company": p.name,
                    "address": p.formatted_address,
                    "source": "Google Maps",
                    "google_place_id": p.place_id,
                },
            }
        )

    with open(out_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=False)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", default="Changhua's Restaurant", help="Text search query")
    ap.add_argument("--query-file", default=None, help="Optional file with one query per line")
    ap.add_argument("--limit", type=int, default=1, help="Max number of place hits to include in config")
    ap.add_argument("--max-reviews", type=int, default=25, help="Max reviews per place for the scraper")
    ap.add_argument("--max-scroll-attempts", type=int, default=10,
                    help="Max scroll iterations per place before stopping")
    ap.add_argument("--scroll-idle-limit", type=int, default=10,
                    help="Stop after this many consecutive no-new-review iterations")
    ap.add_argument("--sort-by", default="newest", choices=["newest", "highest", "lowest", "relevance"])
    ap.add_argument("--rank-by", default="relevance", choices=["relevance", "composite"],
                    help="Result ordering strategy after dedupe/filter")
    ap.add_argument(
        "--dedupe-mode",
        default="place_id",
        choices=["place_id", "name_highest_ratings_total"],
        help=(
            "Dedupe strategy before ranking. "
            "'place_id' keeps first unique place_id. "
            "'name_highest_ratings_total' keeps one listing per normalized name using highest user_ratings_total."
        ),
    )
    ap.add_argument("--min-rating", type=float, default=0.0, help="Minimum place rating threshold")
    ap.add_argument("--min-ratings-total", type=int, default=0, help="Minimum user_ratings_total threshold")
    ap.add_argument("--headless", action="store_true", default=True, help="Run Chrome headless (default: true)")
    ap.add_argument("--headed", action="store_true", help="Force non-headless Chrome")
    ap.add_argument("--download-images", action="store_true", default=False, help="Download review/profile images")
    ap.add_argument("--use-mongodb", action="store_true", default=False,
                    help="Enable MongoDB sync in generated config (disabled by default)")
    ap.add_argument("--google-maps-auth-mode", default="cookie", choices=["anonymous", "cookie"],
                    help="Set generated scraper auth mode")
    ap.add_argument("--fail-on-limited-view", dest="fail_on_limited_view", action="store_true", default=True,
                    help="Enable fail-fast limited view handling in generated config")
    ap.add_argument("--no-fail-on-limited-view", dest="fail_on_limited_view", action="store_false",
                    help="Disable fail-fast limited view handling in generated config")
    ap.add_argument("--debug-on-limited-view", dest="debug_on_limited_view", action="store_true", default=True,
                    help="Enable debug artifact capture for limited-view failures")
    ap.add_argument("--no-debug-on-limited-view", dest="debug_on_limited_view", action="store_false",
                    help="Disable debug artifact capture for limited-view failures")
    ap.add_argument("--location", type=_parse_location, default=None, help="Bias search: 'lat,lng'")
    ap.add_argument("--radius-m", type=int, default=None, help="Bias search radius in meters (requires --location)")
    ap.add_argument("--region", default=None, help="Region code (ccTLD), e.g. us, tw")
    ap.add_argument("--language", default=None, help="Language code, e.g. en, zh-TW")
    ap.add_argument("--timeout-s", type=int, default=30)
    ap.add_argument("--out-config", default="config.yaml", help="Output config path (YAML)")
    ap.add_argument("--out-places-json", default="places_textsearch.json", help="Output places payload (JSON)")
    args = ap.parse_args(argv)

    if args.headed:
        headless = False
    else:
        headless = True

    if args.radius_m and not args.location:
        ap.error("--radius-m requires --location")
    if args.min_rating < 0.0:
        ap.error("--min-rating must be >= 0")
    if args.min_ratings_total < 0:
        ap.error("--min-ratings-total must be >= 0")
    if args.max_scroll_attempts <= 0:
        ap.error("--max-scroll-attempts must be > 0")
    if args.scroll_idle_limit <= 0:
        ap.error("--scroll-idle-limit must be > 0")

    api_key = _get_api_key()
    queries: list[str] = [args.query]
    if args.query_file:
        file_queries = _read_query_file(args.query_file)
        if not file_queries:
            ap.error("--query-file was provided but contains no usable query lines")
        for q in file_queries:
            if q not in queries:
                queries.append(q)

    requested_limit = max(1, int(args.limit))
    fetch_limit_per_query = max(requested_limit, 20)
    all_hits: list[PlaceHit] = []
    for query in queries:
        fetched = _fetch_places_textsearch(
            api_key=api_key,
            query=query,
            location=args.location,
            radius_m=args.radius_m,
            region=args.region,
            language=args.language,
            limit=fetch_limit_per_query,
            timeout_s=int(args.timeout_s),
        )
        all_hits.extend(fetched)

    deduped_by_id = _dedupe_places(all_hits)
    if args.dedupe_mode == "name_highest_ratings_total":
        deduped_hits = _dedupe_places_by_name_highest_ratings_total(deduped_by_id)
    else:
        deduped_hits = deduped_by_id

    places = _rank_and_select_places(
        deduped_hits,
        rank_by=args.rank_by,
        min_rating=float(args.min_rating),
        min_ratings_total=int(args.min_ratings_total),
        limit=requested_limit,
    )

    with open(args.out_places_json, "w", encoding="utf-8") as f:
        json.dump([p.__dict__ for p in places], f, indent=2, ensure_ascii=True)

    if not places:
        print("No places found for query; config not written.", file=sys.stderr)
        return 2

    _write_config(
        out_path=args.out_config,
        places=places,
        max_reviews=int(args.max_reviews),
        sort_by=args.sort_by,
        headless=headless,
        download_images=bool(args.download_images),
        use_mongodb=bool(args.use_mongodb),
        google_maps_auth_mode=str(args.google_maps_auth_mode),
        fail_on_limited_view=bool(args.fail_on_limited_view),
        debug_on_limited_view=bool(args.debug_on_limited_view),
        max_scroll_attempts=int(args.max_scroll_attempts),
        scroll_idle_limit=int(args.scroll_idle_limit),
    )

    print(
        f"Wrote {args.out_config} with {len(places)} place(s) "
        f"(queries={len(queries)}, rank_by={args.rank_by}, dedupe_mode={args.dedupe_mode}, requested_limit={requested_limit})."
    )
    if len(places) < requested_limit:
        print(f"Warning: only {len(places)} place(s) available after dedupe/filter.")
    for i, p in enumerate(places, 1):
        r = f"{p.rating}" if p.rating is not None else "n/a"
        urt = f"{p.user_ratings_total}" if p.user_ratings_total is not None else "n/a"
        print(f"{i}. {p.name} | {p.formatted_address} | rating={r} | ratings_total={urt}")
        print(f"   maps_url={p.to_maps_url()}")

    print("")
    print("Next:")
    print(f"  python3 start.py scrape --config {args.out_config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
