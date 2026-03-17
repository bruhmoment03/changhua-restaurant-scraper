"""
Shared progress helpers for batch scraping selection and reporting.
"""

import urllib.parse
from typing import Any, Dict, List


def resolve_businesses(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Resolve business list from config (supports businesses, urls, or url)."""
    businesses = config.get("businesses", [])
    if businesses:
        return [b if isinstance(b, dict) else {"url": b} for b in businesses]

    urls = config.get("urls", [])
    single_url = config.get("url")
    if not urls and single_url:
        urls = [single_url]
    return [{"url": u} for u in urls]


def normalize_url_for_match(url: str) -> str:
    """Normalize URL for stable equality checks."""
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    query_pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query_pairs.sort()
    return urllib.parse.urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            parsed.params,
            urllib.parse.urlencode(query_pairs, doseq=True),
            "",
        )
    )


def extract_query_place_id(url: str) -> str:
    """Extract query_place_id from a Google Maps URL when present."""
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    values = qs.get("query_place_id", [])
    return values[0].strip() if values else ""


def business_identity(business: Dict[str, Any]) -> Dict[str, str]:
    """Extract normalized identity fields from a business entry."""
    custom_params = business.get("custom_params", {}) if isinstance(business, dict) else {}
    url = (business.get("url", "") if isinstance(business, dict) else "").strip()
    company = str(custom_params.get("company", "")).strip()
    google_place_id = str(custom_params.get("google_place_id", "")).strip() or extract_query_place_id(url)
    return {
        "url": url,
        "url_key": normalize_url_for_match(url),
        "company": company,
        "google_place_id": google_place_id,
    }


def build_db_progress_index(review_db) -> Dict[str, Dict[str, Any]]:
    """Build DB lookup maps keyed by query_place_id and URL."""
    rows = review_db.backend.fetchall(
        "SELECT p.place_id, p.place_name, p.original_url, p.last_scraped, "
        "COALESCE(p.total_reviews, 0) AS cached_total_reviews, "
        "COALESCE(p.reviews_exhausted, 0) AS reviews_exhausted, "
        "COALESCE(p.validation_status, 'unknown') AS validation_status, "
        "p.validation_checked_at, p.validation_reason, "
        "COALESCE(rc.review_count, 0) AS review_count "
        "FROM places p "
        "LEFT JOIN ("
        "  SELECT place_id, COUNT(*) AS review_count "
        "  FROM reviews WHERE is_deleted = 0 GROUP BY place_id"
        ") rc ON rc.place_id = p.place_id"
    )

    by_query_place_id: Dict[str, Dict[str, Any]] = {}
    by_url: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        entry = {
            "place_id": row["place_id"],
            "place_name": row.get("place_name"),
            "original_url": row.get("original_url") or "",
            "last_scraped": row.get("last_scraped"),
            "review_count": int(row.get("review_count", 0) or 0),
            "cached_total_reviews": int(row.get("cached_total_reviews", 0) or 0),
            "reviews_exhausted": bool(row.get("reviews_exhausted", 0)),
            "validation_status": str(row.get("validation_status", "unknown") or "unknown"),
            "validation_checked_at": row.get("validation_checked_at"),
            "validation_reason": row.get("validation_reason"),
        }
        query_place_id = extract_query_place_id(entry["original_url"])
        url_key = normalize_url_for_match(entry["original_url"])

        if query_place_id and query_place_id not in by_query_place_id:
            by_query_place_id[query_place_id] = entry
        if url_key and url_key not in by_url:
            by_url[url_key] = entry

    return {"by_query_place_id": by_query_place_id, "by_url": by_url}


def compute_progress_report(
    businesses: List[Dict[str, Any]],
    review_db,
    min_reviews: int = 1,
) -> Dict[str, Any]:
    """Compare configured businesses to DB state.

    Legacy status fields remain based on min=1 semantics:
      - with_reviews
      - present_zero_reviews
      - missing_from_db
    Additional threshold fields are derived from *min_reviews*.
    """
    min_reviews = max(0, int(min_reviews))
    index = build_db_progress_index(review_db)
    targets: List[Dict[str, Any]] = []
    with_reviews = 0
    present_zero_reviews = 0
    missing_from_db = 0
    meeting_min_reviews = 0
    under_min_reviews = 0
    exhausted_under_threshold_count = 0

    for business in businesses:
        ident = business_identity(business)
        db_entry = None

        if ident["google_place_id"]:
            db_entry = index["by_query_place_id"].get(ident["google_place_id"])
        if not db_entry and ident["url_key"]:
            db_entry = index["by_url"].get(ident["url_key"])

        if db_entry is None:
            status = "missing_from_db"
            review_count = 0
            place_id = None
            place_name = None
            last_scraped = None
            cached_total_reviews = 0
            validation_status = "unknown"
            validation_checked_at = None
            validation_reason = None
            missing_from_db += 1
        else:
            review_count = int(db_entry.get("review_count", 0) or 0)
            cached_total_reviews = int(db_entry.get("cached_total_reviews", 0) or 0)
            place_id = db_entry.get("place_id")
            place_name = db_entry.get("place_name")
            last_scraped = db_entry.get("last_scraped")
            reviews_exhausted = bool(db_entry.get("reviews_exhausted", False))
            validation_status = str(db_entry.get("validation_status", "unknown") or "unknown")
            validation_checked_at = db_entry.get("validation_checked_at")
            validation_reason = db_entry.get("validation_reason")
            if review_count > 0:
                status = "with_reviews"
                with_reviews += 1
            else:
                status = "present_zero_reviews"
                present_zero_reviews += 1
            if reviews_exhausted and review_count < min_reviews:
                status = "exhausted_under_threshold"

        if db_entry is None:
            reviews_exhausted = False

        meets_min_reviews = review_count >= min_reviews
        if status == "exhausted_under_threshold":
            exhausted_under_threshold_count += 1
        elif meets_min_reviews:
            meeting_min_reviews += 1
        else:
            under_min_reviews += 1

        targets.append(
            {
                "company": ident["company"],
                "url": ident["url"],
                "google_place_id": ident["google_place_id"],
                "status": status,
                "review_count": review_count,
                "cached_total_reviews": cached_total_reviews,
                "place_id": place_id,
                "place_name": place_name,
                "last_scraped": last_scraped,
                "reviews_exhausted": reviews_exhausted,
                "validation_status": validation_status,
                "validation_checked_at": validation_checked_at,
                "validation_reason": validation_reason,
                "meets_min_reviews": meets_min_reviews,
                "reviews_needed": 0 if meets_min_reviews or reviews_exhausted else max(0, min_reviews - review_count),
            }
        )

    targets_total = len(targets)
    incomplete_total = present_zero_reviews + missing_from_db
    completed_percent = round((with_reviews / targets_total * 100.0), 2) if targets_total else 0.0

    return {
        "targets_total": targets_total,
        "with_reviews": with_reviews,
        "present_zero_reviews": present_zero_reviews,
        "missing_from_db": missing_from_db,
        "incomplete_total": incomplete_total,
        "completed_percent": completed_percent,
        "min_reviews": min_reviews,
        "meeting_min_reviews": meeting_min_reviews,
        "under_min_reviews": under_min_reviews,
        "exhausted_under_threshold_count": exhausted_under_threshold_count,
        "targets": targets,
    }


def select_businesses_for_scrape(
    businesses: List[Dict[str, Any]],
    report: Dict[str, Any],
    only_missing: bool,
    max_businesses: int | None,
) -> List[Dict[str, Any]]:
    """Select businesses based on progress filters."""
    selected = list(zip(businesses, report["targets"]))
    if only_missing:
        selected = [
            (biz, target)
            for biz, target in selected
            if target["status"] in ("missing_from_db", "present_zero_reviews")
        ]

    if max_businesses is not None:
        selected = selected[:max_businesses]

    return [biz for biz, _ in selected]
