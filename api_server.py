#!/usr/bin/env python3
"""
FastAPI server for Google Reviews Scraper.
Provides REST API endpoints to trigger and manage scraping jobs,
query reviews/places from SQLite, manage API keys, and view audit logs.
"""

import json
import logging
import asyncio
import os
import copy
import time
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Any, List, Optional, Literal, Tuple

import yaml
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Depends, Security, Request, APIRouter
from fastapi.params import Param
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, HttpUrl, Field
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from modules.config import load_config
from modules.google_places_service import (
    GooglePlacesConfigError,
    dedupe_places,
    dedupe_places_by_name_highest_ratings_total,
    fetch_places_textsearch,
    get_api_key as get_google_places_api_key,
    rank_and_select_places,
    validate_place,
)
from modules.job_manager import JobManager, JobStatus
from modules.progress import (
    business_identity,
    compute_progress_report,
    extract_query_place_id,
    resolve_businesses,
)

# --- Load config for API settings ---
_config = load_config()
_api_config = _config.get("api", {})

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(request: Request, key: Optional[str] = Security(_api_key_header)):
    """Authenticate via DB-managed API keys. Open access when no keys exist."""
    api_key_db = getattr(request.app.state, "api_key_db", None)

    # DB keys required when any active key exists
    if api_key_db and api_key_db.has_active_keys():
        if not key:
            raise HTTPException(status_code=401, detail="Missing API key")
        info = api_key_db.verify_key(key)
        if not info:
            raise HTTPException(status_code=401, detail="Invalid or revoked API key")
        request.state.api_key_info = info
        return

    # No keys configured — open access
    request.state.api_key_info = None


log = logging.getLogger("api_server")

# Global job manager instance
job_manager: Optional[JobManager] = None


def _load_env_exports(env_path: Path, override: bool = False) -> int:
    """Load KEY=VALUE lines (optionally prefixed with `export`) into os.environ."""
    if not env_path.exists():
        return 0

    loaded = 0
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if not override and (os.environ.get(key) or "").strip():
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]

        os.environ[key] = value
        loaded += 1

    return loaded


def _load_local_env_files() -> None:
    """Load project-local env exports so direct uvicorn runs match the dev wrapper."""
    root_dir = Path(__file__).resolve().parent
    for name in (".env", ".env.google_maps.cookies"):
        env_path = root_dir / name
        loaded = _load_env_exports(env_path)
        if loaded:
            log.info("Loaded %s env vars from %s", loaded, env_path.name)


def _scrape_concurrency_limit() -> int:
    """
    Default to 3 concurrent scrapers.

    Each scraper uses its own Chrome instance in incognito mode with no shared
    user-data-dir, providing process-level isolation. SQLite WAL mode with
    busy_timeout=30s handles concurrent writes safely. Override via env var
    SCRAPER_MAX_CONCURRENT_JOBS if needed.
    """
    raw = str(os.environ.get("SCRAPER_MAX_CONCURRENT_JOBS", "3")).strip()
    try:
        limit = int(raw)
    except ValueError:
        log.warning(
            "Invalid SCRAPER_MAX_CONCURRENT_JOBS=%r, falling back to isolated mode (1)",
            raw,
        )
        return 1
    return max(1, limit)


def _unwrap_param_value(value: Any) -> Any:
    """Return the default value when a FastAPI Param leaks into direct calls."""
    if isinstance(value, Param):
        return value.default
    return value


def _normalize_export_options(
    format_value: Any,
    include_deleted: Any,
    exclude_empty_text: Any,
    sheet_name: Any,
    columns: Any,
) -> Tuple[str, bool, bool, Optional[str], Optional[List[str]]]:
    normalized_format = str(_unwrap_param_value(format_value) or "xlsx").strip() or "xlsx"
    normalized_include_deleted = bool(_unwrap_param_value(include_deleted))
    normalized_exclude_empty_text = bool(_unwrap_param_value(exclude_empty_text))
    normalized_sheet_name = _unwrap_param_value(sheet_name)
    normalized_columns = _unwrap_param_value(columns)

    if normalized_sheet_name is not None:
        normalized_sheet_name = str(normalized_sheet_name).strip() or None

    if normalized_columns is None:
        col_list = None
    else:
        col_list = [
            col.strip()
            for col in str(normalized_columns).split(",")
            if col.strip()
        ] or None

    return (
        normalized_format,
        normalized_include_deleted,
        normalized_exclude_empty_text,
        normalized_sheet_name,
        col_list,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown"""
    global job_manager

    _load_local_env_files()

    # Startup — structured logging
    from modules.log_manager import setup_logging
    setup_logging(
        level=_config.get("log_level", "INFO"),
        log_dir=_config.get("log_dir", "logs"),
        log_file=_config.get("log_file", "scraper.log"),
    )
    log.info("Starting Google Reviews Scraper API Server")
    concurrency_limit = _scrape_concurrency_limit()
    job_manager = JobManager(max_concurrent_jobs=concurrency_limit)
    log.info("Job manager initialized (max_concurrent_jobs=%d)", concurrency_limit)

    db_path = _config.get("db_path", "reviews.db")

    # Initialize API key DB
    from modules.api_keys import ApiKeyDB
    app.state.api_key_db = ApiKeyDB(db_path)
    log.info("API key database initialized")

    # Initialize Review DB (read-only queries, safe with WAL mode)
    from modules.review_db import ReviewDB
    app.state.review_db = ReviewDB(db_path)
    log.info("Review database initialized")

    # Start auto-cleanup task
    asyncio.create_task(cleanup_jobs_periodically())

    yield

    # Shutdown
    log.info("Shutting down Google Reviews Scraper API Server")
    if hasattr(app.state, "review_db"):
        app.state.review_db.close()
    if hasattr(app.state, "api_key_db"):
        app.state.api_key_db.close()
    if job_manager:
        job_manager.shutdown()


# Initialize FastAPI app
app = FastAPI(
    title="Google Reviews Scraper API",
    description="REST API for triggering and managing Google Maps review scraping jobs",
    version="1.2.1",
    lifespan=lifespan
)


# --- Audit Middleware ---

class AuditMiddleware(BaseHTTPMiddleware):
    """Log every request to the API audit table."""

    async def dispatch(self, request: Request, call_next) -> Response:
        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        api_key_db = getattr(request.app.state, "api_key_db", None)
        if api_key_db is None:
            return response

        key_info = getattr(request.state, "api_key_info", None) if hasattr(request.state, "api_key_info") else None
        key_id = key_info["id"] if key_info else None
        key_name = key_info["name"] if key_info else None
        client_ip = request.client.host if request.client else None

        try:
            api_key_db.log_request(
                key_id=key_id,
                key_name=key_name,
                endpoint=request.url.path,
                method=request.method,
                client_ip=client_ip,
                status_code=response.status_code,
                response_time_ms=elapsed_ms,
            )
        except Exception:
            log.exception("Failed to write audit log entry")

        return response


app.add_middleware(AuditMiddleware)

# CORS — env var takes precedence, then config.yaml, then default "*".
_raw_origins = (
    os.environ.get("ALLOWED_ORIGINS", "")
    or _api_config.get("allowed_origins", "*")
)
_allowed_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=_raw_origins != "*",
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------

def get_review_db(request: Request):
    """Get ReviewDB from app state."""
    db = getattr(request.app.state, "review_db", None)
    if db is None:
        raise HTTPException(status_code=500, detail="Review database not initialized")
    return db


def get_api_key_db(request: Request):
    """Get ApiKeyDB from app state."""
    db = getattr(request.app.state, "api_key_db", None)
    if db is None:
        raise HTTPException(status_code=500, detail="API key database not initialized")
    return db


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

# --- Jobs ---
class ScrapeRequest(BaseModel):
    """Request model for starting a scrape job"""
    url: HttpUrl = Field(..., description="Google Maps URL to scrape")
    headless: Optional[bool] = Field(None, description="Run Chrome in headless mode")
    sort_by: Optional[str] = Field(None, description="Sort order: newest, highest, lowest, relevance")
    scrape_mode: Optional[str] = Field(None, description="Scrape mode: new_only, update, or full")
    stop_threshold: Optional[int] = Field(None, description="Consecutive matched batches before stopping")
    max_reviews: Optional[int] = Field(None, description="Max reviews to scrape (0 = unlimited)")
    max_scroll_attempts: Optional[int] = Field(None, description="Max scroll iterations")
    scroll_idle_limit: Optional[int] = Field(None, description="Max idle iterations with zero new cards")
    download_images: Optional[bool] = Field(None, description="Download images from reviews")
    use_s3: Optional[bool] = Field(None, description="Upload images to S3")
    custom_params: Optional[Dict[str, Any]] = Field(None, description="Custom parameters to add to each document")


class JobResponse(BaseModel):
    """Response model for job information"""
    job_id: str
    status: JobStatus
    url: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    reviews_count: Optional[int] = None
    images_count: Optional[int] = None
    progress: Optional[Dict[str, Any]] = None


class JobStatsResponse(BaseModel):
    """Response model for job statistics"""
    total_jobs: int
    by_status: Dict[str, int]
    running_jobs: int
    max_concurrent_jobs: int


# --- Places ---
class PlaceResponse(BaseModel):
    place_id: str
    place_name: Optional[str] = None
    original_url: str
    resolved_url: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    first_seen: str
    last_scraped: Optional[str] = None
    total_reviews: int = 0
    cached_total_reviews: int = 0
    reviews_exhausted: bool = False
    exhausted_at: Optional[str] = None
    validation_status: str = "unknown"
    validation_checked_at: Optional[str] = None
    validation_reason: Optional[str] = None


# --- Reviews ---
class ReviewResponse(BaseModel):
    review_id: str
    place_id: str
    author: Optional[str] = None
    rating: Optional[float] = None
    review_text: Optional[Any] = None
    review_date: Optional[str] = None
    raw_date: Optional[str] = None
    likes: int = 0
    user_images: Optional[Any] = None
    s3_images: Optional[Any] = None
    profile_url: Optional[str] = None
    profile_picture: Optional[str] = None
    s3_profile_picture: Optional[str] = None
    owner_responses: Optional[Any] = None
    created_date: str
    last_modified: str
    last_seen_session: Optional[int] = None
    last_changed_session: Optional[int] = None
    is_deleted: int = 0
    content_hash: Optional[str] = None
    engagement_hash: Optional[str] = None
    row_version: int = 1


class PaginatedReviewsResponse(BaseModel):
    place_id: str
    total: int
    limit: int
    offset: int
    reviews: List[ReviewResponse]


class ReviewHistoryEntry(BaseModel):
    history_id: int
    review_id: str
    place_id: str
    session_id: Optional[int] = None
    actor: str
    action: str
    changed_fields: Optional[Any] = None
    old_content_hash: Optional[str] = None
    new_content_hash: Optional[str] = None
    old_engagement_hash: Optional[str] = None
    new_engagement_hash: Optional[str] = None
    timestamp: str


# --- Audit ---
class AuditLogEntry(BaseModel):
    id: int
    timestamp: str
    key_id: Optional[int] = None
    key_name: Optional[str] = None
    endpoint: str
    method: str
    client_ip: Optional[str] = None
    status_code: Optional[int] = None
    response_time_ms: Optional[int] = None


# --- DB Stats ---
class PlaceStatRow(BaseModel):
    place_id: str
    place_name: Optional[str] = None
    total_reviews: int = 0
    cached_total_reviews: int = 0
    last_scraped: Optional[str] = None


class DbStatsResponse(BaseModel):
    places_count: int = 0
    reviews_count: int = 0
    scrape_sessions_count: int = 0
    review_history_count: int = 0
    sync_checkpoints_count: int = 0
    place_aliases_count: int = 0
    db_size_bytes: int = 0
    places: List[PlaceStatRow] = []


class ProgressTargetRow(BaseModel):
    company: str = ""
    url: str
    google_place_id: str = ""
    status: str
    review_count: int = 0
    cached_total_reviews: int = 0
    place_id: Optional[str] = None
    place_name: Optional[str] = None
    last_scraped: Optional[str] = None
    reviews_exhausted: bool = False
    validation_status: str = "unknown"
    validation_checked_at: Optional[str] = None
    validation_reason: Optional[str] = None
    meets_min_reviews: bool = False
    reviews_needed: int = 0


class ProgressResponse(BaseModel):
    targets_total: int = 0
    with_reviews: int = 0
    present_zero_reviews: int = 0
    missing_from_db: int = 0
    incomplete_total: int = 0
    completed_percent: float = 0.0
    min_reviews: int = 1
    meeting_min_reviews: int = 0
    under_min_reviews: int = 0
    exhausted_under_threshold_count: int = 0
    targets: List[ProgressTargetRow] = []


class LogTailEntry(BaseModel):
    ts: Optional[str] = None
    level: Optional[str] = None
    logger: Optional[str] = None
    msg: Optional[str] = None
    raw: Optional[str] = None


class ScrapeAllRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    min_reviews: int = Field(50, ge=0)
    headless: Optional[bool] = None
    scrape_mode: Optional[str] = None
    default_max_reviews: Optional[int] = Field(None, ge=1)
    only_below_threshold: bool = True


class ScrapeTargetRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    google_place_id: Optional[str] = None
    place_id: Optional[str] = None
    url: Optional[str] = None
    headless: Optional[bool] = None
    scrape_mode: Optional[str] = None
    max_reviews: Optional[int] = Field(None, ge=1)


class TargetMaxReviewsRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    google_place_id: str
    max_reviews: int = Field(..., ge=1)


class ResetExhaustedTargetsRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    min_reviews: int = Field(100, ge=0)
    google_place_id: Optional[str] = None
    place_id: Optional[str] = None


ExportFormat = Literal["json", "csv", "xlsx"]


class DataQualityConflict(BaseModel):
    review_id: str
    place_ids: List[str]
    place_names: List[str]
    place_count: int
    last_seen_sessions: Dict[str, Optional[int]]
    has_hash_placeholder: bool = False
    has_multiple_real_places: bool = False


class DataQualityConflictReport(BaseModel):
    total_conflicts: int = 0
    conflicts: List[DataQualityConflict] = []


class PlaceValidationResultRow(BaseModel):
    place_id: Optional[str] = None
    company: str = ""
    google_place_id: str
    status: str
    reason: str = ""
    api_name: Optional[str] = None
    api_address: Optional[str] = None
    business_status: Optional[str] = None
    checked_at: str


class ValidatePlacesRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    google_place_ids: List[str] = []
    place_ids: List[str] = []
    language: Optional[str] = None
    timeout_s: int = Field(30, ge=1, le=120)


class ValidatePlacesResponse(BaseModel):
    config_path: str
    validated_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    error_count: int = 0
    results: List[PlaceValidationResultRow] = []


class ArchiveInvalidPlaceRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    google_place_id: Optional[str] = None
    place_id: Optional[str] = None


class InvalidPlaceArchiveRow(BaseModel):
    archive_id: int
    archived_at: str
    config_path: Optional[str] = None
    place_id: Optional[str] = None
    google_place_id: Optional[str] = None
    place_name: Optional[str] = None
    original_url: Optional[str] = None
    resolved_url: Optional[str] = None
    live_total_reviews: int = 0
    cached_total_reviews: int = 0
    validation_status: Optional[str] = None
    validation_checked_at: Optional[str] = None
    validation_reason: Optional[str] = None


class ArchiveInvalidPlaceResponse(BaseModel):
    archived: InvalidPlaceArchiveRow
    deleted_counts: Dict[str, Any]


class DiscoverySearchRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    query: str
    limit: int = Field(50, ge=1, le=200)
    rank_by: Literal["relevance", "composite"] = "relevance"
    dedupe_mode: Literal["place_id", "name_highest_ratings_total"] = "place_id"
    min_rating: float = Field(0.0, ge=0.0)
    min_ratings_total: int = Field(0, ge=0)
    location: Optional[str] = None
    radius_m: Optional[int] = Field(None, ge=1)
    region: Optional[str] = None
    language: Optional[str] = None
    timeout_s: int = Field(30, ge=1, le=120)


class DiscoveryCandidateRow(BaseModel):
    candidate_id: int
    config_path: str
    query: str
    google_place_id: str
    name: Optional[str] = None
    formatted_address: Optional[str] = None
    rating: Optional[float] = None
    user_ratings_total: Optional[int] = None
    maps_url: str
    status: str
    duplicate_source: Optional[str] = None
    discovered_at: str
    updated_at: str


class DiscoverySearchResponse(BaseModel):
    config_path: str
    query: str
    candidate_count: int = 0
    staged_count: int = 0
    candidates: List[DiscoveryCandidateRow] = []


class CandidateSelectionRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    candidate_ids: List[int]


class ApproveDiscoveryCandidatesResponse(BaseModel):
    config_path: str
    approved_count: int = 0
    skipped_count: int = 0
    approved_google_place_ids: List[str] = []
    candidates: List[DiscoveryCandidateRow] = []


class CandidateMutationResponse(BaseModel):
    config_path: str
    updated_count: int = 0
    candidates: List[DiscoveryCandidateRow] = []


class ScrapeTargetsRequest(BaseModel):
    config_path: str = "batch/config.top50.yaml"
    google_place_ids: List[str]
    headless: Optional[bool] = None
    scrape_mode: Optional[str] = None
    max_reviews: Optional[int] = Field(None, ge=1)


class ScrapeTargetsResponse(BaseModel):
    config_path: str
    requested_count: int = 0
    created_count: int = 0
    queued_count: int = 0
    skipped_count: int = 0
    created_jobs: List[Dict[str, Any]] = []
    skipped_targets: List[Dict[str, Any]] = []


class RebuildPlaceTotalsRequest(BaseModel):
    place_ids: List[str] = []


class RebuildPlaceTotalsResponse(BaseModel):
    checked_count: int = 0
    updated_count: int = 0
    updated_places: List[Dict[str, Any]] = []


class DataHealthSummaryResponse(BaseModel):
    config_path: str
    min_reviews: int = 100
    google_places_api_configured: bool = False
    active_config_targets: int = 0
    db_places_count: int = 0
    stale_total_count: int = 0
    conflict_group_count: int = 0
    exhausted_under_threshold_count: int = 0
    staged_candidate_count: int = 0
    invalid_archive_count: int = 0
    stale_total_examples: List[PlaceStatRow] = []
    recent_invalid_places: List[InvalidPlaceArchiveRow] = []


# ---------------------------------------------------------------------------
# Background task for periodic cleanup
# ---------------------------------------------------------------------------

async def cleanup_jobs_periodically():
    """Periodically clean up old jobs"""
    while True:
        await asyncio.sleep(3600)  # Run every hour
        if job_manager:
            job_manager.cleanup_old_jobs(max_age_hours=24)


# ---------------------------------------------------------------------------
# Helper to strip internal keys from deserialized reviews
# ---------------------------------------------------------------------------

def _clean_review(row: Dict[str, Any]) -> Dict[str, Any]:
    """Strip _-prefixed internal keys added by _deserialize_review()."""
    return {k: v for k, v in row.items() if not k.startswith("_")}


def _read_structured_log_tail(log_path: Path, limit: int, level: Optional[str]) -> List[Dict[str, Any]]:
    """Read structured JSON log lines from end of file with optional level filter."""
    if not log_path.exists():
        return []

    with open(log_path, "r", encoding="utf-8") as handle:
        lines = handle.readlines()

    requested_level = (level or "").upper().strip()
    entries: List[Dict[str, Any]] = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        entry: Dict[str, Any]
        try:
            parsed = json.loads(line)
            entry = parsed if isinstance(parsed, dict) else {"raw": line}
        except json.JSONDecodeError:
            entry = {"raw": line}

        if requested_level:
            if str(entry.get("level", "")).upper() != requested_level:
                continue

        entries.append(
            {
                "ts": entry.get("ts"),
                "level": entry.get("level"),
                "logger": entry.get("logger"),
                "msg": entry.get("msg"),
                "raw": entry.get("raw"),
            }
        )
        if len(entries) >= limit:
            break

    entries.reverse()
    return entries


def _resolve_config_path(config_path: str) -> Path:
    """Resolve config path relative to current working directory."""
    path = Path(config_path).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _load_config_raw(cfg_path: Path) -> Dict[str, Any]:
    """Load raw YAML config for persistence-safe editing."""
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Config file root must be a mapping")
    return data


def _save_config_raw(cfg_path: Path, raw_cfg: Dict[str, Any]) -> None:
    tmp_path = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f"{cfg_path.name}.",
            suffix=".tmp",
            dir=str(cfg_path.parent),
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            yaml.safe_dump(raw_cfg, handle, sort_keys=False, allow_unicode=False)
        os.replace(tmp_name, cfg_path)
    except Exception as exc:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Failed to persist config update: {exc}") from exc


def _business_google_place_id(business: Dict[str, Any]) -> str:
    return str(business_identity(business).get("google_place_id") or "").strip()


def _business_company(business: Dict[str, Any]) -> str:
    return str(business_identity(business).get("company") or "").strip()


def _build_job_overrides(
    merged_config: Dict[str, Any],
    business: Dict[str, Any],
    request_overrides: Dict[str, Any],
    default_max_reviews: Optional[int] = None,
) -> Dict[str, Any]:
    """Build job config overrides from global config + business + request overrides."""
    overrides: Dict[str, Any] = {}

    # Start from config defaults used for top50 batches.
    for key, value in merged_config.items():
        if key in ("businesses", "urls", "url"):
            continue
        overrides[key] = copy.deepcopy(value)

    # Merge business-specific overrides (excluding URL itself).
    for key, value in business.items():
        if key == "url":
            continue
        if isinstance(value, dict) and key in overrides and isinstance(overrides[key], dict):
            nested = copy.deepcopy(overrides[key])
            nested.update(value)
            overrides[key] = nested
        else:
            overrides[key] = copy.deepcopy(value)

    if default_max_reviews is not None:
        requested_max = int(default_max_reviews)
        current_max = overrides.get("max_reviews")
        if current_max is None:
            overrides["max_reviews"] = requested_max
        else:
            try:
                overrides["max_reviews"] = max(int(current_max), requested_max)
            except (TypeError, ValueError):
                overrides["max_reviews"] = requested_max

    # Finally apply explicit request overrides.
    for key, value in request_overrides.items():
        if value is not None:
            overrides[key] = value

    return overrides


def _find_business_by_google_place_id(config: Dict[str, Any], google_place_id: str) -> tuple[int, Dict[str, Any]]:
    businesses = config.get("businesses", [])
    for idx, item in enumerate(businesses):
        if not isinstance(item, dict):
            continue
        qpid = _business_google_place_id(item)
        if qpid == google_place_id:
            return idx, item
    raise HTTPException(status_code=404, detail=f"Target with google_place_id '{google_place_id}' not found in config")


def _list_active_jobs_for_business(business: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not job_manager:
        return []

    target_url = str(business.get("url", "") or "").strip()
    target_google_place_id = _business_google_place_id(business)
    active: List[Dict[str, Any]] = []

    for job in job_manager.list_jobs(limit=1000):
        if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
            continue
        job_qpid = extract_query_place_id(job.url)
        if target_google_place_id and job_qpid == target_google_place_id:
            active.append(job.to_dict())
            continue
        if target_url and str(job.url).strip() == target_url:
            active.append(job.to_dict())
    return active


def _resolve_config_target_context(
    *,
    config_path: str,
    review_db,
    google_place_id: Optional[str] = None,
    place_id: Optional[str] = None,
) -> Tuple[Path, Dict[str, Any], Dict[str, Any], int, Dict[str, Any], Dict[str, Any]]:
    identifier_count = sum(1 for value in (google_place_id, place_id) if value)
    if identifier_count != 1:
        raise HTTPException(status_code=400, detail="Provide exactly one identifier: google_place_id or place_id")

    cfg_path = _resolve_config_path(config_path)
    raw_cfg = _load_config_raw(cfg_path)
    businesses_raw = raw_cfg.get("businesses")
    if not isinstance(businesses_raw, list):
        raise HTTPException(status_code=400, detail="Config has no editable 'businesses' list")

    merged_cfg = load_config(cfg_path)
    businesses = resolve_businesses(merged_cfg)
    report = compute_progress_report(businesses, review_db, min_reviews=100)

    if len(businesses) != len(businesses_raw):
        raise HTTPException(status_code=400, detail="Config business resolution is not editable for this operation")

    for idx, (business, target) in enumerate(zip(businesses_raw, report["targets"])):
        if not isinstance(business, dict):
            continue
        qpid = _business_google_place_id(business)
        if google_place_id and qpid == google_place_id:
            return cfg_path, raw_cfg, merged_cfg, idx, business, target
        if place_id and str(target.get("place_id") or "").strip() == place_id:
            return cfg_path, raw_cfg, merged_cfg, idx, business, target

    missing_value = google_place_id or place_id or ""
    raise HTTPException(status_code=404, detail=f"Target '{missing_value}' not found in config")


def _resolve_target_business(
    request: ScrapeTargetRequest,
    merged_config: Dict[str, Any],
    review_db,
) -> Dict[str, Any]:
    businesses = resolve_businesses(merged_config)
    if not businesses:
        raise HTTPException(status_code=400, detail="No businesses/urls found in config")

    identifier_count = sum(1 for x in (request.google_place_id, request.place_id, request.url) if x)
    if identifier_count == 0:
        raise HTTPException(status_code=400, detail="Provide one of: google_place_id, place_id, or url")
    if identifier_count > 1:
        raise HTTPException(status_code=400, detail="Provide only one identifier: google_place_id, place_id, or url")

    if request.google_place_id:
        for biz in businesses:
            qpid = _business_google_place_id(biz)
            if qpid == request.google_place_id:
                return biz
        raise HTTPException(status_code=404, detail=f"Target '{request.google_place_id}' not found in config")

    if request.url:
        for biz in businesses:
            if str(biz.get("url", "")).strip() == str(request.url).strip():
                return biz
        raise HTTPException(status_code=404, detail="Target URL not found in config")

    # place_id resolution path: map config targets to DB place_id via progress report.
    report = compute_progress_report(businesses, review_db)
    for biz, target in zip(businesses, report["targets"]):
        if target.get("place_id") == request.place_id:
            return biz
    raise HTTPException(status_code=404, detail=f"Target place_id '{request.place_id}' not found in config")


# ===========================================================================
# Routers
# ===========================================================================

# --- System Router ---
system_router = APIRouter(tags=["System"])


@system_router.get("/", summary="API Health Check")
async def root():
    """Health check endpoint"""
    return {
        "message": "Google Reviews Scraper API is running",
        "status": "healthy",
        "version": "1.2.1"
    }


@system_router.get("/db-stats", response_model=DbStatsResponse, summary="Database Statistics",
                    dependencies=[Depends(require_api_key)])
async def get_db_stats(review_db=Depends(get_review_db)):
    """Get ReviewDB statistics (places, reviews, sessions, db size)."""
    stats = review_db.get_stats()
    place_rows = [
        PlaceStatRow(
            place_id=p["place_id"],
            place_name=p.get("place_name"),
            total_reviews=p.get("total_reviews", 0),
            cached_total_reviews=p.get("cached_total_reviews", 0),
            last_scraped=p.get("last_scraped"),
        )
        for p in stats.get("places", [])
    ]
    return DbStatsResponse(
        places_count=stats.get("places_count", 0),
        reviews_count=stats.get("reviews_count", 0),
        scrape_sessions_count=stats.get("scrape_sessions_count", 0),
        review_history_count=stats.get("review_history_count", 0),
        sync_checkpoints_count=stats.get("sync_checkpoints_count", 0),
        place_aliases_count=stats.get("place_aliases_count", 0),
        db_size_bytes=stats.get("db_size_bytes", 0),
        places=place_rows,
    )


@system_router.get("/progress", response_model=ProgressResponse, summary="Config-vs-DB Progress",
                   dependencies=[Depends(require_api_key)])
async def get_progress(
    config_path: str = Query(
        "batch/config.top50.yaml",
        description="Path to batch config file with businesses/urls",
    ),
    min_reviews: int = Query(1, ge=0, description="Threshold for dashboard completion status"),
    review_db=Depends(get_review_db),
):
    """Get completion progress for configured targets against current DB."""
    cfg_path = _resolve_config_path(config_path)
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")

    config = load_config(cfg_path)
    businesses = resolve_businesses(config)
    if not businesses:
        raise HTTPException(status_code=400, detail="No businesses/urls found in config")

    report = compute_progress_report(businesses, review_db, min_reviews=min_reviews)
    return ProgressResponse(**report)


@system_router.get("/system/log-tail", response_model=List[LogTailEntry], summary="Structured Log Tail",
                   dependencies=[Depends(require_api_key)])
async def get_log_tail(
    level: Optional[str] = Query(None, description="Filter by level (DEBUG/INFO/WARNING/ERROR/CRITICAL)"),
    limit: int = Query(100, ge=1, le=1000, description="Max log entries to return"),
):
    """Tail structured scraper logs for dashboard diagnostics."""
    log_dir = _config.get("log_dir", "logs")
    log_file = _config.get("log_file", "scraper.log")
    log_path = Path(log_dir) / log_file
    rows = _read_structured_log_tail(log_path, limit=limit, level=level)
    return [LogTailEntry(**r) for r in rows]


@system_router.get(
    "/system/data-quality/conflicts",
    response_model=DataQualityConflictReport,
    summary="Cross-Place Review ID Conflicts",
    dependencies=[Depends(require_api_key)],
)
async def get_data_quality_conflicts(
    include_hash_only: bool = Query(
        False,
        description=(
            "Include lower-risk groups where one real place shares review IDs only "
            "with hash:* placeholder records."
        ),
    ),
    review_db=Depends(get_review_db),
):
    """Report active review_id collisions across places."""
    conflicts = review_db.get_cross_place_conflicts(include_hash_only=include_hash_only)
    return DataQualityConflictReport(
        total_conflicts=len(conflicts),
        conflicts=[DataQualityConflict(**c) for c in conflicts],
    )


@system_router.get(
    "/system/data-health/summary",
    response_model=DataHealthSummaryResponse,
    summary="Dashboard Data Health Summary",
    dependencies=[Depends(require_api_key)],
)
async def get_data_health_summary(
    config_path: str = Query(
        "batch/config.top50.yaml",
        description="Path to batch config file with businesses/urls",
    ),
    min_reviews: int = Query(100, ge=0, description="Threshold for under/exhausted counts"),
    review_db=Depends(get_review_db),
):
    cfg_path = _resolve_config_path(config_path)
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")

    config = load_config(cfg_path)
    businesses = resolve_businesses(config)
    report = compute_progress_report(businesses, review_db, min_reviews=min_reviews)
    stats = review_db.get_stats()
    stale_rows = review_db.list_stale_place_totals(limit=5)
    recent_archives = review_db.list_invalid_place_archives(limit=5)
    archive_row = review_db.backend.fetchone("SELECT COUNT(*) AS cnt FROM invalid_place_archive")
    try:
        get_google_places_api_key()
        google_places_api_configured = True
    except GooglePlacesConfigError:
        google_places_api_configured = False

    return DataHealthSummaryResponse(
        config_path=str(cfg_path),
        min_reviews=int(min_reviews),
        google_places_api_configured=google_places_api_configured,
        active_config_targets=len(businesses),
        db_places_count=int(stats.get("places_count", 0) or 0),
        stale_total_count=review_db.count_stale_place_totals(),
        conflict_group_count=len(review_db.get_cross_place_conflicts(include_hash_only=False)),
        exhausted_under_threshold_count=int(report.get("exhausted_under_threshold_count", 0) or 0),
        staged_candidate_count=review_db.count_discovery_candidates(
            config_path=str(cfg_path),
            status="staged",
        ),
        invalid_archive_count=int(archive_row["cnt"] or 0) if archive_row else 0,
        stale_total_examples=[
            PlaceStatRow(
                place_id=row["place_id"],
                place_name=row.get("place_name"),
                total_reviews=row.get("total_reviews", 0),
                cached_total_reviews=row.get("cached_total_reviews", 0),
                last_scraped=None,
            )
            for row in stale_rows
        ],
        recent_invalid_places=[InvalidPlaceArchiveRow(**row) for row in recent_archives],
    )


@system_router.post("/cleanup", summary="Manual Job Cleanup",
                     dependencies=[Depends(require_api_key)])
async def cleanup_jobs(max_age_hours: int = Query(24, description="Maximum age in hours", ge=1)):
    """Manually trigger cleanup of old completed/failed jobs"""
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    job_manager.cleanup_old_jobs(max_age_hours=max_age_hours)
    return {"message": f"Cleaned up jobs older than {max_age_hours} hours"}


# --- Jobs Router ---
jobs_router = APIRouter(tags=["Jobs"], dependencies=[Depends(require_api_key)])


@jobs_router.post("/scrape", response_model=Dict[str, str], summary="Start Scraping Job")
async def start_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Start a new scraping job in the background.

    Returns the job ID that can be used to check status.
    """
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    config_overrides = {}
    for field, value in request.dict().items():
        if value is not None and field != "url":
            config_overrides[field] = value

    url = str(request.url)

    try:
        job_id = job_manager.create_job(url, config_overrides)
        started = job_manager.start_job(job_id)
        log.info(f"Created scraping job {job_id} for URL: {url}")

        return {
            "job_id": job_id,
            "status": "started" if started else "queued",
            "message": f"Scraping job {'started' if started else 'queued'} successfully"
        }

    except Exception as e:
        log.error(f"Error creating scraping job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create scraping job: {str(e)}")


@jobs_router.get("/jobs/{job_id}", response_model=JobResponse, summary="Get Job Status")
async def get_job(job_id: str):
    """Get detailed information about a specific job"""
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse(**job.to_dict())


@jobs_router.get("/jobs", response_model=List[JobResponse], summary="List Jobs")
async def list_jobs(
    status: Optional[JobStatus] = Query(None, description="Filter by job status"),
    limit: int = Query(100, description="Maximum number of jobs to return", ge=1, le=1000)
):
    """List all jobs, optionally filtered by status"""
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    jobs = job_manager.list_jobs(status=status, limit=limit)
    return [JobResponse(**job.to_dict()) for job in jobs]


@jobs_router.post("/jobs/{job_id}/start", summary="Start Pending Job")
async def start_job(job_id: str):
    """Start a pending job manually"""
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    started = job_manager.start_job(job_id)
    if not started:
        job = job_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job.status != JobStatus.PENDING:
            raise HTTPException(status_code=400, detail=f"Job is not pending (current status: {job.status})")

        raise HTTPException(status_code=429, detail="Maximum concurrent jobs reached")

    return {"message": "Job started successfully"}


@jobs_router.post("/jobs/{job_id}/cancel", summary="Cancel Job")
async def cancel_job(job_id: str):
    """Cancel a pending or running job"""
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    cancelled = job_manager.cancel_job(job_id)
    if not cancelled:
        job = job_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        raise HTTPException(status_code=400, detail="Job cannot be cancelled (already completed, failed, or cancelled)")

    return {"message": "Job cancelled successfully"}


@jobs_router.delete("/jobs/{job_id}", summary="Delete Job")
async def delete_job(job_id: str):
    """Delete a job from the system (only terminal-state jobs)"""
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    deleted = job_manager.delete_job(job_id)
    if not deleted:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot delete job in '{job.status.value}' state. Cancel it first.",
        )

    return {"message": "Job deleted successfully"}


# --- Ops Router ---
ops_router = APIRouter(tags=["Ops"], dependencies=[Depends(require_api_key)])


@ops_router.post(
    "/ops/places/validate",
    response_model=ValidatePlacesResponse,
    summary="Validate Active Places via Google Places API",
)
async def ops_validate_places(request: ValidatePlacesRequest, review_db=Depends(get_review_db)):
    cfg_path = _resolve_config_path(request.config_path)
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")

    config = load_config(cfg_path)
    businesses = resolve_businesses(config)
    if not businesses:
        raise HTTPException(status_code=400, detail="No businesses/urls found in config")

    report = compute_progress_report(businesses, review_db, min_reviews=100)
    selected_google_place_ids = {value.strip() for value in request.google_place_ids if value.strip()}
    selected_place_ids = {value.strip() for value in request.place_ids if value.strip()}

    selected: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for business, target in zip(businesses, report["targets"]):
        qpid = _business_google_place_id(business)
        pid = str(target.get("place_id") or "").strip()
        if selected_google_place_ids or selected_place_ids:
            if qpid not in selected_google_place_ids and pid not in selected_place_ids:
                continue
        selected.append((business, target))

    if (selected_google_place_ids or selected_place_ids) and not selected:
        raise HTTPException(status_code=404, detail="No matching targets found to validate")

    results: List[PlaceValidationResultRow] = []
    valid_count = 0
    invalid_count = 0
    error_count = 0

    for business, target in selected:
        google_place_id = _business_google_place_id(business)
        company = _business_company(business) or str(target.get("place_name") or "").strip()
        place_id = str(target.get("place_id") or "").strip() or None

        if not google_place_id:
            validation = {
                "google_place_id": "",
                "status": "error",
                "reason": "Target is missing google_place_id",
                "api_name": None,
                "api_address": None,
                "business_status": None,
                "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        else:
            validation = validate_place(
                google_place_id=google_place_id,
                expected_name=company,
                language=request.language,
                timeout_s=int(request.timeout_s),
            )

        recorded = review_db.record_place_validation(
            place_id=place_id,
            google_place_id=str(validation.get("google_place_id") or google_place_id),
            config_path=str(cfg_path),
            expected_name=company,
            status=str(validation.get("status") or "error"),
            reason=str(validation.get("reason") or ""),
            api_name=validation.get("api_name"),
            api_address=validation.get("api_address"),
            business_status=validation.get("business_status"),
            checked_at=str(validation.get("checked_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            response_payload=validation,
        )
        result_row = PlaceValidationResultRow(
            place_id=place_id,
            company=company,
            google_place_id=str(recorded.get("google_place_id") or ""),
            status=str(recorded.get("status") or "error"),
            reason=str(recorded.get("reason") or ""),
            api_name=recorded.get("api_name"),
            api_address=recorded.get("api_address"),
            business_status=recorded.get("business_status"),
            checked_at=str(recorded.get("checked_at") or ""),
        )
        results.append(result_row)
        if result_row.status == "valid":
            valid_count += 1
        elif result_row.status.startswith("invalid_"):
            invalid_count += 1
        else:
            error_count += 1

    return ValidatePlacesResponse(
        config_path=str(cfg_path),
        validated_count=len(results),
        valid_count=valid_count,
        invalid_count=invalid_count,
        error_count=error_count,
        results=results,
    )


@ops_router.post(
    "/ops/places/archive-invalid",
    response_model=ArchiveInvalidPlaceResponse,
    summary="Archive and Remove a Confirmed Invalid Place",
)
async def ops_archive_invalid_place(
    request: ArchiveInvalidPlaceRequest,
    review_db=Depends(get_review_db),
):
    cfg_path, raw_cfg, _merged_cfg, idx, business, target = _resolve_config_target_context(
        config_path=request.config_path,
        review_db=review_db,
        google_place_id=request.google_place_id,
        place_id=request.place_id,
    )

    google_place_id = _business_google_place_id(business)
    place_id = str(target.get("place_id") or "").strip()
    if not place_id:
        raise HTTPException(status_code=400, detail="Selected target is not present in the database")

    place = review_db.get_place(place_id)
    if not place:
        raise HTTPException(status_code=404, detail=f"Place not found: {place_id}")

    allowed_statuses = {"invalid_not_found", "invalid_closed", "invalid_mismatch"}
    validation_row = review_db.get_latest_place_validation(place_id=place_id) or {
        "status": place.get("validation_status"),
        "reason": place.get("validation_reason"),
        "checked_at": place.get("validation_checked_at"),
        "api_name": None,
        "api_address": None,
        "business_status": None,
    }
    validation_status = str(validation_row.get("status") or place.get("validation_status") or "unknown")
    if validation_status not in allowed_statuses:
        raise HTTPException(
            status_code=400,
            detail="Place can only be archived after a Google Places API invalidation result",
        )

    active_jobs = _list_active_jobs_for_business(business)
    if active_jobs:
        raise HTTPException(status_code=409, detail="Cannot archive a place with active scrape jobs")

    raw_businesses = raw_cfg.get("businesses")
    if not isinstance(raw_businesses, list):
        raise HTTPException(status_code=400, detail="Config has no editable 'businesses' list")
    config_entry = copy.deepcopy(raw_businesses[idx])
    raw_businesses.pop(idx)
    _save_config_raw(cfg_path, raw_cfg)

    deleted_counts = review_db.clear_place(place_id)
    archived = review_db.archive_invalid_place_record(
        config_path=str(cfg_path),
        place=place,
        google_place_id=google_place_id,
        validation_row=validation_row,
        config_entry=config_entry,
        deleted_counts=deleted_counts,
    )
    return ArchiveInvalidPlaceResponse(
        archived=InvalidPlaceArchiveRow(**archived),
        deleted_counts=deleted_counts,
    )


@ops_router.get(
    "/ops/places/invalid-archive",
    response_model=List[InvalidPlaceArchiveRow],
    summary="List Archived Invalid Places",
)
async def ops_list_invalid_place_archive(
    limit: int = Query(20, ge=1, le=200),
    review_db=Depends(get_review_db),
):
    rows = review_db.list_invalid_place_archives(limit=limit)
    return [InvalidPlaceArchiveRow(**row) for row in rows]


@ops_router.post(
    "/ops/discovery/search",
    response_model=DiscoverySearchResponse,
    summary="Stage Discovery Candidates from Google Places Text Search",
)
async def ops_discovery_search(request: DiscoverySearchRequest, review_db=Depends(get_review_db)):
    cfg_path = _resolve_config_path(request.config_path)
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")
    if request.radius_m is not None and not (request.location or "").strip():
        raise HTTPException(status_code=400, detail="radius_m requires location")

    try:
        api_key = get_google_places_api_key()
    except GooglePlacesConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    query = request.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    hits = fetch_places_textsearch(
        api_key=api_key,
        query=query,
        location=(request.location or "").strip() or None,
        radius_m=request.radius_m,
        region=(request.region or "").strip() or None,
        language=(request.language or "").strip() or None,
        limit=int(max(1, request.limit)),
        timeout_s=int(request.timeout_s),
    )
    deduped_by_id = dedupe_places(hits)
    if request.dedupe_mode == "name_highest_ratings_total":
        deduped_hits = dedupe_places_by_name_highest_ratings_total(deduped_by_id)
    else:
        deduped_hits = deduped_by_id

    selected_hits = rank_and_select_places(
        deduped_hits,
        rank_by=request.rank_by,
        min_rating=float(request.min_rating),
        min_ratings_total=int(request.min_ratings_total),
        limit=int(request.limit),
    )

    config = load_config(cfg_path)
    config_targets = resolve_businesses(config)
    config_google_place_ids = {
        _business_google_place_id(target)
        for target in config_targets
        if _business_google_place_id(target)
    }
    db_google_place_ids = {
        extract_query_place_id(str(row.get("original_url") or ""))
        for row in review_db.list_places()
        if extract_query_place_id(str(row.get("original_url") or ""))
    }

    candidates = []
    for hit in selected_hits:
        status = "staged"
        duplicate_source = None
        if hit.place_id in config_google_place_ids:
            status = "duplicate_config"
            duplicate_source = "config"
        elif hit.place_id in db_google_place_ids:
            status = "duplicate_db"
            duplicate_source = "db"
        candidates.append(
            {
                "google_place_id": hit.place_id,
                "name": hit.name,
                "formatted_address": hit.formatted_address,
                "rating": hit.rating,
                "user_ratings_total": hit.user_ratings_total,
                "maps_url": hit.to_maps_url(),
                "status": status,
                "duplicate_source": duplicate_source,
                "source_payload": {
                    "name": hit.name,
                    "formatted_address": hit.formatted_address,
                    "place_id": hit.place_id,
                    "rating": hit.rating,
                    "user_ratings_total": hit.user_ratings_total,
                },
            }
        )

    rows = review_db.upsert_discovery_candidates(
        config_path=str(cfg_path),
        query=query,
        candidates=candidates,
    )
    return DiscoverySearchResponse(
        config_path=str(cfg_path),
        query=query,
        candidate_count=len(rows),
        staged_count=sum(1 for row in rows if str(row.get("status") or "") == "staged"),
        candidates=[DiscoveryCandidateRow(**row) for row in rows],
    )


@ops_router.get(
    "/ops/discovery/candidates",
    response_model=List[DiscoveryCandidateRow],
    summary="List Discovery Candidates",
)
async def ops_list_discovery_candidates(
    config_path: str = Query("batch/config.top50.yaml"),
    status: Optional[str] = Query(None, description="Optional comma-separated status filter"),
    limit: int = Query(200, ge=1, le=500),
    review_db=Depends(get_review_db),
):
    statuses = [item.strip() for item in (status or "").split(",") if item.strip()]
    rows = review_db.list_discovery_candidates(
        config_path=str(_resolve_config_path(config_path)),
        statuses=statuses or None,
        limit=limit,
    )
    return [DiscoveryCandidateRow(**row) for row in rows]


@ops_router.post(
    "/ops/discovery/approve",
    response_model=ApproveDiscoveryCandidatesResponse,
    summary="Approve Discovery Candidates into Active Config",
)
async def ops_approve_discovery_candidates(
    request: CandidateSelectionRequest,
    review_db=Depends(get_review_db),
):
    if not request.candidate_ids:
        raise HTTPException(status_code=400, detail="candidate_ids is required")

    cfg_path = _resolve_config_path(request.config_path)
    raw_cfg = _load_config_raw(cfg_path)
    raw_businesses = raw_cfg.setdefault("businesses", [])
    if not isinstance(raw_businesses, list):
        raise HTTPException(status_code=400, detail="Config has no editable 'businesses' list")

    rows = review_db.list_discovery_candidates(
        config_path=str(cfg_path),
        candidate_ids=request.candidate_ids,
        limit=max(1, len(request.candidate_ids)),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No matching discovery candidates found")

    by_candidate_id = {int(row["candidate_id"]): row for row in rows}
    existing_google_place_ids = {
        _business_google_place_id(business)
        for business in raw_businesses
        if isinstance(business, dict) and _business_google_place_id(business)
    }
    approved_ids: List[int] = []
    skipped_ids: List[int] = []
    approved_google_place_ids: List[str] = []

    for candidate_id in request.candidate_ids:
        row = by_candidate_id.get(int(candidate_id))
        if not row:
            continue
        google_place_id = str(row.get("google_place_id") or "").strip()
        if not google_place_id or google_place_id in existing_google_place_ids:
            skipped_ids.append(int(candidate_id))
            continue

        raw_businesses.append(
            {
                "url": row["maps_url"],
                "custom_params": {
                    "company": row.get("name") or "",
                    "address": row.get("formatted_address") or "",
                    "source": "Google Maps",
                    "google_place_id": google_place_id,
                },
            }
        )
        existing_google_place_ids.add(google_place_id)
        approved_ids.append(int(candidate_id))
        approved_google_place_ids.append(google_place_id)

    if approved_ids:
        _save_config_raw(cfg_path, raw_cfg)
        review_db.update_discovery_candidate_status(approved_ids, "approved")

    if skipped_ids:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for candidate_id in skipped_ids:
            review_db.backend.execute(
                "UPDATE discovery_candidates SET updated_at = ?, status = ?, duplicate_source = ? "
                "WHERE candidate_id = ?",
                (now, "duplicate_config", "config", int(candidate_id)),
            )
        review_db.backend.commit()

    updated_rows = review_db.list_discovery_candidates(
        config_path=str(cfg_path),
        candidate_ids=request.candidate_ids,
        limit=max(1, len(request.candidate_ids)),
    )
    return ApproveDiscoveryCandidatesResponse(
        config_path=str(cfg_path),
        approved_count=len(approved_ids),
        skipped_count=len(request.candidate_ids) - len(approved_ids),
        approved_google_place_ids=approved_google_place_ids,
        candidates=[DiscoveryCandidateRow(**row) for row in updated_rows],
    )


@ops_router.post(
    "/ops/discovery/reject",
    response_model=CandidateMutationResponse,
    summary="Reject Discovery Candidates",
)
async def ops_reject_discovery_candidates(
    request: CandidateSelectionRequest,
    review_db=Depends(get_review_db),
):
    if not request.candidate_ids:
        raise HTTPException(status_code=400, detail="candidate_ids is required")
    cfg_path = _resolve_config_path(request.config_path)
    updated_count = review_db.update_discovery_candidate_status(request.candidate_ids, "rejected")
    rows = review_db.list_discovery_candidates(
        config_path=str(cfg_path),
        candidate_ids=request.candidate_ids,
        limit=max(1, len(request.candidate_ids)),
    )
    return CandidateMutationResponse(
        config_path=str(cfg_path),
        updated_count=updated_count,
        candidates=[DiscoveryCandidateRow(**row) for row in rows],
    )


@ops_router.post(
    "/ops/scrape-targets",
    response_model=ScrapeTargetsResponse,
    summary="Queue Scrape Jobs for Selected Config Targets",
)
async def ops_scrape_targets(request: ScrapeTargetsRequest):
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")
    if not request.google_place_ids:
        raise HTTPException(status_code=400, detail="google_place_ids is required")

    cfg_path = _resolve_config_path(request.config_path)
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")

    merged_config = load_config(cfg_path)
    businesses = resolve_businesses(merged_config)
    by_google_place_id = {
        _business_google_place_id(business): business
        for business in businesses
        if _business_google_place_id(business)
    }

    created_jobs = []
    queued_jobs = []
    skipped_targets = []
    request_overrides = {
        "headless": request.headless,
        "scrape_mode": request.scrape_mode,
        "max_reviews": request.max_reviews,
    }

    requested_ids: List[str] = []
    seen: set[str] = set()
    for value in request.google_place_ids:
        google_place_id = str(value or "").strip()
        if not google_place_id or google_place_id in seen:
            continue
        seen.add(google_place_id)
        requested_ids.append(google_place_id)

    for google_place_id in requested_ids:
        business = by_google_place_id.get(google_place_id)
        if not business:
            skipped_targets.append(
                {
                    "google_place_id": google_place_id,
                    "reason": "not_found_in_config",
                }
            )
            continue

        url = str(business.get("url", "")).strip()
        if not url:
            skipped_targets.append(
                {
                    "google_place_id": google_place_id,
                    "reason": "missing_url",
                }
            )
            continue

        overrides = _build_job_overrides(merged_config, business, request_overrides)
        job_id = job_manager.create_job(url, overrides)
        started = job_manager.start_job(job_id)
        entry = {
            "job_id": job_id,
            "started": started,
            "url": url,
            "company": _business_company(business),
            "google_place_id": google_place_id,
        }
        created_jobs.append(entry)
        if not started:
            queued_jobs.append(entry)

    return ScrapeTargetsResponse(
        config_path=str(cfg_path),
        requested_count=len(requested_ids),
        created_count=len(created_jobs),
        queued_count=len(queued_jobs),
        skipped_count=len(skipped_targets),
        created_jobs=created_jobs,
        skipped_targets=skipped_targets,
    )


@ops_router.post(
    "/ops/maintenance/rebuild-place-totals",
    response_model=RebuildPlaceTotalsResponse,
    summary="Rebuild Cached Place Review Totals",
)
async def ops_rebuild_place_totals(
    request: RebuildPlaceTotalsRequest,
    review_db=Depends(get_review_db),
):
    result = review_db.rebuild_place_total_reviews(request.place_ids or None)
    return RebuildPlaceTotalsResponse(**result)


@ops_router.post("/ops/scrape-all", summary="Queue Scrape Jobs for Config Targets")
async def ops_scrape_all(request: ScrapeAllRequest, review_db=Depends(get_review_db)):
    """Queue background scrape jobs for configured businesses."""
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    cfg_path = _resolve_config_path(request.config_path)
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")

    merged_config = load_config(cfg_path)
    businesses = resolve_businesses(merged_config)
    if not businesses:
        raise HTTPException(status_code=400, detail="No businesses/urls found in config")

    report = compute_progress_report(businesses, review_db, min_reviews=request.min_reviews)

    selected: List[tuple[Dict[str, Any], Dict[str, Any]]] = list(zip(businesses, report["targets"]))
    if request.only_below_threshold:
        selected = [
            (biz, target)
            for biz, target in selected
            if (not bool(target.get("meets_min_reviews", False)))
            and str(target.get("status") or "") != "exhausted_under_threshold"
        ]

    created_jobs = []
    queued_jobs = []
    skipped_targets = []
    errors = []

    request_overrides = {
        "headless": request.headless,
        "scrape_mode": request.scrape_mode,
    }

    for biz, target in selected:
        url = str(biz.get("url", "")).strip()
        if not url:
            skipped_targets.append(
                {
                    "company": target.get("company"),
                    "google_place_id": target.get("google_place_id"),
                    "reason": "missing_url",
                }
            )
            continue

        try:
            overrides = _build_job_overrides(
                merged_config,
                biz,
                request_overrides,
                default_max_reviews=request.default_max_reviews,
            )
            job_id = job_manager.create_job(url, overrides)
            started = job_manager.start_job(job_id)
            entry = {
                "job_id": job_id,
                "url": url,
                "company": target.get("company"),
                "google_place_id": target.get("google_place_id"),
                "started": started,
            }
            created_jobs.append(entry)
            if not started:
                queued_jobs.append(entry)
        except Exception as exc:
            errors.append(
                {
                    "url": url,
                    "company": target.get("company"),
                    "google_place_id": target.get("google_place_id"),
                    "error": str(exc),
                }
            )

    return {
        "config_path": str(cfg_path),
        "min_reviews": request.min_reviews,
        "selected_targets": len(selected),
        "created_count": len(created_jobs),
        "queued_count": len(queued_jobs),
        "skipped_count": len(skipped_targets),
        "error_count": len(errors),
        "created_jobs": created_jobs,
        "queued_jobs": queued_jobs,
        "skipped_targets": skipped_targets,
        "errors": errors,
    }


@ops_router.post("/ops/scrape-target", summary="Queue Scrape Job for One Target")
async def ops_scrape_target(request: ScrapeTargetRequest, review_db=Depends(get_review_db)):
    """Queue a background scrape job for one configured target."""
    if not job_manager:
        raise HTTPException(status_code=500, detail="Job manager not initialized")

    cfg_path = _resolve_config_path(request.config_path)
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")

    merged_config = load_config(cfg_path)
    biz = _resolve_target_business(request, merged_config, review_db)
    url = str(biz.get("url", "")).strip()
    if not url:
        raise HTTPException(status_code=400, detail="Selected target has no URL")

    request_overrides = {
        "headless": request.headless,
        "scrape_mode": request.scrape_mode,
        "max_reviews": request.max_reviews,
    }
    overrides = _build_job_overrides(merged_config, biz, request_overrides)

    job_id = job_manager.create_job(url, overrides)
    started = job_manager.start_job(job_id)

    custom_params = biz.get("custom_params", {}) if isinstance(biz, dict) else {}
    return {
        "job_id": job_id,
        "started": started,
        "config_path": str(cfg_path),
        "url": url,
        "company": custom_params.get("company"),
        "google_place_id": custom_params.get("google_place_id"),
        "effective_max_reviews": overrides.get("max_reviews"),
    }


@ops_router.post("/ops/targets/max-reviews", summary="Persist Per-Target max_reviews")
async def ops_set_target_max_reviews(request: TargetMaxReviewsRequest):
    """Persist per-target max_reviews override in config file."""
    cfg_path = _resolve_config_path(request.config_path)
    raw_cfg = _load_config_raw(cfg_path)

    if not isinstance(raw_cfg.get("businesses"), list):
        raise HTTPException(status_code=400, detail="Config has no 'businesses' list")

    idx, business = _find_business_by_google_place_id(raw_cfg, request.google_place_id)
    if not isinstance(raw_cfg["businesses"][idx], dict):
        raise HTTPException(status_code=400, detail="Target business entry is malformed")

    raw_cfg["businesses"][idx]["max_reviews"] = int(request.max_reviews)
    _save_config_raw(cfg_path, raw_cfg)

    custom_params = business.get("custom_params", {}) if isinstance(business, dict) else {}
    return {
        "config_path": str(cfg_path),
        "google_place_id": request.google_place_id,
        "company": custom_params.get("company"),
        "url": business.get("url"),
        "max_reviews": int(request.max_reviews),
    }


@ops_router.post("/ops/targets/reset-exhausted", summary="Restore Exhausted Targets To Dashboard Queue")
async def ops_reset_exhausted_targets(
    request: ResetExhaustedTargetsRequest,
    review_db=Depends(get_review_db),
):
    """Clear exhausted-under-threshold flags so targets reappear in the under-threshold queue."""
    cfg_path = _resolve_config_path(request.config_path)
    if not cfg_path.exists():
        raise HTTPException(status_code=404, detail=f"Config not found: {cfg_path}")

    identifier_count = sum(1 for x in (request.google_place_id, request.place_id) if x)
    if identifier_count > 1:
        raise HTTPException(
            status_code=400,
            detail="Provide at most one identifier: google_place_id or place_id",
        )

    config = load_config(cfg_path)
    businesses = resolve_businesses(config)
    if not businesses:
        raise HTTPException(status_code=400, detail="No businesses/urls found in config")

    report = compute_progress_report(businesses, review_db, min_reviews=request.min_reviews)

    reset_targets = []
    for target in report["targets"]:
        if not bool(target.get("reviews_exhausted", False)):
            continue
        if int(target.get("review_count", 0) or 0) >= request.min_reviews:
            continue
        if request.google_place_id and str(target.get("google_place_id", "")).strip() != request.google_place_id:
            continue
        if request.place_id and str(target.get("place_id", "")).strip() != request.place_id:
            continue

        place_id = str(target.get("place_id", "")).strip()
        if not place_id:
            continue

        reset_targets.append(
            {
                "place_id": place_id,
                "google_place_id": str(target.get("google_place_id", "")).strip(),
                "company": str(target.get("company", "")).strip(),
                "review_count": int(target.get("review_count", 0) or 0),
            }
        )

    reset_count = review_db.clear_reviews_exhausted([t["place_id"] for t in reset_targets])

    return {
        "config_path": str(cfg_path),
        "min_reviews": int(request.min_reviews),
        "reset_count": reset_count,
        "reset_targets": reset_targets,
    }


# --- Places Router ---
places_router = APIRouter(tags=["Places"], dependencies=[Depends(require_api_key)])


@places_router.get("/places", response_model=List[PlaceResponse], summary="List Places")
async def list_places(review_db=Depends(get_review_db)):
    """List all registered places from the database."""
    places = review_db.list_places()
    return [PlaceResponse(**p) for p in places]


@places_router.get("/places/{place_id}", response_model=PlaceResponse, summary="Get Place")
async def get_place(place_id: str, review_db=Depends(get_review_db)):
    """Get details for a specific place."""
    place = review_db.get_place(place_id)
    if not place:
        raise HTTPException(status_code=404, detail="Place not found")
    return PlaceResponse(**place)


# --- Reviews Router ---
reviews_router = APIRouter(tags=["Reviews"], dependencies=[Depends(require_api_key)])


@reviews_router.get("/reviews/{place_id}", response_model=PaginatedReviewsResponse,
                     summary="List Reviews for Place")
async def list_reviews(
    place_id: str,
    limit: int = Query(50, ge=1, le=1000, description="Reviews per page"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    include_deleted: bool = Query(False, description="Include soft-deleted reviews"),
    review_db=Depends(get_review_db),
):
    """Get paginated reviews for a place."""
    place = review_db.get_place(place_id)
    if not place:
        raise HTTPException(status_code=404, detail="Place not found")

    total = review_db.count_reviews(place_id, include_deleted=include_deleted)
    rows = review_db.get_reviews(place_id, limit=limit, offset=offset,
                                  include_deleted=include_deleted)
    reviews = [ReviewResponse(**_clean_review(r)) for r in rows]
    return PaginatedReviewsResponse(
        place_id=place_id, total=total, limit=limit, offset=offset, reviews=reviews,
    )


@reviews_router.get("/reviews/{place_id}/{review_id}", response_model=ReviewResponse,
                     summary="Get Single Review")
async def get_review(place_id: str, review_id: str, review_db=Depends(get_review_db)):
    """Get a single review by ID."""
    row = review_db.get_review(review_id, place_id)
    if not row:
        raise HTTPException(status_code=404, detail="Review not found")
    return ReviewResponse(**_clean_review(row))


@reviews_router.get("/reviews/{place_id}/{review_id}/history",
                     response_model=List[ReviewHistoryEntry],
                     summary="Get Review Change History")
async def get_review_history(place_id: str, review_id: str,
                              review_db=Depends(get_review_db)):
    """Get the full change history for a specific review."""
    row = review_db.get_review(review_id, place_id)
    if not row:
        raise HTTPException(status_code=404, detail="Review not found")

    history = review_db.get_review_history(review_id, place_id)
    entries = []
    for h in history:
        h = dict(h)
        # Deserialize changed_fields JSON string
        if isinstance(h.get("changed_fields"), str):
            try:
                h["changed_fields"] = json.loads(h["changed_fields"])
            except (json.JSONDecodeError, TypeError):
                pass
        entries.append(ReviewHistoryEntry(**h))
    return entries


# --- Export Router ---
exports_router = APIRouter(tags=["Exports"], dependencies=[Depends(require_api_key)])


@exports_router.get("/exports/places/{place_id}", summary="Export Single Place")
async def export_place(
    place_id: str,
    format: ExportFormat = Query("xlsx"),
    include_deleted: bool = Query(False, description="Include soft-deleted rows"),
    exclude_empty_text: bool = Query(False, description="Exclude reviews with no text content"),
    sheet_name: Optional[str] = Query(None, description="Custom sheet name for XLSX exports"),
    columns: Optional[str] = Query(None, description="Comma-separated list of columns to include"),
    review_db=Depends(get_review_db),
):
    """Download one place as JSON, CSV, or XLSX."""
    place = review_db.get_place(place_id)
    if not place:
        raise HTTPException(status_code=404, detail="Place not found")

    format, include_deleted, exclude_empty_text, sheet_name, col_list = _normalize_export_options(
        format_value=format,
        include_deleted=include_deleted,
        exclude_empty_text=exclude_empty_text,
        sheet_name=sheet_name,
        columns=columns,
    )

    try:
        from modules.export_service import build_place_export

        payload, media_type, filename = build_place_export(
            review_db=review_db,
            place_id=place_id,
            fmt=format,
            include_deleted=include_deleted,
            exclude_empty_text=exclude_empty_text,
            sheet_name=sheet_name,
            columns=col_list,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return Response(
        content=payload,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@exports_router.get("/exports/all", summary="Export All Places")
async def export_all(
    format: ExportFormat = Query("xlsx"),
    include_deleted: bool = Query(False, description="Include soft-deleted rows"),
    exclude_empty_text: bool = Query(False, description="Exclude reviews with no text content"),
    sheet_name: Optional[str] = Query(None, description="Custom sheet name for XLSX exports"),
    columns: Optional[str] = Query(None, description="Comma-separated list of columns to include"),
    review_db=Depends(get_review_db),
):
    """Download all places as JSON, CSV, or XLSX."""
    format, include_deleted, exclude_empty_text, sheet_name, col_list = _normalize_export_options(
        format_value=format,
        include_deleted=include_deleted,
        exclude_empty_text=exclude_empty_text,
        sheet_name=sheet_name,
        columns=columns,
    )

    try:
        from modules.export_service import build_all_export

        payload, media_type, filename = build_all_export(
            review_db=review_db,
            fmt=format,
            include_deleted=include_deleted,
            exclude_empty_text=exclude_empty_text,
            sheet_name=sheet_name,
            columns=col_list,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return Response(
        content=payload,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# --- Audit Log Router ---
audit_router = APIRouter(tags=["Audit Log"], dependencies=[Depends(require_api_key)])


@audit_router.get("/audit-log", response_model=List[AuditLogEntry],
                   summary="Query Audit Log")
async def query_audit_log(
    key_id: Optional[int] = Query(None, description="Filter by API key ID"),
    limit: int = Query(50, ge=1, le=1000, description="Max entries to return"),
    since: Optional[str] = Query(None, description="Only entries after this ISO timestamp"),
    api_key_db=Depends(get_api_key_db),
):
    """Query the API request audit log."""
    entries = api_key_db.query_audit_log(key_id=key_id, limit=limit, since=since)
    return [AuditLogEntry(**e) for e in entries]


# ===========================================================================
# Register all routers
# ===========================================================================
app.include_router(system_router)
app.include_router(jobs_router)
app.include_router(ops_router)
app.include_router(places_router)
app.include_router(reviews_router)
app.include_router(exports_router)
app.include_router(audit_router)


if __name__ == "__main__":
    import uvicorn

    log.info("Starting FastAPI server...")
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
