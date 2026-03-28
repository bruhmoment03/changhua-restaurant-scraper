"""
Background job manager for Google Reviews Scraper.
"""

import logging
import threading
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlparse

from modules.config import load_config
from modules.scraper import GoogleReviewsScraper

log = logging.getLogger("scraper")


class JobStatus(str, Enum):
    """Job status enumeration"""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ScrapingJob:
    """Scraping job data class"""
    job_id: str
    status: JobStatus
    url: str
    config: Dict[str, Any]
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    reviews_count: Optional[int] = None
    images_count: Optional[int] = None
    progress: Dict[str, Any] = None
    cancel_event: threading.Event = None
    _scraper: Optional[Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary for JSON serialization"""
        data = {
            "job_id": self.job_id,
            "status": self.status.value if isinstance(self.status, JobStatus) else self.status,
            "url": self.url,
            "config": self.config,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "reviews_count": self.reviews_count,
            "images_count": self.images_count,
            "progress": self.progress,
        }
        return data


class JobManager:
    """Manager for background scraping jobs"""
    
    def __init__(self, max_concurrent_jobs: int = 3):
        """Initialize job manager"""
        self.max_concurrent_jobs = max_concurrent_jobs
        self.jobs: Dict[str, ScrapingJob] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_jobs)
        self.lock = threading.Lock()
        self._shutting_down = False

    @staticmethod
    def _target_key_from_url(url: str) -> str:
        """
        Build a stable dedupe key for a Google Maps target URL.
        Prefer query_place_id; otherwise fall back to /maps/place/<name_or_id>; then URL.
        """
        raw = (url or "").strip()
        if not raw:
            return "url:"
        try:
            parsed = urlparse(raw)
            query = parse_qs(parsed.query)
            query_place_id = (query.get("query_place_id") or [""])[0].strip()
            if query_place_id:
                return f"qpid:{query_place_id}"

            path_parts = [part for part in parsed.path.split("/") if part]
            if "place" in path_parts:
                idx = path_parts.index("place")
                if idx + 1 < len(path_parts):
                    place_token = unquote(path_parts[idx + 1]).strip().lower()
                    if place_token:
                        return f"place:{place_token}"

            normalized = parsed._replace(fragment="").geturl()
            return f"url:{normalized}"
        except Exception:
            return f"url:{raw}"

    def _job_target_key(self, job: ScrapingJob) -> str:
        key = str(job.config.get("target_key", "")).strip()
        if key:
            return key
        key = self._target_key_from_url(job.url)
        job.config["target_key"] = key
        return key

    def _find_active_job_for_target_locked(self, target_key: str) -> Optional[ScrapingJob]:
        for job in self.jobs.values():
            if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
                continue
            if self._job_target_key(job) == target_key:
                return job
        return None
        
    def create_job(self, url: str, config_overrides: Dict[str, Any] = None) -> str:
        """
        Create a new scraping job.
        
        Args:
            url: Google Maps URL to scrape
            config_overrides: Optional config overrides
            
        Returns:
            Job ID
        """
        # Load base config
        config = load_config()

        # Apply URL
        config["url"] = url

        # Apply any overrides
        if config_overrides:
            config.update(config_overrides)

        target_key = self._target_key_from_url(url)

        with self.lock:
            if self._shutting_down:
                raise RuntimeError("Job manager is shutting down")
            existing = self._find_active_job_for_target_locked(target_key)
            if existing:
                log.info(
                    "Skipped duplicate job for target %s; reusing active job %s",
                    target_key,
                    existing.job_id,
                )
                return existing.job_id

            job_id = str(uuid.uuid4())
            config["job_id"] = job_id
            config["target_key"] = target_key

            job = ScrapingJob(
                job_id=job_id,
                status=JobStatus.PENDING,
                url=url,
                config=config,
                created_at=datetime.now(),
                progress={"stage": "created", "message": "Job created and queued"},
                cancel_event=threading.Event(),
            )
            self.jobs[job_id] = job

        log.info(f"Created scraping job {job_id} for URL: {url}")
        return job_id

    def _promote_pending_jobs(self) -> None:
        """Start pending jobs while capacity is available."""
        while True:
            with self.lock:
                if self._shutting_down:
                    return
                running_count = sum(1 for j in self.jobs.values() if j.status == JobStatus.RUNNING)
                if running_count >= self.max_concurrent_jobs:
                    return

                pending_jobs = [j for j in self.jobs.values() if j.status == JobStatus.PENDING]
                if not pending_jobs:
                    return

                pending_jobs.sort(key=lambda j: j.created_at)
                job = pending_jobs[0]
                target_key = self._job_target_key(job)
                running_duplicate = any(
                    other.job_id != job.job_id
                    and other.status == JobStatus.RUNNING
                    and self._job_target_key(other) == target_key
                    for other in self.jobs.values()
                )
                if running_duplicate:
                    job.status = JobStatus.CANCELLED
                    job.completed_at = datetime.now()
                    job.progress = {
                        "stage": "cancelled",
                        "message": "Skipped duplicate target; another job is already running",
                    }
                    log.info(
                        "Cancelled pending duplicate job %s for target %s (already running)",
                        job.job_id,
                        target_key,
                    )
                    continue

                job.status = JobStatus.RUNNING
                job.started_at = datetime.now()
                job.progress = {"stage": "starting", "message": "Initializing scraper"}
                job_id = job.job_id

            try:
                self.executor.submit(self._run_scraping_job, job_id)
                log.info(f"Started scraping job {job_id} from pending queue")
            except RuntimeError as exc:
                with self.lock:
                    if job.status == JobStatus.RUNNING:
                        job.status = JobStatus.CANCELLED
                        job.completed_at = datetime.now()
                        job.error_message = str(exc)
                        job.progress = {
                            "stage": "cancelled",
                            "message": "Job manager is shutting down",
                        }
                log.warning("Unable to start pending job %s: %s", job_id, exc)
                return
    
    def start_job(self, job_id: str) -> bool:
        """
        Start a pending job.
        
        Args:
            job_id: Job ID to start
            
        Returns:
            True if job was started, False otherwise
        """
        with self.lock:
            if job_id not in self.jobs:
                return False
                
            job = self.jobs[job_id]
            if job.status != JobStatus.PENDING:
                return False
            if self._shutting_down:
                job.status = JobStatus.CANCELLED
                job.completed_at = datetime.now()
                job.progress = {"stage": "cancelled", "message": "Job manager is shutting down"}
                return False
                
            # Check if we can start more jobs
            running_count = sum(1 for j in self.jobs.values() if j.status == JobStatus.RUNNING)
            if running_count >= self.max_concurrent_jobs:
                return False
                
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now()
            job.progress = {"stage": "starting", "message": "Initializing scraper"}
            
        # Submit job to thread pool
        try:
            self.executor.submit(self._run_scraping_job, job_id)
        except RuntimeError as exc:
            with self.lock:
                if job.status == JobStatus.RUNNING:
                    job.status = JobStatus.CANCELLED
                    job.completed_at = datetime.now()
                    job.error_message = str(exc)
                    job.progress = {"stage": "cancelled", "message": "Job manager is shutting down"}
            log.warning("Unable to start job %s: %s", job_id, exc)
            return False
        
        log.info(f"Started scraping job {job_id}")
        return True
    
    def _run_scraping_job(self, job_id: str):
        """
        Run the actual scraping job in background thread.
        
        Args:
            job_id: Job ID to run
        """
        try:
            with self.lock:
                job = self.jobs[job_id]
                job.progress = {"stage": "initializing", "message": "Setting up scraper"}
            retry_backoffs = (5, 15)
            max_attempts = len(retry_backoffs) + 1
            attempt = 1
            success = False
            last_error = ""
            scraper = None

            while attempt <= max_attempts:
                if job.cancel_event and job.cancel_event.is_set():
                    break

                # Create scraper with job config and cancel event
                scraper = GoogleReviewsScraper(job.config, cancel_event=job.cancel_event)

                with self.lock:
                    job._scraper = scraper
                    if attempt == 1:
                        job.progress = {"stage": "scraping", "message": "Scraping reviews in progress"}
                    else:
                        job.progress = {
                            "stage": "scraping",
                            "message": f"Retry attempt {attempt}/{max_attempts} in progress",
                        }

                # Run the scraping
                success = bool(scraper.scrape())
                last_error = str(getattr(scraper, "last_error_message", "") or "")
                transient = bool(getattr(scraper, "last_error_transient", False))

                with self.lock:
                    job._scraper = None

                if success or (job.cancel_event and job.cancel_event.is_set()):
                    break

                if (not transient) or attempt >= max_attempts:
                    break

                delay = retry_backoffs[min(attempt - 1, len(retry_backoffs) - 1)]
                log.warning(
                    "Transient scrape failure for job %s (attempt %d/%d): %s. Retrying in %ds.",
                    job_id,
                    attempt,
                    max_attempts,
                    last_error or "unknown error",
                    delay,
                )
                with self.lock:
                    job.progress = {
                        "stage": "retrying",
                        "message": f"Transient failure, retrying in {delay}s (attempt {attempt + 1}/{max_attempts})",
                    }
                time.sleep(delay)
                attempt += 1

            # Mark job based on scrape result — never overwrite CANCELLED
            with self.lock:
                if job.status == JobStatus.CANCELLED:
                    if job.completed_at is None:
                        job.completed_at = datetime.now()
                    if not job.error_message and self._shutting_down:
                        job.error_message = "Job manager shutdown"
                    if job.progress is None or job.progress.get("stage") != "cancelled":
                        job.progress = {"stage": "cancelled", "message": "Job was cancelled"}
                    log.info(f"Job {job_id} was cancelled during execution")
                elif success:
                    job.status = JobStatus.COMPLETED
                    job.completed_at = datetime.now()
                    job.progress = {"stage": "completed", "message": "Scraping completed successfully"}
                else:
                    job.status = JobStatus.FAILED
                    job.completed_at = datetime.now()
                    job.error_message = last_error or "Scraper returned failure (no reviews found or navigation error)"
                    job.progress = {"stage": "failed", "message": "Scraping failed"}

                job.reviews_count = getattr(scraper, 'total_reviews', None)
                job.images_count = getattr(scraper, 'total_images', None)
                job._scraper = None

            log.info(f"Completed scraping job {job_id}")
            if not self._shutting_down:
                self._promote_pending_jobs()

        except Exception as e:
            log.error(f"Error in scraping job {job_id}: {e}")
            with self.lock:
                job = self.jobs.get(job_id)
                if job and job.status != JobStatus.CANCELLED:
                    job.status = JobStatus.FAILED
                    job.completed_at = datetime.now()
                    job.error_message = str(e)
                    job.progress = {"stage": "failed", "message": f"Job failed: {str(e)}"}
                if job:
                    job._scraper = None
            if not self._shutting_down:
                self._promote_pending_jobs()
    
    def get_job(self, job_id: str) -> Optional[ScrapingJob]:
        """
        Get job by ID.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job object or None if not found
        """
        with self.lock:
            return self.jobs.get(job_id)
    
    def list_jobs(self, status: Optional[JobStatus] = None, limit: int = 100) -> List[ScrapingJob]:
        """
        List jobs, optionally filtered by status.
        
        Args:
            status: Optional status filter
            limit: Maximum number of jobs to return
            
        Returns:
            List of jobs
        """
        with self.lock:
            jobs = list(self.jobs.values())
            
        if status:
            jobs = [job for job in jobs if job.status == status]
            
        # Sort by creation time (newest first)
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        
        return jobs[:limit]
    
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a pending or running job.

        Sets the cancel event so the scraper's scroll loop exits early.
        """
        with self.lock:
            if job_id not in self.jobs:
                return False

            job = self.jobs[job_id]
            if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                return False

            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.now()
            job.progress = {"stage": "cancelled", "message": "Job was cancelled"}

            # Signal the scraper to stop
            if job.cancel_event:
                job.cancel_event.set()

        log.info(f"Cancelled scraping job {job_id}")
        return True
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job from the manager.

        Only terminal-state jobs (COMPLETED, FAILED, CANCELLED) can be deleted
        to avoid race conditions with running worker threads.
        """
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return False
            if job.status not in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                return False
            del self.jobs[job_id]

        log.info(f"Deleted scraping job {job_id}")
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get job manager statistics.
        
        Returns:
            Statistics dictionary
        """
        with self.lock:
            jobs = list(self.jobs.values())
            
        stats = {
            "total_jobs": len(jobs),
            "by_status": {},
            "running_jobs": 0,
            "max_concurrent_jobs": self.max_concurrent_jobs
        }
        
        for status in JobStatus:
            count = sum(1 for job in jobs if job.status == status)
            stats["by_status"][status.value] = count
            
        stats["running_jobs"] = stats["by_status"].get(JobStatus.RUNNING.value, 0)
        
        return stats

    def set_max_concurrent_jobs(self, limit: int) -> int:
        """Update the live concurrency limit for newly started jobs."""
        next_limit = max(1, int(limit or 1))
        old_executor = None

        with self.lock:
            if self._shutting_down:
                raise RuntimeError("Job manager is shutting down")
            if next_limit == self.max_concurrent_jobs:
                return self.max_concurrent_jobs

            old_executor = self.executor
            self.executor = ThreadPoolExecutor(max_workers=next_limit)
            self.max_concurrent_jobs = next_limit

        if old_executor is not None:
            old_executor.shutdown(wait=False, cancel_futures=False)

        log.info("Updated job manager max_concurrent_jobs=%d", next_limit)
        if not self._shutting_down:
            self._promote_pending_jobs()
        return next_limit
    
    def cleanup_old_jobs(self, max_age_hours: int = 24):
        """
        Clean up old completed/failed jobs.
        
        Args:
            max_age_hours: Maximum age in hours before cleanup
        """
        cutoff_time = datetime.now().timestamp() - (max_age_hours * 3600)
        
        with self.lock:
            to_delete = []
            for job_id, job in self.jobs.items():
                if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    if job.completed_at and job.completed_at.timestamp() < cutoff_time:
                        to_delete.append(job_id)
            
            for job_id in to_delete:
                del self.jobs[job_id]
                
        if to_delete:
            log.info(f"Cleaned up {len(to_delete)} old jobs")
    
    def shutdown(self):
        """Shutdown the job manager"""
        log.info("Shutting down job manager")
        with self.lock:
            self._shutting_down = True
            now = datetime.now()
            for job in self.jobs.values():
                if job.status == JobStatus.PENDING:
                    job.status = JobStatus.CANCELLED
                    job.completed_at = now
                    job.error_message = "Job manager shutdown"
                    job.progress = {"stage": "cancelled", "message": "Job manager shutdown"}
                elif job.status == JobStatus.RUNNING and job.cancel_event:
                    job.status = JobStatus.CANCELLED
                    job.completed_at = now
                    job.error_message = "Job manager shutdown"
                    job.progress = {"stage": "cancelled", "message": "Job manager shutdown"}
                    job.cancel_event.set()
        self.executor.shutdown(wait=True)
