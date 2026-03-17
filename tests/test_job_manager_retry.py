"""Unit tests for JobManager retry behavior."""

from modules import job_manager as jm


def test_job_manager_retries_transient_failures(monkeypatch):
    calls = {"count": 0}

    class _FakeScraper:
        def __init__(self, _config, cancel_event=None):
            self.cancel_event = cancel_event
            self.last_error_message = ""
            self.last_error_transient = False
            self.total_reviews = 0
            self.total_images = 0

        def scrape(self):
            calls["count"] += 1
            if calls["count"] == 1:
                self.last_error_message = "invalid session id"
                self.last_error_transient = True
                return False
            self.last_error_message = ""
            self.last_error_transient = False
            self.total_reviews = 7
            self.total_images = 2
            return True

    monkeypatch.setattr(jm, "GoogleReviewsScraper", _FakeScraper)
    monkeypatch.setattr(jm, "load_config", lambda: {})
    delays = []
    monkeypatch.setattr(jm.time, "sleep", lambda s: delays.append(s))

    manager = jm.JobManager(max_concurrent_jobs=1)
    try:
        job_id = manager.create_job("https://www.google.com/maps/search/?api=1&query=test")
        manager._run_scraping_job(job_id)
        job = manager.get_job(job_id)
        assert job is not None
        assert job.status == jm.JobStatus.COMPLETED
        assert calls["count"] == 2
        assert delays == [5]
    finally:
        manager.shutdown()


def test_job_manager_does_not_retry_non_transient_failures(monkeypatch):
    calls = {"count": 0}

    class _FakeScraper:
        def __init__(self, _config, cancel_event=None):
            self.cancel_event = cancel_event
            self.last_error_message = ""
            self.last_error_transient = False
            self.total_reviews = 0
            self.total_images = 0

        def scrape(self):
            calls["count"] += 1
            self.last_error_message = "Limited view detected"
            self.last_error_transient = False
            return False

    monkeypatch.setattr(jm, "GoogleReviewsScraper", _FakeScraper)
    monkeypatch.setattr(jm, "load_config", lambda: {})
    delays = []
    monkeypatch.setattr(jm.time, "sleep", lambda s: delays.append(s))

    manager = jm.JobManager(max_concurrent_jobs=1)
    try:
        job_id = manager.create_job("https://www.google.com/maps/search/?api=1&query=test")
        manager._run_scraping_job(job_id)
        job = manager.get_job(job_id)
        assert job is not None
        assert job.status == jm.JobStatus.FAILED
        assert job.error_message == "Limited view detected"
        assert calls["count"] == 1
        assert delays == []
    finally:
        manager.shutdown()


def test_start_job_handles_executor_shutdown_runtime(monkeypatch):
    monkeypatch.setattr(jm, "load_config", lambda: {})

    manager = jm.JobManager(max_concurrent_jobs=1)
    try:
        job_id = manager.create_job("https://www.google.com/maps/search/?api=1&query=test")
        monkeypatch.setattr(
            manager.executor,
            "submit",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("cannot schedule new futures after shutdown")
            ),
        )

        started = manager.start_job(job_id)
        job = manager.get_job(job_id)
        assert started is False
        assert job is not None
        assert job.status == jm.JobStatus.CANCELLED
        assert "cannot schedule new futures after shutdown" in (job.error_message or "")
    finally:
        manager.shutdown()
