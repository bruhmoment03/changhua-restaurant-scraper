"""Unit tests for JobManager retry behavior."""

import threading

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


def test_job_manager_retries_connection_refused_transient_failures(monkeypatch):
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
                self.last_error_message = (
                    "HTTPConnectionPool(host='localhost', port=53931): Max retries exceeded with url: "
                    "/session/test/url (Caused by NewConnectionError('connection refused'))"
                )
                self.last_error_transient = True
                return False
            self.last_error_message = ""
            self.last_error_transient = False
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


def test_shutdown_cancels_running_job_without_retry(monkeypatch):
    calls = {"count": 0}
    started = threading.Event()

    class _FakeScraper:
        def __init__(self, _config, cancel_event=None):
            self.cancel_event = cancel_event
            self.last_error_message = ""
            self.last_error_transient = False
            self.total_reviews = 0
            self.total_images = 0

        def scrape(self):
            calls["count"] += 1
            started.set()
            assert self.cancel_event is not None
            self.cancel_event.wait(1)
            self.last_error_message = (
                "HTTPConnectionPool(host='localhost', port=52574): Max retries exceeded with url: "
                "/session/test/url (Caused by NewConnectionError('connection refused'))"
            )
            self.last_error_transient = True
            return False

    monkeypatch.setattr(jm, "GoogleReviewsScraper", _FakeScraper)
    monkeypatch.setattr(jm, "load_config", lambda: {})
    delays = []
    monkeypatch.setattr(jm.time, "sleep", lambda s: delays.append(s))

    manager = jm.JobManager(max_concurrent_jobs=1)
    try:
        job_id = manager.create_job("https://www.google.com/maps/search/?api=1&query=test")
        assert manager.start_job(job_id) is True
        assert started.wait(1)

        manager.shutdown()

        job = manager.get_job(job_id)
        assert job is not None
        assert job.status == jm.JobStatus.CANCELLED
        assert job.error_message == "Job manager shutdown"
        assert job.progress == {"stage": "cancelled", "message": "Job manager shutdown"}
        assert job.completed_at is not None
        assert calls["count"] == 1
        assert delays == []
    finally:
        manager.shutdown()
