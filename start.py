#!/usr/bin/env python3
"""
Google Maps Reviews Scraper Pro
================================

Main entry point supporting scrape + management commands.
"""

import json
import sys
import time
from pathlib import Path

from modules.cli import parse_arguments
from modules.config import load_config
from modules.progress import (
    compute_progress_report as _compute_progress_report,
    resolve_businesses as _resolve_businesses,
    select_businesses_for_scrape as _select_businesses_for_scrape,
)


def _apply_scrape_overrides(config, args):
    """Apply CLI argument overrides to config for scrape command."""
    headless_override = None
    if getattr(args, "headed", False):
        headless_override = False
    elif getattr(args, "headless", False):
        headless_override = True

    overrides = {
        "headless": headless_override,
        "sort_by": args.sort_by,
        "google_maps_auth_mode": getattr(args, "google_maps_auth_mode", None),
        "fail_on_limited_view": getattr(args, "fail_on_limited_view", None),
        "debug_on_limited_view": getattr(args, "debug_on_limited_view", None),
        "debug_artifacts_dir": getattr(args, "debug_artifacts_dir", None),
        "stealth_undetectable": getattr(args, "stealth_undetectable", None),
        "stealth_user_agent": getattr(args, "stealth_user_agent", None),
        "scrape_mode": getattr(args, "scrape_mode", None),
        "stop_threshold": getattr(args, "stop_threshold", None),
        "max_reviews": getattr(args, "max_reviews", None),
        "max_scroll_attempts": getattr(args, "max_scroll_attempts", None),
        "scroll_idle_limit": getattr(args, "scroll_idle_limit", None),
        "url": args.url,
        "use_mongodb": getattr(args, "use_mongodb", None),
        "convert_dates": getattr(args, "convert_dates", None),
        "download_images": getattr(args, "download_images", None),
        "image_dir": getattr(args, "image_dir", None),
        "download_threads": getattr(args, "download_threads", None),
        "store_local_paths": getattr(args, "store_local_paths", None),
        "replace_urls": getattr(args, "replace_urls", None),
        "custom_url_base": getattr(args, "custom_url_base", None),
        "custom_url_profiles": getattr(args, "custom_url_profiles", None),
        "custom_url_reviews": getattr(args, "custom_url_reviews", None),
        "preserve_original_urls": getattr(args, "preserve_original_urls", None),
    }

    # Legacy CLI flags → new config keys
    if getattr(args, "overwrite_existing", False) and not getattr(args, "scrape_mode", None):
        overrides["scrape_mode"] = "full"
    if getattr(args, "stop_on_match", False):
        overrides["stop_threshold"] = overrides.get("stop_threshold") or 3

    for key, value in overrides.items():
        if value is not None:
            config[key] = value

    if getattr(args, "db_path", None):
        config["db_path"] = args.db_path

    custom_params = getattr(args, "custom_params", None)
    if custom_params:
        config.setdefault("custom_params", {}).update(custom_params)


def _get_db_path(config, args):
    """Resolve database path from CLI args or config."""
    if getattr(args, "db_path", None):
        return args.db_path
    return config.get("db_path", "reviews.db")


def _build_business_config(base_config, overrides):
    """Merge per-business overrides into a copy of the global config."""
    import copy
    from modules.config import resolve_aliases
    merged = copy.deepcopy(base_config)
    for key, value in overrides.items():
        if key == "url":
            merged["url"] = value
        elif isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
            merged[key].update(value)
        else:
            merged[key] = value
    resolve_aliases(merged)
    return merged


def _is_transient_failure_message(message: str) -> bool:
    """Detect transient browser/network failures eligible for retry."""
    if not message:
        return False
    lower = message.lower()
    if "limited view" in lower:
        return False
    markers = (
        "err_internet_disconnected",
        "invalid session id",
        "no such window",
        "web view not found",
        "disconnected",
        "timed out",
        "timeout",
        "chrome not reachable",
        "unable to receive message from renderer",
    )
    return any(marker in lower for marker in markers)


def _run_progress(config, args):
    """Show config-vs-DB scraping progress for configured businesses."""
    from modules.review_db import ReviewDB

    businesses = _resolve_businesses(config)
    if not businesses:
        print("Error: No URL configured. Use --url or set 'businesses'/'urls' in config.yaml")
        sys.exit(1)

    db = ReviewDB(_get_db_path(config, args))
    try:
        report = _compute_progress_report(businesses, db)
    finally:
        db.close()

    if getattr(args, "json", False):
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print("Batch Progress")
        print("=" * 40)
        print(f"  targets_total:       {report['targets_total']}")
        print(f"  with_reviews:        {report['with_reviews']}")
        print(f"  present_zero_reviews:{report['present_zero_reviews']}")
        print(f"  missing_from_db:     {report['missing_from_db']}")
        print(f"  completed_percent:   {report['completed_percent']:.2f}%")

        incomplete = [
            t for t in report["targets"]
            if t["status"] in ("missing_from_db", "present_zero_reviews")
        ]
        if incomplete:
            print("\nIncomplete targets:")
            for target in incomplete:
                company = target.get("company") or "<unknown>"
                qpid = target.get("google_place_id") or "<missing>"
                print(
                    f"  - [{target['status']}] {company} | "
                    f"query_place_id={qpid} | url={target.get('url', '')}"
                )
        else:
            print("\nAll configured targets have at least one review in DB.")

    if getattr(args, "fail_if_incomplete", False) and report["incomplete_total"] > 0:
        sys.exit(2)


def _run_scrape(config, args):
    """Run the scrape command."""
    from modules.review_db import ReviewDB
    from modules.scraper import GoogleReviewsScraper

    _apply_scrape_overrides(config, args)

    businesses = _resolve_businesses(config)
    if not businesses:
        print("Error: No URL configured. Use --url or set 'businesses'/'urls' in config.yaml")
        sys.exit(1)

    max_businesses = getattr(args, "max_businesses", None)
    if max_businesses is not None and max_businesses <= 0:
        print("Error: --max-businesses must be greater than 0")
        sys.exit(1)

    if getattr(args, "only_missing", False) or max_businesses is not None:
        db = ReviewDB(_get_db_path(config, args))
        try:
            report = _compute_progress_report(businesses, db)
        finally:
            db.close()
        businesses = _select_businesses_for_scrape(
            businesses,
            report,
            only_missing=bool(getattr(args, "only_missing", False)),
            max_businesses=max_businesses,
        )
        print(
            f"Selected {len(businesses)} businesses "
            f"(only_missing={bool(getattr(args, 'only_missing', False))}, "
            f"max_businesses={max_businesses if max_businesses is not None else 'none'})"
        )
        if not businesses:
            print("No businesses selected for this run.")
            return

    concurrency = max(1, min(4, getattr(args, "concurrency", 2) or 2))
    retry_backoffs = (5, 15)
    max_attempts = 3

    def _scrape_one(index, biz):
        """Scrape a single business with retry logic. Returns (success, url, error)."""
        biz_config = _build_business_config(config, biz)
        url = biz_config.get("url", "")

        # Force isolated Chrome profile when running concurrently
        if concurrency > 1:
            biz_config.pop("chrome_user_data_dir", None)

        label = f"[{index + 1}/{len(businesses)}]"
        print(f"\n--- {label} Starting: {url} ---")

        attempt = 1
        success = False
        last_error = ""
        while attempt <= max_attempts:
            scraper = GoogleReviewsScraper(biz_config)
            try:
                success = bool(scraper.scrape())
                last_error = str(getattr(scraper, "last_error_message", "") or "")
                transient = bool(getattr(scraper, "last_error_transient", False))
            finally:
                scraper.review_db.close()

            if success:
                break

            is_transient = transient or _is_transient_failure_message(last_error)
            if not is_transient or attempt >= max_attempts:
                break

            delay = retry_backoffs[min(attempt - 1, len(retry_backoffs) - 1)]
            print(
                f"{label} Transient scrape failure (attempt {attempt}/{max_attempts})"
                f"{': ' + last_error if last_error else ''}. Retrying in {delay}s..."
            )
            time.sleep(delay)
            attempt += 1

        if success:
            print(f"{label} Completed: {url}")
        else:
            print(f"{label} Failed: {url}\n  error: {last_error}")

        return success, url, last_error

    if concurrency <= 1 or len(businesses) <= 1:
        # Sequential mode — same as before
        failed = 0
        succeeded = 0
        for i, biz in enumerate(businesses):
            ok, _url, _err = _scrape_one(i, biz)
            if ok:
                succeeded += 1
            else:
                failed += 1
    else:
        # Concurrent mode
        from concurrent.futures import ThreadPoolExecutor, as_completed

        print(f"\nRunning with {concurrency} concurrent scrapers...")
        failed = 0
        succeeded = 0

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {}
            for i, biz in enumerate(businesses):
                # Stagger job starts by 5s to avoid simultaneous Google hits
                if i > 0:
                    time.sleep(5)
                future = pool.submit(_scrape_one, i, biz)
                futures[future] = i

            for future in as_completed(futures):
                try:
                    ok, _url, _err = future.result()
                    if ok:
                        succeeded += 1
                    else:
                        failed += 1
                except Exception as exc:
                    failed += 1
                    idx = futures[future]
                    print(f"[{idx + 1}/{len(businesses)}] Unexpected error: {exc}")

    if len(businesses) > 1:
        print(f"\nBatch finished: succeeded={succeeded}, failed={failed}")


def _run_export(config, args):
    """Run the export command."""
    from modules.review_db import ReviewDB

    db = ReviewDB(_get_db_path(config, args))
    try:
        fmt = getattr(args, "format", "json")
        place_id = getattr(args, "place_id", None)
        output = getattr(args, "output", None)
        include_deleted = getattr(args, "include_deleted", False)

        if fmt == "json":
            if place_id:
                data = db.export_reviews_json(place_id, include_deleted)
            else:
                data = db.export_all_json(include_deleted)
            text = json.dumps(data, ensure_ascii=False, indent=2)
            if output:
                with open(output, "w", encoding="utf-8") as f:
                    f.write(text)
                print(f"Exported to {output}")
            else:
                print(text)
        elif fmt == "csv":
            if place_id:
                path = output or f"reviews_{place_id}.csv"
                count = db.export_reviews_csv(place_id, path, include_deleted)
                print(f"Exported {count} reviews to {path}")
            else:
                out_dir = output or "exports"
                counts = db.export_all_csv(out_dir, include_deleted)
                for pid, count in counts.items():
                    print(f"  {pid}: {count} reviews")
                print(f"Exported to {out_dir}/")
    finally:
        db.close()


def _run_dataset_export(config, args):
    """Run the config-scoped dataset bundle export command."""
    from modules.dataset_export import export_dataset_bundle
    from modules.review_db import ReviewDB

    output_dir = getattr(args, "output_dir", None) or "dataset_export"
    min_reviews = max(0, int(getattr(args, "min_reviews", 100) or 0))
    config_path = str(getattr(args, "config", "config.yaml"))

    db = ReviewDB(_get_db_path(config, args))
    try:
        manifest = export_dataset_bundle(
            review_db=db,
            config=config,
            config_path=config_path,
            output_dir=output_dir,
            min_reviews=min_reviews,
            include_deleted=bool(getattr(args, "include_deleted", False)),
        )
    finally:
        db.close()

    print(f"Dataset bundle written to {output_dir}")
    for artifact in manifest.get("artifacts", []):
        print(f"  - {artifact.get('filename')}")


def _run_db_stats(config, args):
    """Run the db-stats command."""
    from modules.review_db import ReviewDB

    db = ReviewDB(_get_db_path(config, args))
    try:
        stats = db.get_stats()
        print("Database Statistics")
        print("=" * 40)
        print(f"  Places:           {stats.get('places_count', 0)}")
        print(f"  Reviews:          {stats.get('reviews_count', 0)}")
        print(f"  Sessions:         {stats.get('scrape_sessions_count', 0)}")
        print(f"  History entries:   {stats.get('review_history_count', 0)}")
        print(f"  Sync checkpoints: {stats.get('sync_checkpoints_count', 0)}")
        print(f"  Aliases:          {stats.get('place_aliases_count', 0)}")
        size_bytes = stats.get("db_size_bytes", 0)
        if size_bytes > 1024 * 1024:
            print(f"  DB size:          {size_bytes / (1024*1024):.1f} MB")
        else:
            print(f"  DB size:          {size_bytes / 1024:.1f} KB")

        places = stats.get("places", [])
        if places:
            print(f"\nPer-place breakdown:")
            for p in places:
                print(f"  {p['place_id']}: {p.get('place_name', '?')} "
                      f"({p.get('total_reviews', 0)} reviews, "
                      f"last scraped: {p.get('last_scraped', 'never')})")
    finally:
        db.close()


def _run_clear(config, args):
    """Run the clear command."""
    from modules.review_db import ReviewDB

    db = ReviewDB(_get_db_path(config, args))
    try:
        place_id = getattr(args, "place_id", None)
        confirm = getattr(args, "confirm", False)

        if not confirm:
            target = place_id or "ALL places"
            answer = input(f"Clear data for {target}? This cannot be undone. [y/N]: ")
            if answer.lower() != "y":
                print("Cancelled.")
                return

        if place_id:
            counts = db.clear_place(place_id)
            print(f"Cleared place {place_id}:")
        else:
            counts = db.clear_all()
            print("Cleared all data:")
        for table, count in counts.items():
            print(f"  {table}: {count} rows")
    finally:
        db.close()


def _run_hide(config, args):
    """Run the hide command."""
    from modules.review_db import ReviewDB

    db = ReviewDB(_get_db_path(config, args))
    try:
        if db.hide_review(args.review_id, args.place_id):
            print(f"Review {args.review_id} hidden.")
        else:
            print(f"Review {args.review_id} not found or already hidden.")
    finally:
        db.close()


def _run_restore(config, args):
    """Run the restore command."""
    from modules.review_db import ReviewDB

    db = ReviewDB(_get_db_path(config, args))
    try:
        if db.restore_review(args.review_id, args.place_id):
            print(f"Review {args.review_id} restored.")
        else:
            print(f"Review {args.review_id} not found or not hidden.")
    finally:
        db.close()


def _run_sync_status(config, args):
    """Run the sync-status command."""
    from modules.review_db import ReviewDB

    db = ReviewDB(_get_db_path(config, args))
    try:
        statuses = db.get_all_sync_status()
        if not statuses:
            print("No sync checkpoints found.")
            return
        print("Sync Checkpoints")
        print("=" * 60)
        for s in statuses:
            print(f"  {s.get('place_id', '?')} -> {s.get('target', '?')}: "
                  f"status={s.get('status', '?')}, "
                  f"last_synced={s.get('last_synced_at', 'never')}, "
                  f"attempts={s.get('attempt_count', 0)}")
            if s.get("error_message"):
                print(f"    error: {s['error_message']}")
    finally:
        db.close()


def _run_prune_history(config, args):
    """Run the prune-history command."""
    from modules.review_db import ReviewDB

    db = ReviewDB(_get_db_path(config, args))
    try:
        older_than = getattr(args, "older_than", 90)
        dry_run = getattr(args, "dry_run", False)
        count = db.prune_history(older_than, dry_run)
        if dry_run:
            print(f"Would prune {count} history entries older than {older_than} days.")
        else:
            print(f"Pruned {count} history entries older than {older_than} days.")
    finally:
        db.close()


def _run_migrate(config, args):
    """Run the migrate command."""
    from modules.migration import migrate_json, migrate_mongodb

    db_path = _get_db_path(config, args)
    source = getattr(args, "source", "json")
    place_url = getattr(args, "place_url", None) or config.get("url", "")

    if source == "json":
        json_path = getattr(args, "json_path", None) or config.get("json_path", "google_reviews.json")
        stats = migrate_json(json_path, db_path, place_url)
        print(f"Migrated from JSON: {stats}")
    elif source == "mongodb":
        stats = migrate_mongodb(config, db_path, place_url)
        print(f"Migrated from MongoDB: {stats}")


# ------------------------------------------------------------------
# API key management commands
# ------------------------------------------------------------------

def _run_api_key_create(config, args):
    """Create a new API key."""
    from modules.api_keys import ApiKeyDB

    db = ApiKeyDB(_get_db_path(config, args))
    try:
        key_id, raw_key = db.create_key(args.name)
        print(f"Created API key #{key_id} for '{args.name}'")
        print(f"Key: {raw_key}")
        print("Store this key securely — it cannot be retrieved later.")
    finally:
        db.close()


def _run_api_key_list(config, args):
    """List all API keys."""
    from modules.api_keys import ApiKeyDB

    db = ApiKeyDB(_get_db_path(config, args))
    try:
        keys = db.list_keys()
        if not keys:
            print("No API keys found.")
            return
        print(f"{'ID':<5} {'Name':<20} {'Prefix':<18} {'Active':<8} {'Uses':<8} {'Last Used':<20}")
        print("=" * 79)
        for k in keys:
            active = "yes" if k["is_active"] else "REVOKED"
            last_used = k["last_used_at"] or "never"
            print(f"{k['id']:<5} {k['name']:<20} {k['key_prefix']:<18} "
                  f"{active:<8} {k['usage_count']:<8} {last_used:<20}")
    finally:
        db.close()


def _run_api_key_revoke(config, args):
    """Revoke an API key."""
    from modules.api_keys import ApiKeyDB

    db = ApiKeyDB(_get_db_path(config, args))
    try:
        if db.revoke_key(args.key_id):
            print(f"API key #{args.key_id} revoked.")
        else:
            print(f"Key #{args.key_id} not found or already revoked.")
    finally:
        db.close()


def _run_api_key_stats(config, args):
    """Show API key usage statistics."""
    from modules.api_keys import ApiKeyDB

    db = ApiKeyDB(_get_db_path(config, args))
    try:
        stats = db.get_key_stats(args.key_id)
        if not stats:
            print(f"Key #{args.key_id} not found.")
            return
        active = "active" if stats["is_active"] else "REVOKED"
        print(f"Key #{stats['id']}: {stats['name']} ({active})")
        print(f"  Prefix:    {stats['key_prefix']}")
        print(f"  Created:   {stats['created_at']}")
        print(f"  Last used: {stats['last_used_at'] or 'never'}")
        print(f"  Uses:      {stats['usage_count']}")
        recent = stats.get("recent_requests", [])
        if recent:
            print(f"\n  Recent requests ({len(recent)}):")
            for r in recent:
                print(f"    {r['timestamp']}  {r['method']} {r['endpoint']}  -> {r['status_code']}")
    finally:
        db.close()


def _run_audit_log(config, args):
    """Query the API audit log."""
    from modules.api_keys import ApiKeyDB

    db = ApiKeyDB(_get_db_path(config, args))
    try:
        rows = db.query_audit_log(
            key_id=getattr(args, "key_id", None),
            limit=getattr(args, "limit", 50),
            since=getattr(args, "since", None),
        )
        if not rows:
            print("No audit log entries found.")
            return
        print(f"{'ID':<6} {'Timestamp':<20} {'Key':<12} {'Method':<8} {'Endpoint':<30} {'Status':<7} {'ms':<6}")
        print("=" * 89)
        for r in rows:
            key_label = r.get("key_name") or str(r.get("key_id") or "-")
            print(f"{r['id']:<6} {r['timestamp']:<20} {key_label:<12} "
                  f"{r['method']:<8} {r['endpoint']:<30} "
                  f"{r.get('status_code') or '-':<7} {r.get('response_time_ms') or '-':<6}")
    finally:
        db.close()


def _run_prune_audit(config, args):
    """Prune old API audit log entries."""
    from modules.api_keys import ApiKeyDB

    db = ApiKeyDB(_get_db_path(config, args))
    try:
        days = getattr(args, "older_than_days", 90)
        dry_run = getattr(args, "dry_run", False)
        count = db.prune_audit_log(days, dry_run)
        if dry_run:
            print(f"Would prune {count} audit entries older than {days} days.")
        else:
            print(f"Pruned {count} audit entries older than {days} days.")
    finally:
        db.close()


def _run_logs(config, args):
    """Run the logs viewer command."""
    import sys
    log_dir = config.get("log_dir", "logs")
    log_file = config.get("log_file", "scraper.log")
    log_path = Path(log_dir) / log_file

    if not log_path.exists():
        print(f"Log file not found: {log_path}")
        sys.exit(1)

    lines = getattr(args, "lines", 50)
    level_filter = (getattr(args, "level", None) or "").upper()
    follow = getattr(args, "follow", False)

    def _print_lines(path, n, level):
        with open(path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        tail = all_lines[-n:] if n < len(all_lines) else all_lines
        for line in tail:
            line = line.rstrip()
            if not line:
                continue
            if level:
                try:
                    entry = json.loads(line)
                    if entry.get("level", "") != level:
                        continue
                except (json.JSONDecodeError, KeyError):
                    pass
            print(line)

    _print_lines(log_path, lines, level_filter)

    if follow:
        import time
        with open(log_path, "r", encoding="utf-8") as f:
            f.seek(0, 2)  # seek to end
            try:
                while True:
                    line = f.readline()
                    if not line:
                        time.sleep(0.3)
                        continue
                    line = line.rstrip()
                    if level_filter:
                        try:
                            entry = json.loads(line)
                            if entry.get("level", "") != level_filter:
                                continue
                        except (json.JSONDecodeError, KeyError):
                            pass
                    print(line)
            except KeyboardInterrupt:
                pass


def main():
    """Main function to initialize and run the scraper or management commands."""
    args = parse_arguments()
    config = load_config(args.config)

    # Setup structured logging (skip for 'logs' viewer — it reads raw files)
    if args.command != "logs":
        from modules.log_manager import setup_logging
        setup_logging(
            level=config.get("log_level", "INFO"),
            log_dir=config.get("log_dir", "logs"),
            log_file=config.get("log_file", "scraper.log"),
        )

    commands = {
        "scrape": _run_scrape,
        "progress": _run_progress,
        "export": _run_export,
        "dataset-export": _run_dataset_export,
        "db-stats": _run_db_stats,
        "clear": _run_clear,
        "hide": _run_hide,
        "restore": _run_restore,
        "sync-status": _run_sync_status,
        "prune-history": _run_prune_history,
        "migrate": _run_migrate,
        "api-key-create": _run_api_key_create,
        "api-key-list": _run_api_key_list,
        "api-key-revoke": _run_api_key_revoke,
        "api-key-stats": _run_api_key_stats,
        "audit-log": _run_audit_log,
        "prune-audit": _run_prune_audit,
        "logs": _run_logs,
    }

    handler = commands.get(args.command)
    if handler:
        handler(config, args)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.")
        sys.exit(130)
