"""
Scheduler for automated daily report generation.

Features:
- Daily job at configurable time (default 6 AM)
- Catch-up logic: fills missing days on startup
- Retry logic: retries failed jobs up to 3 times
- Health tracking: logs all runs to database
- Graceful error handling

Configuration via environment variables:
- SCHEDULER_ENABLED: Set to 'true' to enable (default: true)
- SCHEDULER_HOUR: Hour to run daily job (default: 6)
- SCHEDULER_MINUTE: Minute to run daily job (default: 0)
- SCHEDULER_CATCHUP_DAYS: Days to look back for missing reports (default: 7)
"""

import os
import logging
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from storage import (
    ensure_db_initialized,
    get_all_organizations,
    get_organization,
    save_daily_report,
    get_daily_report,
    get_missing_report_dates,
    log_scheduler_run,
    get_last_successful_run,
    get_recent_scheduler_runs,
    DailyReport,
    Organization,
)

logger = logging.getLogger(__name__)

# Global scheduler instance
_scheduler: Optional[BackgroundScheduler] = None

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 60


def get_scheduler() -> BackgroundScheduler:
    """Get or create the global scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
    return _scheduler


def is_scheduler_enabled() -> bool:
    """Check if scheduler is enabled via environment variable."""
    return os.getenv("SCHEDULER_ENABLED", "true").lower() in ("true", "1", "yes")


def get_schedule_time() -> tuple[int, int]:
    """Get the scheduled hour and minute from environment variables."""
    hour = int(os.getenv("SCHEDULER_HOUR", "6"))
    minute = int(os.getenv("SCHEDULER_MINUTE", "0"))
    return hour, minute


def get_catchup_days() -> int:
    """Get number of days to look back for catch-up."""
    return int(os.getenv("SCHEDULER_CATCHUP_DAYS", "7"))


def generate_daily_report_for_org(
    org: Organization,
    target_date: Optional[str] = None,
    retry_count: int = 0
) -> Optional[DailyReport]:
    """
    Generate and store a daily report for a specific organization.

    Args:
        org: The organization to generate a report for
        target_date: Optional date string (YYYY-MM-DD). Defaults to yesterday.
        retry_count: Current retry attempt (internal use)

    Returns:
        The saved DailyReport, or None if generation failed
    """
    # Import here to avoid circular imports
    from db import (
        fetch_calls_ending_in_each_call_stage_stats,
        fetch_call_classifcation_stats,
        fetch_load_status_stats,
        fetch_pricing_stats,
        fetch_carrier_end_state_stats,
        fetch_carrier_asked_transfer_over_total_transfer_attempts_stats,
        fetch_carrier_asked_transfer_over_total_call_attempts_stats,
        fetch_successfully_transferred_for_booking_stats,
        fetch_non_convertible_calls_with_carrier_not_qualified,
        fetch_non_convertible_calls_without_carrier_not_qualified,
        fetch_carrier_not_qualified_stats,
        fetch_total_calls_and_total_duration,
    )

    try:
        # Determine target date
        tz = ZoneInfo(org.timezone)
        if target_date:
            target = datetime.fromisoformat(target_date).date()
        else:
            # Default to yesterday
            now = datetime.now(tz)
            target = (now - timedelta(days=1)).date()

        target_str = target.isoformat()

        # Check if report already exists
        existing = get_daily_report(org.org_id, target_str)
        if existing:
            logger.info("Report already exists for %s on %s, skipping", org.name, target_str)
            return existing

        logger.info("Generating daily report for %s on %s (attempt %d)", org.name, target_str, retry_count + 1)

        # Calculate date range (full day in org's timezone)
        start_dt = datetime.combine(target, datetime.min.time(), tzinfo=tz)
        end_dt = start_dt + timedelta(days=1)
        start_date = start_dt.isoformat()
        end_date = end_dt.isoformat()

        # Temporarily set environment variables for the org
        original_org_id = os.environ.get("ORG_ID")
        original_node_id = os.environ.get("BROKER_NODE_PERSISTENT_ID")
        original_tz = os.environ.get("DEFAULT_TIMEZONE")

        try:
            os.environ["ORG_ID"] = org.org_id
            os.environ["BROKER_NODE_PERSISTENT_ID"] = org.node_persistent_id
            os.environ["DEFAULT_TIMEZONE"] = org.timezone

            # Fetch all the metrics
            call_stage = fetch_calls_ending_in_each_call_stage_stats(start_date, end_date)
            call_classification = fetch_call_classifcation_stats(start_date, end_date)
            load_status = fetch_load_status_stats(start_date, end_date)
            pricing = fetch_pricing_stats(start_date, end_date)
            carrier_end_state = fetch_carrier_end_state_stats(start_date, end_date)

            carrier_transfer_over_transfer_attempts = fetch_carrier_asked_transfer_over_total_transfer_attempts_stats(start_date, end_date)
            carrier_transfer_over_call_attempts = fetch_carrier_asked_transfer_over_total_call_attempts_stats(start_date, end_date)
            successfully_transferred = fetch_successfully_transferred_for_booking_stats(start_date, end_date)
            non_convertible_with_cnq = fetch_non_convertible_calls_with_carrier_not_qualified(start_date, end_date)
            non_convertible_without_cnq = fetch_non_convertible_calls_without_carrier_not_qualified(start_date, end_date)
            carrier_not_qualified = fetch_carrier_not_qualified_stats(start_date, end_date)
            total_calls_and_duration = fetch_total_calls_and_total_duration(start_date, end_date)

            # Calculate success rate
            success_rate = None
            if call_classification:
                total = sum(int(r.count) for r in call_classification if r is not None)
                success = sum(int(r.count) for r in call_classification if (r is not None and r.call_classification == "success"))
                success_rate = round((success / total) * 100.0, 2) if total else 0.0

            # Build report data structure
            report_data = {
                "date_range": {
                    "tz": org.timezone,
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "kpis": {
                    "total_calls": (total_calls_and_duration.total_calls if total_calls_and_duration else 0),
                    "classified_calls": (non_convertible_with_cnq.total_calls if non_convertible_with_cnq else 0),
                    "total_duration_hours": (round(total_calls_and_duration.total_duration / 3600.0, 2) if total_calls_and_duration else 0.0),
                    "avg_minutes_per_call": (total_calls_and_duration.avg_minutes_per_call if total_calls_and_duration else 0.0),
                    "success_rate_percent": success_rate,
                    "non_convertible_calls_with_carrier_not_qualified": (
                        {
                            "count": non_convertible_with_cnq.non_convertible_calls_count,
                            "total_calls": non_convertible_with_cnq.total_calls,
                            "percentage": non_convertible_with_cnq.non_convertible_calls_percentage,
                        }
                        if non_convertible_with_cnq
                        else None
                    ),
                    "non_convertible_calls_without_carrier_not_qualified": (
                        {
                            "count": non_convertible_without_cnq.non_convertible_calls_count,
                            "total_calls": non_convertible_without_cnq.total_calls,
                            "percentage": non_convertible_without_cnq.non_convertible_calls_percentage,
                        }
                        if non_convertible_without_cnq
                        else None
                    ),
                    "carrier_not_qualified": (
                        {
                            "count": carrier_not_qualified.carrier_not_qualified_count,
                            "total_calls": carrier_not_qualified.total_calls,
                            "percentage": carrier_not_qualified.carrier_not_qualified_percentage,
                        }
                        if carrier_not_qualified
                        else None
                    ),
                    "carrier_transfer_over_total_transfer_attempts": (
                        {
                            "carrier_asked_count": carrier_transfer_over_transfer_attempts.carrier_asked_count,
                            "total_transfer_attempts": carrier_transfer_over_transfer_attempts.total_transfer_attempts,
                            "carrier_asked_percentage": carrier_transfer_over_transfer_attempts.carrier_asked_percentage,
                        }
                        if carrier_transfer_over_transfer_attempts
                        else None
                    ),
                    "carrier_transfer_over_total_call_attempts": (
                        {
                            "carrier_asked_count": carrier_transfer_over_call_attempts.carrier_asked_count,
                            "total_call_attempts": carrier_transfer_over_call_attempts.total_call_attempts,
                            "carrier_asked_percentage": carrier_transfer_over_call_attempts.carrier_asked_percentage,
                        }
                        if carrier_transfer_over_call_attempts
                        else None
                    ),
                    "successfully_transferred_for_booking": (
                        {
                            "successfully_transferred_for_booking_count": successfully_transferred.successfully_transferred_for_booking_count,
                            "total_calls": successfully_transferred.total_calls,
                            "successfully_transferred_for_booking_percentage": successfully_transferred.successfully_transferred_for_booking_percentage,
                        }
                        if successfully_transferred
                        else None
                    ),
                },
                "breakdowns": {
                    "call_stage": [{"call_stage": r.call_stage, "count": r.count, "percentage": r.percentage} for r in (call_stage or [])],
                    "call_classification": [{"call_classification": r.call_classification, "count": r.count, "percentage": r.percentage} for r in (call_classification or [])],
                    "load_status": [{"load_status": r.load_status, "count": r.count, "total_calls": r.total_calls, "load_status_percentage": r.load_status_percentage} for r in (load_status or [])],
                    "pricing_notes": [{"pricing_notes": r.pricing_notes, "count": r.count, "percentage": r.percentage} for r in (pricing or [])],
                    "carrier_end_state": [{"carrier_end_state": r.carrier_end_state, "count": r.count, "percentage": r.percentage} for r in (carrier_end_state or [])],
                },
                "metadata": {
                    "org_id": org.org_id,
                    "org_name": org.name,
                    "generated_at": datetime.now(tz).isoformat(),
                },
            }

        finally:
            # Restore original environment variables
            if original_org_id is not None:
                os.environ["ORG_ID"] = original_org_id
            elif "ORG_ID" in os.environ:
                del os.environ["ORG_ID"]

            if original_node_id is not None:
                os.environ["BROKER_NODE_PERSISTENT_ID"] = original_node_id
            elif "BROKER_NODE_PERSISTENT_ID" in os.environ:
                del os.environ["BROKER_NODE_PERSISTENT_ID"]

            if original_tz is not None:
                os.environ["DEFAULT_TIMEZONE"] = original_tz
            elif "DEFAULT_TIMEZONE" in os.environ:
                del os.environ["DEFAULT_TIMEZONE"]

        # Save the report
        report = DailyReport(
            id=None,
            org_id=org.org_id,
            report_date=target_str,
            report_data=report_data,
        )
        saved_report = save_daily_report(report)
        logger.info("Successfully saved daily report for %s on %s", org.name, target_str)
        return saved_report

    except Exception as e:
        logger.exception("Failed to generate daily report for %s (attempt %d): %s", org.name, retry_count + 1, e)

        # Retry logic
        if retry_count < MAX_RETRIES - 1:
            logger.info("Retrying in %d seconds...", RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)
            return generate_daily_report_for_org(org, target_date, retry_count + 1)

        return None


def run_daily_report_job():
    """
    Job function that runs daily to generate reports for all active organizations.
    """
    logger.info("=" * 60)
    logger.info("Starting daily report generation job...")
    logger.info("=" * 60)

    try:
        ensure_db_initialized()
        organizations = get_all_organizations(active_only=True)

        if not organizations:
            logger.warning("No active organizations found, skipping daily report generation")
            log_scheduler_run("daily", "success", reports_generated=0)
            return

        success_count = 0
        fail_count = 0

        for org in organizations:
            result = generate_daily_report_for_org(org)
            if result:
                success_count += 1
            else:
                fail_count += 1

        status = "success" if fail_count == 0 else "partial"
        error_msg = f"{fail_count} failed" if fail_count > 0 else None

        log_scheduler_run("daily", status, reports_generated=success_count, error_message=error_msg)

        logger.info("=" * 60)
        logger.info("Daily report generation complete: %d successful, %d failed", success_count, fail_count)
        logger.info("=" * 60)

    except Exception as e:
        logger.exception("Error in daily report job: %s", e)
        log_scheduler_run("daily", "error", error_message=str(e))


def run_catchup_job():
    """
    Catch-up job that fills in any missing reports from the last N days.
    Runs on startup to ensure no gaps in data.
    """
    logger.info("=" * 60)
    logger.info("Starting catch-up job...")
    logger.info("=" * 60)

    try:
        ensure_db_initialized()
        organizations = get_all_organizations(active_only=True)
        catchup_days = get_catchup_days()

        if not organizations:
            logger.warning("No active organizations found, skipping catch-up")
            return

        total_generated = 0

        for org in organizations:
            # Pass org's timezone to ensure we only look for completed days
            missing_dates = get_missing_report_dates(org.org_id, days_back=catchup_days, timezone=org.timezone)

            if not missing_dates:
                logger.info("No missing reports for %s in the last %d days", org.name, catchup_days)
                continue

            logger.info("Found %d missing reports for %s: %s", len(missing_dates), org.name, missing_dates)

            for date_str in missing_dates:
                result = generate_daily_report_for_org(org, target_date=date_str)
                if result:
                    total_generated += 1
                    logger.info("Backfilled report for %s on %s", org.name, date_str)

        if total_generated > 0:
            log_scheduler_run("catchup", "success", reports_generated=total_generated)

        logger.info("=" * 60)
        logger.info("Catch-up job complete: %d reports generated", total_generated)
        logger.info("=" * 60)

    except Exception as e:
        logger.exception("Error in catch-up job: %s", e)
        log_scheduler_run("catchup", "error", error_message=str(e))


def start_scheduler():
    """Start the background scheduler."""
    if not is_scheduler_enabled():
        logger.info("Scheduler is disabled via SCHEDULER_ENABLED env var")
        return

    scheduler = get_scheduler()

    if scheduler.running:
        logger.info("Scheduler is already running")
        return

    hour, minute = get_schedule_time()

    # Add the daily job (runs at 6am Pacific Time)
    scheduler.add_job(
        run_daily_report_job,
        CronTrigger(hour=hour, minute=minute, timezone="America/Los_Angeles"),
        id="daily_report_job",
        name="Generate Daily Reports",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started - daily reports will run at %02d:%02d", hour, minute)

    # Run catch-up job immediately (in a non-blocking way)
    logger.info("Running catch-up job to fill any missing reports...")
    try:
        run_catchup_job()
    except Exception as e:
        logger.exception("Catch-up job failed: %s", e)


def stop_scheduler():
    """Stop the background scheduler."""
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def trigger_daily_report_now(org_id: Optional[str] = None, target_date: Optional[str] = None) -> dict:
    """
    Manually trigger daily report generation.

    Args:
        org_id: Optional specific org to generate for. If None, generates for all.
        target_date: Optional date string (YYYY-MM-DD). Defaults to yesterday.

    Returns:
        Dictionary with results summary
    """
    ensure_db_initialized()

    if org_id:
        org = get_organization(org_id)
        if not org:
            return {"success": False, "error": f"Organization {org_id} not found"}

        result = generate_daily_report_for_org(org, target_date)

        if result:
            log_scheduler_run("manual", "success", reports_generated=1)
        else:
            log_scheduler_run("manual", "error", error_message=f"Failed for {org_id}")

        return {
            "success": result is not None,
            "org_id": org_id,
            "date": target_date or "yesterday",
            "report_id": result.id if result else None,
        }
    else:
        # Generate for all orgs
        organizations = get_all_organizations(active_only=True)
        results = []
        success_count = 0

        for org in organizations:
            result = generate_daily_report_for_org(org, target_date)
            success = result is not None
            if success:
                success_count += 1
            results.append({
                "org_id": org.org_id,
                "org_name": org.name,
                "success": success,
            })

        log_scheduler_run("manual", "success" if success_count == len(organizations) else "partial",
                         reports_generated=success_count)

        return {
            "success": all(r["success"] for r in results),
            "results": results,
            "date": target_date or "yesterday",
        }


def backfill_reports(org_id: str, start_date: str, end_date: str) -> dict:
    """
    Backfill historical reports for a date range.

    Args:
        org_id: Organization ID to backfill for
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Dictionary with results summary
    """
    from datetime import datetime, timedelta

    ensure_db_initialized()

    org = get_organization(org_id)
    if not org:
        return {"success": False, "error": f"Organization {org_id} not found"}

    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()

    if start > end:
        return {"success": False, "error": "start_date must be before end_date"}

    results = []
    current = start
    success_count = 0

    while current <= end:
        date_str = current.isoformat()
        result = generate_daily_report_for_org(org, date_str)
        success = result is not None
        if success:
            success_count += 1
        results.append({
            "date": date_str,
            "success": success,
        })
        current += timedelta(days=1)

    log_scheduler_run("backfill", "success" if success_count == len(results) else "partial",
                     reports_generated=success_count)

    return {
        "success": True,
        "org_id": org_id,
        "start_date": start_date,
        "end_date": end_date,
        "total_days": len(results),
        "successful": success_count,
        "failed": len(results) - success_count,
        "results": results,
    }


def get_scheduler_health() -> dict:
    """
    Get comprehensive scheduler health information.
    """
    scheduler = get_scheduler()
    hour, minute = get_schedule_time()

    # Get job info
    jobs = []
    next_run = None
    if scheduler.running:
        for job in scheduler.get_jobs():
            job_next_run = job.next_run_time.isoformat() if job.next_run_time else None
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run_time": job_next_run,
            })
            if job_next_run and (next_run is None or job_next_run < next_run):
                next_run = job_next_run

    # Get last successful run
    last_success = get_last_successful_run()

    # Get recent runs
    recent_runs = get_recent_scheduler_runs(limit=5)

    return {
        "enabled": is_scheduler_enabled(),
        "running": scheduler.running,
        "scheduled_time": f"{hour:02d}:{minute:02d} PT",
        "timezone": "America/Los_Angeles",
        "next_run": next_run,
        "jobs": jobs,
        "last_successful_run": last_success,
        "recent_runs": recent_runs,
        "catchup_days": get_catchup_days(),
    }
