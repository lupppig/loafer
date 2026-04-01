"""Pipeline scheduler — APScheduler-based cron scheduling.

Manages recurring pipeline runs via cron or interval triggers.
Jobs are persisted in a SQLite store so they survive restarts.
"""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from loafer.exceptions import SchedulerError
from loafer.runner import run_pipeline

logger = logging.getLogger("loafer.scheduler")

_DEFAULT_DB = "sqlite:///loafer_jobs.sqlite"


def _run_pipeline_job(config_path: str, name: str = "") -> None:
    """Execute a pipeline run, called by the scheduler."""
    run_id = uuid.uuid4().hex[:12]
    display = f"{name} ({config_path})" if name else config_path
    logger.info("Starting scheduled run %s for %s", run_id, display)
    try:
        run_pipeline(config_path=config_path, verbose=False)
        logger.info("Completed scheduled run %s", run_id)
    except Exception as exc:
        logger.error("Scheduled run %s failed: %s", run_id, exc)
        raise


class PipelineScheduler:
    """Wraps APScheduler to manage recurring pipeline runs."""

    def __init__(self, db_url: str | None = None, timezone: str = "UTC") -> None:
        self._scheduler = BackgroundScheduler(
            jobstores={
                "default": SQLAlchemyJobStore(url=db_url or _DEFAULT_DB),
            },
            timezone=timezone,
        )
        self._scheduler.add_listener(self._on_job_executed, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    @property
    def running(self) -> bool:
        return self._scheduler.running

    def start(self, paused: bool = False) -> None:
        """Start the scheduler."""
        if self.running:
            return
        self._scheduler.start(paused=paused)
        logger.info("Scheduler started")

    def stop(self, wait: bool = True) -> None:
        """Stop the scheduler."""
        if not self.running:
            return
        self._scheduler.shutdown(wait=wait)
        logger.info("Scheduler stopped")

    def add_schedule(
        self,
        config_path: str,
        schedule_id: str | None = None,
        cron: str | None = None,
        interval: str | None = None,
        replace: bool = False,
        name: str = "",
    ) -> str:
        """Schedule a pipeline to run on a cron or interval trigger.

        Args:
            config_path: Path to the pipeline YAML config.
            schedule_id: Unique ID for the job. Auto-generated if not given.
            cron: Cron expression (e.g. "0 9 * * *" for daily at 9am UTC).
            interval: Interval string (e.g. "1h", "30m", "1d").
            replace: If True, replace existing job with same ID.
            name: Human-readable name for the pipeline.

        Returns:
            The job ID.
        """
        if not cron and not interval:
            raise SchedulerError("Either 'cron' or 'interval' must be specified")

        job_id = schedule_id or uuid.uuid4().hex[:12]

        trigger = self._build_trigger(cron, interval)

        # Start scheduler briefly to initialize the job store
        was_running = self.running
        if not was_running:
            self._scheduler.start(paused=True)

        if replace:
            try:
                self._scheduler.remove_job(job_id)
            except Exception:
                pass

        self._scheduler.add_job(
            _run_pipeline_job,
            trigger=trigger,
            args=[config_path, name],
            id=job_id,
            replace_existing=replace,
            misfire_grace_time=300,
        )

        # Stop if we weren't running before (persists the job store)
        if not was_running:
            self._scheduler.shutdown(wait=False)

        logger.info("Scheduled job %s (%s) for config %s", job_id, name or "unnamed", config_path)
        return job_id

    def remove_schedule(self, schedule_id: str) -> None:
        """Remove a scheduled job."""
        was_running = self.running
        if not was_running:
            self._scheduler.start(paused=True)
        try:
            self._scheduler.remove_job(schedule_id)
            logger.info("Removed job %s", schedule_id)
        except Exception as exc:
            raise SchedulerError(f"Job '{schedule_id}' not found") from exc
        finally:
            if not was_running:
                self._scheduler.shutdown(wait=False)

    def list_schedules(self) -> list[dict[str, Any]]:
        """Return all scheduled jobs as dicts."""
        was_running = self.running
        if not was_running:
            self._scheduler.start(paused=True)
        try:
            jobs = self._scheduler.get_jobs()
            result = []
            for job in jobs:
                trigger_info = str(job.trigger) if job.trigger else "once"
                args = job.args or []
                config_path = args[0] if len(args) > 0 else ""
                name = args[1] if len(args) > 1 else ""
                result.append(
                    {
                        "id": job.id,
                        "name": name,
                        "config_path": config_path,
                        "trigger": trigger_info,
                        "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                        "paused": job.next_run_time is None,
                    }
                )
            return result
        finally:
            if not was_running:
                self._scheduler.shutdown(wait=False)

    def pause_job(self, schedule_id: str) -> None:
        """Pause a scheduled job."""
        try:
            self._scheduler.pause_job(schedule_id)
        except Exception as exc:
            raise SchedulerError(f"Job '{schedule_id}' not found") from exc

    def resume_job(self, schedule_id: str) -> None:
        """Resume a paused job."""
        try:
            self._scheduler.resume_job(schedule_id)
        except Exception as exc:
            raise SchedulerError(f"Job '{schedule_id}' not found") from exc

    def _build_trigger(self, cron: str | None, interval: str | None) -> Any:
        """Build an APScheduler trigger from cron or interval string."""
        from apscheduler.triggers.cron import CronTrigger

        if cron:
            parts = cron.split()
            if len(parts) != 5:
                raise SchedulerError(
                    f"Invalid cron expression: '{cron}'. Expected 5 fields: minute hour day month day_of_week"
                )
            return CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4],
            )

        if interval:
            return self._parse_interval(interval)

        raise SchedulerError("Either 'cron' or 'interval' must be specified")

    def _parse_interval(self, spec: str) -> Any:
        """Parse interval string like '1h', '30m', '1d', '2w' into an IntervalTrigger."""
        from apscheduler.triggers.interval import IntervalTrigger

        spec = spec.strip().lower()
        if not spec:
            raise SchedulerError(
                f"Invalid interval: '{spec}'. Use format like '30m', '1h', '1d', '2w'"
            )

        suffix = spec[-1]
        try:
            value = int(spec[:-1])
        except ValueError:
            raise SchedulerError(
                f"Invalid interval: '{spec}'. Use format like '30m', '1h', '1d', '2w'"
            )

        if suffix == "s":
            return IntervalTrigger(seconds=value)
        if suffix == "m":
            return IntervalTrigger(minutes=value)
        if suffix == "h":
            return IntervalTrigger(hours=value)
        if suffix == "d":
            return IntervalTrigger(days=value)
        if suffix == "w":
            return IntervalTrigger(weeks=value)

        raise SchedulerError(f"Invalid interval: '{spec}'. Use format like '30m', '1h', '1d', '2w'")

    def _on_job_executed(self, event: Any) -> None:
        """Log job execution events."""
        if event.exception:
            logger.error("Job %s failed: %s", event.job_id, event.exception)
        else:
            logger.info("Job %s completed successfully", event.job_id)

    def export_jobs(self, path: str | Path) -> None:
        """Export scheduled jobs to a JSON file."""
        jobs = self.list_schedules()
        Path(path).write_text(json.dumps(jobs, indent=2))
        logger.info("Exported %d jobs to %s", len(jobs), path)

    def import_jobs(self, path: str | Path) -> int:
        """Import jobs from a JSON file. Returns count of imported jobs.

        Note: Imported jobs are added with a default daily cron (0 9 * * *).
        Use add_schedule() for fine-grained control over triggers.
        """
        data = json.loads(Path(path).read_text())
        count = 0
        for job in data:
            try:
                self.add_schedule(
                    config_path=job["config_path"],
                    schedule_id=job["id"],
                    cron="0 9 * * *",
                    replace=True,
                    name=job.get("name", ""),
                )
                count += 1
            except SchedulerError:
                logger.warning("Failed to import job %s", job.get("id", "unknown"))
        logger.info("Imported %d jobs from %s", count, path)
        return count
