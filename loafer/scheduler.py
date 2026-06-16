"""Pipeline scheduler — APScheduler-based cron scheduling.

Manages recurring pipeline runs via cron or interval triggers.
Jobs are persisted in a SQLite store so they survive restarts.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from loafer.exceptions import SchedulerError
from loafer.runner import run_pipeline

logger = logging.getLogger("loafer.scheduler")

# Everything lives under ~/.loafer so the jobstore, logs, and run-state are
# resolved by absolute path — independent of the working directory at
# schedule-time vs start-time. A relative jobstore meant the daemon read a
# different DB than `loafer schedule` wrote to (BUG-4).
_LOAFER_DIR = Path.home() / ".loafer"
_DB_PATH = _LOAFER_DIR / "jobs.db"
_LOG_PATH = _LOAFER_DIR / "scheduler.log"
_RUN_STATE_PATH = _LOAFER_DIR / "run_state.json"


def _default_db_url() -> str:
    """Absolute SQLite jobstore URL, creating ~/.loafer if needed."""
    _LOAFER_DIR.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{_DB_PATH}"


def configure_file_logging(level: int = logging.INFO) -> Path:
    """Route the scheduler logger to ~/.loafer/scheduler.log.

    Without this the logger had no handler pointing at the log file, so all
    job execution and errors were invisible — the worst failure mode for an
    unattended scheduler (BUG-4). Idempotent: won't add a duplicate handler.
    """
    _LOAFER_DIR.mkdir(parents=True, exist_ok=True)
    # FileHandler stores the absolute path in baseFilename; match against it
    # so repeated calls (start, then status) don't stack duplicate handlers.
    expected = str(_LOG_PATH.resolve())
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == expected:
            return _LOG_PATH
    file_handler = logging.FileHandler(_LOG_PATH)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(file_handler)
    logger.setLevel(level)
    logger.propagate = False
    return _LOG_PATH


def _record_run(job_id: str, status: str) -> None:
    """Persist the last-run timestamp/status for a job to the run-state sidecar.

    APScheduler doesn't track last-run natively, so list-schedules has nothing
    to show. We keep a tiny JSON sidecar updated from the job-event listener.
    """
    try:
        _LOAFER_DIR.mkdir(parents=True, exist_ok=True)
        state: dict[str, Any] = {}
        if _RUN_STATE_PATH.exists():
            try:
                state = json.loads(_RUN_STATE_PATH.read_text())
            except (ValueError, OSError):
                state = {}
        state[job_id] = {
            "last_run": datetime.now(UTC).isoformat(),
            "last_status": status,
        }
        _RUN_STATE_PATH.write_text(json.dumps(state, indent=2))
    except OSError as exc:  # pragma: no cover - best-effort bookkeeping
        logger.warning("Could not record run state for %s: %s", job_id, exc)


def _read_run_state() -> dict[str, Any]:
    """Return the last-run sidecar, or an empty mapping if absent/corrupt."""
    if not _RUN_STATE_PATH.exists():
        return {}
    try:
        data = json.loads(_RUN_STATE_PATH.read_text())
        return data if isinstance(data, dict) else {}
    except (ValueError, OSError):
        return {}


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
                "default": SQLAlchemyJobStore(url=db_url or _default_db_url()),
            },
            timezone=timezone,
        )
        self._scheduler.add_listener(self._on_job_executed, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    @property
    def running(self) -> bool:
        return bool(self._scheduler.running)

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

    def run_forever(self, poll_interval: float = 1.0) -> None:
        """Start the scheduler and block until interrupted.

        This is the entrypoint the daemon runs. The previous foreground/daemon
        paths started a BackgroundScheduler and returned immediately, so the
        process exited and no jobs ever fired (BUG-4). Blocking here keeps the
        scheduler thread alive so triggers actually execute.
        """
        import time

        self.start()
        try:
            while True:
                time.sleep(poll_interval)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            self.stop()

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
            run_state = _read_run_state()
            result = []
            for job in jobs:
                trigger_info = str(job.trigger) if job.trigger else "once"
                args = job.args or []
                config_path = args[0] if len(args) > 0 else ""
                name = args[1] if len(args) > 1 else ""
                job_runs = run_state.get(job.id, {})
                result.append(
                    {
                        "id": job.id,
                        "name": name,
                        "config_path": config_path,
                        "trigger": trigger_info,
                        "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                        "paused": job.next_run_time is None,
                        "last_run": job_runs.get("last_run"),
                        "last_status": job_runs.get("last_status"),
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
        """Log job execution events and record last-run state."""
        if event.exception:
            logger.error("Job %s failed: %s", event.job_id, event.exception)
            _record_run(event.job_id, "failed")
        else:
            logger.info("Job %s completed successfully", event.job_id)
            _record_run(event.job_id, "success")

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
