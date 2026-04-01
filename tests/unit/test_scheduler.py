"""Tests for PipelineScheduler."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loafer.exceptions import SchedulerError


class TestPipelineScheduler:
    """Tests for the PipelineScheduler class."""

    @pytest.fixture
    def scheduler(self, tmp_path: Path) -> Any:
        """Create a scheduler with a temporary SQLite store."""
        from loafer.scheduler import PipelineScheduler

        db_path = tmp_path / "test_jobs.sqlite"
        return PipelineScheduler(db_url=f"sqlite:///{db_path}")

    def test_start_and_stop(self, scheduler: Any) -> None:
        assert not scheduler.running
        scheduler.start()
        assert scheduler.running
        scheduler.stop()
        assert not scheduler.running

    def test_add_schedule_with_cron(self, scheduler: Any) -> None:
        job_id = scheduler.add_schedule(
            config_path="test.yaml",
            cron="0 9 * * *",
            schedule_id="test1",
        )
        assert job_id == "test1"
        jobs = scheduler.list_schedules()
        assert len(jobs) == 1
        assert jobs[0]["id"] == "test1"
        assert jobs[0]["config_path"] == "test.yaml"

    def test_add_schedule_with_interval(self, scheduler: Any) -> None:
        job_id = scheduler.add_schedule(
            config_path="test.yaml",
            interval="1h",
            schedule_id="test2",
        )
        assert job_id == "test2"
        jobs = scheduler.list_schedules()
        assert len(jobs) == 1

    def test_add_schedule_without_cron_or_interval_raises(self, scheduler: Any) -> None:
        with pytest.raises(SchedulerError, match="Either 'cron' or 'interval'"):
            scheduler.add_schedule(config_path="test.yaml")

    def test_add_schedule_invalid_cron_raises(self, scheduler: Any) -> None:
        with pytest.raises(SchedulerError, match="Invalid cron"):
            scheduler.add_schedule(config_path="test.yaml", cron="bad")

    def test_add_schedule_invalid_interval_raises(self, scheduler: Any) -> None:
        with pytest.raises(SchedulerError, match="Invalid interval"):
            scheduler.add_schedule(config_path="test.yaml", interval="bad")

    def test_remove_schedule(self, scheduler: Any) -> None:
        scheduler.add_schedule(config_path="test.yaml", cron="0 9 * * *", schedule_id="to_remove")
        jobs = scheduler.list_schedules()
        assert len(jobs) == 1
        scheduler.remove_schedule("to_remove")
        jobs = scheduler.list_schedules()
        assert len(jobs) == 0

    def test_remove_nonexistent_schedule_raises(self, scheduler: Any) -> None:
        with pytest.raises(SchedulerError, match="not found"):
            scheduler.remove_schedule("nonexistent")

    def test_replace_schedule(self, scheduler: Any) -> None:
        scheduler.add_schedule(config_path="test.yaml", cron="0 9 * * *", schedule_id="replace_me")
        scheduler.add_schedule(
            config_path="other.yaml",
            cron="0 12 * * *",
            schedule_id="replace_me",
            replace=True,
        )
        jobs = scheduler.list_schedules()
        assert len(jobs) == 1
        assert jobs[0]["config_path"] == "other.yaml"

    def test_list_schedules_empty(self, scheduler: Any) -> None:
        jobs = scheduler.list_schedules()
        assert jobs == []

    def test_multiple_schedules(self, scheduler: Any) -> None:
        scheduler.add_schedule(config_path="a.yaml", cron="0 9 * * *", schedule_id="job_a")
        scheduler.add_schedule(config_path="b.yaml", interval="30m", schedule_id="job_b")
        scheduler.add_schedule(config_path="c.yaml", cron="0 18 * * *", schedule_id="job_c")
        jobs = scheduler.list_schedules()
        assert len(jobs) == 3
        ids = {j["id"] for j in jobs}
        assert ids == {"job_a", "job_b", "job_c"}

    def test_auto_generated_job_id(self, scheduler: Any) -> None:
        job_id = scheduler.add_schedule(config_path="test.yaml", cron="0 9 * * *")
        assert len(job_id) == 12
        assert job_id.isalnum()

    def test_interval_parsing_seconds(self, scheduler: Any) -> None:
        trigger = scheduler._parse_interval("60s")
        assert trigger.interval.total_seconds() == 60

    def test_interval_parsing_minutes(self, scheduler: Any) -> None:
        trigger = scheduler._parse_interval("30m")
        assert trigger.interval.total_seconds() == 30 * 60

    def test_interval_parsing_hours(self, scheduler: Any) -> None:
        trigger = scheduler._parse_interval("2h")
        assert trigger.interval.total_seconds() == 2 * 3600

    def test_interval_parsing_days(self, scheduler: Any) -> None:
        trigger = scheduler._parse_interval("7d")
        assert trigger.interval.total_seconds() == 7 * 86400

    def test_interval_parsing_weeks(self, scheduler: Any) -> None:
        trigger = scheduler._parse_interval("1w")
        assert trigger.interval.total_seconds() == 7 * 86400

    def test_persistence_across_scheduler_restarts(self, tmp_path: Path) -> None:
        """Jobs should persist when the scheduler is stopped and restarted."""
        from loafer.scheduler import PipelineScheduler

        db_path = tmp_path / "persist_jobs.sqlite"
        db_url = f"sqlite:///{db_path}"

        # Create scheduler and add a job
        scheduler1 = PipelineScheduler(db_url=db_url)
        scheduler1.add_schedule(
            config_path="persist.yaml", cron="0 9 * * *", schedule_id="persist_job"
        )
        scheduler1.stop()

        # Create a new scheduler with the same DB
        scheduler2 = PipelineScheduler(db_url=db_url)
        jobs = scheduler2.list_schedules()
        assert len(jobs) == 1
        assert jobs[0]["id"] == "persist_job"
        assert jobs[0]["config_path"] == "persist.yaml"
        scheduler2.stop()

    def test_export_and_import_jobs(self, tmp_path: Path) -> None:
        """Jobs should be exportable and importable via JSON."""
        from loafer.scheduler import PipelineScheduler

        db_path = tmp_path / "export_jobs.sqlite"
        db_url = f"sqlite:///{db_path}"

        scheduler = PipelineScheduler(db_url=db_url)
        scheduler.add_schedule(config_path="a.yaml", cron="0 9 * * *", schedule_id="job_a")
        scheduler.add_schedule(config_path="b.yaml", interval="1h", schedule_id="job_b")

        export_path = tmp_path / "jobs.json"
        scheduler.export_jobs(export_path)

        # Import into a fresh scheduler
        db_path2 = tmp_path / "import_jobs.sqlite"
        db_url2 = f"sqlite:///{db_path2}"
        scheduler2 = PipelineScheduler(db_url=db_url2)
        count = scheduler2.import_jobs(export_path)

        assert count == 2
        jobs = scheduler2.list_schedules()
        assert len(jobs) == 2
