"""Worker roles and the transport names derived from them."""

from __future__ import annotations

from enum import StrEnum

_SUBJECT_ROOT = "loafer.jobs"
_STREAM_NAME = "LOAFER_JOBS"


class WorkerRole(StrEnum):
    """One isolated execution pool.

    Roles exist so an expensive workload cannot consume the capacity of a
    cheap one. Document and browser pools use the shared pipeline executor;
    their specialized extraction and crawling capabilities land independently.
    """

    SCHEDULER = "scheduler"
    ETL = "etl"
    DOCUMENT = "document"
    BROWSER = "browser"

    @property
    def subject(self) -> str:
        """Return the transport subject carrying this role's job ids."""
        return f"{_SUBJECT_ROOT}.{self.value}"

    @property
    def durable_name(self) -> str:
        """Return the durable consumer name for this role."""
        return f"loafer-{self.value}"


def stream_name() -> str:
    """Return the single stream that spans every role subject."""
    return _STREAM_NAME


def stream_subjects() -> tuple[str, ...]:
    """Return every subject the job stream must capture."""
    return tuple(role.subject for role in WorkerRole)


def executable_roles() -> tuple[WorkerRole, ...]:
    """Return the roles that currently have a bound run executor."""
    return (WorkerRole.ETL, WorkerRole.DOCUMENT, WorkerRole.BROWSER)


__all__ = [
    "WorkerRole",
    "executable_roles",
    "stream_name",
    "stream_subjects",
]
