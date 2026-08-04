"""Pure durable-execution state machines and retry policy."""

from __future__ import annotations

from enum import StrEnum

from loafer.exceptions import InvalidStateTransitionError


class RunState(StrEnum):
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    CANCELLING = "cancelling"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class BatchState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMMITTED = "committed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RetryCategory(StrEnum):
    INFRASTRUCTURE = "infrastructure"
    FAILED_BATCH = "failed_batch"
    MANUAL_RERUN = "manual_rerun"
    BACKFILL = "backfill"


_RUN_TRANSITIONS: dict[RunState, frozenset[RunState]] = {
    RunState.QUEUED: frozenset({RunState.CLAIMED, RunState.CANCELLED}),
    RunState.CLAIMED: frozenset(
        {RunState.RUNNING, RunState.CANCELLING, RunState.RETRY_WAIT, RunState.FAILED}
    ),
    RunState.RUNNING: frozenset(
        {
            RunState.CANCELLING,
            RunState.RETRY_WAIT,
            RunState.SUCCEEDED,
            RunState.FAILED,
            RunState.CANCELLED,
        }
    ),
    RunState.CANCELLING: frozenset({RunState.CANCELLED, RunState.RETRY_WAIT, RunState.FAILED}),
    RunState.RETRY_WAIT: frozenset({RunState.QUEUED, RunState.CANCELLED}),
    RunState.SUCCEEDED: frozenset(),
    RunState.FAILED: frozenset(),
    RunState.CANCELLED: frozenset(),
}

_STAGE_TRANSITIONS: dict[StageState, frozenset[StageState]] = {
    StageState.PENDING: frozenset({StageState.RUNNING, StageState.CANCELLED, StageState.SKIPPED}),
    StageState.RUNNING: frozenset({StageState.SUCCEEDED, StageState.FAILED, StageState.CANCELLED}),
    StageState.SUCCEEDED: frozenset(),
    StageState.FAILED: frozenset(),
    StageState.CANCELLED: frozenset(),
    StageState.SKIPPED: frozenset(),
}

_BATCH_TRANSITIONS: dict[BatchState, frozenset[BatchState]] = {
    BatchState.PENDING: frozenset({BatchState.RUNNING, BatchState.CANCELLED}),
    BatchState.RUNNING: frozenset({BatchState.COMMITTED, BatchState.FAILED, BatchState.CANCELLED}),
    BatchState.COMMITTED: frozenset(),
    BatchState.FAILED: frozenset(),
    BatchState.CANCELLED: frozenset(),
}


def require_run_transition(current: RunState, target: RunState) -> None:
    """Reject a non-idempotent transition not allowed by the run machine."""
    _require_transition("run", current, target, _RUN_TRANSITIONS)


def require_stage_transition(current: StageState, target: StageState) -> None:
    """Reject a non-idempotent transition not allowed by the stage machine."""
    _require_transition("stage", current, target, _STAGE_TRANSITIONS)


def require_batch_transition(current: BatchState, target: BatchState) -> None:
    """Reject a non-idempotent transition not allowed by the batch machine."""
    _require_transition("batch", current, target, _BATCH_TRANSITIONS)


def _require_transition(
    machine: str,
    current: StrEnum,
    target: StrEnum,
    transitions: dict[StrEnum, frozenset[StrEnum]],
) -> None:
    if current == target:
        return
    if target not in transitions[current]:
        raise InvalidStateTransitionError(
            f"invalid {machine} state transition: {current.value} -> {target.value}"
        )
