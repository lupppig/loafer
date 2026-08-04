from __future__ import annotations

import pytest

from loafer.core.run_state import (
    BatchState,
    RunState,
    StageState,
    require_batch_transition,
    require_run_transition,
    require_stage_transition,
)
from loafer.exceptions import InvalidStateTransitionError


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (RunState.QUEUED, RunState.CLAIMED),
        (RunState.CLAIMED, RunState.RUNNING),
        (RunState.RUNNING, RunState.SUCCEEDED),
        (RunState.RUNNING, RunState.RETRY_WAIT),
    ],
)
def test_valid_run_transitions(current: RunState, target: RunState) -> None:
    require_run_transition(current, target)


def test_terminal_run_cannot_restart() -> None:
    with pytest.raises(InvalidStateTransitionError, match="succeeded -> running"):
        require_run_transition(RunState.SUCCEEDED, RunState.RUNNING)


def test_stage_and_batch_machines_reject_impossible_commits() -> None:
    with pytest.raises(InvalidStateTransitionError, match="pending -> succeeded"):
        require_stage_transition(StageState.PENDING, StageState.SUCCEEDED)
    with pytest.raises(InvalidStateTransitionError, match="pending -> committed"):
        require_batch_transition(BatchState.PENDING, BatchState.COMMITTED)


def test_idempotent_state_transition_is_allowed() -> None:
    require_run_transition(RunState.RUNNING, RunState.RUNNING)
