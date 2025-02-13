# Test-related imports go last:
import pytest

from snapred.backend.error.LiveDataState import LiveDataState


def test_LiveDataState():
    # all properties are initialized correctly
    state = LiveDataState("message", LiveDataState.Type.RUN_START, "12345", "0")
    assert state.message == "message"
    assert state.transition == LiveDataState.Type.RUN_START
    assert state.endRunNumber == "12345"
    assert state.startRunNumber == "0"


def test_LiveDataState_runStateTransition_start():
    # a transition from "no run" state is recognized as a start of run
    state = LiveDataState.runStateTransition("12345", "0")
    assert state.message.startswith("start of run")
    assert "12345" in state.message
    assert "0" not in state.message


def test_LiveDataState_runStateTransition_end():
    # a transition to a "no run" state is recognized as an end of run
    state = LiveDataState.runStateTransition("0", "12345")
    assert state.message.startswith("end of run")
    assert "12345" in state.message
    assert "0" not in state.message


def test_LiveDataState_runStateTransition_gap():
    # a gap in run-number values, without an intervening "no run" state, is recognized
    state = LiveDataState.runStateTransition("12346", "12345")
    assert state.message.startswith("run-number gap:")
    assert "12346" in state.message
    assert "12345" in state.message


def test_LiveDataState_runStateTransition_unexpected():
    # all run numbers must be greater than zero
    with pytest.raises(ValueError, match=r"unexpected run-state transition:.*"):
        LiveDataState.runStateTransition("-1", "-2")


def test_LiveDataState_runStateTransition_no_transition():
    # start and end run numbers cannot be the same
    with pytest.raises(ValueError, match=r"not a run-state transition:.*"):
        LiveDataState.runStateTransition("12345", "12345")


def test_LiveDataState_runStateTransition_invalid_transition():
    # run numbers cannot decrease, except to zero
    with pytest.raises(ValueError, match=r"not a run-state transition:.*"):
        LiveDataState.runStateTransition("12344", "12345")
