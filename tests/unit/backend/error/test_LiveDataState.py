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


def test_LiveDataState_type_enum_members():
    # LiveDataState.Type has the correct set of members.
    expected = {"UNSET", "RUN_START", "RUN_PAUSE", "RUN_ERROR", "RUN_END", "RUN_GAP"}
    actual = {t.name for t in LiveDataState.Type}
    assert actual == expected


def test_LiveDataState_model_requires_all_fields():
    # Model raises when required fields are missing.
    with pytest.raises(Exception):
        LiveDataState.Model()


def test_LiveDataState_validator_same_run_ok_for_pause():
    # Same endRunNumber and startRunNumber are allowed for RUN_PAUSE.
    model = LiveDataState.Model(
        message="pause",
        transition=LiveDataState.Type.RUN_PAUSE,
        endRunNumber="12345",
        startRunNumber="12345",
    )
    assert model.endRunNumber == "12345"


def test_LiveDataState_validator_same_run_ok_for_error():
    # Same endRunNumber and startRunNumber are allowed for RUN_ERROR.
    model = LiveDataState.Model(
        message="error",
        transition=LiveDataState.Type.RUN_ERROR,
        endRunNumber="12345",
        startRunNumber="12345",
    )
    assert model.endRunNumber == "12345"


def test_LiveDataState_validator_decreasing_raises():
    # Decreasing non-zero run numbers raise ValueError.
    with pytest.raises((ValueError, Exception), match=r"Not a valid run-state transition:.*"):
        LiveDataState.Model(
            message="bad",
            transition=LiveDataState.Type.RUN_GAP,
            endRunNumber="12344",
            startRunNumber="12345",
        )


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


def test_LiveDataState_runStateTransition_pause():
    # equal run numbers create a RUN_PAUSE state (not a raise)
    state = LiveDataState.runStateTransition("12345", "12345")
    assert state.transition == LiveDataState.Type.RUN_PAUSE
    assert "12345" in state.message


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


def test_LiveDataState_runStateTransition_invalid_transition():
    # run numbers cannot decrease, except to zero
    with pytest.raises((ValueError, Exception), match=r"Not a valid run-state transition:.*"):
        LiveDataState.runStateTransition("12344", "12345")


def test_LiveDataState_runError():
    # runError() creates a RUN_ERROR state with the given run number.
    state = LiveDataState.runError("12345")
    assert state.transition == LiveDataState.Type.RUN_ERROR
    assert "12345" in state.message
    assert state.endRunNumber == "12345"
    assert state.startRunNumber == "12345"


def test_LiveDataState_runError_accepts_int():
    # runError() converts an integer run number to string.
    state = LiveDataState.runError(12345)
    assert state.endRunNumber == "12345"
    assert state.startRunNumber == "12345"


def test_LiveDataState_parse_raw():
    # parse_raw() reconstructs a LiveDataState from its JSON model representation.
    original = LiveDataState.runError("12345")
    json_str = original.model.model_dump_json()
    parsed = LiveDataState.parse_raw(json_str)
    assert parsed.transition == original.transition
    assert parsed.endRunNumber == original.endRunNumber
    assert parsed.startRunNumber == original.startRunNumber
    assert parsed.message == original.message

