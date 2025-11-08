# Test-related imports go last:
from unittest import mock

import pytest

from snapred.backend.error.RunStatus import RunStatus


# ---- SNS-specific PV names used by RunStatus.from_run ----
_PV_SCAN_ABORT = "BL3:Exp:ScanAbort"
_PV_SCAN_ABORT_ALT = "BL3:Exp:IM:ScanAbort"
_PV_PAUSE = "pause"
_PV_DET_STATUS = "BL3:Exp:Det:Status"


def _make_run(
    scan_abort=None,
    scan_abort_alt=None,
    has_end_time=False,
    pause=None,
    det_status=None,
):
    """Helper: build a mock mantid.api.Run with configurable SNS log properties.

    Parameters
    ----------
    scan_abort : last value for 'BL3:Exp:ScanAbort', or None if not present.
    scan_abort_alt : last value for 'BL3:Exp:IM:ScanAbort', or None if not present.
    has_end_time : whether the 'end_time' property is present.
    pause : last value for 'pause' log, or None if not present.
    det_status : last string value for 'BL3:Exp:Det:Status', or None if not present.
    """
    run = mock.Mock()

    prop_values = {}
    if scan_abort is not None:
        prop_values[_PV_SCAN_ABORT] = [scan_abort]
    if scan_abort_alt is not None:
        prop_values[_PV_SCAN_ABORT_ALT] = [scan_abort_alt]
    if has_end_time:
        prop_values["end_time"] = ["2026-04-09T00:00:00"]
    if pause is not None:
        prop_values[_PV_PAUSE] = [pause]
    if det_status is not None:
        prop_values[_PV_DET_STATUS] = [det_status]

    def _has_property(name):
        return name in prop_values

    def _get_property(name):
        prop = mock.Mock()
        prop.value = prop_values.get(name, [])
        return prop

    run.hasProperty.side_effect = _has_property
    run.getProperty.side_effect = _get_property
    return run


# --- StrEnum basics ---

def test_RunStatus_enum_members():
    expected = {"STOPPED", "PAUSED", "RUNNING", "ERROR"}
    actual = {s.name for s in RunStatus}
    assert actual == expected


def test_RunStatus_string_values():
    assert str(RunStatus.RUNNING) == "RUNNING"
    assert str(RunStatus.STOPPED) == "STOPPED"
    assert str(RunStatus.PAUSED) == "PAUSED"
    assert str(RunStatus.ERROR) == "ERROR"


def test_RunStatus_from_string():
    assert RunStatus("RUNNING") == RunStatus.RUNNING
    assert RunStatus("STOPPED") == RunStatus.STOPPED
    assert RunStatus("PAUSED") == RunStatus.PAUSED
    assert RunStatus("ERROR") == RunStatus.ERROR


def test_RunStatus_invalid_string_raises():
    with pytest.raises(ValueError):
        RunStatus("UNKNOWN_STATUS")


# --- from_run: step 1 — abort/error detection ---

def test_from_run_error_when_scan_abort_pv_true():
    """BL3:Exp:ScanAbort=True → ERROR."""
    run = _make_run(scan_abort=True)
    assert RunStatus.from_run(run) == RunStatus.ERROR


def test_from_run_error_when_scan_abort_pv_one():
    """BL3:Exp:ScanAbort=1 (truthy integer) → ERROR."""
    run = _make_run(scan_abort=1)
    assert RunStatus.from_run(run) == RunStatus.ERROR


def test_from_run_error_when_alt_scan_abort_pv_true():
    """BL3:Exp:IM:ScanAbort=True → ERROR (when primary ScanAbort not present)."""
    run = _make_run(scan_abort_alt=True)
    assert RunStatus.from_run(run) == RunStatus.ERROR


def test_from_run_no_error_when_scan_abort_pv_false():
    """BL3:Exp:ScanAbort=False → does NOT trigger ERROR; falls through to RUNNING fallback."""
    run = _make_run(scan_abort=False)
    assert RunStatus.from_run(run) == RunStatus.RUNNING


def test_from_run_error_takes_priority_over_end_time():
    """ScanAbort=True overrides end_time; ERROR wins over STOPPED."""
    run = _make_run(scan_abort=True, has_end_time=True)
    assert RunStatus.from_run(run) == RunStatus.ERROR


# --- from_run: step 2 — stopped detection via end_time ---

def test_from_run_stopped_when_end_time_present():
    """'end_time' present and no abort → STOPPED."""
    run = _make_run(has_end_time=True)
    assert RunStatus.from_run(run) == RunStatus.STOPPED


def test_from_run_end_time_does_not_override_abort():
    """end_time + ScanAbort=True → still ERROR (abort checked first)."""
    run = _make_run(scan_abort=True, has_end_time=True)
    assert RunStatus.from_run(run) == RunStatus.ERROR


# --- from_run: step 3 — paused detection via 'pause' log ---

def test_from_run_paused_when_pause_log_true():
    """'pause' log last value = True → PAUSED."""
    run = _make_run(pause=True)
    assert RunStatus.from_run(run) == RunStatus.PAUSED


def test_from_run_paused_when_pause_log_one():
    """'pause' log last value = 1 (truthy) → PAUSED."""
    run = _make_run(pause=1)
    assert RunStatus.from_run(run) == RunStatus.PAUSED


def test_from_run_not_paused_when_pause_log_false():
    """'pause' log last value = False → falls through (RUNNING fallback)."""
    run = _make_run(pause=False)
    assert RunStatus.from_run(run) == RunStatus.RUNNING


def test_from_run_pause_takes_priority_over_det_status_running():
    """'pause'=True wins over a Det:Status of ACQUIRING."""
    run = _make_run(pause=True, det_status="ACQUIRING")
    assert RunStatus.from_run(run) == RunStatus.PAUSED


# --- from_run: step 4 — detector status PV ---

def test_from_run_det_status_pause_returns_paused():
    """Det:Status containing 'PAUSE' → PAUSED."""
    run = _make_run(det_status="PAUSE")
    assert RunStatus.from_run(run) == RunStatus.PAUSED


def test_from_run_det_status_paused_case_insensitive():
    """Det:Status 'paused' (lowercase) → PAUSED."""
    run = _make_run(det_status="paused")
    assert RunStatus.from_run(run) == RunStatus.PAUSED


def test_from_run_det_status_stop_returns_stopped():
    """Det:Status containing 'STOP' → STOPPED."""
    run = _make_run(det_status="STOP")
    assert RunStatus.from_run(run) == RunStatus.STOPPED


def test_from_run_det_status_idle_returns_stopped():
    """Det:Status containing 'IDLE' → STOPPED."""
    run = _make_run(det_status="IDLE")
    assert RunStatus.from_run(run) == RunStatus.STOPPED


def test_from_run_det_status_acquiring_returns_running():
    """Det:Status containing 'ACQUIRING' → RUNNING."""
    run = _make_run(det_status="ACQUIRING")
    assert RunStatus.from_run(run) == RunStatus.RUNNING


def test_from_run_det_status_run_returns_running():
    """Det:Status containing 'RUN' → RUNNING."""
    run = _make_run(det_status="RUN")
    assert RunStatus.from_run(run) == RunStatus.RUNNING


def test_from_run_det_status_unknown_falls_through_to_running():
    """Det:Status with an unrecognized value falls through to RUNNING fallback."""
    run = _make_run(det_status="UNKNOWN_STATE")
    assert RunStatus.from_run(run) == RunStatus.RUNNING


# --- from_run: step 5 — fallback ---

def test_from_run_fallback_running_when_no_relevant_logs():
    """No abort PVs, no end_time, no pause log, no Det:Status → fallback RUNNING."""
    run = _make_run()
    assert RunStatus.from_run(run) == RunStatus.RUNNING


# --- priority ordering ---

def test_from_run_abort_takes_priority_over_pause():
    """ScanAbort=True beats pause=True; ERROR wins."""
    run = _make_run(scan_abort=True, pause=True)
    assert RunStatus.from_run(run) == RunStatus.ERROR


def test_from_run_abort_takes_priority_over_det_status_paused():
    """ScanAbort=True beats Det:Status='PAUSE'; ERROR wins."""
    run = _make_run(scan_abort=True, det_status="PAUSE")
    assert RunStatus.from_run(run) == RunStatus.ERROR


def test_from_run_end_time_takes_priority_over_pause_log():
    """end_time present beats pause=True; STOPPED wins (abort checked first)."""
    run = _make_run(has_end_time=True, pause=True)
    assert RunStatus.from_run(run) == RunStatus.STOPPED


def test_from_run_end_time_takes_priority_over_det_status():
    """end_time present beats any Det:Status value."""
    run = _make_run(has_end_time=True, det_status="ACQUIRING")
    assert RunStatus.from_run(run) == RunStatus.STOPPED


def test_from_run_primary_scan_abort_takes_priority_over_alt():
    """If primary ScanAbort is present and False, alt abort is not checked even if True."""
    # Primary is False → skip alt; no other signals → RUNNING fallback
    run = _make_run(scan_abort=False, scan_abort_alt=True)
    # Primary ScanAbort is False so abort condition is False; alt is not checked.
    assert RunStatus.from_run(run) == RunStatus.RUNNING
