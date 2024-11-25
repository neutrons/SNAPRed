# Unit tests for `tests/util/Config_helpers.py`

from util.Config_helpers import Config_override

from snapred.meta.Config import Config


def test_Config_override_enter():
    newPath = "some/new/path"
    with Config_override("instrument.calibration.home", "some/new/path"):
        assert Config["instrument.calibration.home"] == newPath


def test_Config_override_exit():
    originalPath = Config["instrument.calibration.home"]
    newPath = "some/new/path"
    with Config_override("instrument.calibration.home", "some/new/path"):
        assert Config["instrument.calibration.home"] == newPath
    assert Config["instrument.calibration.home"] == originalPath
