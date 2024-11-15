from unittest.mock import MagicMock

import pytest
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.ui.view.DiffCalAssessmentView import DiffCalAssessmentView


@pytest.mark.ui()
def test_calibration_record_dropdown(qtbot):
    view = DiffCalAssessmentView()
    assert view.getCalibrationRecordCount() == 0

    # test filling in the dropdown
    runNumber = "1234"
    useLiteMode = False
    version = 1
    calibrationIndexEntries = [
        IndexEntry(runNumber=runNumber, useLiteMode=useLiteMode, version=version, comments="", author="")
    ]
    view.updateCalibrationRecordList(calibrationIndexEntries)
    assert view.getCalibrationRecordCount() == 1
    assert view.getSelectedCalibrationRecordIndex() == -1

    # test making a selection in the dropdown
    qtbot.addWidget(view.calibrationRecordDropdown)
    qtbot.keyClicks(view.calibrationRecordDropdown, "Version: 1; Run: 1234")
    assert view.getSelectedCalibrationRecordIndex() == 0
    assert view.getSelectedCalibrationRecordData() == (runNumber, useLiteMode, version)


@pytest.mark.ui()
def test_error_on_load_calibration_record(qtbot):
    view = DiffCalAssessmentView()
    qtbot.addWidget(view.loadButton)
    view.onError = MagicMock()
    view.loadButton.click()
    view.onError.assert_called_once()
