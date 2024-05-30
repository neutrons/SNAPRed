from unittest.mock import MagicMock

from snapred.backend.dao.calibration import CalibrationIndexEntry
from snapred.ui.view.DiffCalAssessmentView import DiffCalAssessmentView


def test_calibration_record_dropdown(qtbot):
    view = DiffCalAssessmentView(name="Test", jsonSchemaMap=MagicMock())
    assert view.getCalibrationRecordCount() == 0

    # test filling in the dropdown
    runNumber = "1234"
    useLiteMode = False
    version = 1
    calibrationIndexEntries = [
        CalibrationIndexEntry(runNumber=runNumber, useLiteMode=useLiteMode, version=version, comments="", author="")
    ]
    view.updateCalibrationRecordList(calibrationIndexEntries)
    assert view.getCalibrationRecordCount() == 1
    assert view.getSelectedCalibrationRecordIndex() == -1

    # test making a selection in the dropdown
    qtbot.addWidget(view.calibrationRecordDropdown)
    qtbot.keyClicks(view.calibrationRecordDropdown, "Version: 1; Run: 1234")
    assert view.getSelectedCalibrationRecordIndex() == 0
    assert view.getSelectedCalibrationRecordData() == (runNumber, useLiteMode, version)


def test_error_on_load_calibration_record(qtbot):
    view = DiffCalAssessmentView(name="Test", jsonSchemaMap=MagicMock())
    qtbot.addWidget(view.loadButton)
    view.onError = MagicMock()
    view.loadButton.click()
    view.onError.assert_called_once()
