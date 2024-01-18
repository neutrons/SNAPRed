from unittest.mock import MagicMock, Mock, call, patch

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QComboBox, QMessageBox
from snapred.backend.dao.calibration import CalibrationIndexEntry
from snapred.ui.view.CalibrationAssessmentView import CalibrationAssessmentView


def test_calibrationrecord_dropdown(qtbot):
    view = CalibrationAssessmentView(name="Test", jsonSchemaMap=MagicMock())
    assert view.getCalibrationRecordCount() == 0

    # test filling in the dropdown
    runNumber = "1234"
    version = "1"
    calibrationIndexEntries = [CalibrationIndexEntry(runNumber=runNumber, version=version, comments="", author="")]
    view.updateCalibrationRecordList(calibrationIndexEntries)
    assert view.getCalibrationRecordCount() == 1
    assert view.getSelectedCalibrationRecordIndex() == -1

    # test making a selection in the dropdown
    qtbot.addWidget(view.calibrationRecordDropdown)
    qtbot.keyClicks(view.calibrationRecordDropdown, "Version: 1; Run: 1234")
    assert view.getSelectedCalibrationRecordIndex() == 0
    assert view.getSelectedCalibrationRecordData() == (runNumber, version)
