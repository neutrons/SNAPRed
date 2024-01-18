from unittest.mock import MagicMock, call, patch

import pytest
from PyQt5.QtWidgets import QMessageBox
from snapred.backend.dao.request.CalibrationLoadAssessmentRequest import CalibrationLoadAssessmentRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.ui.presenter.CalibrationAssessmentPresenter import CalibrationAssessmentLoader


@pytest.fixture()
def calibrationAssessmentLoader():
    view = MagicMock()
    return CalibrationAssessmentLoader(view=view)


def test_handleLoad(calibrationAssessmentLoader):
    view = calibrationAssessmentLoader.view
    view.getCalibrationRecordCount = MagicMock(return_value=1)
    view.getSelectedCalibrationRecordIndex = MagicMock(return_value=0)
    view.getSelectedCalibrationRecordData = MagicMock(return_value=("12345", "1"))

    with patch.object(calibrationAssessmentLoader, "worker_pool") as worker_pool, patch.object(
        calibrationAssessmentLoader, "interfaceController"
    ) as interfaceController:
        payload = CalibrationLoadAssessmentRequest(
            runId="12345",
            version="1",
            checkExistent=True,
        )

        request = SNAPRequest(path="/calibration/loadAssessment", payload=payload.json())

        calibrationAssessmentLoader.handleLoadRequested()

        view.getCalibrationRecordCount.assert_called_once()
        view.getSelectedCalibrationRecordIndex.assert_called_once()
        view.getSelectedCalibrationRecordData.assert_called_once()

        worker_pool.createWorker.assert_called_once_with(target=interfaceController.executeRequest, args=(request))
        worker_pool.submitWorker.assert_called_once()


def test_handleLoad_no_records_available(calibrationAssessmentLoader):
    view = calibrationAssessmentLoader.view
    view.getCalibrationRecordCount = MagicMock(return_value=0)

    calibrationAssessmentLoader.handleLoadRequested()

    view.getCalibrationRecordCount.assert_called_once()
    view.onLoadError.assert_called_once_with("No calibration records available.")


def test_handleLoad_no_record_selected(calibrationAssessmentLoader):
    view = calibrationAssessmentLoader.view
    view.getCalibrationRecordCount = MagicMock(return_value=1)
    view.getSelectedCalibrationRecordIndex = MagicMock(return_value=-1)

    calibrationAssessmentLoader.handleLoadRequested()

    view.getCalibrationRecordCount.assert_called_once()
    view.getSelectedCalibrationRecordIndex.assert_called_once()
    view.onLoadError.assert_called_once_with("No calibration record selected.")
