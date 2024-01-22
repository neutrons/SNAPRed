from unittest.mock import MagicMock, call, patch

import pytest
from PyQt5.QtWidgets import QMessageBox
from snapred.backend.dao.request.CalibrationLoadAssessmentRequest import CalibrationLoadAssessmentRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.ui.presenter.CalibrationAssessmentPresenter import CalibrationAssessmentPresenter


@pytest.fixture()
def calibrationAssessmentPresenter():
    view = MagicMock()
    return CalibrationAssessmentPresenter(view=view)


def test_load_record(calibrationAssessmentPresenter):
    view = calibrationAssessmentPresenter.view
    view.getCalibrationRecordCount = MagicMock(return_value=1)
    view.getSelectedCalibrationRecordIndex = MagicMock(return_value=0)
    view.getSelectedCalibrationRecordData = MagicMock(return_value=("12345", "1"))

    with patch.object(calibrationAssessmentPresenter, "worker_pool") as worker_pool, patch.object(
        calibrationAssessmentPresenter, "interfaceController"
    ) as interfaceController:
        calibrationAssessmentPresenter.handleLoadRequested()

        view.getCalibrationRecordCount.assert_called_once()
        view.getSelectedCalibrationRecordIndex.assert_called_once()
        view.getSelectedCalibrationRecordData.assert_called_once()

        payload = CalibrationLoadAssessmentRequest(
            runId="12345",
            version="1",
            checkExistent=True,
        )
        request = SNAPRequest(path="/calibration/loadQualityAssessment", payload=payload.json())
        worker_pool.createWorker.assert_called_once_with(target=interfaceController.executeRequest, args=(request))
        worker_pool.submitWorker.assert_called_once()


def test_load_record_with_no_records_available(calibrationAssessmentPresenter):
    view = calibrationAssessmentPresenter.view
    view.getCalibrationRecordCount = MagicMock(return_value=0)

    calibrationAssessmentPresenter.handleLoadRequested()

    view.getCalibrationRecordCount.assert_called_once()
    view.onError.assert_called_once_with("No calibration records available.")


def test_load_record_with_no_record_selected(calibrationAssessmentPresenter):
    view = calibrationAssessmentPresenter.view
    view.getCalibrationRecordCount = MagicMock(return_value=1)
    view.getSelectedCalibrationRecordIndex = MagicMock(return_value=-1)

    calibrationAssessmentPresenter.handleLoadRequested()

    view.getCalibrationRecordCount.assert_called_once()
    view.getSelectedCalibrationRecordIndex.assert_called_once()
    view.onError.assert_called_once_with("No calibration record selected.")
