from unittest.mock import MagicMock, patch

import pytest
from snapred.backend.dao.request.CalibrationLoadAssessmentRequest import CalibrationLoadAssessmentRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.presenter.CalibrationAssessmentPresenter import CalibrationAssessmentPresenter


@pytest.fixture()
def calibrationAssessmentPresenter():
    view = MagicMock()
    return CalibrationAssessmentPresenter(view=view)


def test_load_record(calibrationAssessmentPresenter):
    runNumber = "12345"
    useLiteMode = False
    version = "1"
    view = calibrationAssessmentPresenter.view
    view.getCalibrationRecordCount = MagicMock(return_value=1)
    view.getSelectedCalibrationRecordIndex = MagicMock(return_value=0)
    view.getSelectedCalibrationRecordData = MagicMock(return_value=(runNumber, useLiteMode, version))

    with patch.object(calibrationAssessmentPresenter, "worker_pool") as worker_pool, patch.object(
        calibrationAssessmentPresenter, "interfaceController"
    ) as interfaceController:
        calibrationAssessmentPresenter.loadSelectedCalibrationAssessment()

        view.getCalibrationRecordCount.assert_called_once()
        view.getSelectedCalibrationRecordIndex.assert_called_once()
        view.getSelectedCalibrationRecordData.assert_called_once()

        payload = CalibrationLoadAssessmentRequest(
            runId=runNumber,
            useLiteMode=useLiteMode,
            version=version,
            checkExistent=True,
        )
        request = SNAPRequest(path="/calibration/loadQualityAssessment", payload=payload.json())
        worker_pool.createWorker.assert_called_once_with(target=interfaceController.executeRequest, args=(request))
        worker_pool.submitWorker.assert_called_once()


def test_load_record_with_no_records_available(calibrationAssessmentPresenter):
    view = calibrationAssessmentPresenter.view
    view.getCalibrationRecordCount = MagicMock(return_value=0)

    calibrationAssessmentPresenter.loadSelectedCalibrationAssessment()

    view.getCalibrationRecordCount.assert_called_once()
    view.onError.assert_called_once_with("No calibration records available.")


def test_load_record_with_no_record_selected(calibrationAssessmentPresenter):
    view = calibrationAssessmentPresenter.view
    view.getCalibrationRecordCount = MagicMock(return_value=1)
    view.getSelectedCalibrationRecordIndex = MagicMock(return_value=-1)

    calibrationAssessmentPresenter.loadSelectedCalibrationAssessment()

    view.getCalibrationRecordCount.assert_called_once()
    view.getSelectedCalibrationRecordIndex.assert_called_once()
    view.onError.assert_called_once_with("No calibration record selected.")
