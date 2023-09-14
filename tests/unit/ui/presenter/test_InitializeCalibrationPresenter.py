from unittest.mock import Mock

import pytest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.ui.presenter.InitializeCalibrationPresenter import CalibrationCheck


@pytest.fixture()
def calibrationCheck():
    view = Mock()
    return CalibrationCheck(view=view)


def test_handleButtonClicked(mocker, calibrationCheck):
    view = calibrationCheck.view
    view.getRunNumber.return_value = "12345"

    worker_pool = mocker.patch.object(calibrationCheck, "worker_pool")
    interfaceController = mocker.patch.object(calibrationCheck, "interfaceController")
    stateCheckRequest = SNAPRequest(path="/calibration/hasState", payload="12345")

    submitWorker = worker_pool.submitWorker

    calibrationCheck.handleButtonClicked()

    view.getRunNumber.assert_called_once()
    worker_pool.createWorker.assert_called_once_with(
        target=interfaceController.executeRequest, args=(stateCheckRequest)
    )
    submitWorker.assert_called_once()


def test_handleStateCheckResult(mocker, calibrationCheck):
    response = Mock(spec=SNAPResponse)
    response.data = False
    response.message = "Sample message"

    labelView = mocker.patch.object(calibrationCheck, "_labelView")
    spawnStateCreationWorkflow = mocker.patch.object(calibrationCheck, "_spawnStateCreationWorkflow")

    calibrationCheck.handleStateCheckResult(response)

    labelView.assert_called_once_with("Sample message")
    spawnStateCreationWorkflow.assert_called_once()
