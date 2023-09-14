from unittest.mock import Mock, patch

import pytest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.ui.presenter.InitializeCalibrationPresenter import CalibrationCheck


@pytest.fixture()
def calibrationCheck():
    view = Mock()
    return CalibrationCheck(view=view)


def test_handleButtonClicked(calibrationCheck):
    view = calibrationCheck.view
    view.getRunNumber.return_value = "12345"

    with patch.object(calibrationCheck, "worker_pool") as worker_pool, patch.object(
        calibrationCheck, "interfaceController"
    ) as interfaceController:
        stateCheckRequest = SNAPRequest(path="/calibration/hasState", payload="12345")

        calibrationCheck.handleButtonClicked()

        view.getRunNumber.assert_called_once()
        worker_pool.createWorker.assert_called_once_with(
            target=interfaceController.executeRequest, args=(stateCheckRequest)
        )
        worker_pool.submitWorker.assert_called_once()


def test_handleStateCheckResult(calibrationCheck):
    response = Mock(spec=SNAPResponse)
    response.data = False
    response.message = "Sample message"

    with patch.object(calibrationCheck, "_labelView") as labelView, patch.object(
        calibrationCheck, "_spawnStateCreationWorkflow"
    ) as spawnStateCreationWorkflow:
        calibrationCheck.handleStateCheckResult(response)

        labelView.assert_called_once_with("Sample message")
        spawnStateCreationWorkflow.assert_called_once()
