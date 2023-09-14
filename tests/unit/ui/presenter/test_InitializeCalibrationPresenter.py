from unittest.mock import Mock, patch

import pytest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.ui.presenter.InitializeCalibrationPresenter import CalibrationCheck


@pytest.fixture()
def calibrationCheck():
    mock_view = Mock()
    calibrationCheck = CalibrationCheck(view=mock_view)
    return calibrationCheck


def test_handleButtonClicked(mocker, calibrationCheck):  # noqa: F811
    mock_view = calibrationCheck.view
    mock_view.getRunNumber.return_value = "12345"

    mock_worker_pool = mocker.patch.object(calibrationCheck, "worker_pool")

    mock_interfaceController = mocker.patch.object(calibrationCheck, "interfaceController")
    stateCheckRequest = SNAPRequest(path="/calibration/hasState", payload="12345")

    mock_submitWorker = mock_worker_pool.submitWorker

    calibrationCheck.handleButtonClicked()

    mock_view.getRunNumber.assert_called_once()
    mock_worker_pool.createWorker.assert_called_once_with(
        target=mock_interfaceController.executeRequest, args=(stateCheckRequest)
    )
    mock_submitWorker.assert_called_once()


def test_handleStateCheckResult(mocker, calibrationCheck):  # noqa: F811
    mock_response = Mock(spec=SNAPResponse)
    mock_response.data = False
    mock_response.message = "Sample message"

    mock__labelView = mocker.patch.object(calibrationCheck, "_labelView")
    mock__spawnStateCreationWorkflow = mocker.patch.object(calibrationCheck, "_spawnStateCreationWorkflow")

    calibrationCheck.handleStateCheckResult(mock_response)

    mock__labelView.assert_called_once_with("Sample message")
    mock__spawnStateCreationWorkflow.assert_called_once()
