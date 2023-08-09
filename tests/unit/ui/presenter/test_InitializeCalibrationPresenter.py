from unittest.mock import MagicMock, Mock, patch

import pytest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.presenter.InitializeCalibrationPresenter import CalibrationCheck


@pytest.fixture()
def mock_View():
    view = Mock()
    view.beginFlowButton = Mock()
    view.beginFlowButton.setEnabled = Mock()
    view.getRunNumber = Mock(return_value=12345)
    view.layout = Mock()
    view.layout().addWidget = Mock()
    return view


def test_labelView(mock_View):
    calibrationCheck = CalibrationCheck(mock_View)
    test_text = "Test"
    calibrationCheck._labelView(test_text)

    mock_View.layout().addWidget.assert_called()


def test_handleButtonClicked(mock_View):
    with patch(
        "snapred.ui.presenter.InitializeCalibrationPresenter.CalibrationCheck.worker_pool.createWorker",
        return_value=Mock(),
    ) as mock_createWorker:
        calibrationCheck = CalibrationCheck(mock_View)
        calibrationCheck.handleButtonClicked()

        mock_createWorker.assert_called_once_with(
            target=calibrationCheck.interfaceController.executeRequest,
            args=SNAPRequest(path="/calibration/checkDataExists", payload='{"runNumber": "12345"}'),
        )


def test_handleDataCheckResult(mock_View):
    calibrationCheck = CalibrationCheck(mock_View)
    mock_response = Mock()
    mock_response.responseCode = 404

    mock_labelView = Mock()
    calibrationCheck._labelView = mock_labelView

    calibrationCheck.handleDataCheckResult(mock_response)

    calibrationCheck.view.beginFlowButton.setEnabled.assert_called_with(True)
    mock_labelView.assert_called_with("Error, data doesn't exist")


def test_handleStateCheckResult(mock_View):
    calibrationCheck = CalibrationCheck(mock_View)
    mock_response = Mock()
    mock_response.responseCode = 200

    # Mock out the problematic StateConfig.focusGroups.definition TODO: Needs to be fixed and tested.
    mock_focusGroups = MagicMock()
    mock_focusGroups.definition = "mocked_definition"
    with patch("snapred.ui.presenter.InitializeCalibrationPresenter.StateConfig.focusGroups", mock_focusGroups):
        with patch("snapred.ui.presenter.InitializeCalibrationPresenter.SNAPRequest"):
            calibrationCheck.handleStateCheckResult(mock_response)

            calibrationCheck.worker_pool.createWorker.assert_called()
            calibrationCheck.worker_pool.submitWorker.assert_called_with(calibrationCheck.worker)


def test_handlePixelGroupingResult(mock_View):
    calibrationCheck = CalibrationCheck(mock_View)
    mock_response = Mock()
    mock_response.responseCode = 200

    mock_labelView = Mock()
    calibrationCheck._labelView = mock_labelView

    calibrationCheck.handlePixelGroupingResult(mock_response)

    calibrationCheck._labelView.assert_called_with("Ready to Calibrate!")
