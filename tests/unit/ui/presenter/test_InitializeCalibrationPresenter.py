from unittest.mock import Mock, patch

import pytest
from qtpy import QtCore
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
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


def test_labelView(mock_View, qtbot):  # noqa: ARG001
    calibrationCheck = CalibrationCheck(mock_View)
    test_text = "Test"
    calibrationCheck._labelView(test_text)

    mock_View.layout().addWidget.assert_called()


def test_handleButtonClicked(mock_View, qtbot):  # noqa: ARG001
    with patch(
        "snapred.ui.presenter.InitializeCalibrationPresenter.CalibrationCheck.worker_pool.createWorker",
        return_value=Mock(),
    ) as mock_createWorker:
        calibrationCheck = CalibrationCheck(mock_View)
        calibrationCheck.handleButtonClicked()

        mock_createWorker.assert_called_once_with(
            target=calibrationCheck.interfaceController.executeRequest,
            args=SNAPRequest(path="/calibration/hasState", payload="12345"),
        )


def test_handlePixelGroupingResult(mock_View, qtbot):  # noqa: ARG001
    calibrationCheck = CalibrationCheck(mock_View)
    mock_response = Mock()
    mock_response.code = 200

    mock_labelView = Mock()
    calibrationCheck._labelView = mock_labelView

    calibrationCheck.handlePixelGroupingResult(mock_response)

    calibrationCheck._labelView.assert_called_with("Ready to Calibrate!")
