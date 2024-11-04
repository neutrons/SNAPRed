import sys
from unittest.mock import MagicMock, patch

import pytest
from qtpy.QtWidgets import QApplication, QWidget
from util.script_as_test import not_a_test

from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.ui.presenter.InitializeStatePresenter import InitializeStatePresenter
from snapred.ui.widget.LoadingCursor import LoadingCursor

app = QApplication(sys.argv)


@not_a_test
class TestableQWidget(QWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setEnabled = MagicMock()

        self.getRunNumber = MagicMock(return_value="12345")
        self.getStateName = MagicMock(return_value="Test State")
        self.getMode = MagicMock(return_value=True)
        self.beginFlowButton = MagicMock()


@pytest.fixture
def setup_view_and_workflow():
    view = TestableQWidget()
    workflow = InitializeStatePresenter(view=view)
    return view, workflow


def test_handleButtonClicked_with_valid_input(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    view.getRunNumber.return_value = "12345"
    view.getStateName.return_value = "Test State"
    view.getMode.return_value = True

    with patch.object(workflow, "_initializeState") as mock_initializeState:
        workflow.handleButtonClicked()
        mock_initializeState.assert_called_once_with("12345", "Test State", True)


def test_handleButtonClicked_with_invalid_input(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    view.getRunNumber.return_value = "invalid"

    with patch("qtpy.QtWidgets.QMessageBox.warning") as mock_warning:
        workflow.handleButtonClicked()
        mock_warning.assert_called_once()


def test__initializeState(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    view.getRunNumber.return_value = "12345"
    view.getStateName.return_value = "Test State"
    view.getMode.return_value = "True"
    mock_response = SNAPResponse(code=ResponseCode.OK)

    with (
        patch.object(workflow.interfaceController, "executeRequest", return_value=mock_response),
        patch("snapred.ui.widget.SuccessPrompt.SuccessPrompt.prompt") as mock_dialog_showSuccess,
    ):
        workflow._initializeState("12345", "Test State", True)
        mock_dialog_showSuccess.assert_called_once()


def test__handleResponse_error(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    error_response = SNAPResponse(code=ResponseCode.ERROR, message="Error message")

    # Initialize loadingCursor
    workflow.loadingCursor = LoadingCursor(view)

    with patch("qtpy.QtWidgets.QMessageBox.critical") as mock_critical:
        workflow._handleResponse(error_response)
        mock_critical.assert_called_once()


def test__handleResponse_success(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    success_response = SNAPResponse(code=ResponseCode.OK)

    # Initialize loadingCursor
    workflow.loadingCursor = LoadingCursor(view)

    with patch("snapred.ui.widget.SuccessPrompt.SuccessPrompt.prompt") as mock_dialog_showSuccess:
        workflow._handleResponse(success_response)
        mock_dialog_showSuccess.assert_called_once()
