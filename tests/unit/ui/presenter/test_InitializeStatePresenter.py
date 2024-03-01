from unittest.mock import Mock, patch

import pytest
from PyQt5.QtWidgets import QMessageBox
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.ui.presenter.InitializeStatePresenter import InitializationWorkflow


@pytest.fixture()
def setup_view_and_workflow():
    view = Mock()
    workflow = InitializationWorkflow(view=view)
    return view, workflow


def test_handleButtonClicked_with_valid_input(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    view.getRunNumber.return_value = "12345"
    view.getStateName.return_value = "Test State"

    with patch.object(workflow, "_initializeState") as mock_initializeState:
        workflow.handleButtonClicked()
        mock_initializeState.assert_called_once_with("12345", "Test State")
        view.beginFlowButton.setEnabled.assert_called_once_with(False)


def test_handleButtonClicked_with_invalid_input(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    view.getRunNumber.return_value = "invalid"

    with patch("PyQt5.QtWidgets.QMessageBox.warning") as mock_warning:
        workflow.handleButtonClicked()
        mock_warning.assert_called_once()


def test__initializeState(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    view.getRunNumber.return_value = "12345"
    view.getStateName.return_value = "Test State"
    mock_response = SNAPResponse(code=ResponseCode.OK)  # Assuming OK is a valid response code

    with patch.object(workflow.interfaceController, "executeRequest", return_value=mock_response), patch(
        "PyQt5.QtWidgets.QMessageBox.information"
    ) as mock_info:
        workflow._initializeState("12345", "Test State")
        mock_info.assert_called_once_with(workflow.view, "Success", "State initialized successfully.")


def test__handleResponse_error(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    error_response = SNAPResponse(code=ResponseCode.ERROR, message="Error message")

    with patch("PyQt5.QtWidgets.QMessageBox.critical") as mock_critical:
        workflow._handleResponse(error_response)
        mock_critical.assert_called_once()


def test__handleResponse_success(setup_view_and_workflow):
    view, workflow = setup_view_and_workflow
    success_response = SNAPResponse(code=ResponseCode.OK)

    with patch("PyQt5.QtWidgets.QMessageBox.information") as mock_information:
        workflow._handleResponse(success_response)
        mock_information.assert_called_once()
