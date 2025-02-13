from unittest.mock import MagicMock

import pytest
from qtpy.QtCore import Qt, Slot
from qtpy.QtWidgets import QGridLayout, QPushButton, QWidget

from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder


@pytest.fixture
def qtbot(qtbot):
    return qtbot


class _TestView(QWidget):
    def __init__(self, parent=None):
        super(_TestView, self).__init__(parent)
        self.continueButton = QPushButton("Continue")
        self.continueButton.clicked.connect(self.handleContinueButtonClicked)
        self.layout = QGridLayout()
        self.setLayout(self.layout)

    @Slot()
    def handleContinueButtonClicked(self):
        pass

    ##
    ## Required abstract methods from `BackendRequestView`.
    ##

    def verify(self):
        return True

    def setInteractive(self, flag: bool):
        pass


def _generateWorkflow():
    # Create a mock WorkflowNodeModel
    view = _TestView()

    def continueAction(workflowPresenter):  # noqa: ARG001
        return None

    WorkflowNodeModel(view, continueAction, None)
    return WorkflowBuilder().addNode(continueAction, view, "Test").build()


@pytest.mark.ui
def test_workflowPresenterHandleContinueButtonClicked(qtbot):
    # Mock the worker pool
    mockWorkerPool = MagicMock()
    workflow = _generateWorkflow()
    workflow.presenter.worker_pool = mockWorkerPool

    # Simulate button click
    qtbot.addWidget(workflow.widget)
    qtbot.mouseClick(workflow.widget.continueButton, Qt.LeftButton)

    # Check that worker pool methods were called correctly
    mockWorkerPool.createWorker.assert_called()
    mockWorkerPool.submitWorker.assert_called_once_with(mockWorkerPool.createWorker.return_value)
