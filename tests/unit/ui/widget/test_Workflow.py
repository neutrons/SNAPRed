from unittest.mock import MagicMock

import pytest
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication, QGridLayout, QPushButton, QWidget
from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.presenter.WorkflowPresenter import WorkflowPresenter
from snapred.ui.view.WorkflowView import WorkflowView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder


@pytest.fixture()
def qtbot(qtbot):
    return qtbot


class _TestView(QWidget):
    def __init__(self, parent=None):
        super(_TestView, self).__init__(parent)
        self.continueButton = QPushButton("Continue")
        self.continueButton.clicked.connect(self.handleContinueButtonClicked)
        self.layout = QGridLayout()
        self.setLayout(self.layout)

    def handleContinueButtonClicked(self):
        pass

    def verify(self):
        return True


def _generateWorkflow():
    # Create a mock WorkflowNodeModel
    view = _TestView()

    def continueAction(workflowPresenter):  # noqa: ARG001
        return None

    WorkflowNodeModel(view, continueAction, None)
    return WorkflowBuilder(None).addNode(continueAction, view, "Test").build()


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
