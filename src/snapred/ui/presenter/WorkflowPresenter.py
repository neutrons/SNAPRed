from os import path

from qtpy.QtWidgets import QMainWindow, QMessageBox

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import SNAPRequest
from snapred.backend.dao.request import ClearWorkspaceRequest
from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.log.logger import snapredLogger
from snapred.ui.handler.SNAPResponseHandler import SNAPResponseHandler
from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.WorkflowView import WorkflowView
from snapred.ui.widget.ActionPrompt import ActionPrompt

logger = snapredLogger.getLogger(__name__)


class WorkflowPresenter(object):
    worker_pool = WorkerPool()

    def __init__(self, model: WorkflowNodeModel, cancelLambda=None, iterateLambda=None, parent=None):
        self.view = WorkflowView(model, parent)
        self._iteration = 1
        self.model = model
        self._cancelLambda = cancelLambda
        self._iterateLambda = iterateLambda
        self.resetLambda = self.resetAndClear
        self._hookupSignals()
        self.responseHandler = SNAPResponseHandler(self.view)
        self.responseHandler.continueAnyway.connect(self.continueAnyway)

    @property
    def widget(self):
        return self.view

    @property
    def nextView(self):
        return self.view.nextTabView

    def show(self):
        # wrap view in QApplication
        self.window = QMainWindow(self.view.parent())
        self.window.setCentralWidget(self.view)
        self.window.show()

    def resetSoft(self):
        self.widget.reset(hard=False)

    def clearWorkspacesRequest(self):
        interfaceController = InterfaceController()
        clearWorkspacesRequest = ClearWorkspaceRequest(cache=True, exclude=[])
        snapRequest = SNAPRequest(path="workspace/clear", payload=clearWorkspacesRequest.json())
        interfaceController.executeRequest(snapRequest)

    def resetHard(self):
        self.widget.reset(hard=True)

    def resetAndClear(self):
        self.resetSoft()
        self.clearWorkspacesRequest()
        self._iteration = 1

    def cancel(self):
        ActionPrompt(
            "Are you sure?",
            "Are you sure you want to cancel the workflow? This will clear all workspaces.",
            self.resetAndClear,
            self.view,
        )

    def iterate(self):
        self._iteration += 1
        self.resetSoft()

    @property
    def iteration(self):
        return self._iteration

    def _hookupSignals(self):
        for i, model in enumerate(self.model):
            widget = self.view.tabWidget.widget(i)
            logger.debug(
                f"Hooking up signals for tab {widget.view} to {widget.model.continueAction} \
                    to continue button {widget.continueButton}"
            )
            if not model.required:
                widget.onSkipButtonClicked(self.handleSkipButtonClicked)
                widget.enableSkip()

            if model.iterate:
                widget.onIterateButtonClicked(self.handleIterateButtonClicked)
                widget.enableIterate()

            widget.onContinueButtonClicked(self.handleContinueButtonClicked)

            if self._cancelLambda:
                widget.onCancelButtonClicked(self._cancelLambda)
            else:
                widget.onCancelButtonClicked(self.cancel)

    def handleIterateButtonClicked(self):
        self._iterateLambda(self)
        self.iterate()

    def handleSkipButtonClicked(self):
        self.advanceWorkflow()

    def advanceWorkflow(self):
        if self.view.currentTab >= self.view.totalNodes - 1:
            ActionPrompt(
                "‧₊Workflow Complete‧₊",
                "‧₊‧₊The workflow has been completed successfully!‧₊‧₊",
                lambda: None,
                self.view,
            )
            self.resetLambda()
        else:
            self.view.advanceWorkflow()

    def setResetLambda(self, resetLambda):
        self.resetLambda = resetLambda

    def handleContinueButtonClicked(self, model):
        # disable navigation buttons during run
        self._enableButtons(False)

        # scoped action to verify before running
        def verifyAndContinue():
            # This will toss any exceptions and stop the continue
            self.view.tabView.verify()
            # this will handle the request if no verification failed, getting the true snapresponse
            return model.continueAction(self)

        # do action
        self.worker = self.worker_pool.createWorker(target=verifyAndContinue, args=None)
        self.worker.finished.connect(lambda: self._enableButtons(True))  # renable buttons on finish
        self.worker.result.connect(self._handleComplications)
        self.worker.success.connect(lambda success: self.advanceWorkflow() if success else None)
        self.worker_pool.submitWorker(self.worker)

    def _enableButtons(self, enable):
        # NOTE this is necessary in order for the buttons to actually be updated from worker
        buttons = [self.view.continueButton, self.view.cancelButton, self.view.skipButton]
        for button in buttons:
            button.setEnabled(enable)

    def _handleComplications(self, result):
        self.responseHandler.handle(result)

    def continueAnyway(self):
        self.model.nextModel.continueAction(self)
        self.advanceWorkflow()
