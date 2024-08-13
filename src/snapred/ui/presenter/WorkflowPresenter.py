from typing import Callable, List

from qtpy.QtCore import QObject, Signal, Slot
from qtpy.QtWidgets import QMainWindow

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.ui.handler.SNAPResponseHandler import SNAPResponseHandler
from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.WorkflowView import WorkflowView
from snapred.ui.widget.ActionPrompt import ActionPrompt

logger = snapredLogger.getLogger(__name__)


class WorkflowPresenter(QObject):
    enableAllWorkflows = Signal()
    disableOtherWorkflows = Signal()
    worker_pool = WorkerPool()

    actionCompleted = Signal()  # Allow an observer (e.g. ``qtbot``) to monitor action completion.

    def __init__(
        self,
        model: WorkflowNodeModel,
        startLambda=None,
        iterateLambda=None,
        resetLambda=None,
        cancelLambda=None,
        parent=None,
    ):
        super().__init__()
        self.view = WorkflowView(model, parent)
        self._iteration = 1
        self.model = model

        # All workflow "hook" methods must be initialized, either to a bound method
        #   or to an equivalent lambda function taking arguments as specified.
        self._startLambda: Callable[[], None] = startLambda if startLambda is not None else self._NOP
        self._iterateLambda: Callable[[WorkflowPresenter], None] = (
            iterateLambda if iterateLambda is not None else self._NOP
        )
        self._resetLambda: Callable[[], None] = resetLambda if resetLambda is not None else self.reset
        self._cancelLambda: Callable[[], None] = cancelLambda if cancelLambda is not None else self.resetWithPermission

        self.externalWorkspaces: List[str] = []
        # Retain list of ADS-resident workspaces at start of workflow

        self.interfaceController = InterfaceController()

        self._hookupSignals()
        self.responseHandler = SNAPResponseHandler(self.view)
        self.responseHandler.continueAnyway.connect(self.continueAnyway)

        if self.view.parent() is not None:
            self.enableAllWorkflows.connect(self.view.parent().enableAllWorkflows)
            self.disableOtherWorkflows.connect(self.view.parent().disableOtherWorkflows)

    def _NOP(self):
        pass

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

    def resetHard(self):
        self.widget.reset(hard=True)

    def reset(self):
        self._resetLambda()
        self.resetSoft()
        self._iteration = 1

        # Workflow is complete: enable the other workflow tabs.
        self.enableAllWorkflows.emit()

    def resetWithPermission(self):
        ActionPrompt.prompt(
            "Are you sure?",
            "Are you sure you want to cancel the workflow? This will clear all workspaces.",
            self.reset,
            self.view,
        )

    def iterate(self):
        self._iterateLambda(self)
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
            widget.onCancelButtonClicked(self._cancelLambda)

    def handleIterateButtonClicked(self):
        self.iterate()

    def handleSkipButtonClicked(self):
        self.advanceWorkflow()

    def advanceWorkflow(self):
        if self.view.currentTab >= self.view.totalNodes - 1:
            ActionPrompt.prompt(
                "‧₊Workflow Complete‧₊",
                "‧₊‧₊The workflow has been completed successfully!‧₊‧₊",
                lambda: None,
                self.view,
            )
            self.reset()
        else:
            self.view.advanceWorkflow()

    def handleContinueButtonClicked(self, model):
        if self.view.currentTab == 0:
            self._startLambda()

            # disable other workflow-tabs during workflow execution
            self.disableOtherWorkflows.emit()

        # disable navigation buttons during run
        self._enableButtons(False)

        # scoped action to verify before running
        def verifyAndContinue():
            # On verification failure: this will raise an exception and abort the continue.
            self.view.tabView.verify()

            # On verification success: this will return the correct SNAPResponse.
            return model.continueAction(self)

        # do action
        self.worker = self.worker_pool.createWorker(target=verifyAndContinue, args=None)
        self.worker.finished.connect(lambda: self._enableButtons(True))  # re-enable panel buttons on finish
        self.worker.result.connect(self._handleComplications)
        self.worker.success.connect(lambda success: self.advanceWorkflow() if success else None)
        self.worker_pool.submitWorker(self.worker)
        self.actionCompleted.emit()

    @Slot(bool)
    def _enableButtons(self, enable):
        # This slot is necessary in order for the buttons to actually be updated from the worker.
        buttons = [self.view.continueButton, self.view.cancelButton, self.view.skipButton]
        for button in buttons:
            button.setEnabled(enable)

    @Slot(object)
    def _handleComplications(self, result):
        # The associated signal is of type ``Signal(Worker.result) as Signal(object)``
        self.responseHandler.handle(result)

    @Slot(object)
    def continueAnyway(self, continueInfo: ContinueWarning.Model):
        # The associated signal is of type ``Signal(SNAPResponseHandler.continueAnyway) as Signal(object)``
        if self.view.tabModel.continueAnywayHandler:
            self.view.tabModel.continueAnywayHandler(continueInfo)
        else:
            raise NotImplementedError(f"Continue anyway handler not implemented: {self.view.tabModel}")
        self.handleContinueButtonClicked(self.view.tabModel)
