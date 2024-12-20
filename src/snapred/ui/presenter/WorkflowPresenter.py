from typing import Any, Callable, List, Optional, Tuple

from qtpy.QtCore import QObject, Signal, Slot
from qtpy.QtWidgets import QMainWindow, QMessageBox

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.UserCancellation import UserCancellation
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.ui.handler.SNAPResponseHandler import SNAPResponseHandler
from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.WorkflowView import WorkflowView
from snapred.ui.widget.ActionPrompt import ActionPrompt

logger = snapredLogger.getLogger(__name__)


class WorkflowPresenter(QObject):
    cancellationRequest = Signal()
    enableAllWorkflows = Signal()
    disableOtherWorkflows = Signal()

    # Allow an observer (e.g. `qtbot` or the `reductionWorkflow` itself) to monitor action completion.
    actionCompleted = Signal()
    
    # Sent at the end of the reset method -- this is necessary to allow the parent workflow to override reset specifics.
    # (Arguably, this is less than ideal, but the workflow's <reset hooks> are called at the _start_ of the reset method.)
    resetCompleted = Signal()

    def __init__(
        self,
        model: WorkflowNodeModel,
        startLambda=None,
        iterateLambda=None,
        resetLambda=None,
        cancelLambda=None,
        completeWorkflowLambda=None,
        parent=None,
    ):
        super().__init__()

        # 'WorkerPool' is a singleton:
        #    declaring it as an instance attribute, rather than a class attribute,
        #    allows singleton reset during testing.

        self.worker = None
        self._setWorkflowIsRunning(False)
        self.worker_pool = WorkerPool()

        self.view = WorkflowView(model, parent)
        self._iteration = 1
        self.model = model

        # All workflow "hook" methods must be initialized, either to a bound method
        #   or to an equivalent lambda function taking arguments as specified.
        self._startLambda: Callable[[], None] = startLambda if startLambda is not None else self._NOP
        self._iterateLambda: Callable[[WorkflowPresenter], None] = (
            iterateLambda if iterateLambda is not None else self._NOP
        )

        # WARNING: `reset` calls `self._resetLambda` -- don't make it circular!
        self._resetLambda: Callable[[], None] = resetLambda if resetLambda is not None else self._NOP
        
        self._cancelLambda: Callable[[], None] = cancelLambda if cancelLambda is not None else self.resetWithPermission

        self._completeWorkflowLambda: Callable[[], None] = completeWorkflowLambda if completeWorkflowLambda is not None else self.completeWorkflow
        
        self.externalWorkspaces: List[str] = []
        # Retain list of ADS-resident workspaces at start of workflow

        self.interfaceController = InterfaceController()

        self._hookupSignals()
        self.responseHandler = SNAPResponseHandler(self.view)
        self.responseHandler.continueAnyway.connect(self.continueAnyway)
        self.responseHandler.userCancellation.connect(self.userCancellation)
        self.responseHandler.liveDataStateTransition.connect(self.liveDataStateTransition)

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

        # Signal that the reset sequence is complete.
        self.resetCompleted.emit()

    def safeShutdown(self):
        # Request any executing worker thread to shut down.
        if self.workflowIsRunning:
            self.cancellationRequest.emit()
        else:
            self.reset()

    def resetWithPermission(self, shutdownLambda: Optional[Callable[[], None]] = None):
        shutdown = safeShutdown if shutdownLambda is None else shutdownLambda
        ActionPrompt.prompt(
            "Are you sure?",
            "Are you sure you want to cancel the workflow?\n"
            + "This will clear any partially-calculated results.",
            shutdown,
            parent=self.view,
            
            # Previously this used "Continue" / "Cancel" and was really confusing!
            # For a workflow cancellation request specifically,
            # please use "Yes" or "No", _not_ "Continue" or "Cancel"!
            
            buttonNames=("Yes", "No")
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
            self._completeWorkflowLambda()
        else:
            self.view.advanceWorkflow()

    def handleContinueButtonClicked(self, model):
        if self.view.currentTab == 0:
            self._startLambda()

            # disable other workflow-tabs during workflow execution
            self.disableOtherWorkflows.emit()

        # disable navigation buttons during run
        self.enableButtons(False)

        # scoped action to verify before running
        def verifyAndContinue():
            # On verification failure: this will raise an exception and abort the continue.
            self.view.tabView.verify()

            # On verification success: this will return the correct SNAPResponse.
            return model.continueAction(self)

        # do action
        continueOnSuccess = lambda success: self.advanceWorkflow() if success else None  # noqa E731
        self.handleAction(verifyAndContinue, None, continueOnSuccess)

    def handleAction(
        self,
        action: Callable[[Any], Any],
        args: Tuple[Any, ...] | Any | None,
        onSuccess: Callable[[None], None],
    ):
        """
        Send front-end task to a separate worker to complete.
        @param action : a Callable to be called on worker
        @param args : the argument action is to be called with
        @param onSuccess : another Callable, called on completion, must take no parameters
        """
        # do action
        self.worker = self.worker_pool.createWorker(target=action, args=args)
        self.worker.finished.connect(lambda: self.enableButtons(True))  # re-enable panel buttons on finish
        self.worker.finished.connect(lambda: self._setWorkflowIsRunning(False))
        self.worker.finished.connect(self.actionCompleted)
        self.worker.result.connect(self._handleComplications)
        self.worker.success.connect(onSuccess)
        self.cancellationRequest.connect(self.requestCancellation)
        self._setWorkflowIsRunning(True)
        self.worker_pool.submitWorker(self.worker)

    @Slot(bool)
    def _setWorkflowIsRunning(self, flag: bool):
        self._workflowIsRunning = flag
        if not flag:
            # Transfer ownership to `worker_pool` for final deletion.
            self.worker = None
        return self._workflowIsRunning
    
    @property
    def workflowIsRunning(self):
        return self._workflowIsRunning
    
    @Slot(bool)
    def enableButtons(self, enable):
        # This slot is necessary in order for the buttons to actually be updated from the worker.
        
        # *** DEBUG *** allow user cancellation
        # buttons = [self.view.continueButton, self.view.cancelButton, self.view.skipButton]
        buttons = [self.view.continueButton, self.view.skipButton]
        
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

    @Slot(object)
    def liveDataStateTransition(self, liveDataInfo: LiveDataState.Model):
        # The associated signal is of type ``Signal(SNAPResponseHandler.liveDataStateTransition) as Signal(object)``
        QMessageBox.information(
            self.view,
            "Live Data:",
            liveDataInfo.message
        )
        # Any live-data transition resets the workflow:
        #   at which point the request-view should display the new live-data status.
        self.reset()

    @Slot()
    def requestCancellation(self):
        if self.worker:
            # This supports coarse grained cancellation:
            #   possible only after each service request completes.
            #   Disabling the button here gives the user feedback that their action
            #   has actually had any effect.
            self.view.cancelButton.setEnabled(False)
            
            # This needs to be executed _off_ the worker's thread.
            # Otherwise it wouldn't happen until after the entire task
            #   is completed.
            self.worker.requestCancellation()
    
    @Slot(object)
    def userCancellation(self, userCancellationInfo: UserCancellation.Model):
        # We've already asked for permission.
        self.reset()
        
    def completeWorkflow(self, message: Optional[str] = Config["ui.default.workflow.completionMessage"]):
        # Directly show the completion message and reset the workflow
        if message is not None:
            QMessageBox.information(
                self.view,
                "‧₊Workflow Complete‧₊",
                message,
            )
        self.reset()
