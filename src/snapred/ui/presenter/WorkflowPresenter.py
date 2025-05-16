from typing import Any, Callable, List, Optional, Tuple

from qtpy.QtCore import QObject, Signal, Slot
from qtpy.QtWidgets import QMainWindow, QMessageBox

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.error.UserCancellation import UserCancellation
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.ConfigDefault import ConfigDefault, ConfigValue
from snapred.ui.handler.SNAPResponseHandler import SNAPResponseHandler
from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.WorkflowView import WorkflowView
from snapred.ui.widget.ActionPrompt import ActionPrompt

logger = snapredLogger.getLogger(__name__)


class WorkflowPresenter(QObject):
    workflowInProgressChange = Signal(bool)
    cancellationRequest = Signal()
    enableAllWorkflows = Signal()
    disableOtherWorkflows = Signal()

    # Allow an observer (e.g. `qtbot` or the `reductionWorkflow` itself) to monitor action completion.
    actionCompleted = Signal()

    # Sent at the _end_ of the reset method
    # -- this is necessary to allow the parent workflow to override reset specifics.
    # (This is less than ideal, but the workflow's <reset hooks> are called at the _start_ of the reset method.)
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
        # No worker thread is actively engaged in running a workflow task.
        self._setWorkflowIsRunning(False)
        self.worker_pool = WorkerPool()

        # The first workflow node (i.e. tab) has not yet been started.
        self._setWorkflowInProgress(False)

        self.view = WorkflowView(model, parent)
        self._iteration = 1
        self.model = model

        # Normal operating mode for any workflow is _manual_ mode:
        #   e.g., in this mode the user clicks the `continue` button to start or to continue the workflow.
        # Alternatively, when manual mode is False, the workflow continues automatically, based on
        #   timing considerations.  Reduction of live data uses this mode.
        #   In this operating mode, the user clicks the `cancel` button to exit this automatic workflow loop.
        self._manualMode = True

        # All workflow "hook" methods must be initialized, either to a bound method
        #   or to an equivalent lambda function taking arguments as specified.
        self._startLambda: Callable[[], None] = startLambda if startLambda is not None else self._NOP
        self._iterateLambda: Callable[[WorkflowPresenter], None] = (
            iterateLambda if iterateLambda is not None else self._NOP
        )

        # WARNING: `reset` calls `self._resetLambda` -- don't make it circular!
        self._resetLambda: Callable[[], None] = resetLambda if resetLambda is not None else self._NOP

        self._cancelLambda: Callable[[], None] = cancelLambda if cancelLambda is not None else self.resetWithPermission

        self._completeWorkflowLambda: Callable[[], None] = (
            completeWorkflowLambda if completeWorkflowLambda is not None else self.completeWorkflow
        )

        self.externalWorkspaces: List[str] = []
        # Retain list of ADS-resident workspaces at start of workflow

        self.interfaceController = InterfaceController()

        self._hookupSignals()
        self.cancellationRequest.connect(self.requestCancellation)

        self.responseHandler = SNAPResponseHandler(self.view)
        self.responseHandler.continueAnyway.connect(self.continueAnyway)
        self.responseHandler.resetWorkflow.connect(self.reset)
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

    def resetSoft(self, manualOverride: bool = False):
        self.widget.reset(hard=False, manualMode=self.manualMode or manualOverride)

    def resetHard(self):
        self.widget.reset(hard=True)

    def reset(self):
        self._resetLambda()
        self.resetSoft(manualOverride=True)
        self._iteration = 1
        self._setWorkflowInProgress(False)

        # The following are required for live-data mode:
        #   but should be redundant for other modes.
        self.enableButtons(True)
        self.setInteractive(True)

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

    def resetWithPermission(self, *, shutdownLambda: Optional[Callable[[], None]] = None):
        # TODO: tracking a known defect:
        #   "`shutdownLambda` assigned to `bool`".

        shutdown = self.safeShutdown if shutdownLambda is None else shutdownLambda
        ActionPrompt.prompt(
            "Are you sure?",
            "Are you sure you want to cancel the workflow?\n" + "This will clear any partially-calculated results.",
            shutdown,
            parent=self.view,
            # Previously this used "Continue" / "Cancel" and was really confusing!
            # For a workflow cancellation request specifically,
            # please use "Yes" or "No", _not_ "Continue" or "Cancel"!
            buttonNames=("Yes", "No"),
        )

    def iterate(self):
        self._iterateLambda(self)
        self._iteration += 1
        self.resetSoft()

    @property
    def iteration(self):
        return self._iteration

    @property
    def manualMode(self) -> bool:
        return self._manualMode

    def setManualMode(self, flag: bool):
        # Manual mode is a _dynamic_ property, to be set by the workflow itself when it enters or leaves this mode.
        # (Alternatively, this could be a `@property.setter`, but that's not consistent with `qtpy` usage.)
        self._manualMode = flag

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

    @Slot()
    def handleIterateButtonClicked(self):
        self.iterate()

    @Slot()
    def handleSkipButtonClicked(self):
        self.advanceWorkflow()

    @Slot()
    def advanceWorkflow(self):
        if self.view.currentTab >= self.view.totalNodes - 1:
            self._completeWorkflowLambda()
        else:
            self.view.advanceWorkflow()

    @Slot(object)
    def handleContinueButtonClicked(self, model):
        # The associated signal is of type ``Signal(WorkflowNodeModel) as Signal(object)``
        if self.view.currentTab == 0:
            self._startLambda()

            # disable other workflow-tabs during workflow execution
            self.disableOtherWorkflows.emit()

            # indicate that the first workflow node (i.e. tab) has been started
            self._setWorkflowInProgress(True)

        # disable navigation buttons during run
        self.enableButtons(False)
        self.setInteractive(False)

        # scoped action to verify before running
        def verifyAndContinue():
            # On verification failure: this will raise an exception and abort the continue.
            self.view.tabView.verify()

            # On verification success: this will return the correct SNAPResponse.
            return model.continueAction(self)

        # do action
        self.handleAction(verifyAndContinue, None, self.continueOnSuccess)

    def handleAction(
        self,
        action: Callable[[Any], Any],
        args: Tuple[Any, ...] | Any | None,
        onSuccess: Callable[[bool], None],
        # Allow this thread pool to also be used for non-workflow actions,
        #   such as the really slow live-metadata update :( .
        isWorkflow=True,
    ):
        """
        Send front-end task to a separate worker to complete.
        @param action : a Callable to be called on worker
        @param args : the argument action is to be called with
        @param onSuccess : another Callable, called on completion, must take no parameters
        """
        # do action
        self.worker = self.worker_pool.createWorker(target=action, args=args)

        # Re-enable the panel buttons on finish:
        #   note that the reduction workflow in live-data mode doesn't do this,
        #   because its loop is only exited by clicking the cancel button.
        if self.manualMode:
            self.worker.finished.connect(lambda: self.enableButtons(True))

        self.worker.finished.connect(self.actionCompleted)
        self.worker.result.connect(self._handleComplications)
        self.worker.success.connect(onSuccess)

        if isWorkflow:
            self.worker.finished.connect(lambda: self._setWorkflowIsRunning(False))
            self._setWorkflowIsRunning(True)

        self.worker_pool.submitWorker(self.worker)

    @Slot(bool)
    def continueOnSuccess(self, success: bool):
        if success:
            self.advanceWorkflow()

    @Slot(bool)
    def _setWorkflowIsRunning(self, flag: bool):
        # `workflowIsRunning` property:
        #   a worker thread is actively engaged in processing a workflow-related task.

        self._workflowIsRunning = flag
        if not flag:
            # Transfer ownership to `worker_pool` for final deletion.
            self.worker = None
        return self._workflowIsRunning

    @property
    def workflowIsRunning(self):
        return self._workflowIsRunning

    @Slot(bool)
    def _setWorkflowInProgress(self, flag: bool):
        # `workflowInProgress` property:
        #   the workflow has started its first node(i.e. tab).

        self._workflowInProgress = flag
        self.workflowInProgressChange.emit(flag)
        return self._workflowInProgress

    @property
    def workflowInProgress(self):
        return self._workflowInProgress

    @Slot(bool)
    def enableButtons(self, enable):
        # This slot is necessary in order for the buttons to actually be updated from the worker.

        # A user-cancellation request is now supported in all views,
        #   so the `self.cancelButton` should never be disabled.
        #   (And should always be re-enabled after its use.)
        self.view.cancelButton.setEnabled(True)

        buttons = [self.view.continueButton, self.view.skipButton]
        for button in buttons:
            button.setEnabled(enable)

    @Slot(bool)
    def setInteractive(self, flag: bool):
        # Enable or disable all other controls (except the workflow-node buttons, see `enableButtons`).
        self.view.tabView.setInteractive(flag)

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
        QMessageBox.information(self.view, "Live Data:", liveDataInfo.message)
        # Any live-data transition resets the workflow:
        #   at which point the live-data part of the request view should display the new live-data status.

    @Slot()
    def requestCancellation(self):
        if self.worker:
            # This supports coarse-grained cancellation:
            #   possible only after each service request completes.

            # Disabling the button here gives the user feedback that their action
            #   has actually had any effect.
            self.view.cancelButton.setEnabled(False)

            # This needs to be executed _off_ the worker's thread.
            # Otherwise it wouldn't happen until after the entire task
            #   is completed.
            self.worker.requestCancellation()

    @Slot(object)
    def userCancellation(self, userCancellationInfo: UserCancellation.Model):  # noqa: ARG002
        # We've already asked for permission.
        self.reset()

    @ConfigDefault
    def completeWorkflow(self, message: str = ConfigValue("ui.default.workflow.completionMessage")):
        # Directly show the completion message and reset the workflow
        QMessageBox.information(
            self.view,
            "‧₊Workflow Complete‧₊",
            message,
        )
        self.reset()
