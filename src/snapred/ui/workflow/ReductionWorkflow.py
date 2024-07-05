from snapred.backend.dao.request import ReductionRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.ui.view.reduction.ReductionSaveView import ReductionSaveView
from snapred.ui.view.reduction.ReductionView import ReductionView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class ReductionWorkflow(WorkflowImplementer):
    def __init__(self, parent=None):
        super().__init__(parent)

        self._reductionView = ReductionView(parent=parent)
        self.continueAnywayFlags = None

        self._reductionView.enterRunNumberButton.clicked.connect(lambda: self._populatePixelMaskDropdown())

        # TODO; Save Screen, to give users a chance to save their work before the reduction
        # completes and erases the data

        self.workflow = (
            WorkflowBuilder(cancelLambda=self.resetWithPermission, parent=parent)
            .addNode(
                self._triggerReduction,
                self._reductionView,
                "Reduction",
                continueAnywayHandler=self._continueReductionHandler,
            )
            .addNode(self._nothing, ReductionSaveView(parent=parent), "Save")
            .build()
        )
        self.workflow.presenter.setResetLambda(self.reset)

    def _nothing(self, workflowPresenter):  # noqa: ARG002
        return SNAPResponse(code=200)

    def _continueReductionHandler(self, continueInfo):
        if isinstance(continueInfo, ContinueWarning.Model):
            self.continueAnywayFlags = self.continueAnywayFlags | continueInfo.flag
        else:
            raise ValueError(f"Invalid continueInfo type: {type(continueInfo)}, expecting ContinueWarning.Model.")

    @ExceptionToErrLog
    def _populatePixelMaskDropdown(self):
        if len(self._reductionView.getRunNumbers()) == 0:
            return

        runNumbers = self._reductionView.getRunNumbers()
        useLiteMode = self._reductionView.liteModeToggle.field.getState()  # noqa: F841

        self._reductionView.liteModeToggle.setEnabled(False)
        self._reductionView.pixelMaskDropdown.setEnabled(False)

        for runNumber in runNumbers:
            try:
                self.request(path="reduction/hasState", payload=runNumber).data
            except Exception as e:  # noqa: BLE001
                print(e)

        self._reductionView.liteModeToggle.setEnabled(True)
        self._reductionView.pixelMaskDropdown.setEnabled(True)

    def _triggerReduction(self, workflowPresenter):
        view = workflowPresenter.widget.tabView  # noqa: F841

        runNumbers = self._reductionView.getRunNumbers()

        for runNumber in runNumbers:
            payload = ReductionRequest(
                runNumber=runNumber,
                useLiteMode=self._reductionView.liteModeToggle.field.getState(),
                continueFlags=self.continueAnywayFlags,
            )
            # TODO: Handle Continue Anyway
            self.request(path="reduction/", payload=payload.json())

            # Note: the run number is deliberately not deleted from the run numbers list.

        return self.responses[-1]

    @property
    def widget(self):
        return self.workflow.presenter.widget
