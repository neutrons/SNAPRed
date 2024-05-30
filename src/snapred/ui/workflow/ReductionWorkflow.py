from snapred.backend.dao.request import ReductionRequest
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.ui.view.ReductionView import ReductionView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class ReductionWorkflow(WorkflowImplementer):
    def __init__(self, parent=None):
        super().__init__(parent)

        self._reductionView = ReductionView(parent=parent)

        self._reductionView.enterRunNumberButton.clicked.connect(lambda: self._populatePixelMaskDropdown())

        self.workflow = (
            WorkflowBuilder(cancelLambda=self.resetWithPermission, parent=parent)
            .addNode(
                self._triggerReduction,
                self._reductionView,
                "Reduction",
            )
            .build()
        )
        self.workflow.presenter.setResetLambda(self.reset)

    @ExceptionToErrLog
    def _populatePixelMaskDropdown(self):
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
                # TODO: hardcoded for now till we pull it from the right file
                calibrantSamplePath="SNS/SNAP/shared/Calibration/CalibrationSamples/Diamond_001.json",
            )
            # TODO: Handle Continue Anyway
            self.request(path="reduction/", payload=payload.json())
            self._reductionView.removeRunNumber(runNumber)

        return self.responses[-1]

    @property
    def widget(self):
        return self.workflow.presenter.widget
