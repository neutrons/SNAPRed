from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import RunConfig, SNAPRequest
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.ui.view.ReductionView import ReductionView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class ReductionWorkflow(WorkflowImplementer):
    def __init__(self, parent=None):
        self.requests = []
        self.responses = []
        self.interfaceController = InterfaceController()

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

        responses = []
        for runNumber in runNumbers:
            payload = RunConfig(runNumber=runNumber)
            # TODO: Handle Continue Anyway
            request = SNAPRequest(path="reduction/reduce", payload=payload.json())
            response = self.interfaceController.executeRequest(request)
            responses.append(response)
            # pop runnumber from the list
            self._reductionView.removeRunNumber(runNumber)
            self.responses.append(response)

        return responses

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        # wrap workflow.presenter.widget in a QMainWindow
        # show the QMainWindow
        pass
