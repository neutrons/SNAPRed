import json

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
        cancelLambda = None
        if parent is not None and hasattr(parent, "close"):
            cancelLambda = parent.close

        self._reductionView = ReductionView(parent=parent)

        self._reductionView.runNumberField.editingFinished.connect(self._populatePixelMaskDropdown)

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
        runNumbers = self._reductionView.runNumberField.text().split(",")
        useLiteMode = self._reductionView.litemodeToggle.field.getState()

        self._reductionView.litemodeToggle.setEnabled(False)
        self._reductionView.pixelMaskDropdown.setEnabled(False)

        for runNumber in runNumbers:
            try:
                self.request(path="reduction/hasState", payload=runNumber).data
            except Exception as e:
                print(e)

        self._reductionView.litemodeToggle.setEnabled(True)
        self._reductionView.pixelMaskDropdown.setEnabled(True)

    def _triggerReduction(
        self, workflowPresenter
    ):  # NOTE: Ths only works for a single run number, will need to refactor to support multiple run numbers.
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration reduction

        self.runNumber = view.fieldRunNumber.text()

        payload = RunConfig(runNumber=self.runNumber)
        request = SNAPRequest(path="reduction", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        # wrap workflow.presenter.widget in a QMainWindow
        # show the QMainWindow
        pass
