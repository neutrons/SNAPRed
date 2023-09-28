from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import RunConfig, SNAPRequest
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.ReductionView import ReductionView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder

logger = snapredLogger.getLogger(__name__)


class ReductionWorkflow:
    def __init__(self, parent=None):
        # create a tree of flows for the user to successfully execute diffraction calibration
        # Calibrate     ->
        # Assess        ->
        # Save?         ->
        self.requests = []
        self.responses = []
        self.interfaceController = InterfaceController()
        cancelLambda = None
        if parent is not None and hasattr(parent, "close"):
            cancelLambda = parent.close

        self._reductionView = ReductionView(parent=parent)

        self.workflow = (
            WorkflowBuilder(cancelLambda=cancelLambda, parent=parent)
            .addNode(
                self._triggerReduction,
                self._reductionView,
                "Reduction",
            )
            .build()
        )

    def _triggerReduction(self, workflowPresenter):
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
