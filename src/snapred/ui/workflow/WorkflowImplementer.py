from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import SNAPRequest
from snapred.backend.dao.request import (
    ClearWorkspaceRequest,
    RenameWorkspaceRequest,
)
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.IterateView import IterateView

logger = snapredLogger.getLogger(__name__)


class WorkflowImplementer:
    def __init__(self, parent=None):
        self.requests = []
        self.responses = []
        self.outputs = []
        self.collectiveOutputs = []
        self.interfaceController = InterfaceController()
        self.renameTemplate = "{workspaceName}_{iteration:02d}"

        self._iterateView = IterateView(parent)
        self.workflow = None

    def _iterate(self, workflowPresenter):
        # rename output workspaces
        for i, workspaceName in enumerate(self.outputs.copy()):
            newName = self.renameTemplate.format(workspaceName=workspaceName, iteration=workflowPresenter.iteration)
            self.outputs[i] = newName
            payload = RenameWorkspaceRequest(oldName=workspaceName, newName=newName)
            response = self.request(path="workspace/rename", payload=payload.json())

        # add outputs to list of all outputs of every iteration
        self.collectiveOutputs.extend(self.outputs)
        # reset outputs for next set of outputs
        self.outputs = []

        # clear every other workspace
        payload = ClearWorkspaceRequest(exclude=self.collectiveOutputs)
        response = self.request(path="workspace/clear", payload=payload.json())
        return response

    @property
    def iterateStepTuple(self):
        return (self._iterate, self._iterateView, "Go again?")

    def request(self, path, payload=None):
        request = SNAPRequest(path=path, payload=payload)
        response = self.interfaceController.executeRequest(request)
        self._handleComplications(response)
        self.requests.append(request)
        self.responses.append(response)
        return response

    def verifyForm(self, form):
        try:
            form.verify()
            return True
        except ValueError as e:
            return SNAPResponse(code=500, message=f"Missing Fields!{e}")

    def _handleComplications(self, result):
        if result.code >= 300:
            raise RuntimeError(result.message)

    @property
    def widget(self):
        return self.workflow.presenter.widget
