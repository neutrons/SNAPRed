from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import SNAPRequest
from snapred.backend.dao.request import (
    ClearWorkspaceRequest,
    RenameWorkspaceFromTemplateRequest,
)
from snapred.backend.log.logger import snapredLogger
from snapred.ui.handler.SNAPResponseHandler import SNAPResponseHandler
from snapred.ui.widget.ActionPrompt import ActionPrompt
from snapred.ui.widget.Workflow import Workflow

logger = snapredLogger.getLogger(__name__)


class WorkflowImplementer:
    def __init__(self, parent=None):
        self.requests = []
        self.responses = []
        self.outputs = []
        self.collectiveOutputs = []
        self.interfaceController = InterfaceController()
        self.renameTemplate = "{workspaceName}_{iteration:02d}"
        self.parent = parent
        self.workflow: Workflow = None
        self.responseHandler = SNAPResponseHandler(self.parent)

    def _iterate(self, workflowPresenter):
        # rename output workspaces
        payload = RenameWorkspaceFromTemplateRequest(
            workspaces=self.outputs.copy(),
            renameTemplate=self.renameTemplate.format(
                workspaceName="{workspaceName}", iteration=workflowPresenter.iteration
            ),
        )
        response = self.request(path="workspace/renameFromTemplate", payload=payload.model_dump_json())
        self.outputs = response.data

        # add outputs to list of all outputs of every iteration
        self.collectiveOutputs.extend(self.outputs)
        # reset outputs for next set of outputs
        self.outputs = []

        # clear every other workspace
        payload = ClearWorkspaceRequest(exclude=self.collectiveOutputs)
        response = self.request(path="workspace/clear", payload=payload.model_dump_json())
        return response

    def reset(self):
        self.workflow.presenter.resetAndClear()
        self.requests = []
        self.responses = []
        self.outputs = []
        self.collectiveOutputs = []

    def resetWithPermission(self):
        ActionPrompt(
            "Are you sure?",
            "Are you sure you want to cancel the workflow? This will clear all workspaces.",
            self.reset,
            self.workflow.widget,
        )

    def _request(self, request: SNAPRequest):
        response = self.interfaceController.executeRequest(request)
        self._handleComplications(response)
        return response

    def request(self, path, payload=None):
        request = SNAPRequest(path=path, payload=payload)
        response = self._request(request)
        self.requests.append(request)
        self.responses.append(response)
        return response

    def _handleComplications(self, result):
        if result.code == 400:
            self.responseHandler.rethrow(result)
        else:
            self.responseHandler.handle(result)

    @property
    def widget(self):
        return self.workflow.presenter.widget
