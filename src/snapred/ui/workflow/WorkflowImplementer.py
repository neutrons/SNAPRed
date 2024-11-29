import threading
from typing import Callable, List

from qtpy.QtCore import QObject, Signal

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import SNAPRequest
from snapred.backend.dao.request import (
    ClearWorkspacesRequest,
    ListWorkspacesRequest,
    RenameWorkspacesFromTemplateRequest,
)
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.ui.handler.SNAPResponseHandler import SNAPResponseHandler
from snapred.ui.widget.Workflow import Workflow

logger = snapredLogger.getLogger(__name__)


class WorkflowImplementer(QObject):
    # I'm not sure, but this seems to be deprecated:
    # enableAllWorkflows = Signal()

    def __init__(self, parent=None):
        super().__init__()
        self.parent = parent

        # 'InterfaceController' is a singleton:
        #   declaring it as an instance attribute, instead of a class attribute,
        #   allows singleton reset during testing.
        self.interfaceController = InterfaceController()

        self.responseHandler = SNAPResponseHandler(parent)
        self.workflow: Workflow = None

        self.requests = []
        self.responses = []

        # Output workspaces from each workflow node.
        self.outputs = []
        # Collected output workspaces from all iterations
        #   of the current workflow node.
        self.collectedOutputs = []

        # List of ADS-resident workspaces external to SNAPRed:
        #   * This list is updated before the start of each workflow node.
        #     This allows an end user to work in Mantid, while the SNAPRed panel is open,
        #     And have any workspaces they create not be deleted by SNAPRed.
        #   * As an interim solution, to-be persisted workspaces created by SNAPRed
        #     can also be added to this list as required(, for example, reduction-output workspaces).
        self.externalWorkspaces: List[str] = []

        self.renameTemplate = "{workspaceName}_{iteration:02d}"
        self.parent = parent
        self.workflow: Workflow = None
        self.continueAnywayFlags = ContinueWarning.Type.UNSET
        self.responseHandler = SNAPResponseHandler(self.parent)

        self.resetHooks = []

    def addResetHook(self, hook: Callable[[], None]):
        self.resetHooks.append(hook)

    def iterate(self, workflowPresenter):
        # rename output workspaces
        payload = RenameWorkspacesFromTemplateRequest(
            workspaces=self.outputs.copy(),
            renameTemplate=self.renameTemplate.format(
                workspaceName="{workspaceName}", iteration=workflowPresenter.iteration
            ),
        )
        response = self.request(path="workspace/renameFromTemplate", payload=payload.model_dump_json())
        self.outputs = response.data

        # Add output workspaces to the list of outputs including all iterations
        self.collectedOutputs.extend(self.outputs)
        # reset outputs for next set of outputs
        self.outputs = []

        # Remove all other SNAPRed workspaces
        response = self._clearWorkspaces(exclude=self.collectedOutputs, clearCachedWorkspaces=False)
        return response

    def start(self):
        # Retain the list of ADS-resident workspaces at a timepoint before the start of each workflow node.
        self.externalWorkspaces = self.request(
            path="workspace/getResidentWorkspaces",
            payload=ListWorkspacesRequest(excludeCache=True),
        ).data

        # If this "start" follows an iteration, remove the "collectedOutputs" from the
        #   "externalWorkspaces" list:
        if len(self.collectedOutputs):
            externalWorkspaces = set(self.externalWorkspaces).difference(self.collectedOutputs)
            self.externalWorkspaces = list(externalWorkspaces)

    def reset(self, retainOutputs=False):
        exclude = self.outputs if retainOutputs else []
        self._clearWorkspaces(exclude=exclude, clearCachedWorkspaces=True)
        self.requests = []
        self.responses = []
        self.outputs = []
        self.collectedOutputs = []

        for hook in self.resetHooks:
            logger.info(f"Calling reset hook: {hook}")
            hook()

    def _clearWorkspaces(self, *, exclude: List[str], clearCachedWorkspaces: bool):
        # Always exclude any external workspaces.
        exclude_ = set(self.externalWorkspaces)
        exclude_.update(exclude)
        payload = ClearWorkspacesRequest(exclude=list(exclude_), clearCache=clearCachedWorkspaces)
        response = self.request(path="workspace/clear", payload=payload.json())
        return response

    def complete(self):
        for hook in self.resetHooks:
            hook()

    def completionMessage(self):
        return Config["ui.default.workflow.completionMessage"]

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
        # WARNING: here we are probably not on the thread that executed the request,
        #   and a `rethrow` isn't going to magically change the thread; for that we
        #   need to send a signal.
        self.responseHandler.handle(result)
        
    def _continueAnywayHandler(self, continueInfo):
        if isinstance(continueInfo, ContinueWarning.Model):
            self.continueAnywayFlags = self.continueAnywayFlags | continueInfo.flags
        else:
            raise ValueError(f"Invalid continueInfo type: {type(continueInfo)}, expecting ContinueWarning.Model.")

    @property
    def widget(self):
        return self.workflow.presenter.widget
