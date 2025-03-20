from typing import Callable, Set

from qtpy.QtCore import QObject, QThread

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.request import (
    ClearWorkspacesRequest,
    ListWorkspacesRequest,
    RenameWorkspacesFromTemplateRequest,
)
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.UserCancellation import UserCancellation
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.ui.handler.SNAPResponseHandler import SNAPResponseHandler
from snapred.ui.widget.Workflow import Workflow

logger = snapredLogger.getLogger(__name__)


class WorkflowImplementer(QObject):
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
        self.outputs: Set[WorkspaceName] = set()

        # Collected output workspaces from all iterations
        #   of the current workflow node.
        self.collectedOutputs: Set[WorkspaceName] = set()

        # List of ADS-resident workspaces external to SNAPRed:
        #   * This set is updated before the start of each workflow node.
        #     This allows an end user to work in Mantid, while the SNAPRed panel is open,
        #     And have any workspaces they create not be deleted by SNAPRed.
        #   * As an interim solution, to-be persisted workspaces created by SNAPRed
        #     can also be added to this set as required(, for example, reduction-output workspaces).
        self.externalWorkspaces: Set[str] = set()

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
            workspaces=list(self.outputs),
            renameTemplate=self.renameTemplate.format(
                workspaceName="{workspaceName}", iteration=workflowPresenter.iteration
            ),
        )
        response = self.request(path="workspace/renameFromTemplate", payload=payload.model_dump_json())
        self.outputs = set(response.data)

        # Add output workspaces to the list of outputs including all iterations
        self.collectedOutputs.update(self.outputs)

        # reset outputs for next set of outputs
        self.outputs.clear()

        # Remove all other SNAPRed workspaces
        response = self._clearWorkspaces(exclude=self.collectedOutputs, clearCachedWorkspaces=False)
        return response

    def start(self):
        # Retain the list of ADS-resident workspaces at a timepoint before the start of each workflow node.
        self.externalWorkspaces = set(
            self.request(
                path="workspace/getResidentWorkspaces",
                payload=ListWorkspacesRequest(excludeCache=True),
            ).data
        )

        # If this "start" follows an iteration, remove the "collectedOutputs" from the
        #   "externalWorkspaces" list:
        if len(self.collectedOutputs):
            self.externalWorkspaces = self.externalWorkspaces.difference(self.collectedOutputs)

    def reset(self, retainOutputs=False):
        exclude = self.outputs if retainOutputs else set()
        self._clearWorkspaces(exclude=exclude, clearCachedWorkspaces=True)
        self.requests = []
        self.responses = []
        self.outputs.clear()
        self.collectedOutputs.clear()
        self.continueAnywayFlags = ContinueWarning.Type.UNSET

        for hook in self.resetHooks:
            logger.debug(f"Calling reset hook: {hook}")
            hook()

    def _clearWorkspaces(self, *, exclude: Set[str], clearCachedWorkspaces: bool):
        # Always exclude any external workspaces.
        exclude_ = self.externalWorkspaces.copy()
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
        # Coarse-granularity user-cancellation request:
        #   this supports a possible cancellation, but only between each service call.
        if QThread.currentThread().isInterruptionRequested():
            logger.error("User cancellation request")
            return SNAPResponse(
                code=ResponseCode.USER_CANCELLATION,
                message=UserCancellation(
                    f"WorkflowImplementer: user cancellation: before execution of: {request.json()}."
                ).model.model_dump_json(),
            )

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
        # requests never execute on the main thread
        # so this should just rethrow and let the main thread handle it
        self.responseHandler.rethrow(result)

    def _continueAnywayHandler(self, continueInfo):
        if isinstance(continueInfo, ContinueWarning.Model):
            self.continueAnywayFlags = self.continueAnywayFlags | continueInfo.flags
        else:
            raise ValueError(f"Invalid continueInfo type: {type(continueInfo)}, expecting ContinueWarning.Model.")

    @property
    def widget(self):
        return self.workflow.presenter.widget
