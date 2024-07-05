from typing import List

from snapred.backend.dao.request import (
    ClearWorkspacesRequest,
    ListWorkspacesRequest,
    RenameWorkspaceRequest,
    RenameWorkspacesFromTemplateRequest,
)
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class WorkspaceService(Service):
    """
    This service is mostly an api for the front end to manipulate workspaces
    without letting implementation details leak into the front end.
    """

    def __init__(self):
        super().__init__()
        self.groceryService = GroceryService()
        self.registerPath("rename", self.rename)
        self.registerPath("renameFromTemplate", self.renameFromTemplate)
        self.registerPath("clear", self.clear)
        self.registerPath("getResidentWorkspaces", self.getResidentWorkspaces)
        return

    @staticmethod
    def name():
        return "workspace"

    @FromString
    def rename(self, request: RenameWorkspaceRequest):
        """
        Renames the workspace with the given name to the new name.
        """
        self.groceryService.renameWorkspace(request.oldName, request.newName)

    @FromString
    def renameFromTemplate(self, request: RenameWorkspacesFromTemplateRequest) -> List[WorkspaceName]:
        """
        Renames workspaces by applying a template to them.
        """
        newWorkspaces = [request.renameTemplate.format(workspaceName=oldName) for oldName in request.workspaces]
        # if any workspace is a group, rename all sub-workspaces using a recursive call
        for workspace in request.workspaces:
            ws = self.groceryService.getWorkspaceForName(workspace)
            if ws.isGroup():
                subRequest = RenameWorkspacesFromTemplateRequest(
                    workspaces=ws.getNames(),
                    renameTemplate=request.renameTemplate,
                )
                self.renameFromTemplate(subRequest)
        self.groceryService.renameWorkspaces(request.workspaces, newWorkspaces)
        return newWorkspaces

    @FromString
    def clear(self, request: ClearWorkspacesRequest):
        """
        Clears the workspaces, excluding the given list of items and cache.
        """
        self.groceryService.clearADS(request.exclude, request.clearCache)

    def getResidentWorkspaces(self, request: ListWorkspacesRequest):
        """
        Gets the list of workspaces resident in the ADS:

        - optionally excludes the cached workspaces from this list.
        """
        return self.groceryService.getResidentWorkspaces(excludeCache=request.excludeCache)
