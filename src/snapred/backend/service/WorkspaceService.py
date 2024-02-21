from snapred.backend.dao.request import ClearWorkspaceRequest, RenameWorkspaceRequest
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString


class WorkspaceService(Service):
    """
    This service is mostly an api for the front end to manipulate workspaces
    without letting implementation details leak into the front end.
    """

    def __init__(self):
        super().__init__()
        self.groceryService = GroceryService()
        self.registerPath("rename", self.rename)
        self.registerPath("clear", self.clear)
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
    def clear(self, request: ClearWorkspaceRequest):
        """
        Clears the workspaces, excluding the given list of items and cache.
        """
        self.groceryService.clearADS(request.exclude, request.cache)
