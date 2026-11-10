from typing import Any, Protocol

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class WorkspaceOperations(Protocol):
    """The ADS operations that data sources use.  `GroceryService` provides them."""

    def workspaceDoesExist(self, name: WorkspaceName) -> bool: ...

    def getWorkspaceForName(self, name: WorkspaceName) -> Any: ...

    def getCloneOfWorkspace(self, name: WorkspaceName, copy: WorkspaceName) -> WorkspaceName: ...

    def renameWorkspace(self, oldName: WorkspaceName, newName: WorkspaceName) -> WorkspaceName: ...

    def deleteWorkspaceUnconditional(self, name: WorkspaceName): ...
