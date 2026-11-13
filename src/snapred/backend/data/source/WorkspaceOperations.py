from typing import Any

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class WorkspaceOperations:
    """The ADS operations that the data sources use, and that `GroceryService` provides."""

    def __init__(self, mantidSnapper: MantidSnapper):
        self.mantidSnapper = mantidSnapper

    def workspaceDoesExist(self, name: WorkspaceName) -> bool:
        return self.mantidSnapper.mtd.doesExist(name)

    def getWorkspaceForName(self, name: WorkspaceName) -> Any:
        """The workspace, or `None` if it does not exist."""
        return self.mantidSnapper.mtd[name] if self.workspaceDoesExist(name) else None

    def getCloneOfWorkspace(self, name: WorkspaceName, copy: WorkspaceName) -> WorkspaceName:
        self.mantidSnapper.CloneWorkspace(f"Cloning {name} to {copy}", InputWorkspace=name, OutputWorkspace=copy)
        self.mantidSnapper.executeQueue()
        return copy

    def renameWorkspace(self, oldName: WorkspaceName, newName: WorkspaceName) -> WorkspaceName:
        if oldName != newName:
            self.mantidSnapper.RenameWorkspace(
                f"Renaming '{oldName}' to '{newName}'", InputWorkspace=oldName, OutputWorkspace=newName
            )
            self.mantidSnapper.executeQueue()
        return newName

    def deleteWorkspaceUnconditional(self, name: WorkspaceName):
        """Delete a workspace, if it exists, regardless of CIS mode."""
        if self.workspaceDoesExist(name):
            self.mantidSnapper.DeleteWorkspace(f"Deleting workspace {name}", Workspace=name)
            self.mantidSnapper.executeQueue()
