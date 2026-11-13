from typing import List, Set

from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class ResidentDataCache:
    """A workspace that is resident in the ADS, under its name, is reused."""

    def __init__(self, workspaces: WorkspaceOperations):
        self.workspaces = workspaces

    def contains(self, workspace: WorkspaceName) -> bool:
        return self.workspaces.workspaceDoesExist(workspace)

    def beforeLoad(self, workspace: WorkspaceName):
        """Called before a workspace is loaded."""

    def record(self, workspace: WorkspaceName):
        """Called after a workspace is loaded."""

    def cachedWorkspaces(self) -> List[WorkspaceName]:
        return []


class SingleEntryDataCache(ResidentDataCache):
    """As `ResidentDataCache`, but only the last workspace that was loaded is kept: the others are deleted."""

    def __init__(self, workspaces: WorkspaceOperations):
        super().__init__(workspaces)
        self.entries: Set[WorkspaceName] = set()

    def clear(self):
        for workspace in self.entries:
            self.workspaces.deleteWorkspaceUnconditional(workspace)
        self.entries = set()

    def beforeLoad(self, workspace: WorkspaceName):  # noqa: ARG002
        self.clear()

    def record(self, workspace: WorkspaceName):
        self.entries.add(workspace)

    def cachedWorkspaces(self) -> List[WorkspaceName]:
        return list(self.entries)
