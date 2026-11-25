from abc import ABC, abstractmethod
from typing import List, Set, Tuple

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class DataCache(ABC):
    """
    The cache of a `DataResolver`.

    The resolver passes the name that the chosen source gives for an item.  The cache decides the name that is
    used, whether the data is reused or loaded: a cache that is keyed by more than the name (for example, by
    state) may name a workspace that was loaded for another item.
    """

    @abstractmethod
    def lookup(self, item: GroceryListItem, proposedName: WorkspaceName) -> Tuple[WorkspaceName, bool]:
        """The name to use for the item's data, and whether that data is cached."""

    def beforeLoad(self, item: GroceryListItem, workspace: WorkspaceName):
        """Called before a workspace is loaded for the item."""

    def record(self, item: GroceryListItem, workspace: WorkspaceName):
        """Called after a workspace is loaded for the item."""

    def cachedWorkspaces(self) -> List[WorkspaceName]:
        return []


class ResidentDataCache(DataCache):
    """A workspace that is resident in the ADS, under its name, is reused.  The item is not used."""

    def __init__(self, workspaces: WorkspaceOperations):
        self.workspaces = workspaces

    def lookup(self, item: GroceryListItem, proposedName: WorkspaceName) -> Tuple[WorkspaceName, bool]:  # noqa: ARG002
        return proposedName, self.workspaces.workspaceDoesExist(proposedName)


class SingleEntryDataCache(ResidentDataCache):
    """As `ResidentDataCache`, but only the last workspace that was loaded is kept: the others are deleted."""

    def __init__(self, workspaces: WorkspaceOperations):
        super().__init__(workspaces)
        self.entries: Set[WorkspaceName] = set()

    def clear(self):
        for workspace in self.entries:
            self.workspaces.deleteWorkspaceUnconditional(workspace)
        self.entries = set()

    def beforeLoad(self, item: GroceryListItem, workspace: WorkspaceName):  # noqa: ARG002
        self.clear()

    def record(self, item: GroceryListItem, workspace: WorkspaceName):  # noqa: ARG002
        self.entries.add(workspace)

    def cachedWorkspaces(self) -> List[WorkspaceName]:
        return list(self.entries)
