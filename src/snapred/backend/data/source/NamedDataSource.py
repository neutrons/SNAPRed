from abc import abstractmethod
from typing import Optional

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataSource import DataSource
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class NamedDataSource(DataSource):
    """
    A source that names the workspace of its data.

    Where the name depends on where the data comes from (a calibration record, or an explicit file), the
    source decides it: the resolver asks the chosen source for the name, and then checks the cache.
    """

    @abstractmethod
    def outputName(self, item: GroceryListItem) -> Optional[WorkspaceName]:
        """The name of the workspace for the item, or `None` if this source has no such data for it."""
