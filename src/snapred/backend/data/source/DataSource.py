from abc import ABC, abstractmethod
from typing import ClassVar, Optional

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class DataSource(ABC):
    """
    A source of one kind of data, at one resolution.

    * `requires`: the resolution of the input data that a transforming source needs, if any;
      the resolver produces this input, and passes it to `load`.
    * `cacheable`: whether the data that it produces may be reused from the cache.
    """

    cacheable: ClassVar[bool] = True

    def __init__(self, provides: Resolution, requires: Optional[Resolution] = None):
        self.provides = provides
        self.requires = requires

    @property
    @abstractmethod
    def kind(self) -> DataSourceKind:
        """The kind of this source, as recorded in the provenance of its data."""

    @abstractmethod
    def available(self, item: GroceryListItem) -> bool:
        """Whether this source can provide the data for the item."""

    @abstractmethod
    def load(
        self, item: GroceryListItem, outputWorkspace: WorkspaceName, inputData: Optional[SourcedData] = None
    ) -> SourcedData:
        """Bring the data for the item into the ADS, as `outputWorkspace`."""

    def _sourced(self, workspace: WorkspaceName, loader: str, inputData: Optional[SourcedData] = None) -> SourcedData:
        provenance = ([*inputData.provenance] if inputData is not None else []) + [self.kind]
        return SourcedData(workspace=workspace, loader=loader, provenance=provenance, inputData=inputData)
