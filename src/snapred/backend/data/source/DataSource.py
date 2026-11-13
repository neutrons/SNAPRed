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

    A source that derives its data from other data (for example, lite-mode data converted from
    native-resolution data) obtains that data itself, from the sources that provide it.

    * `cacheable`: whether the data that it produces may be reused from the cache.
    * `exclusiveFor(item)`: whether this source is the only one that may provide the data for the item.
    """

    cacheable: ClassVar[bool] = True

    def __init__(self, provides: Resolution):
        self.provides = provides

    @property
    @abstractmethod
    def kind(self) -> DataSourceKind:
        """The kind of this source, as recorded in the provenance of its data."""

    @abstractmethod
    def available(self, item: GroceryListItem) -> bool:
        """Whether this source can provide the data for the item."""

    def exclusiveFor(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        """Whether this source must provide the data for the item, to the exclusion of any other source."""
        return False

    @abstractmethod
    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        """Bring the data for the item into the ADS, as `outputWorkspace`."""

    def _sourced(self, workspace: WorkspaceName, loader: str, derivedFrom: Optional[SourcedData] = None) -> SourcedData:
        provenance = ([*derivedFrom.provenance] if derivedFrom is not None else []) + [self.kind]
        return SourcedData(
            workspace=workspace,
            loader=loader,
            resolution=self.provides,
            provenance=provenance,
            derivedFrom=derivedFrom,
        )
