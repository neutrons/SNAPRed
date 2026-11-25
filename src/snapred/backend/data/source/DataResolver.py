from typing import List, Optional

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.data.source.DataCache import ResidentDataCache
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService

CACHED_LOADER = "cached"


class DataResolver:
    """
    Produces the data of one product, at one resolution, for a grocery-list item, from a registry of named sources.

    1. An exclusive source is used, if there is one; otherwise, the first available source, in the configured order.
    2. That source names the workspace.  If it has no such data for the item, there is no data (`None`).
    3. If the cache has the workspace, it is reused.  Otherwise, the source loads it.

    A workspace that has logs is tagged with its provenance, as the `dataSource` workspace metadata.
    (A table workspace has no logs: its provenance is only returned.)
    """

    def __init__(self, sources: DataSourceRegistry, cache: ResidentDataCache, workspaces: WorkspaceOperations):
        self.sources = sources
        self.cache = cache
        self.workspaces = workspaces

    @property
    def resolution(self):
        return self.sources.resolution

    def _hasLogs(self, workspace) -> bool:
        return hasattr(self.workspaces.getWorkspaceForName(workspace), "getRun")

    def _provenanceOf(self, workspace) -> List[DataSourceKind]:
        if not self._hasLogs(workspace):
            return []
        dataSource = WorkspaceMetadataService().readMetadataTag(workspace, "dataSource")
        return [] if dataSource == UNSET else SourcedData.provenanceFrom(dataSource)

    def _tag(self, data: SourcedData):
        if self._hasLogs(data.workspace):
            WorkspaceMetadataService().writeWorkspaceMetadata(
                data.workspace, WorkspaceMetadata(dataSource=data.dataSource)
            )

    def resolve(self, item: GroceryListItem) -> Optional[SourcedData]:
        if item.useLiteMode != self.resolution.useLiteMode:
            raise RuntimeError(
                f"implementation error: the resolver of {self.resolution} {self.sources.product} data "
                f"was asked for lite mode: {item.useLiteMode}"
            )
        sources = self.sources.ordered()
        source = next((source for source in sources if source.exclusiveFor(item)), None)
        if source is None:
            source = next((source for source in sources if source.available(item)), None)
        if source is None:
            raise RuntimeError(
                f"No source of {self.resolution} {self.sources.product} data is available for run '{item.runNumber}'"
            )

        workspace = source.outputName(item)
        if workspace is None:
            return None
        if self.cache.contains(workspace):
            return SourcedData(
                workspace=workspace,
                loader=CACHED_LOADER,
                resolution=self.resolution,
                provenance=self._provenanceOf(workspace),
            )
        self.cache.beforeLoad(workspace)
        data = source.load(item, workspace)
        self._tag(data)
        self.cache.record(workspace)
        return data
