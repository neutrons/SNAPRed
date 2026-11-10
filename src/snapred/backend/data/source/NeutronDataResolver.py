from typing import Any, Dict, List, Optional

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.NeutronWorkspaceNames import NeutronWorkspaceNames as names
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.meta.Config import Config

CACHED_LOADER = "cached"


class NeutronDataResolver:
    """
    Produces the neutron data for a grocery-list item, from the first available data source.

    For each resolution, the sources are tried in the order set by `neutronData.sources.<resolution>`.
    If the chosen source requires input data, that input is resolved first, in the same way.
    The cache is checked before any source, at each resolution.
    Every workspace that is produced is tagged with its provenance, as the `dataSource` workspace metadata.
    """

    def __init__(self, sources: List[DataSource], cache: NeutronDataCache, workspaces: WorkspaceOperations):
        self.sources: Dict[DataSourceKind, DataSource] = {source.kind: source for source in sources}
        self.cache = cache
        self.workspaces = workspaces

    def _order(self, resolution: Resolution) -> List[DataSource]:
        order = []
        for kind in Config[f"neutronData.sources.{resolution}"]:
            source = self.sources[DataSourceKind(kind)]
            if source.provides != resolution:
                raise RuntimeError(
                    f"configuration error: 'neutronData.sources.{resolution}' lists '{kind}', "
                    f"which provides {source.provides} data"
                )
            order.append(source)
        return order

    @staticmethod
    def _notFound(item: GroceryListItem) -> RuntimeError:
        return RuntimeError(
            f"Neutron data for run '{item.runNumber}' is not present on disk, and no live-data connection is available."
        )

    def _provenanceOf(self, workspace) -> List[DataSourceKind]:
        dataSource = WorkspaceMetadataService().readMetadataTag(workspace, "dataSource")
        return [] if dataSource == UNSET else SourcedData.provenanceFrom(dataSource)

    def _tag(self, data: SourcedData):
        WorkspaceMetadataService().writeWorkspaceMetadata(data.workspace, WorkspaceMetadata(dataSource=data.dataSource))

    def resolve(self, item: GroceryListItem, resolution: Resolution) -> SourcedData:
        """
        Produce the data at the requested resolution, under its plain workspace name.

        A cached workspace is copied; the cache is not otherwise changed, except to record data from
        a source that is not cacheable.
        """
        runNumber = item.runNumber
        useLiteMode = resolution.useLiteMode
        key = self.cache.key(runNumber, useLiteMode)
        outputWorkspace = names.plain(runNumber, useLiteMode)

        # Make sure that the existing cache entry is valid.
        self.cache.updateFromADS(runNumber, useLiteMode)
        if self.cache.contains(key):
            rawWorkspace = names.raw(runNumber, useLiteMode)
            self.workspaces.getCloneOfWorkspace(rawWorkspace, outputWorkspace)
            return SourcedData(
                workspace=outputWorkspace, loader=CACHED_LOADER, provenance=self._provenanceOf(rawWorkspace)
            )

        source = next((source for source in self._order(resolution) if source.available(item)), None)
        if source is None:
            raise self._notFound(item)

        inputData: Optional[SourcedData] = None
        if source.requires is not None:
            inputData = self.resolve(item, source.requires)
        data = source.load(item, outputWorkspace, inputData)
        self._tag(data)
        if not source.cacheable:
            self.cache.recordUncacheable(key)
        return data

    def fetchSingleUse(self, item: GroceryListItem) -> Dict[str, Any]:
        data = self.resolve(item, Resolution.fromLiteMode(item.useLiteMode))
        return self._result(data, data.workspace)

    def fetchCached(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Produce a numbered copy of the data, keeping the data itself in the cache as its raw workspace.

        Where lite-mode data was converted from native data, the native data is cached as well.
        """
        runNumber, useLiteMode = item.runNumber, item.useLiteMode
        data = self.resolve(item, Resolution.fromLiteMode(useLiteMode))

        # Data that is already in the cache (including data from an uncacheable source, which has a cache
        #   entry but no raw workspace) is copied from where it is.
        sourceWorkspace = data.workspace
        key = self.cache.key(runNumber, useLiteMode)
        if not self.cache.contains(key):
            self.cache.loadedRuns[key] = 0
            sourceWorkspace = self.workspaces.renameWorkspace(data.workspace, names.raw(runNumber, useLiteMode))
        if data.inputData is not None:
            nativeKey = self.cache.key(runNumber, False)
            if not self.cache.contains(nativeKey):
                self.cache.loadedRuns[nativeKey] = 0
                self.workspaces.renameWorkspace(data.inputData.workspace, names.raw(runNumber, False))

        workspace = names.copy(runNumber, useLiteMode, self.cache.loadedRuns[key] + 1)
        self.workspaces.getCloneOfWorkspace(sourceWorkspace, workspace)
        self.cache.loadedRuns[key] += 1
        return self._result(data, workspace)

    @staticmethod
    def _result(data: SourcedData, workspace) -> Dict[str, Any]:
        return {"result": True, "loader": data.loader, "workspace": workspace, "dataSource": data.dataSource}
