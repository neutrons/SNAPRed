from typing import Any, Dict, List

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.NeutronWorkspaceNames import NeutronWorkspaceNames as names
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService

CACHED_LOADER = "cached"


class NeutronDataResolver:
    """
    Produces the neutron data for a grocery-list item, at one resolution, from the first available
    source in a registry.

    The cache is checked before any source.
    Every workspace that is produced is tagged with its provenance, as the `dataSource` workspace metadata.
    """

    def __init__(self, sources: DataSourceRegistry, cache: NeutronDataCache, workspaces: WorkspaceOperations):
        self.sources = sources
        self.cache = cache
        self.workspaces = workspaces

    @property
    def resolution(self) -> Resolution:
        return self.sources.resolution

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

    def resolve(self, item: GroceryListItem) -> SourcedData:
        """
        Produce the data, under its plain workspace name.

        A cached workspace is copied; the cache is not otherwise changed, except to record data from
        a source that is not cacheable.
        """
        runNumber = item.runNumber
        useLiteMode = self.resolution.useLiteMode
        key = self.cache.key(runNumber, useLiteMode)
        outputWorkspace = names.plain(runNumber, useLiteMode)

        # Make sure that the existing cache entry is valid.
        self.cache.updateFromADS(runNumber, useLiteMode)
        if self.cache.contains(key):
            rawWorkspace = names.raw(runNumber, useLiteMode)
            self.workspaces.getCloneOfWorkspace(rawWorkspace, outputWorkspace)
            return SourcedData(
                workspace=outputWorkspace,
                loader=CACHED_LOADER,
                resolution=self.resolution,
                provenance=self._provenanceOf(rawWorkspace),
            )

        source = next((source for source in self.sources.ordered() if source.available(item)), None)
        if source is None:
            raise self._notFound(item)

        data = source.load(item, outputWorkspace)
        self._tag(data)
        if not source.cacheable:
            self.cache.recordUncacheable(key)
        return data

    def _checkResolution(self, item: GroceryListItem):
        if item.useLiteMode != self.resolution.useLiteMode:
            raise RuntimeError(
                f"implementation error: the resolver of {self.resolution} data was asked for "
                f"{Resolution.fromLiteMode(item.useLiteMode)} data"
            )

    def fetchSingleUse(self, item: GroceryListItem) -> Dict[str, Any]:
        self._checkResolution(item)
        data = self.resolve(item)
        return self._result(data, data.workspace)

    def fetchCached(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Produce a numbered copy of the data, keeping the data itself in the cache as its raw workspace.

        Where the data was derived from other data (lite-mode data converted from native data), that data
        is cached as well, at its own resolution.
        """
        self._checkResolution(item)
        runNumber, useLiteMode = item.runNumber, item.useLiteMode
        data = self.resolve(item)

        # Data that is already in the cache (including data from an uncacheable source, which has a cache
        #   entry but no raw workspace) is copied from where it is.
        sourceWorkspace = data.workspace
        key = self.cache.key(runNumber, useLiteMode)
        if not self.cache.contains(key):
            self.cache.loadedRuns[key] = 0
            sourceWorkspace = self.workspaces.renameWorkspace(data.workspace, names.raw(runNumber, useLiteMode))
        derived = data.derivedFrom
        while derived is not None:
            derivedLiteMode = derived.resolution.useLiteMode
            derivedKey = self.cache.key(runNumber, derivedLiteMode)
            if not self.cache.contains(derivedKey):
                self.cache.loadedRuns[derivedKey] = 0
                self.workspaces.renameWorkspace(derived.workspace, names.raw(runNumber, derivedLiteMode))
            derived = derived.derivedFrom

        workspace = names.copy(runNumber, useLiteMode, self.cache.loadedRuns[key] + 1)
        self.workspaces.getCloneOfWorkspace(sourceWorkspace, workspace)
        self.cache.loadedRuns[key] += 1
        return self._result(data, workspace)

    @staticmethod
    def _result(data: SourcedData, workspace) -> Dict[str, Any]:
        return {"result": True, "loader": data.loader, "workspace": workspace, "dataSource": data.dataSource}
