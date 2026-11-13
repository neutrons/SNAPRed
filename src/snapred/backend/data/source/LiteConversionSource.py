from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.NeutronDataResolver import NeutronDataResolver
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class LiteConversionSource(DataSource):
    """
    Lite-mode neutron data, converted from native-resolution data.

    The native-resolution data is obtained from the native-resolution sources, through a resolver of
    its own.  The cache is shared, so cached native data is reused, and native data from a source that
    is not cacheable is recorded as such.

    The converted data is exported to disk, where it becomes the lite-mode file for later fetches,
    only when the native data came from the NeXus file: data from a run that is still acquiring must
    never be written as though it were the complete data.
    """

    def __init__(self, nativeSources: DataSourceRegistry, cache: NeutronDataCache, workspaces: WorkspaceOperations):
        super().__init__(provides=Resolution.LITE)
        if nativeSources.resolution != Resolution.NATIVE:
            raise RuntimeError(
                f"implementation error: lite-mode conversion requires native sources, not {nativeSources.resolution}"
            )
        self.nativeData = NeutronDataResolver(nativeSources, cache, workspaces)
        self.workspaces = workspaces

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.LITE_CONVERSION

    def available(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        # Whether native data is available is decided by the native resolver, when the data is loaded:
        #   if it is not available, that resolver's error is the one that should be reported.
        return True

    def exclusiveFor(self, item: GroceryListItem) -> bool:
        # Data from an exclusive native source (registered data) is converted: a lite-mode file must not
        #   be used instead.
        return any(source.exclusiveFor(item) for source in self.nativeData.sources.ordered())

    @staticmethod
    def exportsFrom(nativeData: SourcedData) -> bool:
        """
        Whether data converted from this native data is exported.

        Data with an unknown provenance (a cached workspace that was not loaded through a data source)
        is treated as file data, as it was before provenance was recorded.
        """
        return not nativeData.provenance or nativeData.provenance[0] == DataSourceKind.NEXUS_FILE

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        nativeData = self.nativeData.resolve(item)
        self.workspaces.getCloneOfWorkspace(nativeData.workspace, outputWorkspace)
        self.convert(outputWorkspace, export=self.exportsFrom(nativeData))
        return self._sourced(outputWorkspace, nativeData.loader, derivedFrom=nativeData)

    def convert(self, workspace: WorkspaceName, export: bool = True):
        """Convert a workspace (in place) to lite mode, and optionally export it to disk."""
        from snapred.backend.service.LiteDataService import LiteDataService

        _, tolerance = LiteDataService().createLiteData(workspace, workspace, export=export)

        tag = UNSET
        if Config["constants.LiteDataCreationAlgo.toggleCompressionTolerance"]:
            tag = tolerance
        if tag != UNSET:
            WorkspaceMetadataService().writeWorkspaceMetadata(
                workspace, WorkspaceMetadata(liteDataCompressionTolerance=tag)
            )
