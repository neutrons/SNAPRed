from typing import Optional

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class LiteConversionSource(DataSource):
    """
    Lite-mode neutron data, converted from native-resolution data.

    The converted data is exported to disk, where it becomes the lite-mode file for later fetches,
    only when its input came from the NeXus file: data from a run that is still acquiring must
    never be written as though it were the complete data.
    """

    def __init__(self, workspaces: WorkspaceOperations):
        super().__init__(provides=Resolution.LITE, requires=Resolution.NATIVE)
        self.workspaces = workspaces

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.LITE_CONVERSION

    def available(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        # Availability depends only on the input, which the resolver produces.
        return True

    @staticmethod
    def exportsFrom(inputData: SourcedData) -> bool:
        """
        Whether data converted from this input is exported.

        Data with an unknown provenance (a cached workspace that was not loaded through a data source)
        is treated as file data, as it was before provenance was recorded.
        """
        return not inputData.provenance or inputData.provenance[0] == DataSourceKind.NEXUS_FILE

    def load(
        self, item: GroceryListItem, outputWorkspace: WorkspaceName, inputData: Optional[SourcedData] = None
    ) -> SourcedData:
        if inputData is None:
            raise RuntimeError(f"implementation error: lite-mode conversion of run '{item.runNumber}' has no input")
        self.workspaces.getCloneOfWorkspace(inputData.workspace, outputWorkspace)
        self.convert(outputWorkspace, export=self.exportsFrom(inputData))
        return self._sourced(outputWorkspace, inputData.loader, inputData)

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
