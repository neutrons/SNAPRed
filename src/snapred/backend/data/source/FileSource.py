from mantid.api import FileLoaderRegistry

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class FileSource(DataSource):
    """
    Neutron data from its file on disk: the NeXus file, or the lite-mode file.

    The file is not used when live data is requested.
    """

    def __init__(self, resolution: Resolution, dataService, grocer):
        super().__init__(provides=resolution)
        self.dataService = dataService
        self.grocer = grocer

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.LITE_FILE if self.provides.useLiteMode else DataSourceKind.NEXUS_FILE

    def _filePath(self, item: GroceryListItem):
        # Warning: this will be `None` if no IPTS directory exists!
        #   This situation occurs during diagnostic runs in live-data mode.
        return self.dataService.createNeutronFilePath(item.runNumber, self.provides.useLiteMode)

    def available(self, item: GroceryListItem) -> bool:
        if item.liveDataArgs is not None:
            return False
        filePath = self._filePath(item)
        return bool(filePath) and filePath.exists()

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        filePath = self._filePath(item)
        loader = item.loader
        loaderArgs = "{}"
        if not bool(loader) and Config["nexus.dataFormat.event"]:
            # If the loader hasn't been specified, check if this is original input data in event format.
            # In this case, specifying only a single bin allows a much faster load.
            loader = FileLoaderRegistry.Instance().chooseLoader(str(filePath)).name()
            if loader == "LoadEventNexus":
                loaderArgs = '{"NumberOfBins": 1}'
        data = self.grocer.executeRecipe(str(filePath), outputWorkspace, loader, loaderArgs=loaderArgs)
        return self._sourced(outputWorkspace, data["loader"])
