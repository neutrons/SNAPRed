from typing import Optional

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.NamedDataSource import NamedDataSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

LOADER = "LoadNexusMonitors"


def monitorName(runNumber: str) -> WorkspaceName:
    """The name of the monitor of a run.  A monitor has no lite-mode form."""
    return wng.monitor().runNumber(runNumber).build()


class MonitorSource(NamedDataSource):
    """
    The monitor of a run, from its native NeXus file.  A monitor has no lite-mode form: this source is native only.

    It is always available: if the run has no IPTS directory, the load raises an error that `ReductionService`
    recognizes, by its message.
    """

    def __init__(self, dataService, grocer):
        super().__init__(provides=Resolution.NATIVE, product=Product.MONITOR)
        self.dataService = dataService
        self.grocer = grocer

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.NEXUS_FILE

    def available(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        return True

    def outputName(self, item: GroceryListItem) -> Optional[WorkspaceName]:
        return monitorName(item.runNumber)

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        # Warning: `filePath` will be `None` if no IPTS directory exists!
        #   This situation occurs during diagnostic runs in live-data mode.
        filePath = self.dataService.createNeutronFilePath(item.runNumber, False)
        if not bool(filePath):
            raise RuntimeError(f"Cannot find IPTS directory for run '{item.runNumber}'")
        data = self.grocer.executeRecipe(str(filePath), outputWorkspace, LOADER)
        return self._sourced(outputWorkspace, data["loader"])
