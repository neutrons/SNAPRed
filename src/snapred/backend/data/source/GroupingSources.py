from typing import Dict, List, Optional, Tuple

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataCache import DataCache
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.InstrumentDonors import InstrumentDonors
from snapred.backend.data.source.NamedDataSource import NamedDataSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

LOADER = "LoadGroupingDefinition"

# The grouping scheme of the lite-mode map: its file is `instrument.lite.map.file`, not in the grouping map.
LITE_SCHEME = "Lite"


def groupingWorkspaceName(groupingScheme: str, runNumber: str, useLiteMode: bool) -> WorkspaceName:
    """The name of a grouping workspace, as loaded for a run."""
    # TODO: use WNG here!
    if groupingScheme == LITE_SCHEME:
        return wng.liteDataMap().build()
    instr = "lite" if useLiteMode else "native"
    return f"__{Config['grouping.workspacename.' + instr]}_{groupingScheme}_{runNumber}"


class GroupingFileSource(NamedDataSource):
    """
    A grouping workspace, from the grouping-definition file that the state's grouping map names for the
    item's grouping scheme (or, for the `Lite` scheme, from `instrument.lite.map.file`).

    Native and lite mode share this implementation: the resolution only selects which definition is found.
    """

    def __init__(self, resolution: Resolution, dataService, grocer, instrumentDonors: InstrumentDonors):
        super().__init__(provides=resolution, product=Product.GROUPING)
        self.dataService = dataService
        self.grocer = grocer
        self.instrumentDonors = instrumentDonors

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.GROUPING_FILE

    def available(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        # A missing definition is reported by the lookup.
        return True

    def outputName(self, item: GroceryListItem) -> Optional[WorkspaceName]:
        return groupingWorkspaceName(item.groupingScheme, item.runNumber, item.useLiteMode)

    def filename(self, item: GroceryListItem) -> str:
        if item.groupingScheme == LITE_SCHEME:
            return str(Config["instrument.lite.map.file"])
        groupingMap = self.dataService.readGroupingMap(item.runNumber)
        return str(groupingMap.getMap(item.useLiteMode)[item.groupingScheme].definition)

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        # Unless overridden: use a cached workspace as the instrument donor.
        instrumentPropertySource, instrumentSource = (
            ("InstrumentDonor", self.instrumentDonors.fetch(item.runNumber, item.useLiteMode))
            if not item.instrumentPropertySource
            else (item.instrumentPropertySource, item.instrumentSource)
        )
        data = self.grocer.executeRecipe(
            filename=self.filename(item),
            workspace=outputWorkspace,
            loader=LOADER,
            instrumentPropertySource=instrumentPropertySource,
            instrumentSource=instrumentSource,
        )
        return self._sourced(data["workspace"], data["loader"])


class GroupingDataCache(DataCache):
    """
    The grouping workspaces, by grouping scheme, state and resolution.

    A grouping is reused for every run of the same state, under the name that it was first loaded with.  That name
    is used even when the workspace is no longer in the ADS, so that it is then reloaded under that name.
    Each entry is made to agree with the ADS when it is looked up: a resident workspace without an entry is
    recorded, and an entry whose workspace is no longer resident is dropped.
    """

    def __init__(self, dataService, workspaces: WorkspaceOperations):
        self.dataService = dataService
        self.workspaces = workspaces
        #   (<grouping scheme>, stateId, useLiteMode) -> <workspace name>
        self.entries: Dict[Tuple[str, str, bool], WorkspaceName] = {}

    def key(self, item: GroceryListItem) -> Tuple[str, str, bool]:
        stateId, _ = self.dataService.generateStateId(item.runNumber)
        return (item.groupingScheme, stateId, item.useLiteMode)

    def updateFromADS(self, key: Tuple[str, str, bool], workspace: WorkspaceName):
        """Make the entry for the key agree with the ADS, for the workspace that it names."""
        if self.workspaces.workspaceDoesExist(workspace) and self.entries.get(key) is None:
            self.entries[key] = workspace
        elif self.entries.get(key) is not None and not self.workspaces.workspaceDoesExist(workspace):
            del self.entries[key]

    def lookup(self, item: GroceryListItem, proposedName: WorkspaceName) -> Tuple[WorkspaceName, bool]:
        key = self.key(item)
        workspace = self.entries.get(key, proposedName)
        self.updateFromADS(key, workspace)
        return workspace, key in self.entries

    def record(self, item: GroceryListItem, workspace: WorkspaceName):
        self.entries[self.key(item)] = workspace

    def rebuild(self):
        """Drop the entries whose workspaces are no longer in the ADS."""
        for key, workspace in self.entries.copy().items():
            self.updateFromADS(key, workspace)

    def cachedWorkspaces(self) -> List[WorkspaceName]:
        return list(self.entries.values())
