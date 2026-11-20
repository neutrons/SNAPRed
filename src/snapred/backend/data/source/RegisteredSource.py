from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.NeutronDataRegistrations import NeutronDataRegistrations
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class RegisteredSource(DataSource):
    """
    Native-resolution neutron data from a workspace registered for the run (see `NeutronDataRegistrations`).

    * The registered workspace is cloned, and only the clone is used: the registered workspace itself
      is never changed or deleted.
    * It is not cacheable: the registered workspace may change between fetches.
    * It is exclusive: while a run is registered, its data comes from no other source.
    """

    LOADER = "CloneWorkspace"
    cacheable = False

    def __init__(self, workspaces: WorkspaceOperations):
        super().__init__(provides=Resolution.NATIVE)
        self.workspaces = workspaces

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.REGISTERED

    def exclusiveFor(self, item: GroceryListItem) -> bool:
        registrations = NeutronDataRegistrations()
        if not registrations.isRegistered(item.runNumber):
            return False
        if item.liveDataArgs is not None:
            raise RuntimeError(
                f"Live data was requested for run '{item.runNumber}', but a workspace is registered as its "
                "neutron data.  Remove the registration first."
            )
        # A registration that is no longer valid is an error: the lookup raises.
        registrations.lookup(item.runNumber)
        return True

    def available(self, item: GroceryListItem) -> bool:
        return self.exclusiveFor(item)

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        workspace = NeutronDataRegistrations().lookup(item.runNumber)
        if workspace is None:
            raise RuntimeError(f"no workspace is registered as the neutron data for run '{item.runNumber}'")
        self.workspaces.getCloneOfWorkspace(workspace, outputWorkspace)

        # The clone has the run number of the registration: the live-data listener does not always set the
        #   'run_number' log, it may be '0', and it is not checked after registration (it may be anything for
        #   `ReservedRunNumber.LIVE`).  It is set on the clone, never on the registered workspace.
        run = self.workspaces.getWorkspaceForName(outputWorkspace).mutableRun()
        run.addProperty("run_number", str(item.runNumber), True)
        return self._sourced(outputWorkspace, self.LOADER)
