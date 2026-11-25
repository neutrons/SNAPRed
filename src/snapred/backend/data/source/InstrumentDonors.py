from typing import Dict, List, Tuple

from snapred.backend.dao.state import DetectorState
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.meta.Config import Config
from snapred.meta.InternalConstants import ReservedRunNumber
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

InstrumentKey = Tuple[str, bool]


class InstrumentDonors:
    """
    Workspaces that donate their instrument, in the instrument state of a run, to data that is loaded without
    one (grouping, calibration and mask workspaces).

    A donor is cached for each (stateId, use lite mode).  If possible, the cached neutron data of the run is
    used as the donor; otherwise, an empty instrument is loaded, and its instrument parameters are set from the
    run's detector state.
    """

    def __init__(self, dataService, workspaces: WorkspaceOperations, neutronDataCache: NeutronDataCache):
        self.dataService = dataService
        self.workspaces = workspaces
        self.neutronDataCache = neutronDataCache

        # (stateId, useLiteMode) -> <workspace name>
        self.loadedInstruments: Dict[InstrumentKey, WorkspaceName] = {}

    @property
    def mantidSnapper(self):
        return self.workspaces.mantidSnapper

    def updateFromADS(self, runNumber: str, useLiteMode: bool, key: InstrumentKey):
        """
        Ensure cache consistency for a single instrument-donor workspace.

        The workspace in the cache may be either a neutron-data workspace or an empty-instrument workspace.
        """
        workspace = self.neutronDataCache.rawWorkspace(runNumber, useLiteMode)
        if self.workspaces.workspaceDoesExist(workspace) and self.loadedInstruments.get(key) is None:
            self.loadedInstruments[key] = workspace
        elif self.loadedInstruments.get(key) is not None:
            workspace = self.loadedInstruments.get(key)
            if not self.workspaces.workspaceDoesExist(workspace):
                del self.loadedInstruments[key]

    def rebuild(self):
        """
        Remove the entries whose workspaces no longer exist.

        Entries are not added here, because that requires the run number, and the cache is keyed by stateId;
        that is done by `updateFromADS`, at each call to `fetch`.
        """
        for key, workspace in self.loadedInstruments.copy().items():
            if not self.workspaces.workspaceDoesExist(workspace):
                del self.loadedInstruments[key]

    def cachedWorkspaces(self) -> List[WorkspaceName]:
        return list(self.loadedInstruments.values())

    def fetch(self, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        """
        A workspace in the ADS whose instrument is in the instrument state of the run.

        :param runNumber: a run number that was taken in the desired instrument state
        :param useLiteMode: whether to use lite or native resolution
        """
        stateId, detectorState = self.dataService.generateStateId(runNumber)
        instrumentKey = (stateId, useLiteMode)
        runKey = self.neutronDataCache.key(runNumber, useLiteMode)
        self.updateFromADS(runNumber, useLiteMode, instrumentKey)

        wsName = self.loadedInstruments.get(instrumentKey)
        if wsName is None:
            self.neutronDataCache.updateFromADS(runNumber, useLiteMode)
            if self.neutronDataCache.contains(runKey):
                # If possible, use a cached neutron-data workspace as an instrument donor
                wsName = self.neutronDataCache.rawWorkspace(runNumber, useLiteMode)
            else:
                # Otherwise, create an instrument donor.
                #   Alternatively, depending on performance, loading the corresponding neutron-data workspace
                #   could also be triggered here.
                wsName = self.mantidSnapper.mtd.unique_hidden_name()

                # Load the bare instrument:
                instrumentFilename = (
                    Config["instrument.lite.definition.file"]
                    if useLiteMode
                    else Config["instrument.native.definition.file"]
                )
                self.mantidSnapper.LoadEmptyInstrument(
                    f"Loading instrument at {instrumentFilename} to {wsName}",
                    Filename=instrumentFilename,
                    OutputWorkspace=wsName,
                )
                self.mantidSnapper.executeQueue()

                # Initialize the instrument parameters
                # (Reserved run-numbers will use the unmodified instrument.)
                if runNumber not in ReservedRunNumber.unmodifiedInstrument():
                    self.updateInstrumentParameters(wsName, detectorState)
            self.loadedInstruments[instrumentKey] = wsName
        return wsName

    def updateInstrumentParameters(self, wsName: WorkspaceName, detectorState: DetectorState):
        """
        Set the instrument parameters of a workspace from a detector state.

        The SNAP instrument has moveable panels, and certain properties of a calculation will depend on how the
        panels are oriented (the instrument state).  For Mantid algorithms to use the instrument state, the
        parameters specifying the positions of the panels must be set in the workspace's logs.
        """
        # Add _ALL_ of the sample logs required to determine the instrument state:
        #
        #   * these logs include `det_arc..` and `det_lin..` parameters, which determine
        #   the instrument position, as well as the logs for <wavelength>, <frequency>
        #   and <guide state>.
        #
        #   * NOTE: after adding the logs, it is necessary to update the instrument to
        #   factor in these new parameters, or else calculations will be inconsistent.
        #   This is done with a call to `ws->populateInstrumentParameters()` from within mantid.
        #   This call only needs to happen with the last log

        # It's important here to add the ACTUAL PVs that are used by the `DetectorState`:
        #   these may change depending on the current `InstrumentConfig.stateIdSchema`.
        logNames = [k for k in detectorState.PVs.keys()]
        logTypes = ["Number Series" for _ in logNames]
        logValues = [f"{detectorState[k]:.16f}" for k in logNames]

        self.mantidSnapper.AddSampleLogMultiple(
            f"Updating instrument parameters for {wsName}",
            Workspace=wsName,
            LogNames=logNames[:-1],
            LogValues=logValues[:-1],
            LogTypes=logTypes[:-1],
            ParseType=False,
        )
        self.mantidSnapper.AddSampleLog(
            "...",
            Workspace=wsName,
            LogName=logNames[-1],
            logText=logValues[-1],
            logType=logTypes[-1],
            UpdateInstrumentParameters=True,
        )
        self.mantidSnapper.executeQueue()
