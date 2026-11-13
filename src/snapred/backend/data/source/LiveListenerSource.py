import json
from datetime import datetime, timedelta

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.error.RunStatus import RunStatus
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)


class LiveListenerSource(DataSource):
    """
    Native-resolution neutron data, from SNAPRed's own connection to the live-data listener.

    This is used when live data is requested (`item.liveDataArgs` is set), and as the fallback when
    there is no file for the run.  Live data is not reused from the cache: the live run continues
    to acquire data.
    """

    LOADER = "LoadLiveDataInterval"
    cacheable = False

    def __init__(self, dataService, grocer, workspaces: WorkspaceOperations):
        super().__init__(provides=Resolution.NATIVE)
        self.dataService = dataService
        self.grocer = grocer
        self.workspaces = workspaces

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.LIVE

    def available(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        return self.dataService.hasLiveDataConnection()

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        runNumber, liveDataArgs = item.runNumber, item.liveDataArgs
        if not self.dataService.hasLiveDataConnection():
            raise ConnectionError("no live-data connection is available")

        if liveDataArgs is None:
            # Only warn if live-data mode hasn't been explicitly requested.
            logger.warning(f"Input data for run '{runNumber}' was not found in IPTS, trying live-data fallback...")

        # When not specified in the `liveDataArgs` or when `liveDataArgs.duration == timedelta(0)`,
        #   the default behavior will be to load the entire run.
        startTime = (
            (datetime.utcnow() - liveDataArgs.duration).isoformat()
            if liveDataArgs is not None and liveDataArgs.duration != timedelta(0)
            else RunMetadata.FROM_START_ISO8601
        )

        loaderArgs = {
            "Facility": Config["liveData.facility.name"],
            "Instrument": Config["liveData.instrument.name"],
            "PreserveEvents": True,
            "StartTime": startTime,
        }
        data = self.grocer.executeRecipe(
            workspace=outputWorkspace, loader=self.LOADER, loaderArgs=json.dumps(loaderArgs)
        )
        # `executeRecipe` raises if the load fails, so `result` is normally true.
        if data["result"]:
            self._checkRunStatus(data, runNumber, liveDataArgs, outputWorkspace)
            self._checkRunNumber(runNumber, liveDataArgs, outputWorkspace)
        return self._sourced(outputWorkspace, data["loader"])

    def _checkRunStatus(self, data, runNumber: str, liveDataArgs, workspace: WorkspaceName):
        if not data.get("runStatus"):
            return
        try:
            runStatus = RunStatus(data["runStatus"])
        except ValueError as e:
            logger.debug(
                f"Error when parsing 'RunStatus' returned by `FetchGroceriesAlgorithm`:\n"
                f"    {data['runStatus']}:\n"
                f"    {e}."
            )
            return
        if runStatus == RunStatus.RUNNING:
            return

        # There is some type of issue with the live run.
        self.workspaces.deleteWorkspaceUnconditional(workspace)
        if liveDataArgs is not None:
            match runStatus:
                case RunStatus.PAUSED:
                    raise LiveDataState.runStateTransition(runNumber, runNumber)
                case RunStatus.STOPPED:
                    raise LiveDataState.runStateTransition("0", runNumber)
                case RunStatus.ERROR:
                    raise LiveDataState.runError(runNumber)
                case _:
                    raise RuntimeError(f"implementation error: unexpected 'RunStatus' value: '{runStatus}'")
        raise RuntimeError(
            f"Neutron data for run '{runNumber}' is not present on disk, "
            "and there is a problem with the live-data run:\n"
            f"    live-run status: {runStatus}."
        )

    def _checkRunNumber(self, runNumber: str, liveDataArgs, workspace: WorkspaceName):
        ws = self.workspaces.getWorkspaceForName(workspace)
        run = ws.getRun()
        # IMPORTANT: due to issues with the `SNSLiveEventData` listener implementation, we cannot assume
        #   that the 'run_number' property has actually been set.
        liveRunNumber = run.getProperty("run_number").value if run.hasProperty("run_number") else None

        if liveRunNumber is not None:
            if int(runNumber) != int(liveRunNumber):
                # Live run number is unexpected => there has been a live-data run-state change.
                self.workspaces.deleteWorkspaceUnconditional(workspace)
                if liveDataArgs is not None:
                    raise LiveDataState.runStateTransition(liveRunNumber, runNumber)
                raise RuntimeError(
                    f"Neutron data for run '{runNumber}' is not present on disk, nor is it the live-data run"
                )
        elif liveDataArgs is not None:
            # Temporary fix for `SNSLiveEventDataListener` not setting the run-number:
            #   just set it here.
            ws.mutableRun()["run_number"] = str(runNumber)
        else:
            # Do NOT fix the run-number in the fallback case.
            self.workspaces.deleteWorkspaceUnconditional(workspace)
            raise RuntimeError(
                f"Neutron data for run '{runNumber}' is not present on disk, nor is it the live-data run"
            )
