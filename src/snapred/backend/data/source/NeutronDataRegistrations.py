from typing import Dict, List, Optional

from mantid.api import mtd

from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.InternalConstants import ReservedRunNumber
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)


@Singleton
class NeutronDataRegistrations:
    """
    The workspaces registered with SNAPRed as the neutron data for a run number.

    A registered workspace is owned by the process that registered it (for example, the ``livereduce``
    post-processing script), which may go on to change or delete it.  So each registration is validated
    on every lookup.  SNAPRed never changes or deletes a registered workspace itself.

    A registration is removed only when it is explicitly removed, or replaced by another registration for the
    same run number: a registration that is no longer valid is an error, not a reason to use another source
    of data for the run.  The registrations of different run numbers are independent, even when they are of
    the same workspace.

    Data without a run number (for example, live data between runs) is registered under
    `ReservedRunNumber.LIVE`.  The run number in the data is checked only once, when it is registered under
    a real run number: after that, run changes are the business of the registering process, which
    registers its data again on each update.
    """

    def __init__(self):
        # run number -> registered workspace
        self._workspaces: Dict[str, WorkspaceName] = {}

    @staticmethod
    def _logRunNumber(workspace: WorkspaceName) -> Optional[str]:
        # The live-data listener does not always set the 'run_number' log: a missing value is not an error.
        run = mtd[workspace].getRun()
        return str(run.getProperty("run_number").value) if run.hasProperty("run_number") else None

    @staticmethod
    def _matches(runNumber: str, logRunNumber: Optional[str]) -> bool:
        return runNumber == ReservedRunNumber.LIVE or logRunNumber is None or int(logRunNumber) == int(runNumber)

    def register(self, runNumber: str, workspace: WorkspaceName):
        """
        Register a workspace as the neutron data for a run number.

        Registering it again is harmless.  It replaces any earlier registration for the run number.  The same
        workspace may be registered for several run numbers: its other registrations are kept.

        Its 'run_number' log, if set, must match a real run number.  Any data can be registered under
        `ReservedRunNumber.LIVE`.
        """
        runNumber = str(runNumber)
        if not mtd.doesExist(workspace):
            raise ValueError(f"cannot register workspace '{workspace}' for run '{runNumber}': it does not exist")
        logRunNumber = self._logRunNumber(workspace)
        if not self._matches(runNumber, logRunNumber):
            raise ValueError(
                f"cannot register workspace '{workspace}' for run '{runNumber}': "
                f"its 'run_number' log is '{logRunNumber}'"
            )
        self._workspaces[runNumber] = workspace

    def unregister(self, runNumber: str):
        """Remove the registration for a run number, if there is one."""
        if self._workspaces.pop(str(runNumber), None) is None:
            logger.debug(f"run '{runNumber}' has no registered neutron data")

    def lookup(self, runNumber: str) -> Optional[WorkspaceName]:
        """
        The workspace registered for a run number, or `None` if the run is not registered.

        If the workspace no longer exists, `RuntimeError` is raised, and the registration is kept: it must be
        explicitly removed, or replaced.  Its 'run_number' log is not checked again: see the class description.
        """
        runNumber = str(runNumber)
        workspace = self._workspaces.get(runNumber)
        if workspace is None:
            return None
        if not mtd.doesExist(workspace):
            raise RuntimeError(
                f"The workspace '{workspace}' that is registered as the neutron data for run '{runNumber}' "
                "no longer exists.  Register the data for the run again, or remove the registration."
            )
        return workspace

    def isRegistered(self, runNumber: str) -> bool:
        """Whether the run is registered, without validation."""
        return str(runNumber) in self._workspaces

    def registeredWorkspaces(self) -> List[WorkspaceName]:
        """All of the registered workspaces, each once, without validation."""
        return list(dict.fromkeys(self._workspaces.values()))
