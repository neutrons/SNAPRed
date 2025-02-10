from collections.abc import Sequence
from typing import Dict, List

from mantid.simpleapi import (
    AddSampleLog,
    AddSampleLogMultiple,
    CreateLogPropertyTable,
    DeleteWorkspace,
    mtd,
)

from snapred.backend.dao.state.DetectorState import DetectorState


def mapFromSampleLogs(wsName: str, sampleLogKeys: Sequence[str]) -> Dict[str, float]:
    """
    Extract the workspace sample log entries, corresponding to the specified list of log names.
    """
    # WARNING: `RuntimeError` is raised if any sample-log entries don't exist.
    logTableName = mtd.unique_hidden_name()
    CreateLogPropertyTable(
        InputWorkspaces=[wsName],
        LogPropertyNames=list(sampleLogKeys),
        OutputWorkspace=logTableName,
    )
    assert mtd.doesExist(logTableName)

    logs: Dict[str, float] = {}
    logTable = mtd[logTableName]
    for index, name in enumerate(logTable.getColumnNames()):
        logs[name] = float(logTable.column(index)[0])
    DeleteWorkspace(logTableName)
    return logs


def getInstrumentLogDescriptors(detectorState: DetectorState):
    # Standard logs used by `GroceryService.updateInstrumentParameters`
    return {
        "logNames": [
            "det_arc1",
            "det_arc2",
            "BL3:Chop:Skf1:WavelengthUserReq",
            "BL3:Det:TH:BL:Frequency",
            "BL3:Mot:OpticsPos:Pos",
            "det_lin1",
            "det_lin2",
        ],
        "logTypes": [
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
        ],
        "logValues": [
            f"{detectorState.arc[0]:.16f}",
            f"{detectorState.arc[1]:.16f}",
            f"{detectorState.wav:.16f}",
            f"{detectorState.freq:.16f}",
            f"{detectorState.guideStat:.16f}",
            f"{detectorState.lin[0]:.16f}",
            f"{detectorState.lin[1]:.16f}",
        ],
    }


def addInstrumentLogs(
    wsName: str,
    *,
    logNames: List[str],
    logTypes: List[str],
    logValues: List[str],
):
    """
    Set instrument-related log entries on the specified workspace:
      * the instrument parameters are updated at the last entry.
    """
    AddSampleLogMultiple(
        Workspace=wsName,
        LogNames=logNames[:-1],
        LogValues=logValues[:-1],
        LogTypes=logTypes[:-1],
        ParseType=False,
    )
    AddSampleLog(
        Workspace=wsName,
        LogName=logNames[-1],
        logText=logValues[-1],
        logType=logTypes[-1],
        UpdateInstrumentParameters=True,
    )
