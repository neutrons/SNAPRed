from typing import Dict
from collections.abc import Sequence
from mantid.simpleapi import (
    mtd,
    CreateLogPropertyTable,
    DeleteWorkspace,
) 

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
