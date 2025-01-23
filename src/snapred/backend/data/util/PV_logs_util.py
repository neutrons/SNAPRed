"""
Python `Mapping`-interface adapters and utility-methods relating to Mantid workspace logs (i.e. `Run`)
  and process-variable(PV) logs.
"""
# Note: in the upcoming reduction live-data PR this file includes several additional `Mapping` adapters.

from collections.abc import Iterable

from mantid.api import Run
from mantid.simpleapi import (
    AddSampleLog,
    mtd
)

from snapred.meta.Config import Config

def transferInstrumentPVLogs(dest: Run, src: Run, keys: Iterable):
    # Transfer instrument-specific PV-log values, between the `Run` attributes
    #   of source and destination workspaces.
    
    # Placed here for use by various `FetchGroceriesAlgorithm` loaders.
    for key in keys:
        if src.hasProperty(key):
            dest.addProperty(key, src.getProperty(key), True)
    # REMINDER: the instrument-parameter update still needs to be explicitly triggered!

def populateInstrumentParameters(wsName: str):
    # This utility function is a "stand in" until Mantid PR #38684 can be merged.
    # (see https://github.com/mantidproject/mantid/pull/38684)
    # After that, `mtd[wsName].populateInstrumentParameters()` should be used instead.
    
    # Any PV-log key will do, so long as it is one that always exists in the logs.
    pvLogKey = "run_title"
    pvLogValue = mtd[wsName].run().getProperty(pvLogKey).value
    
    AddSampleLog(
        Workspace=wsName,
        LogName=pvLogKey,
        logText=pvLogValue,
        logType="String",
        UpdateInstrumentParameters=True,
    )
