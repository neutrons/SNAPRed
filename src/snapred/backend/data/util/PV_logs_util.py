"""
Python `Mapping`-interface adapters and utility-methods relating to Mantid workspace logs (i.e. `Run`)
  and process-variable(PV) logs.
"""

import datetime
from collections.abc import Iterable

import numpy as np
from mantid.api import Run, WorkspaceGroup
from mantid.dataobjects import TableWorkspace

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)

_snapper = None  # Prevent circular import.


def allInstrumentPVLogKeys(keysWithAlternates: Iterable[str | Iterable[str]]):
    # Flatten a list of keys, where each list item is either a single key,
    #   or a list of alternative keys.
    keys = []

    for ks in keysWithAlternates:
        if isinstance(ks, str):
            # append a single key
            keys.append(ks)
        elif isinstance(ks, Iterable) and all([isinstance(k, str) for k in ks]):
            # append several alternate keys
            keys.extend(ks)
        else:
            raise RuntimeError("unexpected format: list of keys with alternates")
    return keys


def transferInstrumentPVLogs(dest: Run, src: Run, keys: Iterable[str]):
    # Transfer instrument-specific PV-log values, between the `Run` attributes
    #   of source and destination workspaces.

    # Placed here for use by various `FetchGroceriesAlgorithm` loaders.
    for key in keys:
        if src.hasProperty(key):
            dest.addProperty(key, src.getProperty(key), True)
    # REMINDER: the instrument-parameter update still needs to be explicitly triggered!


def populateInstrumentParameters(wsName: str):
    global _snapper
    if _snapper is None:
        # Prevent circular import.
        from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

        _snapper = MantidSnapper(parentAlgorithm=None, name=__name__)

    # This utility function is a "stand in" until Mantid PR #38684 can be merged.
    # (see https://github.com/mantidproject/mantid/pull/38684)
    # After that, `mtd[wsName].populateInstrumentParameters()` should be used instead.

    # Any PV-log key will do, so long as it is one that always exists in the logs.
    pvLogKey = "run_title"

    # In case it's a workspace group: call `populateInstrumentParameters` for each workspace separately.
    names = (
        [
            wsName,
        ]
        if not isinstance(_snapper.mtd[wsName], WorkspaceGroup)
        else _snapper.mtd[wsName].getNames()
    )
    for name in names:
        ws = _snapper.mtd[name]
        if isinstance(ws, TableWorkspace):
            continue
        pvLogValue = ws.run().getProperty(pvLogKey).value
        _snapper.AddSampleLog(
            f"populating instrument parameters for '{name}'",
            Workspace=name,
            LogName=pvLogKey,
            logText=pvLogValue,
            logType="String",
            UpdateInstrumentParameters=True,
        )
    _snapper.executeQueue()


def datetimeFromLogTime(logTime: np.datetime64) -> datetime.datetime:
    # Convert from PV-log time with nanoseconds resolution
    #   to `datetime` with microseconds resolution.

    # PV-log time values are of type `numpy.datetime64` with nanosecond resolution.
    # From the documentation, they are measured with respect to the "utc" timezone.
    # WARNING: without converting to the  "us" datetime64 representation first,
    #   the `astype(datetime.datetime)` will fallback to returning an <int64>.
    return datetime.datetime.replace(
        np.datetime64(logTime, "us").astype(datetime.datetime), tzinfo=datetime.timezone.utc
    )
