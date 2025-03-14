"""
Python `Mapping`-interface adapters and utility-methods relating to Mantid workspace logs (i.e. `Run`)
  and process-variable(PV) logs.
"""

import datetime
from collections.abc import Iterable, Mapping
from typing import Any

import h5py
import numpy as np
from mantid.api import Run

from snapred.meta.Config import Config


def transferInstrumentPVLogs(dest: Run, src: Run, keys: Iterable[str]):
    # Transfer instrument-specific PV-log values, between the `Run` attributes
    #   of source and destination workspaces.

    # Placed here for use by various `FetchGroceriesAlgorithm` loaders.
    for key in keys:
        if src.hasProperty(key):
            # WARNING: known Mantid defect: `addPropery` 'name' arg will not be used.
            #   'name' of new property will be taken from the source property.
            dest.addProperty(key, src.getProperty(key), True)
    # REMINDER: the instrument-parameter update still needs to be explicitly triggered!


def populateInstrumentParameters(wsName: str):
    from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

    mantidSnapper = MantidSnapper(None, "Utensils")
    # This utility function is a "stand in" until Mantid PR #38684 can be merged.
    # (see https://github.com/mantidproject/mantid/pull/38684)
    # After that, `mtd[wsName].populateInstrumentParameters()` should be used instead.

    # Any PV-log key will do, so long as it is one that always exists in the logs.
    pvLogKey = "run_title"
    pvLogValue = mantidSnapper.mtd[wsName].run().getProperty(pvLogKey).value
    # pvLogValue = mtd[wsName].run().getProperty(pvLogKey).value

    # AddSampleLog(
    #     "Adding sample log",
    #     Workspace=wsName,
    #     LogName=pvLogKey,
    #     logText=pvLogValue,
    #     logType="String",
    #     UpdateInstrumentParameters=True,
    # )
    mantidSnapper.AddSampleLog(
        "Adding sample log",
        Workspace=wsName,
        LogName=pvLogKey,
        logText=pvLogValue,
        logType="String",
        UpdateInstrumentParameters=True,
    )
    mantidSnapper.executeQueue()


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


def mappingFromRun(run: Run) -> Mapping:
    # Normalize `mantid.api.run` to a standard Python Mapping.

    class _Mapping(Mapping):
        def __init__(self, run: Run):
            self._run = run

        def __getitem__(self, key: str) -> Any:
            # Deal with PV-logs special cases:
            #   map getter methods to keys.
            value = None
            match key:
                # These time values are of type `numpy.datetime64` with nanosecond resolution.
                # To convert to `datetime.datetime`, which only supports microseconds:
                #   see the `datetimeFromLogTime` method above.

                case "end_time":
                    value = self._run.endTime().to_datetime64()

                case "start_time":
                    value = self._run.startTime().to_datetime64()

                case "proton_charge":
                    value = self._run.getProtonCharge()

                case "run_number":
                    value = self._run.getProperty("run_number").value if self._run.hasProperty("run_number") else 0

                case _:
                    try:
                        value = self._run.getProperty(key).value
                    except RuntimeError as e:
                        if "Unknown property search object" in str(e):
                            raise KeyError(key) from e
                        raise
            return value

        def __iter__(self):
            return self._run.keys().__iter__()

        def __len__(
            self,
        ):
            return len(self._run.keys())

        def __contains__(self, key: str):
            return self._run.hasProperty(key)

        def keys(self):
            return self._run.keys()

    return _Mapping(run)


def mappingFromNeXusLogs(h5: h5py.File) -> Mapping:
    # Normalize NeXus hdf5 logs to a standard Python Mapping.

    class _Mapping(Mapping):
        def __init__(self, h5: h5py.File):
            self._logs = h5[Config["instrument.PVLogs.rootGroup"]]

        def __getitem__(self, key: str) -> Any:
            return self._logs[key + "/value"]

        def __iter__(self):
            return self.keys().__iter__()

        def __len__(
            self,
        ):
            return len(self._logs.keys())

        def __contains__(self, key: str):
            return self._logs.__contains__(key + "/value")

        def keys(self):
            return [k[0 : k.rfind("/value")] for k in self._logs.keys()]

    return _Mapping(h5)
