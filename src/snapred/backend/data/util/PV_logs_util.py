"""
Python `Mapping`-interface adapters and utility-methods relating to Mantid workspace logs (i.e. `Run`)
  and process-variable(PV) logs.
"""

import datetime
import logging
from collections.abc import Iterable, Mapping
from typing import Any, Dict

import h5py
import numpy as np
from mantid.api import Run, WorkspaceGroup
from mantid.dataobjects import TableWorkspace

from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

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


def mappingFromRun(run: Run) -> Mapping:
    # Normalize `mantid.api.run` to a standard Python Mapping.

    # TODO: a possible alternative implementation would be to just construct a `dict` from the `Run` instance.
    #   It was an optimization choice NOT to use this approach, although once alternative PV-log keys are considered,
    #   the code would probably be much simpler.

    class _Mapping(Mapping):
        def __init__(self, run: Run):
            self._run = run

            # Save a self-consistent value for "now": this allows the fallback case for 'start_time' and 'end_time'
            #  to return the same value, regardless of calling order.
            self._now = np.datetime64(datetime.datetime.utcnow().isoformat(), "ns")

            # A primary key may be used to reference values at an alternative key, if they exist.
            self._alternateKeys: Dict[str, str] = {}
            for ks in Config["instrument.PVLogs.instrumentKeys"]:
                if isinstance(ks, str):
                    continue
                for k in ks[1:]:
                    if run.hasProperty(k):
                        self._alternateKeys[ks[0]] = k
                        break

        def __getitem__(self, key: str) -> Any:
            # Deal with PV-logs special cases:
            #   map getter methods to keys.
            value = None
            match key:
                # These time values are of type `numpy.datetime64` with nanosecond resolution.
                # To convert to `datetime.datetime`, which only supports microseconds:
                #   see the `datetimeFromLogTime` method above.

                case "end_time":
                    try:
                        value = self._run.endTime().to_datetime64()
                    except RuntimeError as e:
                        # Any exception => no run is active
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.warning(str(e))
                        value = self._now

                case "start_time":
                    try:
                        value = self._run.startTime().to_datetime64()
                    except RuntimeError as e:
                        # Any exception => no run is active
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.warning(str(e))
                        value = self._now

                case "proton_charge":
                    try:
                        value = self._run.getProtonCharge()
                    except RuntimeError as e:
                        # Any exception => no run is active.
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.warning(str(e))
                        value = 0.0

                case "run_number":
                    # TODO: this case does not normalize 'run_number' value to `string`,
                    #   because Mantid itself does not do that:
                    #   probably that should be fixed here, and in Mantid.
                    value = self._run.getProperty("run_number").value if self._run.hasProperty("run_number") else 0

                case _:
                    try:
                        value = self._run.getProperty(key).value
                    except RuntimeError as e:
                        # A primary PV-log key may be used to reference a value at an alternative key.
                        if "Unknown property search object" in str(e):
                            if key in self._alternateKeys:
                                value = self._run.getProperty(self._alternateKeys[key]).value
                            else:
                                raise KeyError(key) from e
                        else:
                            raise
            return value

        def __iter__(self):
            return self.keys().__iter__()

        def __len__(
            self,
        ):
            return len(self.keys())

        def __contains__(self, key: str):
            if self._run.hasProperty(key):
                return True
            # Check for special cases:
            if key in ("end_time", "start_time", "proton_charge", "run_number"):
                return True
            # Check for _alternate_ instrument PV-log keys:
            if key in self._alternateKeys:
                return True
            return False

        def keys(self):
            keys_ = set(self._run.keys())
            keys_.update(["end_time", "start_time", "proton_charge", "run_number"])
            keys_.update(self._alternateKeys.keys())
            return list(keys_)

    return _Mapping(run)


def mappingFromNeXusLogs(h5: h5py.File) -> Mapping:
    # Normalize NeXus hdf5 logs to a standard Python Mapping.

    class _Mapping(Mapping):
        def __init__(self, h5: h5py.File):
            self._logGroup = h5[Config["instrument.PVLogs.rootGroup"]]
            self._h5 = h5

        def __getitem__(self, key: str) -> Any:
            if key == "title":
                return self._h5["/entry/title"][...]
            else:
                return self._logGroup[f"{key}/value"][...]

        def __contains__(self, key: str) -> bool:
            if key == "title":
                return "/entry/title" in self._h5
            return f"{key}/value" in self._logGroup

        def keys(self):
            baseKeys = []
            for fullKey in self._logGroup.keys():
                if fullKey.endswith("/value"):
                    baseKeys.append(fullKey[: fullKey.rfind("/value")])
            if "/entry/title" in self._h5:
                baseKeys.append("title")
            return baseKeys

        def __iter__(self):
            return iter(self.keys())

        def __len__(self):
            return len(self.keys())

    return _Mapping(h5)
