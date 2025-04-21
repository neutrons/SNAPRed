import datetime
import logging
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, ClassVar, Dict

import h5py
from mantid.api import Run
from mantid.kernel import DateAndTime
from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.StateId import StateId
from snapred.backend.data.util.PV_logs_util import datetimeFromLogTime
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class RunMetadata(BaseModel, Mapping):
    """Common interface required to present `mantid.api.run` and <NeXus-format file: logs>
    as a Python `Mapping`.
    """

    # Implementation notes:

    #   * Neither `mantid.api.Run` nor `h5py.File` may be safely cached.  In the former case
    #     if the parent workspace is deleted, accessing the `Run` instance generates a SEGFAULT.
    #     In the latter case, having the same file-descriptor open multiple times for read access
    #     is not supported by HDF5 (, except under the special restrictions of SWMR).
    #
    #   * If there's no run currently active, the `runNumber` will be `INACTIVE_RUN`.
    #
    #   * Live metadata may change at any time.  For example, the run may terminate and become inactive.
    #     For this reason, in most cases this DAO should never be cached in the live-data case.
    #

    ## Special Live-listener <start time> strings in ISO8601 format:
    #  Return data from _now_ (<epoch zero>):
    FROM_NOW_ISO8601: ClassVar[str] = "1990-01-01T00:00:00"

    #  Return data from start of run (<epoch +1 second>):
    FROM_START_ISO8601: ClassVar[str] = "1990-01-01T00:00:01"

    INACTIVE_RUN: ClassVar[int] = 0

    runNumber: str | int

    runTitle: str

    startTime: datetime
    endTime: datetime

    protonCharge: float

    detectorState: DetectorState | None
    stateId: ObjectSHA | None

    liveData: bool

    def __init__(
        self,
        # copy of DASlogs:
        logs: Dict[str, Any],
        *,
        # Mandatory keyword arguments:
        runNumber: str | int | None,
        runTitle: str | None,
        startTime: datetime.datetime | None,
        endTime: datetime.datetime | None,
        protonCharge: float | None,
        detectorState: DetectorState | None,
        stateId: ObjectSHA | None,
        liveData: bool,
    ):
        # WARNING: this __init__ should only be called via the `RunMetadata` factory methods only!
        #   The incoming `PVLogs` dict, and the keyword args must have a special format.

        ############################
        ## ASSIGN default values: ##
        ############################

        # Inspect information about any default values that will be used.
        defaultKeys = []
        if logger.isEnabledFor(logging.DEBUG):
            defaultKeys = [
                k for k in ("runNumber", "runTitle", "startTime", "endTime", "protonCharge") if locals().get(k) is None
            ]

        _now = datetime.datetime.utcnow()

        runNumber = runNumber if runNumber is not None else 0
        runTitle = runTitle if bool(runTitle) else ""

        startTime = startTime if startTime is not None else _now
        endTime = endTime if endTime is not None else _now

        protonCharge = protonCharge if protonCharge is not None else 0.0
        # `detectorState` and `stateId` are optional

        # Log messages about any default values that were substituted.
        if logger.isEnabledFor(logging.DEBUG) and bool(defaultKeys):
            for k in defaultKeys:
                logger.warning(
                    f"information for '{k}' was not present in the logs: using a default value of '{locals().get(k)}'"
                )

        ################################
        ## end: ASSIGN default values ##
        ################################

        # Normalize to the `mantid.api.Run` keys:
        #   add 'run_number' and 'run_title' to the logs.
        logs = logs.copy()
        if "run_number" not in logs:
            logs["run_number"] = runNumber
        if "run_title" not in logs:
            logs["run_title"] = runTitle

        # Call `BaseModel.__init__`:
        super().__init__(
            runNumber=runNumber,
            runTitle=runTitle,
            startTime=startTime,
            endTime=endTime,
            liveData=liveData,
            detectorState=detectorState,
            stateId=stateId,
            protonCharge=protonCharge,
        )

        # This cannot be set prior to `super().__init__(...)`: for some reason pydantic deletes it.
        self._PVLogs = logs

        # A primary key may be used to reference values at an alternative key, if such values exist.
        self._alternateKeys: Dict[str, str] = {}
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                continue
            for k in ks[1:]:
                if k in self._PVLogs:
                    self._alternateKeys[ks[0]] = k
                    break

    @staticmethod
    def _convertChargeTo_uAh(charge: float, units: str) -> float:
        # See `mantid/Framework/API/src/Run.cpp` L322-330.
        factor = 1.0
        if "picoCoulomb" in units:
            # Conversion factor between picoColumbs and microAmp*hours
            factor = 1.0e-6 / 3600.0
        elif bool(units) and units != "uAh":
            logger.warning(
                f"The 'entry/proton_charge' log has units of '{units}'.\n"
                + "  Only units of 'uAh' or 'picoCoulomb' will be converted."
            )
        # Note: this entry should already contain the integrated charge value.
        #   The corresponding time series is at 'entry/DASlogs/proton_charge/value'.
        return factor * charge

    # ------ Live-data methods: ---------------------------------

    def hasActiveRun(self):
        return int(self.runNumber) != RunMetadata.INACTIVE_RUN

    def beamState(self):
        return self.protonCharge > 0.0

    # ------ Pass through `Mapping` methods to the PVLogs: ------

    def __getitem__(self, key: str) -> Any:
        value = None
        try:
            value = self._PVLogs.__getitem__(key)
        except KeyError:
            # A primary PV-log key may be used to reference a value at an alternative key.
            if key in self._alternateKeys:
                try:
                    value = self._PVLogs[self._alternateKeys[key]]
                except KeyError:
                    raise RuntimeError(
                        f"value for key '{key}' should have been present at alternate key '{self._alternateKeys[key]}'"
                    )
            else:
                raise
        return value

    def __setitem__(self, key: str):
        raise RuntimeError("`RunMetadata` is immutable: `__setitem__` is not available")

    def __iter__(self) -> Iterator:
        return self._PVLogs.__iter__()

    def __len__(self) -> int:
        return self._PVLogs.__len__()

    def __contains__(self, key: str) -> bool:
        if self._PVLogs.__contains__(key):
            return True
        # Check for _alternate_ instrument PV-log keys:
        if key in self._alternateKeys:
            return True
        return False

    def keys(self) -> Iterable:
        keys_ = set(self._PVLogs.keys())
        keys_.update(self._alternateKeys.keys())
        return keys_

    # ------ FACTORY methods: ----------------------------------------------------------------

    @classmethod
    def fromNeXusLogs(cls, h5: h5py.File) -> "RunMetadata":
        # Usually these next are `DASlogsPath == "entry/DASlogs"` and `rootPath == "entry"` respectively.
        # However, for ultralite data these are "mantid_workspace1/logs" and "mantid_workspace1" instead.
        DASlogsPath = Config["instrument.PVLogs.rootGroup"]
        rootPath = DASlogsPath.split("/")[0]
        DASlogsGroup = h5[DASlogsPath]
        rootGroup = h5[rootPath]

        if "run_number" not in rootGroup:
            # Notes:
            # * live-data always uses the `fromRun` method, so it should never reach this clause.
            # * the 'mantid_workspace_/run_number' dataset is entered as a tuple like this:
            #      `(np.bytes_(<run-number string>),)`.
            raise RuntimeError(
                "For 'tests/data/snapred-data' input files, the 'run_number' dataset must be added "
                " to the 'mantid_workspace_1' group \"by hand\".\n"
                "  This key-value pair should always be present in non-synthetic input data!"
            )
        runNumber = str(rootGroup["run_number"][0], encoding="utf8") if "run_number" in rootGroup else None
        runTitle = str(rootGroup["title"][0], encoding="utf8") if "title" in rootGroup else None

        # See comments at `snapred.backend.data.util.PV_logs_util.datetimeFromLogTime` about this conversion.
        startTime = (
            datetimeFromLogTime(DateAndTime(str(rootGroup["start_time"][0], encoding="utf8")).to_datetime64())
            if "start_time" in rootGroup
            else None
        )
        endTime = (
            datetimeFromLogTime(DateAndTime(str(rootGroup["end_time"][0], encoding="utf8")).to_datetime64())
            if "end_time" in rootGroup
            else None
        )

        protonCharge = None
        try:
            chargeData = rootGroup["proton_charge"]
            units = str(chargeData.attrs["units"], encoding="utf8") if "units" in chargeData.attrs else ""
            protonCharge = cls._convertChargeTo_uAh(chargeData[0], units)
        except KeyError:
            pass

        # Make a copy of the DASlogs.

        # Implementation notes:
        # * In general, <key> gets to the `h5py.group` holding the dataset,
        #   and 'value' or 'values' accesses the useful content of the dataset
        #   as a numpy array.
        # * Usually, there are other sub-datasets besides 'value' and 'values'.
        # * The primary objective here is to duplicate the behavior of `mantid.api.Run`.

        logs = {}
        for k in DASlogsGroup:
            ds = DASlogsGroup[k]
            ds_key = "value" if "value" in ds else "values"
            if ds_key not in ds:
                continue
            logs[k] = ds[ds_key][...]

        metadata = RunMetadata(
            logs,
            runNumber=runNumber,
            runTitle=runTitle,
            startTime=startTime,
            endTime=endTime,
            protonCharge=protonCharge,
            # `detectorState` and `stateId` are initialized after __init__,
            #    so that the alternative-keys implementation can be used.
            detectorState=None,
            stateId=None,
            # Live data does not transfer via HDF5.
            liveData=False,
        )

        # Now initialize `detectorState` and `stateId`.
        detectorState = None
        stateId = None
        runNumber = str(metadata.runNumber)
        try:
            detectorState = DetectorState.fromLogs(metadata)
            stateId = StateId.fromDetectorState(detectorState).SHA()
            metadata.detectorState = detectorState
            metadata.stateId = stateId
        except RuntimeError:
            # If there is an active run number, this is an error.
            #   However, if a run is inactive, many required log values may not be present,
            #   and in that case this is _not_ an error.

            # Note specifically in case of `MaskWorkspace` and `GroupingWorkspace`,
            #   run number will be `RunMetadata.INACTIVE_RUN`, but we should not trigger this clause,
            #   because the `DetectorState` should be valid.
            if runNumber != str(cls.INACTIVE_RUN):
                raise

        return metadata

    @classmethod
    def fromRun(cls, run: Run, liveData: bool = False) -> "RunMetadata":
        def _optionalAccessor(run: Run, key: str):
            # If `mantid.api` had been written by Chimpanzees, we'd be better off!
            value = None
            try:
                value = getattr(run, key)()
            except RuntimeError:
                pass
            return value

        # Notes:
        #   In the live-data case, the 'run_number' property often does not exist: that's not an error.
        #   However, for 'tests/data/snapred-data' input files, the 'mantid_workspace_1/Daslogs/run_number' group
        #   and its 'value' dataset may need to be added to the 'mantid_workspace_1/Daslogs' group "by hand".
        #   For non-synthetic input data, this key-value pair would have been created automatically!
        #   The 'mantid_workspace_/Daslogs/run_number/value' dataset should be entered as a tuple like this:
        #       `(np.bytes_(<run-number string>),)`.
        runNumber = run.getProperty("run_number").value if run.hasProperty("run_number") else None
        runTitle = run.getProperty("run_title").value if run.hasProperty("run_title") else None

        # See comments at `snapred.backend.data.util.PV_logs_util.datetimeFromLogTime` about this conversion.
        startTime = _optionalAccessor(run, "startTime")
        if startTime is not None:
            startTime = datetimeFromLogTime(startTime.to_datetime64())
        endTime = _optionalAccessor(run, "endTime")
        if endTime is not None:
            endTime = datetimeFromLogTime(run.endTime().to_datetime64())

        protonCharge = _optionalAccessor(run, "getProtonCharge")

        # Make a copy of the DASlogs.
        logs = {}
        for k in run.keys():
            logs[k] = run.getProperty(k).value

        metadata = RunMetadata(
            logs,
            runNumber=runNumber,
            runTitle=runTitle,
            startTime=startTime,
            endTime=endTime,
            protonCharge=protonCharge,
            # `detectorState` and `stateId` are initialized after __init__,
            #    so that the alternative-keys implementation can be used.
            detectorState=None,
            stateId=None,
            # Live data does not transfer via HDF5.
            liveData=liveData,
        )

        # Now initialize `detectorState` and `stateId`.
        detectorState = None
        stateId = None
        runNumber = str(metadata.runNumber)
        try:
            detectorState = DetectorState.fromLogs(metadata)
            stateId = StateId.fromDetectorState(detectorState).SHA()
            metadata.detectorState = detectorState
            metadata.stateId = stateId
        except RuntimeError:
            # If there is an active run number, this is an error.
            #   However, if a run is inactive, many required log values may not be present,
            #   and in that case this is _not_ an error.

            # Note specifically in case of `MaskWorkspace` and `GroupingWorkspace`,
            #   run number will be `RunMetadata.INACTIVE_RUN`, but we should not trigger this clause,
            #   because the `DetectorState` should be valid.
            if runNumber != str(cls.INACTIVE_RUN):
                raise

        return metadata

    model_config = ConfigDict(arbitrary_types_allowed=True)
