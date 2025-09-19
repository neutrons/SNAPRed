import datetime
from time import sleep
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import ConfigService, DateAndTime, Direction

from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.data.util.PV_logs_util import datetimeFromLogTime
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.decorators.ConfigDefault import ConfigDefault, ConfigValue

logger = snapredLogger.getLogger(__name__)


class LoadLiveDataInterval(PythonAlgorithm):
    """
    Load a sub-interval of live data from the active run.
    """

    @classproperty
    def USING_ADARA_FileReader(cls):
        # Enable a correction for the limitations of the mock "ADARA_FileReader":
        return (
            Config["liveData.facility.name"] == "TEST_LIVE" and Config["liveData.instrument.name"] == "ADARA_FileReader"
        )

    def category(self):
        return "Live data"

    #
    # Implementation notes:
    #
    #   * The use of `<Algorithm>.createChildAlgorithm` in key places ties the lifetime of the child algorithm
    #     to the lifetime of the parent -- this is critical for the `LoadLiveData` algorithm, which instantiates
    #     an instance of `SNSLiveListener` internally.
    #
    #   * Where fully-managed algorithm instances are appropriate: `MantidSnapper` will also be used.
    #
    #   * In regards to cancellation: the bulk of execution time for this algorithm occurs during the `LoadLiveData`
    #     child algorithm.  Therefore, `LoadLiveData`'s `interruption_point` should be sufficient to this parent
    #     algorithm as well.
    #
    #   * To avoid any unnecessary data collection by the `SNSLiveListener`, the `LoadLiveData` child is deleted
    #     immediately following the successful execution of its parent.  This is automatic in the implementation
    #     of the `AlgorithmManager`.
    #
    #   * All of this should be sufficient to allow `mantidworkbench` to successfully shut down
    #     without generating a SEGFAULT.
    #

    def PyInit(self):
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace", defaultValue="", direction=Direction.Output, optional=PropertyMode.Mandatory
            ),
            doc="Output event workspace containing the data interval",
        )

        self.declareProperty(
            "StartTime",
            defaultValue="",
            direction=Direction.Input,
            doc="absolute time of start of interval in ISO-8601 format (UTC)",
        )
        self.declareProperty(
            "EndTime",
            defaultValue=RunMetadata.FROM_NOW_ISO8601,
            direction=Direction.Input,
            doc="[optional] absolute time of end of interval in ISO-8601 format (UTC)",
        )

        self.declareProperty("Instrument", defaultValue="", direction=Direction.Input)

        # TODO: should "PreserveEvents" even be a declared property?
        #  Does this algorithm even work when this is turned off?
        self.declareProperty("PreserveEvents", defaultValue=True, direction=Direction.Input)

        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        if mtd.doesExist(self.getProperty("OutputWorkspace").valueAsStr):
            logger.warning(
                f"Output workspace '{self.getProperty('OutputWorkspace').valueAsStr}' already exists."
                "  Its contents will be replaced!"
            )

        # These times are converted from ISO8601 here, in order to verify the input.
        #   However, when calling `LoadLiveData` the ISO8601-format strings will be passed.
        try:
            self._startTime = np.datetime64(self.getProperty("StartTime").value)
        except ValueError as e:
            errors["StartTime"] = str(e)

        try:
            self._endTime = np.datetime64(self.getProperty("EndTime").value)
        except ValueError as e:
            errors["EndTime"] = str(e)

        if not self.getProperty("EndTime").isDefault and not ("StartTime" in errors or "EndTime" in errors):
            # FROM_NOW_ISO8601 is encoded as 1 second after the epoch,
            #   so this comparison usually only makes sense if an 'EndTime' has been specified.
            if not self._endTime > self._startTime:
                errors["EndTime"] = "'StartTime' must be before 'EndTime'."

        try:
            instrument = ConfigService.getFacility().instrument(self.getProperty("Instrument").value)  # noqa: F841
        except RuntimeError as e:
            if "FacilityInfo search object" not in str(e):
                raise
            errors["Instrument"] = (
                f"Instrument '{self.getProperty('Instrument').value}' not found in current facility.\n"
                "  Please execute `ConfigService.setFacility(...)` before using this algorithm."
            )

        return errors

    # --- Break-out `LoadLiveDataInterval` calls, and wrap `Algorithm` calls as `classmethod` to help with mocking. ---
    @classmethod
    def _requiredLoadInterval(cls, wsName: str, startTime: str) -> Tuple[np.datetime64, np.datetime64]:
        # Calculate the required loaded-data interval given the run interval and the argument values.
        # This interval will be used to check whether or not the data-loading is complete.

        # Implementation notes:
        #
        #   * Note that the end-time returned does not necessarily correspond to the requested filter interval.
        #     The current listener implementation does not filter by end-time, so we need to load
        #     to the latest time-point possible.
        #
        #   * Here we assume that each chunk is marked with the correct run start-time, but in most cases
        #     not the correct run end-time.  The end-time may not yet be known at the time the chunk is
        #     transferred.

        run = mtd[wsName].getRun()
        runStartTime = run.startTime().to_datetime64()

        if not cls.USING_ADARA_FileReader:
            requiredStartTime = (
                runStartTime
                if (startTime == RunMetadata.FROM_START_ISO8601)
                else DateAndTime(startTime).to_datetime64()
            )
            requiredEndTime = np.datetime64(datetime.datetime.now(datetime.timezone.utc).isoformat(), "ns")
        else:
            # Make corrections for the limitations of the mock "ADARA_FileReader":
            runEndTime = run.endTime().to_datetime64()

            # 1) Move the requested start-time to be relative to the end of the run.
            startTimeDelta = datetime.datetime.now(datetime.timezone.utc) - datetimeFromLogTime(
                DateAndTime(startTime).to_datetime64()
            )
            requiredStartTime = (
                runStartTime
                if (startTime == RunMetadata.FROM_START_ISO8601)
                else DateAndTime((datetimeFromLogTime(runEndTime) - startTimeDelta).isoformat()).to_datetime64()
            )

            # 2) Use the actual run end-time.
            requiredEndTime = runEndTime

        if requiredStartTime < runStartTime:
            requiredStartTime = runStartTime

        return requiredStartTime, requiredEndTime

    @classmethod
    @ConfigDefault
    def _compareIntervalEndpoints(
        # `numpy.datetime64` is used here as an intermediate type.
        #   * Python generally uses `datetime` and `timedelta`;
        #   * Mantid uses `mantid.kernel.DateAndTime`.
        cls,
        requiredStartTime: np.datetime64,
        requiredEndTime: np.datetime64,
        intervalPulseTimeMin: np.datetime64,
        intervalPulseTimeMax: np.datetime64,
        *,
        # `exact` => match the boundary points; otherwise cover, but don't necessarily match the inverval boundaries
        exact,
        comparisonThreshold=ConfigValue("liveData.time_comparison_threshold"),
    ):
        startDelta = requiredStartTime - intervalPulseTimeMin
        endDelta = intervalPulseTimeMax - requiredEndTime

        if not exact:
            # Test whether or not the data completely covers the interval:
            #   time-comparison delta is (-comparisonThreshold, comparisonThreshold).
            if startDelta > np.timedelta64(-comparisonThreshold, "s") and endDelta > np.timedelta64(
                -comparisonThreshold, "s"
            ):
                return True
        else:
            # Test whether the data boundary points match those of the requested interval:
            #   time-comparison delta is (-comparisonThreshold, comparisonThreshold).
            if abs(startDelta) < np.timedelta64(comparisonThreshold, "s") and abs(endDelta) < np.timedelta64(
                comparisonThreshold, "s"
            ):
                return True
        return False

    @classmethod
    def _loadIsComplete(
        cls, wsName: str, startTime: str, chunkIntervals: List[Tuple[np.datetime64, np.datetime64]]
    ) -> bool:
        # Determine whether or not the data completely covers the interval.
        # When the data covers the interval, determine whether or not there are any gaps.

        # Implementation notes:
        #
        #   * When the ADARA reader is busy, it may return the data in more than one chunk.
        #
        #   * In this case, it has been observed that historical data is returned after more-recent data,
        #     but we cannot assume that this is always the case.
        #
        #   * A complete dataset will include data with pulse times spanning the interval: (<start time>, <end time>),
        #     where the boundary points must match to within `Config['liveData.time_comparison_threshold']` seconds.
        #
        #   * In addition, a complete dataset will not have any pulse-time gaps that are larger than this same
        #     comparison threshold.
        #

        requiredStartTime, requiredEndTime = cls._requiredLoadInterval(wsName, startTime)

        ws = mtd[wsName]
        dataStartTime = ws.getPulseTimeMin().to_datetime64()
        dataEndTime = ws.getPulseTimeMax().to_datetime64()

        boundariesOK = cls._compareIntervalEndpoints(
            requiredStartTime,
            requiredEndTime,
            dataStartTime,
            dataEndTime,
            # Test for coverage: no need to match the boundary points.
            exact=False,
        )
        if boundariesOK:
            return cls._noDataGaps(chunkIntervals)
        return False

    @classmethod
    @ConfigDefault
    def _noDataGaps(
        cls,
        intervals: List[Tuple[np.datetime64, np.datetime64]],
        comparisonThreshold=ConfigValue("liveData.time_comparison_threshold"),
    ) -> bool:
        # Check a list of intervals for interval-to-interval gaps.

        # Sort intervals by start time.
        intervals_ = sorted(intervals, key=lambda dt: dt[0])

        gapsOK = True
        for n, dt0 in enumerate(intervals_[:-1]):
            dt1 = intervals_[n + 1]
            if dt1[0] - dt0[1] > np.timedelta64(comparisonThreshold, "s"):
                gapsOK = False
                break
        return gapsOK

    @classmethod
    def _createChildAlgorithm(cls, self_, *args, **kwargs):
        return self_.createChildAlgorithm(*args, **kwargs)

    # --------- end: `LoadLiveDataInterval` call break out to static methods. ------------------------------------------

    def PyExec(self):
        chunkWs = self.mantidSnapper.mtd.unique_hidden_name()
        self.chunkIntervals = []
        try:
            outputWs = self.getProperty("OutputWorkspace").valueAsStr
            startTime = self.getProperty("StartTime").value

            waitTimeIncrement = 10  # seconds
            dataLoadTimeout = Config["liveData.dataLoadTimeout"]

            # Create the "LoadLiveData" child and set its properties.
            loadLiveData = self._createChildAlgorithm(self, "LoadLiveData", 0.0, 0.75, self.isLogging())
            loadLiveData.initialize()
            loadLiveData.setAlwaysStoreInADS(True)
            loadLiveData.setRethrows(True)
            loadLiveData.setPropertyValue("OutputWorkspace", chunkWs)
            loadLiveData.setProperty("Instrument", self.getProperty("Instrument").value)
            loadLiveData.setProperty("StartTime", startTime)
            loadLiveData.setProperty("PreserveEvents", self.getProperty("PreserveEvents").value)
            #   In order to extract the chunk pulse-time span:
            #     each chunk of data will be loaded to the `chunkWs` first,
            #     before transferring its events to the output workspace.
            loadLiveData.setProperty("AccumulationMethod", "Replace")

            # Load the first data-chunk: this replaces any contents of the output workspace.
            loadLiveData.execute()

            run = self.mantidSnapper.mtd[chunkWs].getRun()
            activeRunNumber = self.mantidSnapper.mtd[chunkWs].getRunNumber()

            logger.info(f"Run interval: ({run.startTime().to_datetime64()}, {run.endTime().to_datetime64()})")
            logger.info(
                f"Loaded-chunk interval: ({self.mantidSnapper.mtd[chunkWs].getPulseTimeMin().to_datetime64()}, "
                f"{self.mantidSnapper.mtd[chunkWs].getPulseTimeMax().to_datetime64()})"
            )

            self.chunkIntervals.append(
                (
                    self.mantidSnapper.mtd[chunkWs].getPulseTimeMin().to_datetime64(),
                    self.mantidSnapper.mtd[chunkWs].getPulseTimeMax().to_datetime64(),
                )
            )

            self.mantidSnapper.CloneWorkspace(
                "replace output workspace", OutputWorkspace=outputWs, InputWorkspace=chunkWs
            )
            self.mantidSnapper.executeQueue()

            # For any remaining data, events will be added to those already in the output workspace.
            waitTime = 0
            dataLoadTimeout = Config["liveData.dataLoadTimeout"]
            waitTimeIncrement = Config["liveData.chunkLoadWait"]
            while (waitTime < dataLoadTimeout) and not self._loadIsComplete(outputWs, startTime, self.chunkIntervals):
                sleep(waitTimeIncrement)
                waitTime += waitTimeIncrement
                # Load another chunk of data.
                loadLiveData.execute()

                # Check for possible run-state change:
                if activeRunNumber != self.mantidSnapper.mtd[chunkWs].getRunNumber():
                    break

                logger.info(
                    f"Loaded-chunk interval: ({self.mantidSnapper.mtd[chunkWs].getPulseTimeMin().to_datetime64()}, "
                    f"{self.mantidSnapper.mtd[chunkWs].getPulseTimeMax().to_datetime64()})"
                )

                self.chunkIntervals.append(
                    (
                        self.mantidSnapper.mtd[chunkWs].getPulseTimeMin().to_datetime64(),
                        self.mantidSnapper.mtd[chunkWs].getPulseTimeMax().to_datetime64(),
                    )
                )
                self.mantidSnapper.Plus(
                    "accumulate chunk to output workspace",
                    OutputWorkspace=outputWs,
                    LHSWorkspace=outputWs,
                    RHSWorkspace=chunkWs,
                    ClearRHSWorkspace=True,
                )
                self.mantidSnapper.executeQueue()

            if not self._loadIsComplete(outputWs, startTime, self.chunkIntervals):
                if waitTime >= dataLoadTimeout:
                    logger.warning("A timeout occurred during data loading.")
                logger.warning(
                    "The complete data interval could not be loaded -- please check ADARA status at 'monitor.sns.gov'"
                )
        finally:
            if self.mantidSnapper.mtd.doesExist(chunkWs):
                self.mantidSnapper.DeleteWorkspace("delete chunk workspace", Workspace=chunkWs)
                self.mantidSnapper.executeQueue()

        if not self.getProperty("EndTime").isDefault and not self.USING_ADARA_FileReader:
            # Use the specified time interval to filter the output-workspace events.
            self.mantidSnapper.FilterByTime(
                "filter time interval of output workspace",
                OutputWorkspace=outputWs,
                InputWorkspace=outputWs,
                AbsoluteStartTime=self.getProperty("StartTime").value,
                AbsoluteStopTime=self.getProperty("EndTime").value,
            )
            self.mantidSnapper.executeQueue()

        self.setProperty("OutputWorkspace", outputWs)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LoadLiveDataInterval)
