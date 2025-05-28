import datetime
import inspect
import unittest
from datetime import timedelta
from unittest import mock

import numpy as np
import pytest
from mantid.api import MatrixWorkspaceProperty, Run, mtd
from mantid.kernel import DateAndTime
from mantid.simpleapi import (
    CloneWorkspace,
    CreateSampleWorkspace,
    DeleteWorkspace,
    DeleteWorkspaces,
    FilterByTime,
    Plus,
)
from util.Config_helpers import Config_override

from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.data.util.PV_logs_util import datetimeFromLogTime
from snapred.backend.recipe.algorithm.LoadLiveDataInterval import LoadLiveDataInterval
from snapred.meta.Config import Config


class TestLoadLiveDataInterval(unittest.TestCase):
    def setUp(self):
        # Use `LoadLiveDataInterval()` here instead of `AlgorithmManager.create("LoadLiveDataInterval")`:
        #   initializing the mocks will require the complete instance, rather than just the `IAlgorithm`.
        self.instance = LoadLiveDataInterval()  # AlgorithmManager.create("LoadLiveDataInterval")
        self.outputWs = "output_ws"
        self.startTime = (datetime.datetime.now(datetime.timezone.utc) - timedelta(minutes=10)).isoformat()
        self.endTime = (datetime.datetime.now(datetime.timezone.utc) - timedelta(minutes=9)).isoformat()
        self.instrument = Config["instrument.name"]
        self.preserveEvents = True

    def tearDown(self):
        if mtd.doesExist(self.outputWs):
            DeleteWorkspace(self.outputWs)

    @classmethod
    def setUpClass(cls):
        # Create a workspace representing the full time interval,
        #   and break it into several chunks.
        cls.N_chunk = 4

        # The full workspace, which is the completely loaded data interval:
        #   * ['start_time', 'end_time']: ['2010-01-01T00:00:00', '2010-01-01T01:00:00']
        #   * 16 spectra, each of which includes events spanning
        #     [<minimum pulse time>, <maximum pulse time>]: <approximately the run interval above>

        cls.fullWs = "fullWs"
        CreateSampleWorkspace(OutputWorkspace=cls.fullWs, WorkspaceType="Event", NumBanks=4, BankPixelWidth=2)

        # Mantid time arguments are generally ISO8601 strings,
        #   but it's easiest to deal either with `numpy.datetime64` (just an int64)
        #   or with `datetime` and `timedelta`.
        run = mtd[cls.fullWs].getRun()
        cls.sampleRunInterval = (
            DateAndTime(run["start_time"].value).to_datetime64(),
            DateAndTime(run["end_time"].value).to_datetime64(),
        )

        # The following does not work with `np.linspace` for some reason.
        chunkDelta = (cls.sampleRunInterval[1] - cls.sampleRunInterval[0]) // cls.N_chunk
        cls.chunkEdges = np.arange(cls.sampleRunInterval[0], cls.sampleRunInterval[1] + chunkDelta, chunkDelta)

        cls.chunkWss = [f"chunk_{n}" for n in range(cls.N_chunk)]
        for n in range(cls.N_chunk):
            FilterByTime(
                OutputWorkspace=cls.chunkWss[n],
                InputWorkspace=cls.fullWs,
                AbsoluteStartTime=str(DateAndTime(cls.chunkEdges[n])),
                AbsoluteStopTime=str(DateAndTime(cls.chunkEdges[n + 1])),
            )

    @classmethod
    def tearDownClass(cls):
        wss = [cls.fullWs]
        wss.extend(cls.chunkWss)
        DeleteWorkspaces(wss)

    class _MockLoadLiveData:
        def __init__(self, chunkWss):
            # properties dict:
            self._properties = {}

            # Chunks to use during `execute` calls,
            #   for either `AccumulationMethod='Replace'` or
            #   `AccumulationMethod='Add'.
            self._chunkWss = chunkWss

            # workspace from `chunkWss` to use during the next call.
            self._wsIndex = 0

        def setProperty(self, key, value):
            self._properties[key] = value

        def setPropertyValue(self, key, value):
            self._properties[key] = value

        def getProperty(self, key):
            return self._properties[key]

        def initialize(self):
            pass

        def setAlwaysStoreInADS(self, flag: bool):
            pass

        def setRethrows(self, flag: bool):
            pass

        def execute(self):
            if self._properties["AccumulationMethod"] == "Replace":
                CloneWorkspace(
                    OutputWorkspace=self._properties["OutputWorkspace"],
                    InputWorkspace=self._chunkWss[self._wsIndex],
                )
            elif self._properties["AccumulationMethod"] == "Add":
                if not mtd.doesExist(self._properties["OutputWorkspace"]):
                    CloneWorkspace(
                        OutputWorkspace=self._properties["OutputWorkspace"],
                        InputWorkspace=self._chunkWss[self._wsIndex],
                    )
                else:
                    Plus(
                        OutputWorkspace=self._properties["OutputWorkspace"],
                        LHSWorkspace=self._properties["OutputWorkspace"],
                        RHSWorkspace=self._chunkWss[self._wsIndex],
                    )
            else:
                raise RuntimeError(f"Unimplemented accumulation method: '{self._properties['AccumulationMethod']}'")
            self._wsIndex += 1

    @classmethod
    def _mockLoadLiveData(cls, chunkWss) -> mock.Mock:
        # Create a simple `LoadLiveData` emulation that on `execute`,
        #   applies the next chunk from the `chunkWss` workspace sequence,
        #   using either `AccumulationMode="Replace"` or `AccumulationMode="Add"`.
        return mock.Mock(wraps=TestLoadLiveDataInterval._MockLoadLiveData(chunkWss))

    def _setProperties(self, instance, **kwargs):
        for key, value in kwargs.items():
            if isinstance(instance.getProperty(key), MatrixWorkspaceProperty):
                instance.setPropertyValue(key, value)
            else:
                instance.setProperty(key, value)

    def test_init(self):
        self.instance.initialize()

        assert set([p.name for p in self.instance.getProperties()]) == set(
            ("OutputWorkspace", "StartTime", "EndTime", "Instrument", "PreserveEvents")
        )

        # verify default values
        assert self.instance.getProperty("EndTime").isDefault
        assert self.instance.getProperty("EndTime").value == RunMetadata.FROM_NOW_ISO8601

        assert self.instance.getProperty("PreserveEvents").value

    def test_validateInputs(self):
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_mtd.doesExist.return_value = False

            self.instance.initialize()
            self._setProperties(
                self.instance,
                OutputWorkspace=self.outputWs,
                StartTime=self.startTime,
                EndTime=self.endTime,
                Instrument=Config["instrument.name"],
                PreserveEvents=self.preserveEvents,
            )
            self.instance.validateInputs()

    def test_validateInputs_output_exists(self):
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_mtd.doesExist.return_value = True

            self.instance.initialize()
            self._setProperties(
                self.instance,
                OutputWorkspace=self.outputWs,
                StartTime=self.startTime,
                EndTime=self.endTime,
                Instrument=Config["instrument.name"],
                PreserveEvents=self.preserveEvents,
            )
            self.instance.validateInputs()

            warningMessage = mock_logger.warning.mock_calls[0].args[0]
            assert f"Output workspace '{self.outputWs}' already exists." in warningMessage

    def test_validateInputs_StartTime_format(self):
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_mtd.doesExist.return_value = False

            self.instance.initialize()
            self._setProperties(
                self.instance,
                OutputWorkspace=self.outputWs,
                StartTime="12.00",
                EndTime=self.endTime,
                Instrument=Config["instrument.name"],
                PreserveEvents=self.preserveEvents,
            )
            errors = self.instance.validateInputs()

            assert "StartTime" in errors
            assert "Error parsing datetime string" in errors["StartTime"]

    def test_validateInputs_EndTime_format(self):
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_mtd.doesExist.return_value = False

            self.instance.initialize()
            self._setProperties(
                self.instance,
                OutputWorkspace=self.outputWs,
                StartTime=self.startTime,
                EndTime="12.00",
                Instrument=Config["instrument.name"],
                PreserveEvents=self.preserveEvents,
            )
            errors = self.instance.validateInputs()

            assert "EndTime" in errors
            assert "Error parsing datetime string" in errors["EndTime"]

    def test_validateInputs_time_order(self):
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_mtd.doesExist.return_value = False

            now = datetime.datetime.now(datetime.timezone.utc)
            self.instance.initialize()
            self._setProperties(
                self.instance,
                OutputWorkspace=self.outputWs,
                # End time cannot be before start time.
                StartTime=(now - timedelta(minutes=10)).isoformat(),
                EndTime=(now - timedelta(minutes=11)).isoformat(),
                Instrument=Config["instrument.name"],
                PreserveEvents=self.preserveEvents,
            )
            errors = self.instance.validateInputs()

            assert "EndTime" in errors
            assert "'StartTime' must be before 'EndTime'." in errors["EndTime"]

    def test_validateInputs_instrument(self):
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
        ):
            mock_ConfigService.getFacility.return_value.instrument.side_effect = RuntimeError(
                "FacilityInfo search object"
            )
            mock_mtd.doesExist.return_value = False

            self.instance.initialize()
            self._setProperties(
                self.instance,
                OutputWorkspace=self.outputWs,
                StartTime=self.startTime,
                EndTime=self.endTime,
                Instrument=Config["instrument.name"],
                PreserveEvents=self.preserveEvents,
            )
            errors = self.instance.validateInputs()

            assert "Instrument" in errors
            assert f"Instrument '{Config['instrument.name']}' not found in current facility." in errors["Instrument"]

    def test_validateInputs_instrument_other_exception(self):
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
        ):
            mock_ConfigService.getFacility.return_value.instrument.side_effect = RuntimeError(
                "Some other facility error"
            )
            mock_mtd.doesExist.return_value = False

            self.instance.initialize()
            self._setProperties(
                self.instance,
                OutputWorkspace=self.outputWs,
                StartTime=self.startTime,
                EndTime=self.endTime,
                Instrument=Config["instrument.name"],
                PreserveEvents=self.preserveEvents,
            )
            with pytest.raises(RuntimeError, match="Some other facility error"):
                errors = self.instance.validateInputs()  # noqa: F841

    def test__requiredLoadInterval(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
        ):
            runStartTime = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) - timedelta(minutes=10)).isoformat()
            )
            runEndTime = DateAndTime(now.isoformat())
            mock_run = mock.Mock(spec=Run)
            mock_run.startTime.return_value = runStartTime
            mock_run.endTime.return_value = runEndTime
            mock_mtd.__getitem__.return_value.getRun.return_value = mock_run
            mock_datetime.datetime.now.return_value = now

            expectedInterval = (
                DateAndTime(self.startTime).to_datetime64(),
                DateAndTime(now.isoformat()).to_datetime64(),
            )
            actualInterval = LoadLiveDataInterval._requiredLoadInterval(self.outputWs, self.startTime)
            assert actualInterval == expectedInterval

    def test__requiredLoadInterval_start_of_run(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
        ):
            runStartTime = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) - timedelta(minutes=10)).isoformat()
            )
            runEndTime = DateAndTime(now.isoformat())
            mock_run = mock.Mock(spec=Run)
            mock_run.startTime.return_value = runStartTime
            mock_run.endTime.return_value = runEndTime
            mock_mtd.__getitem__.return_value.getRun.return_value = mock_run
            mock_datetime.datetime.now.return_value = now

            fromStartCode = RunMetadata.FROM_START_ISO8601
            expectedInterval = (runStartTime.to_datetime64(), DateAndTime(now.isoformat()).to_datetime64())
            actualInterval = LoadLiveDataInterval._requiredLoadInterval(self.outputWs, fromStartCode)

            assert actualInterval == expectedInterval

    def test__loadIsComplete(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
        ):
            runStartTime = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) - timedelta(minutes=10)).isoformat()
            )
            runEndTime = DateAndTime(now.isoformat())
            mock_run = mock.Mock(spec=Run)
            mock_run.startTime.return_value = runStartTime
            mock_run.endTime.return_value = runEndTime
            mock_mtd.__getitem__.return_value.getRun.return_value = mock_run
            mock_mtd.__getitem__.return_value.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_mtd.__getitem__.return_value.getPulseTimeMax.return_value = DateAndTime(now.isoformat())
            mock_datetime.datetime.now.return_value = now

            actual = LoadLiveDataInterval._loadIsComplete(self.outputWs, self.startTime, chunkIntervals=[])

            assert actual
            # ... assert various calls ...

    def test__loadIsComplete_min_pulse_too_late(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
        ):
            runStartTime = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) - timedelta(minutes=10)).isoformat()
            )
            runEndTime = DateAndTime(now.isoformat())
            mock_run = mock.Mock(spec=Run)
            mock_run.startTime.return_value = runStartTime
            mock_run.endTime.return_value = runEndTime
            mock_mtd.__getitem__.return_value.getRun.return_value = mock_run

            comparisonThreshold = Config["liveData.time_comparison_threshold"]
            minPulseTime = DateAndTime(
                (
                    datetime.datetime.fromisoformat(self.startTime) + timedelta(seconds=2 * comparisonThreshold)
                ).isoformat()
            )
            mock_mtd.__getitem__.return_value.getPulseTimeMin.return_value = minPulseTime
            mock_mtd.__getitem__.return_value.getPulseTimeMax.return_value = DateAndTime(now.isoformat())
            mock_datetime.datetime.now.return_value = now

            actual = LoadLiveDataInterval._loadIsComplete(self.outputWs, self.startTime, chunkIntervals=[])

            assert not actual

    def test__loadIsComplete_max_pulse_too_early(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
        ):
            runStartTime = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) - timedelta(minutes=10)).isoformat()
            )
            runEndTime = DateAndTime(now.isoformat())
            mock_run = mock.Mock(spec=Run)
            mock_run.startTime.return_value = runStartTime
            mock_run.endTime.return_value = runEndTime
            mock_mtd.__getitem__.return_value.getRun.return_value = mock_run

            comparisonThreshold = Config["liveData.time_comparison_threshold"]
            maxPulseTime = DateAndTime((now - timedelta(seconds=2 * comparisonThreshold)).isoformat())
            mock_mtd.__getitem__.return_value.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_mtd.__getitem__.return_value.getPulseTimeMax.return_value = maxPulseTime
            mock_datetime.datetime.now.return_value = now

            actual = LoadLiveDataInterval._loadIsComplete(self.outputWs, self.startTime, chunkIntervals=[])

            assert not actual

    def test__noDataGaps_chunk_intervals_no_gap(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
        ):
            runStartTime = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) - timedelta(minutes=10)).isoformat()
            )
            runEndTime = DateAndTime(now.isoformat())
            mock_run = mock.Mock(spec=Run)
            mock_run.startTime.return_value = runStartTime
            mock_run.endTime.return_value = runEndTime
            mock_mtd.__getitem__.return_value.getRun.return_value = mock_run

            minPulseTime, maxPulseTime = DateAndTime(self.startTime), DateAndTime(now.isoformat())
            mock_mtd.__getitem__.return_value.getPulseTimeMin.return_value = minPulseTime
            mock_mtd.__getitem__.return_value.getPulseTimeMax.return_value = maxPulseTime
            mock_datetime.datetime.now.return_value = now

            # Break [self.startTime, now] into four chunks:
            N_chunk = 4
            dt = (maxPulseTime.to_datetime64() - minPulseTime.to_datetime64()) / (N_chunk + 1)
            ts = [(minPulseTime.to_datetime64() + n * dt) for n in range(N_chunk + 1)]
            chunkIntervals = [(ts[n], ts[n + 1]) for n in range(N_chunk)]

            actual = LoadLiveDataInterval._noDataGaps(intervals=chunkIntervals)

            assert actual

    def test__noDataGaps_chunk_intervals_no_gap_out_of_order(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
        ):
            runStartTime = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) - timedelta(minutes=10)).isoformat()
            )
            runEndTime = DateAndTime(now.isoformat())
            mock_run = mock.Mock(spec=Run)
            mock_run.startTime.return_value = runStartTime
            mock_run.endTime.return_value = runEndTime
            mock_mtd.__getitem__.return_value.getRun.return_value = mock_run

            minPulseTime, maxPulseTime = DateAndTime(self.startTime), DateAndTime(now.isoformat())
            mock_mtd.__getitem__.return_value.getPulseTimeMin.return_value = minPulseTime
            mock_mtd.__getitem__.return_value.getPulseTimeMax.return_value = maxPulseTime
            mock_datetime.datetime.now.return_value = now

            # Break [self.startTime, now] into four chunks:
            N_chunk = 4
            dt = (maxPulseTime.to_datetime64() - minPulseTime.to_datetime64()) / (N_chunk + 1)
            ts = [(minPulseTime.to_datetime64() + n * dt) for n in range(N_chunk + 1)]
            chunkIntervals = [(ts[n], ts[n + 1]) for n in range(N_chunk)]

            actual = LoadLiveDataInterval._noDataGaps(intervals=[chunkIntervals[n] for n in (0, 3, 1, 2)])

            assert actual

    def test__noDataGaps_chunk_intervals_with_gap(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "mtd") as mock_mtd,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
        ):
            runStartTime = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) - timedelta(minutes=10)).isoformat()
            )
            runEndTime = DateAndTime(now.isoformat())
            mock_run = mock.Mock(spec=Run)
            mock_run.startTime.return_value = runStartTime
            mock_run.endTime.return_value = runEndTime
            mock_mtd.__getitem__.return_value.getRun.return_value = mock_run

            minPulseTime, maxPulseTime = DateAndTime(self.startTime), DateAndTime(now.isoformat())
            mock_mtd.__getitem__.return_value.getPulseTimeMin.return_value = minPulseTime
            mock_mtd.__getitem__.return_value.getPulseTimeMax.return_value = maxPulseTime
            mock_datetime.datetime.now.return_value = now

            # Break [self.startTime, now] into four chunks:
            N_chunk = 4
            dt = (maxPulseTime.to_datetime64() - minPulseTime.to_datetime64()) / (N_chunk + 1)
            ts = [(minPulseTime.to_datetime64() + n * dt) for n in range(N_chunk + 1)]
            chunkIntervals = [(ts[n], ts[n + 1]) for n in range(N_chunk)]

            actual = LoadLiveDataInterval._noDataGaps(
                # dt #2 is missing
                intervals=[chunkIntervals[n] for n in (0, 1, 3)]
            )

            assert not actual

    def test_exec_child_init(self):
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            # bypass `FilterByTime` call: use the default-value for EndTime
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging") as mock_isLogging,
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )
            mock_chunkIntervals = [
                (
                    mock_chunkWs.getPulseTimeMin.return_value.to_datetime64(),
                    mock_chunkWs.getPulseTimeMax.return_value.to_datetime64(),
                )
            ]
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_isLogging.return_value = mock.sentinel.isLogging
            mock_loadIsComplete.return_value = True

            self.instance.execute()
            mock_createChildAlgorithm.assert_called_once_with(
                "LoadLiveData", 0.0, 0.75, self.instance.isLogging.return_value
            )
            mock_LoadLiveData.initialize.assert_called_once()

            mock_LoadLiveData.setPropertyValue.assert_any_call("OutputWorkspace", mock.sentinel.chunkWs)
            mock_LoadLiveData.setProperty.assert_any_call("Instrument", self.instance.getProperty("Instrument").value)
            mock_LoadLiveData.setProperty.assert_any_call("StartTime", self.instance.getProperty("StartTime").value)
            mock_LoadLiveData.setProperty.assert_any_call(
                "PreserveEvents", self.instance.getProperty("PreserveEvents").value
            )
            mock_LoadLiveData.setProperty.assert_any_call("AccumulationMethod", "Replace")
            mock_LoadLiveData.execute.assert_called_once()

            mock_snapper.CloneWorkspace.assert_called_once_with(
                "replace output workspace", OutputWorkspace=self.outputWs, InputWorkspace=mock.sentinel.chunkWs
            )

            mock_loadIsComplete.call_count == 2
            mock_loadIsComplete.assert_any_call(self.outputWs, self.startTime, mock_chunkIntervals)

            mock_snapper.FilterByTime.assert_not_called()

    def test_exec_incomplete_load_warning(self):
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            # bypass `FilterByTime` call: use the default-value for EndTime
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging") as mock_isLogging,
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )
            mock_chunkIntervals = [  # noqa: F841
                (mock_chunkWs.getPulseTimeMin.return_value, mock_chunkWs.getPulseTimeMax.return_value)
            ]
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_isLogging.return_value = mock.sentinel.isLogging

            # `<class>._loadIsComplete`:
            #   * first call checks whether to exit the chunk-loading loap;
            #   * second call checks after the loop whether or not there was a complete load.
            mock_loadIsComplete.side_effect = (
                True,
                False,
                RuntimeError("'_loadIsComplete' mock called too many times!"),
            )

            self.instance.execute()
            msg = mock_logger.warning.mock_calls[0].args[0]
            assert "The complete data interval could not be loaded" in msg

    def test_exec_load_timeout(self):
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            # bypass `FilterByTime` call: use the default-value for EndTime
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging") as mock_isLogging,
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
            Config_override("liveData.dataLoadTimeout", 10),
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )
            mock_chunkIntervals = [  # noqa: F841
                (mock_chunkWs.getPulseTimeMin.return_value, mock_chunkWs.getPulseTimeMax.return_value)
            ]
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_isLogging.return_value = mock.sentinel.isLogging

            mock_loadIsComplete.return_value = False
            mock_sleep.side_effect = lambda _: None

            self.instance.execute()
            msg = mock_logger.warning.mock_calls[0].args[0]
            assert "A timeout occurred during data loading" in msg

    def test_exec_filter_init(self):
        mock_LoadLiveData = mock.Mock()

        def createChildAlgorithm_(self_, *args, **_kwargs):  # noqa: ARG001
            name = args[0]
            match name:
                case "LoadLiveData":
                    return mock_LoadLiveData
                case _:
                    raise RuntimeError(f"'createChildAlgorithm' mock unimplemented for args: '{args}'")

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            # trigger the `FilterByTime` call:
            EndTime=self.endTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,  # noqa: F841
            mock.patch.object(LoadLiveDataInterval, "_createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging") as mock_isLogging,
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.side_effect = createChildAlgorithm_

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )
            mock_chunkIntervals = [  # noqa: F841
                (mock_chunkWs.getPulseTimeMin.return_value, mock_chunkWs.getPulseTimeMax.return_value)
            ]
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_isLogging.return_value = mock.sentinel.isLogging
            mock_loadIsComplete.return_value = True

            self.instance.execute()
            mock_snapper.CloneWorkspace.assert_called_once_with(
                "replace output workspace", OutputWorkspace=self.outputWs, InputWorkspace=mock.sentinel.chunkWs
            )

            mock_snapper.DeleteWorkspace.assert_called_once_with(
                "delete chunk workspace", Workspace=mock.sentinel.chunkWs
            )

            mock_snapper.FilterByTime.assert_called_once_with(
                "filter time interval of output workspace",
                OutputWorkspace=self.outputWs,
                InputWorkspace=self.outputWs,
                AbsoluteStartTime=self.startTime,
                AbsoluteStopTime=self.endTime,
            )
            assert mock_snapper.executeQueue.call_count == 3

    def test_exec_filter(self):
        # For this test, the complete interval will be loaded at once.
        mock_LoadLiveData = self._mockLoadLiveData((self.fullWs,))

        def createChildAlgorithm_(self_, *args, **_kwargs):  # noqa: ARG001
            name = args[0]
            match name:
                case "LoadLiveData":
                    return mock_LoadLiveData
                case _:
                    raise RuntimeError(f"'createChildAlgorithm' mock unimplemented for args: '{args}'")

        # Set the interval start and end times so that they are _within_ the sample-workspace interval.
        requiredStartTime = DateAndTime(
            (datetimeFromLogTime(self.sampleRunInterval[0]) + timedelta(minutes=15)).isoformat()
        ).to_datetime64()
        requiredEndTime = DateAndTime(
            (datetimeFromLogTime(self.sampleRunInterval[1]) - timedelta(minutes=15)).isoformat()
        ).to_datetime64()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=str(DateAndTime(requiredStartTime)),
            EndTime=str(DateAndTime(requiredEndTime)),
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(LoadLiveDataInterval, "_createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.side_effect = createChildAlgorithm_

            # `datetime.now` is used to determine the load-complete interval end time.
            mock_datetime.datetime.now.return_value = datetimeFromLogTime(self.sampleRunInterval[1])
            mock_sleep.side_effect = lambda _: None

            self.instance.execute()

            # Verify that the data pulse-time interval of the output workspace is correct.
            ws = mtd[self.outputWs]
            assert LoadLiveDataInterval._compareIntervalEndpoints(
                requiredStartTime,
                requiredEndTime,
                ws.getPulseTimeMin().to_datetime64(),
                ws.getPulseTimeMax().to_datetime64(),
                exact=True,
            )

    def test_exec_forward_chunks(self):
        # For this test, the interval is loaded as four chunks, moving forwards in time.
        mock_LoadLiveData = self._mockLoadLiveData(self.chunkWss)
        mock_FilterByTime = mock.Mock()

        def createChildAlgorithm_(self_, *args, **_kwargs):  # noqa: ARG001
            name = args[0]
            match name:
                case "LoadLiveData":
                    return mock_LoadLiveData
                case _:
                    raise RuntimeError(f"'createChildAlgorithm' mock unimplemented for args: '{args}'")

        # Set the start and end times so that no filtering is required.
        requiredStartTime = self.sampleRunInterval[0]
        requiredEndTime = self.sampleRunInterval[1]

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=str(DateAndTime(requiredStartTime)),
            # default 'EndTime' is now.
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(LoadLiveDataInterval, "_createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.side_effect = createChildAlgorithm_
            self.instance.mantidSnapper.FilterByTime = mock_FilterByTime

            # `datetime.now` is used to determine the load-complete interval end time.
            #  (For this test, this is the same as the `requiredEndTime`, but here we will be explicit.)
            mock_datetime.datetime.now.return_value = datetimeFromLogTime(self.sampleRunInterval[1])
            mock_sleep.side_effect = lambda _: None

            self.instance.execute()

            # Verify that the data pulse-time interval of the output workspace is correct:
            #   this should include the entire run.
            ws = mtd[self.outputWs]
            assert LoadLiveDataInterval._compareIntervalEndpoints(
                requiredStartTime,
                requiredEndTime,
                ws.getPulseTimeMin().to_datetime64(),
                ws.getPulseTimeMax().to_datetime64(),
                exact=True,
            )

            # Verify the key 'LoadLiveData' calls:
            mock_LoadLiveData.setProperty.assert_any_call("AccumulationMethod", "Replace")
            assert mock_LoadLiveData.execute.call_count == len(self.chunkWss)

            # Verify that no filtering was required:
            self.instance.mantidSnapper.FilterByTime.assert_not_called()

    def test_exec_backward_chunks(self):
        # For this test, the interval is loaded as four chunks, moving backwards in time.
        mock_LoadLiveData = self._mockLoadLiveData(self.chunkWss[-1::-1])
        mock_FilterByTime = mock.Mock()

        def createChildAlgorithm_(self_, *args, **_kwargs):  # noqa: ARG001
            name = args[0]
            match name:
                case "LoadLiveData":
                    return mock_LoadLiveData
                case _:
                    raise RuntimeError(f"'createChildAlgorithm' mock unimplemented for args: '{args}'")

        # Set the start and end times so that no filtering is required.
        requiredStartTime = self.sampleRunInterval[0]
        requiredEndTime = self.sampleRunInterval[1]

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=str(DateAndTime(requiredStartTime)),
            # default 'EndTime' is now.
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(LoadLiveDataInterval, "_createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.side_effect = createChildAlgorithm_
            self.instance.mantidSnapper.FilterByTime = mock_FilterByTime

            # `datetime.now` is used to determine the load-complete interval end time.
            #  (For this test, this is the same as the `requiredEndTime`, but here we will be explicit.)
            mock_datetime.datetime.now.return_value = datetimeFromLogTime(self.sampleRunInterval[1])
            mock_sleep.side_effect = lambda _: None

            self.instance.execute()

            # Verify that the data pulse-time interval of the output workspace is correct:
            #   this should include the entire run.
            ws = mtd[self.outputWs]
            assert LoadLiveDataInterval._compareIntervalEndpoints(
                requiredStartTime,
                requiredEndTime,
                ws.getPulseTimeMin().to_datetime64(),
                ws.getPulseTimeMax().to_datetime64(),
                exact=True,
            )

            # Verify the key 'LoadLiveData' calls:
            mock_LoadLiveData.setProperty.assert_any_call("AccumulationMethod", "Replace")
            assert mock_LoadLiveData.execute.call_count == len(self.chunkWss)

            # Verify that no filtering was required:
            self.instance.mantidSnapper.FilterByTime.assert_not_called()

    def test_exec_scrambled_chunks(self):
        # For this test, the interval is loaded as four chunks,
        #   but in a random order so that there's a time-gap in the required interval, prior to its completion.

        mock_LoadLiveData = self._mockLoadLiveData([self.chunkWss[i] for i in (0, 3, 1, 2)])
        mock_FilterByTime = mock.Mock()

        def createChildAlgorithm_(self_, *args, **_kwargs):  # noqa: ARG001
            name = args[0]
            match name:
                case "LoadLiveData":
                    return mock_LoadLiveData
                case _:
                    raise RuntimeError(f"'createChildAlgorithm' mock unimplemented for args: '{args}'")

        # Set the start and end times so that no filtering is required.
        requiredStartTime = self.sampleRunInterval[0]
        requiredEndTime = self.sampleRunInterval[1]

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=str(DateAndTime(requiredStartTime)),
            # default 'EndTime' is now.
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(LoadLiveDataInterval, "_createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.side_effect = createChildAlgorithm_
            self.instance.mantidSnapper.FilterByTime = mock_FilterByTime

            # `datetime.now` is used to determine the load-complete interval end time.
            #  (For this test, this is the same as the `requiredEndTime`, but here we will be explicit.)
            mock_datetime.datetime.now.return_value = datetimeFromLogTime(self.sampleRunInterval[1])
            mock_sleep.side_effect = lambda _: None

            self.instance.execute()

            # Verify that the data pulse-time interval of the output workspace is correct:
            #   this should include the entire run.
            ws = mtd[self.outputWs]
            assert LoadLiveDataInterval._compareIntervalEndpoints(
                requiredStartTime,
                requiredEndTime,
                ws.getPulseTimeMin().to_datetime64(),
                ws.getPulseTimeMax().to_datetime64(),
                exact=True,
            )

            # Verify the key 'LoadLiveData' calls:
            mock_LoadLiveData.setProperty.assert_any_call("AccumulationMethod", "Replace")
            assert mock_LoadLiveData.execute.call_count == len(self.chunkWss)

            # Verify that no filtering was required:
            self.instance.mantidSnapper.FilterByTime.assert_not_called()

    def test_exec_incomplete_load(self):
        # For this test, the interval is loaded as four chunks, moving backwards in time.
        mock_LoadLiveData = self._mockLoadLiveData(
            # Accumulate 70s of wait time, without any complete load.
            [
                self.chunkWss[1],
                self.chunkWss[2],
                self.chunkWss[1],
                self.chunkWss[2],
                self.chunkWss[1],
                self.chunkWss[2],
                self.chunkWss[1],
                self.chunkWss[2],
            ]
        )
        mock_FilterByTime = mock.Mock()

        def createChildAlgorithm_(self_, *args, **_kwargs):  # noqa: ARG001
            name = args[0]
            match name:
                case "LoadLiveData":
                    return mock_LoadLiveData
                case _:
                    raise RuntimeError(f"'createChildAlgorithm' mock unimplemented for args: '{args}'")

        # Set the start and end times so that no filtering is required.
        requiredStartTime = self.sampleRunInterval[0]
        requiredEndTime = self.sampleRunInterval[1]  # noqa: F841

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=str(DateAndTime(requiredStartTime)),
            # default 'EndTime' is now.
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,
            mock.patch.object(LoadLiveDataInterval, "_createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.side_effect = createChildAlgorithm_
            self.instance.mantidSnapper.FilterByTime = mock_FilterByTime

            # `datetime.now` is used to determine the load-complete interval end time.
            #  (For this test, this is the same as the `requiredEndTime`, but here we will be explicit.)
            mock_datetime.datetime.now.return_value = datetimeFromLogTime(self.sampleRunInterval[1])
            mock_sleep.side_effect = lambda _: None

            self.instance.execute()

            # Check that the correct warnings were logged.
            msg = mock_logger.warning.mock_calls[0].args[0]
            assert "A timeout occurred during data loading." in msg
            msg = mock_logger.warning.mock_calls[1].args[0]
            assert "The complete data interval could not be loaded" in msg

            # Verify the key 'LoadLiveData' calls:
            mock_LoadLiveData.setProperty.assert_any_call("AccumulationMethod", "Replace")
            assert mock_LoadLiveData.execute.call_count == 7  # 10s wait per call => 60s timeout.

            # Verify that no filtering was required:
            mock_FilterByTime.execute.assert_not_called()
