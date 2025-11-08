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
from snapred.backend.error.RunStatus import RunStatus
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

        # Default: treat all chunks as from a RUNNING run.
        # Tests for RunStatus-based detection can override via self._from_run_patcher.side_effect.
        self._from_run_patch = mock.patch.object(RunStatus, "from_run", return_value=RunStatus.RUNNING)
        self._from_run_patcher = self._from_run_patch.start()

    def tearDown(self):
        if mtd.doesExist(self.outputWs):
            DeleteWorkspace(self.outputWs)
        self._from_run_patch.stop()

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

        # `CreateSampleWorkspace` creates workspaces with run number 0.
        # Set a non-zero run number on all workspaces so that
        # `LoadLiveDataInterval`'s inactive-run guard does not fire.
        _runNumber = "12345"
        mtd[cls.fullWs].mutableRun()["run_number"] = _runNumber
        for ws in cls.chunkWss:
            mtd[ws].mutableRun()["run_number"] = _runNumber

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
            ("OutputWorkspace", "StartTime", "EndTime", "Instrument", "PreserveEvents", "RunStatus")
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

    def test_exec_load_run_state_change(self):
        # Force a run-state change during the chunk-assembly loop via RunStatus.from_run:
        # * The initial chunk is RUNNING, one loop chunk is RUNNING, then the next is STOPPED.
        # * This causes the loop to break, resulting in an incomplete load.
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
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )

            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_isLogging.return_value = mock.sentinel.isLogging

            mock_loadIsComplete.return_value = False
            mock_sleep.side_effect = lambda _: None

            # Simulate a run-state change on the 3rd call to RunStatus.from_run:
            #   - Call 1 (initial chunk): RUNNING
            #   - Call 2 (loop iter 1): RUNNING
            #   - Call 3 (loop iter 2): STOPPED → break
            self._from_run_patcher.side_effect = [RunStatus.RUNNING, RunStatus.RUNNING, RunStatus.STOPPED]

            self.instance.execute()
            msg = mock_logger.warning.mock_calls[0].args[0]
            assert "The complete data interval could not be loaded" in msg

            # Three execute calls: 1 initial + 2 loop iterations.
            assert mock_LoadLiveData.execute.call_count == 3

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

    # ---------------------------------------------------------------------------
    # Tests for _checkIntervalOverlaps
    # ---------------------------------------------------------------------------

    def test__checkIntervalOverlaps_empty_list(self):
        t0 = np.datetime64("2010-01-01T00:00:00", "ns")
        t1 = np.datetime64("2010-01-01T01:00:00", "ns")
        assert not LoadLiveDataInterval._checkIntervalOverlaps((t0, t1), [])

    def test__checkIntervalOverlaps_no_overlap(self):
        # [t0, t1] and [t2, t3] are non-overlapping (t1 <= t2).
        t0 = np.datetime64("2010-01-01T00:00:00", "ns")
        t1 = np.datetime64("2010-01-01T01:00:00", "ns")
        t2 = np.datetime64("2010-01-01T01:00:00", "ns")
        t3 = np.datetime64("2010-01-01T02:00:00", "ns")
        assert not LoadLiveDataInterval._checkIntervalOverlaps((t0, t1), [(t2, t3)])

    def test__checkIntervalOverlaps_overlap(self):
        t0 = np.datetime64("2010-01-01T00:00:00", "ns")
        t1 = np.datetime64("2010-01-01T01:30:00", "ns")
        t2 = np.datetime64("2010-01-01T01:00:00", "ns")
        t3 = np.datetime64("2010-01-01T02:00:00", "ns")
        assert LoadLiveDataInterval._checkIntervalOverlaps((t0, t1), [(t2, t3)])

    def test__checkIntervalOverlaps_contained(self):
        # candidate is fully contained within an existing interval.
        t0 = np.datetime64("2010-01-01T00:30:00", "ns")
        t1 = np.datetime64("2010-01-01T00:45:00", "ns")
        t2 = np.datetime64("2010-01-01T00:00:00", "ns")
        t3 = np.datetime64("2010-01-01T01:00:00", "ns")
        assert LoadLiveDataInterval._checkIntervalOverlaps((t0, t1), [(t2, t3)])

    def test__checkIntervalOverlaps_multiple_no_overlap(self):
        t0 = np.datetime64("2010-01-01T02:00:00", "ns")
        t1 = np.datetime64("2010-01-01T03:00:00", "ns")
        existing = [
            (np.datetime64("2010-01-01T00:00:00", "ns"), np.datetime64("2010-01-01T01:00:00", "ns")),
            (np.datetime64("2010-01-01T01:00:00", "ns"), np.datetime64("2010-01-01T02:00:00", "ns")),
        ]
        assert not LoadLiveDataInterval._checkIntervalOverlaps((t0, t1), existing)

    def test__checkIntervalOverlaps_multiple_one_overlaps(self):
        t0 = np.datetime64("2010-01-01T01:30:00", "ns")
        t1 = np.datetime64("2010-01-01T02:30:00", "ns")
        existing = [
            (np.datetime64("2010-01-01T00:00:00", "ns"), np.datetime64("2010-01-01T01:00:00", "ns")),
            (np.datetime64("2010-01-01T02:00:00", "ns"), np.datetime64("2010-01-01T03:00:00", "ns")),
        ]
        assert LoadLiveDataInterval._checkIntervalOverlaps((t0, t1), existing)

    # ---------------------------------------------------------------------------
    # Tests for _fallbackChunkInterval
    # ---------------------------------------------------------------------------

    def _make_tsp_mock(self, name, start_iso, end_iso, size=1):
        """Helper: create a mock TimeSeriesProperty-like object."""
        from unittest.mock import MagicMock
        from mantid.kernel import FloatTimeSeriesProperty

        prop = mock.MagicMock(spec=FloatTimeSeriesProperty)
        prop.name = name
        prop.size.return_value = size
        prop.firstTime.return_value.to_datetime64.return_value = (
            np.datetime64(start_iso, "ns") if start_iso else None
        )
        prop.lastTime.return_value.to_datetime64.return_value = (
            np.datetime64(end_iso, "ns") if end_iso else None
        )
        return prop

    def test__fallbackChunkInterval_no_properties(self):
        ws = mock.Mock()
        ws.getRun.return_value.getProperties.return_value = []
        requiredStart = np.datetime64("2010-01-01T00:00:00", "ns")
        result = LoadLiveDataInterval._fallbackChunkInterval(ws, requiredStart, [])
        assert result is None

    def test__fallbackChunkInterval_empty_tsp_ignored(self):
        # A TSP with size() == 0 should be ignored (not called for firstTime/lastTime).
        from mantid.kernel import FloatTimeSeriesProperty

        prop = mock.MagicMock(spec=FloatTimeSeriesProperty)
        prop.size.return_value = 0

        ws = mock.Mock()
        ws.getRun.return_value.getProperties.return_value = [prop]
        requiredStart = np.datetime64("2010-01-01T00:00:00", "ns")
        result = LoadLiveDataInterval._fallbackChunkInterval(ws, requiredStart, [])
        assert result is None
        prop.firstTime.assert_not_called()

    def test__fallbackChunkInterval_non_tsp_ignored(self):
        # A non-TSP property (e.g., a plain mock without FloatTimeSeriesProperty spec) should be ignored.
        prop = mock.Mock()  # not spec'd to any TSP type
        prop.size.return_value = 5
        ws = mock.Mock()
        ws.getRun.return_value.getProperties.return_value = [prop]
        requiredStart = np.datetime64("2010-01-01T00:00:00", "ns")
        result = LoadLiveDataInterval._fallbackChunkInterval(ws, requiredStart, [])
        assert result is None

    def test__fallbackChunkInterval_before_required_start_ignored(self):
        # A TSP whose start_dt is before requiredStartTime should be ignored.
        prop = self._make_tsp_mock("log1", "2010-01-01T00:00:00", "2010-01-01T01:00:00")
        ws = mock.Mock()
        ws.getRun.return_value.getProperties.return_value = [prop]
        # requiredStart is after the TSP interval start.
        requiredStart = np.datetime64("2010-01-01T00:30:00", "ns")
        result = LoadLiveDataInterval._fallbackChunkInterval(ws, requiredStart, [])
        assert result is None

    def test__fallbackChunkInterval_overlapping_ignored(self):
        # A TSP that overlaps with an existing chunkInterval should be ignored.
        prop = self._make_tsp_mock("log1", "2010-01-01T00:00:00", "2010-01-01T01:00:00")
        ws = mock.Mock()
        ws.getRun.return_value.getProperties.return_value = [prop]
        requiredStart = np.datetime64("2010-01-01T00:00:00", "ns")
        existing = [
            (np.datetime64("2010-01-01T00:30:00", "ns"), np.datetime64("2010-01-01T01:30:00", "ns"))
        ]
        result = LoadLiveDataInterval._fallbackChunkInterval(ws, requiredStart, existing)
        assert result is None

    def test__fallbackChunkInterval_returns_largest_span(self):
        # Given two valid TSP candidates, the one with the larger span should be returned.
        prop_small = self._make_tsp_mock("small", "2010-01-01T00:00:00", "2010-01-01T00:30:00")
        prop_large = self._make_tsp_mock("large", "2010-01-01T00:00:00", "2010-01-01T01:00:00")
        ws = mock.Mock()
        ws.getRun.return_value.getProperties.return_value = [prop_small, prop_large]
        requiredStart = np.datetime64("2010-01-01T00:00:00", "ns")

        result = LoadLiveDataInterval._fallbackChunkInterval(ws, requiredStart, [])

        assert result == (
            np.datetime64("2010-01-01T00:00:00", "ns"),
            np.datetime64("2010-01-01T01:00:00", "ns"),
        )

    def test__fallbackChunkInterval_logs_warning_for_all_candidates(self):
        # Verify that a WARNING is logged listing all valid candidates.
        prop_a = self._make_tsp_mock("prop_a", "2010-01-01T00:00:00", "2010-01-01T00:30:00")
        prop_b = self._make_tsp_mock("prop_b", "2010-01-01T00:00:00", "2010-01-01T01:00:00")
        ws = mock.Mock()
        ws.getRun.return_value.getProperties.return_value = [prop_a, prop_b]
        requiredStart = np.datetime64("2010-01-01T00:00:00", "ns")

        with mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger:
            LoadLiveDataInterval._fallbackChunkInterval(ws, requiredStart, [])
            # At least one WARNING call should have been made listing both candidate names.
            warning_calls = mock_logger.warning.call_args_list
            assert len(warning_calls) >= 1
            combined = " ".join(str(c) for c in warning_calls)
            assert "prop_a" in combined
            assert "prop_b" in combined

    def test__fallbackChunkInterval_all_tsp_types_accepted(self):
        # Verify that all five TSP types are accepted.
        from mantid.kernel import (
            BoolTimeSeriesProperty,
            FloatTimeSeriesProperty,
            Int32TimeSeriesProperty,
            Int64TimeSeriesProperty,
            StringTimeSeriesProperty,
        )

        start = "2010-01-01T00:00:00"
        end = "2010-01-01T01:00:00"
        props = [
            mock.MagicMock(spec=FloatTimeSeriesProperty),
            mock.MagicMock(spec=BoolTimeSeriesProperty),
            mock.MagicMock(spec=Int32TimeSeriesProperty),
            mock.MagicMock(spec=Int64TimeSeriesProperty),
            mock.MagicMock(spec=StringTimeSeriesProperty),
        ]
        for i, p in enumerate(props):
            p.name = f"prop_{i}"
            p.size.return_value = 1
            p.firstTime.return_value.to_datetime64.return_value = np.datetime64(start, "ns")
            p.lastTime.return_value.to_datetime64.return_value = np.datetime64(end, "ns")

        ws = mock.Mock()
        ws.getRun.return_value.getProperties.return_value = props
        requiredStart = np.datetime64(start, "ns")

        result = LoadLiveDataInterval._fallbackChunkInterval(ws, requiredStart, [])
        # All five should be valid candidates; result should not be None.
        assert result is not None

    # ---------------------------------------------------------------------------
    # Tests for new PyExec behaviors
    # ---------------------------------------------------------------------------

    def test_exec_initial_chunk_no_events_fallback_found(self):
        # When the initial chunk has no events but a fallback interval is found,
        # the fallback interval should be appended to chunkIntervals.
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        fallback_interval = (
            np.datetime64("2010-01-01T00:00:00", "ns"),
            np.datetime64("2010-01-01T01:00:00", "ns"),
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging"),
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
            mock.patch.object(LoadLiveDataInterval, "_requiredLoadInterval") as mock_requiredLoadInterval,
            mock.patch.object(LoadLiveDataInterval, "_fallbackChunkInterval") as mock_fallback,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getNumberEvents.return_value = 0
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_requiredLoadInterval.return_value = (fallback_interval[0], fallback_interval[1])
            mock_fallback.return_value = fallback_interval
            mock_loadIsComplete.return_value = True

            self.instance.execute()

            mock_fallback.assert_called_once()
            # A warning about using the fallback should be logged.
            warning_msgs = " ".join(c.args[0] for c in mock_logger.warning.call_args_list)
            assert "fallback" in warning_msgs.lower() or "NO EVENTS" in warning_msgs

    def test_exec_initial_chunk_no_events_no_fallback_no_allow_dead_time_raises(self):
        # When the initial chunk has no events, no fallback is found, and
        # liveData.allowDeadTime is False (or missing), a RuntimeError should be raised.
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging"),
            mock.patch.object(LoadLiveDataInterval, "_requiredLoadInterval") as mock_requiredLoadInterval,
            mock.patch.object(LoadLiveDataInterval, "_fallbackChunkInterval") as mock_fallback,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getNumberEvents.return_value = 0
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_requiredLoadInterval.return_value = (
                np.datetime64("2010-01-01T00:00:00", "ns"),
                np.datetime64("2010-01-01T01:00:00", "ns"),
            )
            mock_fallback.return_value = None

            # allowDeadTime is False (default in test config) => should raise.
            with pytest.raises(RuntimeError, match="Initial chunk contained no events"):
                self.instance.PyExec()

    def test_exec_initial_chunk_no_events_no_fallback_allow_dead_time_continues(self):
        # When the initial chunk has no events, no fallback is found, but
        # liveData.allowDeadTime is True, the existing dead-time behavior should proceed.
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging"),
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
            mock.patch.object(LoadLiveDataInterval, "_requiredLoadInterval") as mock_requiredLoadInterval,
            mock.patch.object(LoadLiveDataInterval, "_fallbackChunkInterval") as mock_fallback,
            Config_override("liveData.allowDeadTime", True),
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData
            mock_sleep.side_effect = lambda _: None

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getNumberEvents.return_value = 0
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_requiredLoadInterval.return_value = (
                np.datetime64("2010-01-01T00:00:00", "ns"),
                np.datetime64("2010-01-01T01:00:00", "ns"),
            )
            mock_fallback.return_value = None
            mock_loadIsComplete.return_value = True

            # Should NOT raise; should complete (dead-time mode).
            self.instance.execute()

    def test_exec_loop_chunk_no_events_fallback_found(self):
        # In the loop, when a chunk has no events but a fallback is found,
        # the fallback should be appended and deadTimeDuration reset to 0.
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        fallback_interval = (
            np.datetime64("2010-01-01T00:00:00", "ns"),
            np.datetime64("2010-01-01T01:00:00", "ns"),
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging"),
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
            mock.patch.object(LoadLiveDataInterval, "_requiredLoadInterval") as mock_requiredLoadInterval,
            mock.patch.object(LoadLiveDataInterval, "_fallbackChunkInterval") as mock_fallback,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData
            mock_sleep.side_effect = lambda _: None

            # First call (initial chunk) has events; second call (loop chunk) has none.
            mock_chunkWs = mock.Mock()
            mock_chunkWs.getNumberEvents.side_effect = [1, 0]
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_requiredLoadInterval.return_value = (fallback_interval[0], fallback_interval[1])
            mock_fallback.return_value = fallback_interval
            # First call: don't complete. Second call: complete.
            mock_loadIsComplete.side_effect = [False, True]

            self.instance.execute()

            # The fallback should have been called (for the loop empty-chunk case).
            mock_fallback.assert_called()
            # A warning about using the fallback should be logged.
            warning_msgs = " ".join(c.args[0] for c in mock_logger.warning.call_args_list)
            assert "fallback" in warning_msgs.lower() or "NO NEW EVENTS" in warning_msgs

    def test_exec_loop_chunk_no_events_no_fallback_no_allow_dead_time_breaks(self):
        # In the loop, when a chunk has no events, no fallback is found, and
        # liveData.allowDeadTime is False, the loop should break gracefully.
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging"),
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
            mock.patch.object(LoadLiveDataInterval, "_requiredLoadInterval") as mock_requiredLoadInterval,
            mock.patch.object(LoadLiveDataInterval, "_fallbackChunkInterval") as mock_fallback,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData
            mock_sleep.side_effect = lambda _: None

            # Initial chunk has events; loop chunk has none.
            mock_chunkWs = mock.Mock()
            mock_chunkWs.getNumberEvents.side_effect = [1, 0]
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_requiredLoadInterval.return_value = (
                np.datetime64("2010-01-01T00:00:00", "ns"),
                np.datetime64("2010-01-01T01:00:00", "ns"),
            )
            mock_fallback.return_value = None
            # Never complete so we rely on the break.
            mock_loadIsComplete.return_value = False

            self.instance.execute()

            # LoadLiveData.execute should have been called exactly twice:
            #   once for the initial chunk, once in the loop before the break.
            assert mock_LoadLiveData.execute.call_count == 2

            # A graceful-exit warning should be logged.
            warning_msgs = " ".join(c.args[0] for c in mock_logger.warning.call_args_list)
            assert "gracefully" in warning_msgs.lower() or "NO NEW EVENTS" in warning_msgs

    def test_exec_loop_chunk_no_events_no_fallback_allow_dead_time_continues(self):
        # In the loop, when a chunk has no events, no fallback, and allowDeadTime is True,
        # the existing dead-time behavior should apply (increment and eventually break on maxDeadTime).
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "logger") as mock_logger,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging"),
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
            mock.patch.object(LoadLiveDataInterval, "_requiredLoadInterval") as mock_requiredLoadInterval,
            mock.patch.object(LoadLiveDataInterval, "_fallbackChunkInterval") as mock_fallback,
            Config_override("liveData.allowDeadTime", True),
            Config_override("liveData.chunkLoadWait", 3),
            Config_override("liveData.time_comparison_threshold", 6),  # => 2 empty chunks before break
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData
            mock_sleep.side_effect = lambda _: None

            # Initial chunk has events; all loop chunks have none.
            mock_chunkWs = mock.Mock()
            mock_chunkWs.getNumberEvents.side_effect = [1, 0, 0, 0, 0, 0]
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_requiredLoadInterval.return_value = (
                np.datetime64("2010-01-01T00:00:00", "ns"),
                np.datetime64("2010-01-01T01:00:00", "ns"),
            )
            mock_fallback.return_value = None
            mock_loadIsComplete.return_value = False

            self.instance.execute()

            # With chunkLoadWait=3 and maxDeadTime=6: after 2 empty loop chunks (6s) it should break.
            # So total execute calls = 1 (initial) + 2 (loop) = 3.
            assert mock_LoadLiveData.execute.call_count == 3

            # NO NEW EVENTS warnings should be logged.
            warning_msgs = " ".join(c.args[0] for c in mock_logger.warning.call_args_list)
            assert "NO NEW EVENTS" in warning_msgs

    def test_exec_initial_chunk_inactive_run_raises(self):
        # When the initial chunk has events but RunStatus.from_run returns non-RUNNING,
        # a RuntimeError should be raised.
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging"),
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getNumberEvents.return_value = 1  # has events → triggers from_run check
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            # Simulate an inactive run.
            self._from_run_patcher.return_value = RunStatus.STOPPED

            with pytest.raises(RuntimeError, match="cannot extract initial chunk from inactive run"):
                self.instance.PyExec()

    def test_exec_loop_chunk_inactive_run_breaks(self):
        # When a loop chunk's RunStatus.from_run returns non-RUNNING, the loop should break.
        mock_LoadLiveData = mock.Mock()

        self.instance.initialize()
        self._setProperties(
            self.instance,
            OutputWorkspace=self.outputWs,
            StartTime=self.startTime,
            Instrument=Config["instrument.name"],
            PreserveEvents=self.preserveEvents,
        )

        with (
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "ConfigService") as mock_ConfigService,
            mock.patch.object(inspect.getmodule(LoadLiveDataInterval), "sleep") as mock_sleep,
            mock.patch.object(self.instance, "createChildAlgorithm") as mock_createChildAlgorithm,
            mock.patch.object(self.instance, "mantidSnapper") as mock_snapper,
            mock.patch.object(self.instance, "isLogging"),
            mock.patch.object(LoadLiveDataInterval, "_loadIsComplete") as mock_loadIsComplete,
        ):
            mock_ConfigService.getFacility.return_value.instrument.return_value = mock.sentinel.InstrumentInfo
            mock_createChildAlgorithm.return_value = mock_LoadLiveData
            mock_sleep.side_effect = lambda _: None

            mock_chunkWs = mock.Mock()
            mock_chunkWs.getNumberEvents.return_value = 1  # always has events
            mock_chunkWs.getPulseTimeMin.return_value = DateAndTime(self.startTime)
            mock_chunkWs.getPulseTimeMax.return_value = DateAndTime(
                (datetime.datetime.fromisoformat(self.startTime) + timedelta(minutes=15)).isoformat()
            )
            mock_mtd = mock.MagicMock()
            mock_mtd.__getitem__.return_value = mock_chunkWs
            mock_mtd.doesExist.side_effect = lambda ws: ws == mock.sentinel.chunkWs
            mock_mtd.unique_hidden_name.return_value = mock.sentinel.chunkWs
            mock_snapper.mtd = mock_mtd

            mock_loadIsComplete.return_value = False

            # Initial chunk: RUNNING; first loop chunk: STOPPED → break.
            self._from_run_patcher.side_effect = [RunStatus.RUNNING, RunStatus.STOPPED]

            self.instance.execute()

            # Only 2 execute calls: 1 initial + 1 loop (then break on STOPPED).
            assert mock_LoadLiveData.execute.call_count == 2

