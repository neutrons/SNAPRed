import unittest
from unittest import mock

import pytest
from mantid.kernel import ConfigService, Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Callback import callback
from snapred.meta.Config import Config

PatchRoot: str = "snapred.backend.recipe.algorithm.MantidSnapper.{0}"

DEFAULT_FACILITY_KEY = "default.facility"
DEFAULT_INSTRUMENT_KEY = "default.instrument"

# A facility which is neither SNS nor HFIR, and which has no live-data listeners.
OTHER_FACILITY = "ILL"
OTHER_INSTRUMENT = "IN5"


class TestMantidSnapper(unittest.TestCase):
    def setUp(self):
        self.fakeOutput = mock.Mock()
        self.fakeOutput.name = "fakeOutput"
        self.fakeOutput.direction = Direction.Output

        self.fakeFunction = mock.Mock()
        self.fakeFunction.getProperties.return_value = [self.fakeOutput]
        self.fakeFunction.getProperty.return_value = self.fakeOutput

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_snapper_fake_algo(self, mock_AlgorithmManager):
        mock_AlgorithmManager.create.return_value = self.fakeFunction
        return_of_algo = "return of algo"
        mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
        test = mantidSnapper.fakeFunction("test", fakeOutput=return_of_algo)
        assert str(test.__class__) == str(callback(return_of_algo.__class__).__class__)

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_snapper_fake_queue(self, mock_AlgorithmManager):
        mock_AlgorithmManager.create.return_value = self.fakeFunction
        return_of_algo = "return of algo"
        mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
        mantidSnapper.fakeFunction("test", fakeOutput=return_of_algo)
        mantidSnapper.executeQueue()
        assert self.fakeFunction.execute.called

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_timeout(self, mockAlgorithmManager):
        mockAlgorithmManager.runningInstancesOf = mock.Mock(return_value=["theAlgoThatNeverEnds"])
        mockAlgorithmManager.create.return_value = self.fakeFunction

        with (
            mock.patch.object(MantidSnapper, "_timeout", 0.2),
            mock.patch.object(MantidSnapper, "_nonConcurrentAlgorithms", ["fakeFunction"]),
            mock.patch.object(MantidSnapper, "_nonConcurrentAlgorithmMutex", mock.Mock()),
        ):
            mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
            mantidSnapper.fakeFunction("test", fakeOutput="output")

            with pytest.raises(TimeoutError, match="Timeout occurred while waiting for instance of"):
                mantidSnapper.executeQueue()

            # Mutex must still be released even when TimeoutError is raised
            assert MantidSnapper._nonConcurrentAlgorithmMutex.release.called

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_timeout_nonReentrant_mutex_released(self, mockAlgorithmManager):
        """Verify non-reentrant mutex is released even when _waitForAlgorithmCompletion raises TimeoutError."""
        mockAlgorithmManager.runningInstancesOf = mock.Mock(return_value=["theAlgoThatNeverEnds"])
        mockAlgorithmManager.create.return_value = self.fakeFunction

        fakeMutex = mock.Mock()
        with (
            mock.patch.object(MantidSnapper, "_timeout", 0.2),
            mock.patch.object(MantidSnapper, "_nonReentrantAlgorithms", ("fakeFunction",)),
            mock.patch.object(MantidSnapper, "_nonReentrantMutexes", {"fakeFunction": fakeMutex}),
        ):
            mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
            mantidSnapper.fakeFunction("test", fakeOutput="output")

            with pytest.raises(TimeoutError, match="Timeout occurred while waiting for instance of"):
                mantidSnapper.executeQueue()

            # The mutex MUST be released even though TimeoutError was raised
            assert fakeMutex.acquire.called
            assert fakeMutex.release.called

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_timeout_concurrent(self, mockAlgorithmManager):
        mockAlgorithmManager.runningInstancesOf = mock.Mock(return_value=["theAlgoThatNeverEnds"])
        mockAlgorithmManager.create.return_value = self.fakeFunction

        with (
            mock.patch.object(MantidSnapper, "_timeout", 0.2),
            mock.patch.object(MantidSnapper, "_nonConcurrentAlgorithmMutex", mock.Mock()),
        ):
            mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
            mantidSnapper.fakeFunction("test", fakeOutput="output")

            mantidSnapper.executeQueue()
            assert not MantidSnapper._nonConcurrentAlgorithmMutex.acquire.called

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_mutexIsObtained_nonConcurrent(self, mockAlgorithmManager):
        mockAlgorithmManager.create.return_value = self.fakeFunction

        with (
            mock.patch.object(MantidSnapper, "_nonConcurrentAlgorithms", ["fakeFunction"]),
            mock.patch.object(MantidSnapper, "_nonConcurrentAlgorithmMutex", mock.Mock()),
        ):
            mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
            mantidSnapper.fakeFunction("test", fakeOutput="output")

            mantidSnapper.executeQueue()
            assert MantidSnapper._nonConcurrentAlgorithmMutex.acquire.called
            assert MantidSnapper._nonConcurrentAlgorithmMutex.release.called

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_mutexIsObtained_nonReentrant(self, mockAlgorithmManager):
        mockAlgorithmManager.create.return_value = self.fakeFunction
        with mock.patch.object(MantidSnapper, "_nonReentrantMutexes", {"fakeFunction": mock.Mock()}):
            mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
            mantidSnapper.fakeFunction("test", fakeOutput="output")

            mantidSnapper.executeQueue()
            assert MantidSnapper._nonReentrantMutexes["fakeFunction"].acquire.called
            assert MantidSnapper._nonReentrantMutexes["fakeFunction"].release.called


class TestMantidSnapperLiveDataFacility(unittest.TestCase):
    """`MantidSnapper` must make SNAPRed's facility the Mantid default while running live-data algorithms.

    Mantid's live-data algorithms validate their `Instrument` property against the *default facility*,
      and do so at algorithm *initialization* -- which `AlgorithmManager.create` performs.  So the
      facility must already be correct at the point of creation, not merely before `execute`.

    See `snapred.meta.mantid.LiveDataFacility` for the full explanation.
    """

    def setUp(self):
        self.fakeOutput = mock.Mock()
        self.fakeOutput.name = "fakeOutput"
        self.fakeOutput.direction = Direction.Output

        self.fakeAlgorithm = mock.Mock()
        self.fakeAlgorithm.getProperties.return_value = [self.fakeOutput]
        self.fakeAlgorithm.getProperty.return_value = self.fakeOutput

        self.mantidConfig = ConfigService.Instance()
        self._saved = {key: self.mantidConfig[key] for key in (DEFAULT_FACILITY_KEY, DEFAULT_INSTRUMENT_KEY)}
        self.mantidConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
        self.mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

        # Record the default facility and instrument as seen at each algorithm creation.
        self.observedAtCreate = []

    def tearDown(self):
        self.mantidConfig.setString(DEFAULT_FACILITY_KEY, self._saved[DEFAULT_FACILITY_KEY])
        self.mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, self._saved[DEFAULT_INSTRUMENT_KEY])

    def _recordingCreate(self, *_args, **_kwargs):
        self.observedAtCreate.append(
            (self.mantidConfig[DEFAULT_FACILITY_KEY], self.mantidConfig[DEFAULT_INSTRUMENT_KEY])
        )
        return self.fakeAlgorithm

    def _runAlgorithm(self, mockAlgorithmManager, name):
        mockAlgorithmManager.create.side_effect = self._recordingCreate
        mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
        getattr(mantidSnapper, name)("test", fakeOutput="output")
        mantidSnapper.executeQueue()

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_liveDataAlgorithmIsCreatedUnderTheSNAPRedFacility(self, mockAlgorithmManager):
        self._runAlgorithm(mockAlgorithmManager, "LoadLiveData")

        expected = (Config["liveData.facility.name"], Config["liveData.instrument.name"])
        # The final creation is the one inside `executeAlgorithm`: it must see SNAPRed's facility.
        assert self.observedAtCreate[-1] == expected

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_liveDataIntervalIsCreatedUnderTheSNAPRedFacility(self, mockAlgorithmManager):
        # `LoadLiveDataInterval` is a SNAPRed algorithm which creates a `LoadLiveData` child.
        self._runAlgorithm(mockAlgorithmManager, "LoadLiveDataInterval")

        expected = (Config["liveData.facility.name"], Config["liveData.instrument.name"])
        assert self.observedAtCreate[-1] == expected

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_facilityIsRestoredAfterALiveDataAlgorithm(self, mockAlgorithmManager):
        self._runAlgorithm(mockAlgorithmManager, "LoadLiveData")

        assert self.mantidConfig[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
        assert self.mantidConfig[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_facilityIsRestoredWhenALiveDataAlgorithmFails(self, mockAlgorithmManager):
        self.fakeAlgorithm.execute.return_value = False

        with pytest.raises(Exception, match="LoadLiveData"):
            self._runAlgorithm(mockAlgorithmManager, "LoadLiveData")

        assert self.mantidConfig[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
        assert self.mantidConfig[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT

    @mock.patch(PatchRoot.format("AlgorithmManager"))
    def test_nonLiveDataAlgorithmDoesNotChangeTheFacility(self, mockAlgorithmManager):
        self._runAlgorithm(mockAlgorithmManager, "LoadEventNexus")

        # The user's configuration must be untouched throughout.
        assert self.observedAtCreate[-1] == (OTHER_FACILITY, OTHER_INSTRUMENT)
        assert self.mantidConfig[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
        assert self.mantidConfig[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT
