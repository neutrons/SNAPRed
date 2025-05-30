import unittest
from unittest import mock

import pytest
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Callback import callback

PatchRoot: str = "snapred.backend.recipe.algorithm.MantidSnapper.{0}"


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
