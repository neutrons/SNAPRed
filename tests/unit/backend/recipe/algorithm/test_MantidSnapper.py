import unittest
from unittest import mock

from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Callback import callback


class TestMantidSnapper(unittest.TestCase):
    def setUp(self):
        self.fakeOutput = mock.Mock()
        self.fakeOutput.name = "fakeOutput"
        self.fakeOutput.direction = Direction.Output

        self.fakeFunction = mock.Mock()
        self.fakeFunction.getProperties.return_value = [self.fakeOutput]
        self.fakeFunction.getProperty.return_value = self.fakeOutput

    @mock.patch("snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager")
    def test_snapper_fake_algo(self, mock_AlgorithmManager):
        mock_AlgorithmManager.create.return_value = self.fakeFunction
        return_of_algo = "return of algo"
        mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
        test = mantidSnapper.fakeFunction("test", fakeOutput=return_of_algo)
        assert str(test.__class__) == str(callback(return_of_algo.__class__).__class__)

    @mock.patch("snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager")
    def test_snapper_fake_queue(self, mock_AlgorithmManager):
        mock_AlgorithmManager.create.return_value = self.fakeFunction
        return_of_algo = "return of algo"
        mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
        mantidSnapper.fakeFunction("test", fakeOutput=return_of_algo)
        mantidSnapper.executeQueue()
        assert self.fakeFunction.execute.called
