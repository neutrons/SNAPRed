import unittest.mock as mock

with mock.patch("mantid.api.AlgorithmManager") as MockAlgorithmManager:
    from mantid.kernel import Direction
    from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

    fakeOutput = mock.Mock()
    fakeOutput.name = "fakeOutput"
    fakeOutput.direction = Direction.Output

    fakeFunction = mock.Mock()
    fakeFunction.getProperties.return_value = {}
    fakeFunction.getProperty.return_value = fakeOutput
    MockAlgorithmManager.create.return_value = fakeFunction

    def test_snapper_fake_algo():
        return_of_algo = "return of algo"
        mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
        test = mantidSnapper.fakeFunction("test", fakeOutput=return_of_algo)
        assert test == return_of_algo

    def test_snapper_fake_queue():
        return_of_algo = "return of algo"
        mantidSnapper = MantidSnapper(parentAlgorithm=None, name="")
        mantidSnapper.fakeFunction("test", fakeOutput=return_of_algo)
        mantidSnapper.executeQueue()
        assert fakeFunction.execute.called


mock.patch.stopall()
