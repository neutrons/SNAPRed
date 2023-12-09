import socket
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.ingredients import ReductionIngredients
    from snapred.backend.dao.RunConfig import RunConfig
    from snapred.backend.recipe.algorithm.ReductionAlgorithm import ReductionAlgorithm  # noqa: E402
    from snapred.meta.Config import Resource

    IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

    def mock_reduction_ingredients():
        runConfig = RunConfig.parse_raw(f'{{"runNumber":"1", "IPTS":"{Resource.getPath("inputs/reduction/")}"}}')
        reductionIngredients = ReductionIngredients.parse_raw(Resource.read("/inputs/reduction/input_ingredients.json"))
        reductionState = reductionIngredients.reductionState
        reductionState.instrumentConfig.calibrationDirectory = Resource.getPath("inputs/reduction/")
        reductionState.instrumentConfig.pixelGroupingDirectory = Resource.getPath("inputs/reduction/")
        reductionState.instrumentConfig.sharedDirectory = Resource.getPath("inputs/reduction/")
        reductionState.instrumentConfig.nexusDirectory = Resource.getPath("inputs/reduction/")
        reductionState.instrumentConfig.reducedDataDirectory = Resource.getPath("inputs/reduction/")
        reductionState.instrumentConfig.reductionRecordDirectory = Resource.getPath("inputs/reduction/")
        reductionState.stateConfig.vanadiumFilePath = Resource.getPath("inputs/reduction/shared/lite/fake_vanadium.nxs")
        pixelGroup = reductionIngredients.pixelGroup
        return ReductionIngredients(runConfig=runConfig, reductionState=reductionState, pixelGroup=pixelGroup)

    def test_init():
        """Test ability to initialize ReductionAlgorithm"""
        reductionIngredients = mock_reduction_ingredients()
        algo = ReductionAlgorithm()
        algo.initialize()
        algo.setProperty("ReductionIngredients", reductionIngredients.json())

        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
        assert rawDataPath == Resource.getPath("inputs/reduction/shared/lite/SNAP_1.lite.nxs.h5")
        initReductionIngredients = algo.getProperty("ReductionIngredients").value
        assert initReductionIngredients == reductionIngredients.json()

    def test_init_with_file():
        """Test ability to initialize ReductionAlgorithm from a real file"""
        reductionIngredients = ReductionIngredients.parse_raw(Resource.read("/inputs/reduction/input_ingredients.json"))
        algo = ReductionAlgorithm()
        algo.initialize()
        algo.setProperty("ReductionIngredients", reductionIngredients.json())
        initReductionIngredients = algo.getProperty("ReductionIngredients").value
        assert initReductionIngredients == reductionIngredients.json()

    # @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    # def test_exec_with_file():
    #     """Test ability to execute ReductionAlgorithm with a real file"""
    #     reductionIngredients = ReductionIngredients.parse_raw(
    #    Resource.read("/inputs/reduction/input_ingredients.json"))
    #     algo = ReductionAlgorithm()
    #     algo.initialize()
    #     algo.setProperty("ReductionIngredients", reductionIngredients.json())
    #     assert algo.execute()

    def test_failing_exec():
        """Test failure to execute ReductionAlgorithm with bad inputs"""
        reductionIngredients = ReductionIngredients.parse_raw(Resource.read("/inputs/reduction/fake_file.json"))
        assert reductionIngredients.runConfig.runNumber == "nope"
        assert reductionIngredients.runConfig.IPTS == "nope"
        algo = ReductionAlgorithm()
        algo.initialize()
        algo.setProperty("ReductionIngredients", reductionIngredients.json())
        with pytest.raises(Exception):  # noqa: PT011
            algo.execute()
