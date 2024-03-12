import socket
import unittest.mock as mock

import pytest
from mantid.simpleapi import AddSampleLog

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.ingredients import ReductionIngredients
    from snapred.backend.recipe.algorithm.ReductionAlgorithm import ReductionAlgorithm  # noqa: E402
    from snapred.meta.Config import Resource
    from util.diffraction_calibration_synthetic_data import SyntheticData
    from util.helpers import createCompatibleDiffCalTable

    IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

    def mock_reduction_ingredients():
        return ReductionIngredients.parse_raw(Resource.read("/inputs/reduction/input_ingredients.json"))

    def test_exec():
        """Test ability to initialize ReductionAlgorithm"""
        reductionIngredients = mock_reduction_ingredients()
        dataSynthesizer = SyntheticData()
        dataSynthesizer.generateWorkspaces("input_ws", "grouping_ws", "mask_ws")
        dataSynthesizer.generateWorkspaces("vanadium_ws", "grouping_ws", "mask)ws")
        createCompatibleDiffCalTable("caltable", "input_ws")
        reductionIngredients.pixelGroup = dataSynthesizer.ingredients.pixelGroup
        # have to add a proton charge in order to normalize
        AddSampleLog(
            Workspace="input_ws",
            LogName="gd_prtn_chrg",
            LogText="10.0",
            LogType="Number",
        )

        algo = ReductionAlgorithm()
        algo.initialize()
        algo.setProperty("InputWorkspace", "input_ws")
        algo.setProperty("VanadiumWorkspace", "vanadium_ws")
        algo.setProperty("GroupingWorkspace", "grouping_ws")
        algo.setProperty("MaskWorkspace", "mask_ws")
        algo.setProperty("CalibrationWorkspace", "caltable")
        algo.setProperty("Ingredients", reductionIngredients.json())
        assert algo.execute()

    def test_failing_exec():
        """Test failure to execute ReductionAlgorithm with bad inputs"""
        reductionIngredients = ReductionIngredients.parse_raw(Resource.read("/inputs/reduction/fake_file.json"))
        dataSynthesizer = SyntheticData()
        dataSynthesizer.generateWorkspaces("input_ws", "grouping_ws", "mask_ws")
        dataSynthesizer.generateWorkspaces("vanadium_ws", "grouping_ws", "mask)ws")
        createCompatibleDiffCalTable("caltable", "input_ws")
        # have to add a proton charge in order to normalize
        AddSampleLog(
            Workspace="input_ws",
            LogName="gd_prtn_chrg",
            LogText="10.0",
            LogType="Number",
        )

        algo = ReductionAlgorithm()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace", "input_ws")
        algo.setPropertyValue("VanadiumWorkspace", "vanadium_ws")
        algo.setPropertyValue("GroupingWorkspace", "grouping_ws")
        algo.setPropertyValue("MaskWorkspace", "mask_ws")
        algo.setPropertyValue("CalibrationWorkspace", "caltable")
        algo.setPropertyValue("Ingredients", reductionIngredients.json())
        with pytest.raises(Exception):  # noqa: PT011
            algo.execute()
