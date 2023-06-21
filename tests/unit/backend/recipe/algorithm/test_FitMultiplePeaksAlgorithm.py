import unittest.mock as mock
import os 
with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    import json

    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
    from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
    from snapred.meta.Config import Resource
    from mantid.simpleapi import LoadNexusProcessed
    from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import (
        FitMultiplePeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    def test_init():
        """Test ability to initialize purge overlapping peaks algo"""
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        crystalInfo = CrystallographicInfo.parse_raw(
            Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        wsName = 'testWS'
        fitIngredients = FitMultiplePeaksIngredients(InstrumentState=instrumentState,
                                                     CrystalInfo=crystalInfo,
                                                     InputWorkspace=wsName
                                                     )
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setProperty("FitMultiplePeaksIngredients", fitIngredients.json())
        assert fmpAlgo.getProperty("FitMultiplePeaksIngredients").value == fitIngredients.json()
        
    def test_execute():
        inputFile = os.path.join(Resource._resourcesPath, 'FitMultiplePeaksTestWS.nxs').replace('tests', 'src/snapred')
        tstWS = LoadNexusProcessed(Filename=inputFile,
                                   OutputWorkspace='testWS')
        instrumentState = Calibration.parse_raw(
            Resource.read("/inputs/purge_peaks/input_parameters.json")
        ).instrumentState
        crystalInfo = CrystallographicInfo.parse_raw(
            Resource.read("/inputs/purge_peaks/input_crystalInfo.json"))
        wsName = 'testWS'
        fitIngredients = FitMultiplePeaksIngredients(InstrumentState=instrumentState,
                                                     CrystalInfo=crystalInfo,
                                                     InputWorkspace=wsName
                                                     )
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setProperty("FitMultiplePeaksIngredients", fitIngredients.json())
        fmpAlgo.execute()
        wsGroup = fmpAlgo.getProperty("OutputWorkspaceGroup").value
        assert wsGroup == 'fitPeaksWSGroup'
