import os
import unittest.mock as mock
from typing import List

import pydantic

from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.meta.mantid.FitPeaksOutput import FIT_PEAK_DIAG_SUFFIX

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import CreateSingleValuedWorkspace, CreateWorkspace, LoadNexusProcessed, mtd
    from util.SculleryBoy import SculleryBoy

    from snapred.backend.dao.GroupPeakList import GroupPeakList
    from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import (
        FitMultiplePeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource
    from snapred.meta.redantic import list_to_raw

    def test_init():
        """Test ability to initialize fit multiple peaks algo"""
        wsName = "testWS"
        CreateSingleValuedWorkspace(OutputWorkspace=wsName)
        mockFarmFresh = mock.Mock(spec_set=FarmFreshIngredients)
        peaks = SculleryBoy().prepDetectorPeaks(mockFarmFresh)
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setPropertyValue("InputWorkspace", wsName)
        fmpAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        assert fmpAlgo.getPropertyValue("InputWorkspace") == wsName
        assert (
            pydantic.TypeAdapter(List[GroupPeakList]).validate_json(fmpAlgo.getPropertyValue("DetectorPeaks")) == peaks
        )

    def test_execute():
        inputFile = os.path.join(Resource._resourcesPath, "inputs", "fitMultPeaks", "FitMultiplePeaksTestWS.nxs")
        LoadNexusProcessed(Filename=inputFile, OutputWorkspace="testWS")
        wsName = "testWS"
        CreateWorkspace(
            OutputWorkspace=wsName,
            DataX=[1] * 6,
            DataY=[1] * 6,
            NSpec=6,
        )
        mockFarmFresh = mock.Mock(spec_set=FarmFreshIngredients)
        peaks = SculleryBoy().prepDetectorPeaks(mockFarmFresh)
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setPropertyValue("InputWorkspace", wsName)
        fmpAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        fmpAlgo.execute()
        wsGroupName = fmpAlgo.getProperty("OutputWorkspaceGroup").value
        assert wsGroupName == "fitPeaksWSGroup"
        wsGroup = list(mtd[wsGroupName].getNames())
        expected = [f"{wsGroupName}{suffix}" for suffix in FIT_PEAK_DIAG_SUFFIX.values()]
        assert wsGroup == expected
        assert not mtd.doesExist("fitPeaksWSGroup_fitted_1")
        assert not mtd.doesExist("fitPeaksWSGroup_fitparam_1")
