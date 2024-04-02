import os
import unittest.mock as mock
from typing import List

from pydantic import parse_raw_as

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import CreateSingleValuedWorkspace, CreateWorkspace, LoadNexusProcessed, mtd
    from snapred.backend.dao.GroupPeakList import GroupPeakList
    from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import (
        FitMultiplePeaksAlgorithm,  # noqa: E402
    )
    from snapred.meta.Config import Resource
    from snapred.meta.redantic import list_to_raw
    from util.SculleryBoy import SculleryBoy

    def test_init():
        """Test ability to initialize fit multiple peaks algo"""
        wsName = "testWS"
        CreateSingleValuedWorkspace(OutputWorkspace=wsName)
        peaks = SculleryBoy().prepDetectorPeaks({})
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setPropertyValue("InputWorkspace", wsName)
        fmpAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        assert fmpAlgo.getPropertyValue("InputWorkspace") == wsName
        assert parse_raw_as(List[GroupPeakList], fmpAlgo.getPropertyValue("DetectorPeaks")) == peaks

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
        peaks = SculleryBoy().prepDetectorPeaks({"good": ""})
        fmpAlgo = FitMultiplePeaksAlgorithm()
        fmpAlgo.initialize()
        fmpAlgo.setPropertyValue("InputWorkspace", wsName)
        fmpAlgo.setProperty("DetectorPeaks", list_to_raw(peaks))
        fmpAlgo.execute()
        wsGroupName = fmpAlgo.getProperty("OutputWorkspaceGroup").value
        assert wsGroupName == "fitPeaksWSGroup"
        wsGroup = list(mtd[wsGroupName].getNames())
        expected = [
            "fitPeaksWSGroup_peakpos",
            "fitPeaksWSGroup_fitparam",
            "fitPeaksWSGroup_fitted",
            "fitPeaksWSGroup_fiterror",
        ]
        assert wsGroup == expected
        assert not mtd.doesExist("fitPeaksWSGroup_fitted_1")
        assert not mtd.doesExist("fitPeaksWSGroup_fitparam_1")
