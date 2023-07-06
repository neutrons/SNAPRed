import json
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import CreateWorkspace, mtd
    from snapred.backend.recipe.algorithm.SumWorkspaces import (
        SumWorkspaces,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    def test_set_properties():
        """Test that list can be initialized"""
        fakeList = ["fakeWS1", "fakeWS2", "fakeWS3"]
        algo = SumWorkspaces()
        algo.initialize()
        algo.setProperty("InputWorkspaces", fakeList)
        assert fakeList == list(algo.getProperty("InputWorkspaces").value)

    def test_empty_sum():
        """Test that an empty list results in nothing"""
        emptyList = []
        emptyWS = "emptyWS"
        algo = SumWorkspaces()
        algo.initialize()
        algo.setProperty("InputWorkspaces", emptyList)
        algo.setProperty("OutputWorkspace", emptyWS)
        assert algo.execute()
        out = mtd[emptyWS]
        assert out.extractX() == [0]
        assert out.extractY() == [0]


    def test_scalar_sum():
        """Test that a list of scalars sums correctly"""
        dataX = [0]
        wsList = ["ws1", "ws2", "ws3"]
        trueSum = 0
        for i,ws in enumerate(wsList):
            CreateWorkspace(OutputWorkspace=ws, DataX=dataX, DataY=[i])
            trueSum = trueSum + i
        algo = SumWorkspaces()
        algo.initialize()
        outName = "result"
        algo.setProperty("InputWorkspaces", wsList)
        algo.setProperty("OutputWorkspace", outName)
        algo.execute()
        out = mtd[outName]
        assert out.extractY() == [trueSum]

    def test_list_sum():
        """Test that a list of vectors sums correctly"""
        dataX = [0,1,2,3]
        wsList = ["ws1", "ws2", "ws3"]
        trueSum = [0]*len(dataX)
        for i,ws in enumerate(wsList):
            CreateWorkspace(OutputWorkspace=ws, DataX=dataX, DataY=[i+j for j in range(len(trueSum))])
            trueSum = [x + i+j for j,x in enumerate(trueSum)]
        algo = SumWorkspaces()
        algo.initialize()
        outName = "result"
        algo.setProperty("InputWorkspaces", wsList)
        algo.setProperty("OutputWorkspace", outName)
        algo.execute()
        out = mtd[outName]
        assert dataX == list(out.extractX().ravel())
        assert trueSum == list(out.extractY().ravel())