import random
import unittest.mock as mock

import numpy as np

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import (
        CompareWorkspaces,
        CreateEmptyTableWorkspace,
        CreateWorkspace,
        mtd,
    )
    from snapred.backend.recipe.algorithm.ConvertDiffCalLog import (
        ConvertDiffCalLog as ThisAlgo,  # noqa: E402
    )

    def createDIFCTable(tableName, detids, difcs):
        CreateEmptyTableWorkspace(
            OutputWorkspace=tableName,
        )
        table = mtd[tableName]
        table.addColumn(type="int", name="detid", plottype=6)
        table.addColumn(type="double", name="difc", plottype=6)
        for detid, difc in zip(detids, difcs):
            table.addRow({"detid": detid, "difc": difc})

    def test_set_properties():
        """Test that properties can be initialized"""
        fakeOffsetWS = "fake offset ws"
        fakePrevCal = "fake previous calibration"
        fakeOutputWS = "fake output ws"
        fakeBinWidth = 0.002
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("OffsetsWorkspace", fakeOffsetWS)
        algo.setProperty("PreviousCalibration", fakePrevCal)
        algo.setProperty("OutputWorkspace", fakeOutputWS)
        algo.setProperty("BinWidth", fakeBinWidth)
        assert fakeOffsetWS == algo.getProperty("OffsetsWorkspace").value
        assert fakePrevCal == algo.getProperty("PreviousCalibration").value
        assert fakeOutputWS == algo.getProperty("OutputWorkspace").value
        assert fakeBinWidth == algo.getProperty("BinWidth").value

    def test_zero_bin():
        """Test that a zero bin width results in no change"""
        offsetWS = "offsets"
        prevCal = "previouscalibration"
        outputWS = "outputWS"
        dBin = 0.0
        lenTest = 10
        detids = range(lenTest)
        difcs = [1] * lenTest
        # create an offset workspace
        CreateWorkspace(OutputWorkspace=offsetWS, DataX=detids, DataY=[0.1 for i in range(lenTest)])
        # create a previous calibration workspace
        createDIFCTable(prevCal, detids, difcs)
        # run the algo
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("OffsetsWorkspace", offsetWS)
        algo.setProperty("PreviousCalibration", prevCal)
        algo.setProperty("OutputWorkspace", outputWS)
        algo.setProperty("BinWidth", dBin)
        assert algo.execute()
        assert CompareWorkspaces(Workspace1=prevCal, Workspace2=outputWS)

    def test_zero_offset():
        """Test that zero offsets result in no change"""
        offsetWS = "offsets"
        prevCal = "previouscalibration"
        outputWS = "outputWS"
        dBin = 1.0
        lenTest = 10
        dataX = range(lenTest)
        # create an offsets workspace
        CreateWorkspace(OutputWorkspace=offsetWS, DataX=dataX, DataY=[0.0 for i in range(lenTest)])
        # create a previous calibration workspace
        createDIFCTable(prevCal, dataX, [2] * lenTest)
        # run algo
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("OffsetsWorkspace", offsetWS)
        algo.setProperty("PreviousCalibration", prevCal)
        algo.setProperty("OutputWorkspace", outputWS)
        algo.setProperty("BinWidth", dBin)
        assert algo.execute()
        assert CompareWorkspaces(Workspace1=prevCal, Workspace2=outputWS)

    def test_by_half():
        """
        Test that offset of 1 and bin width of 1 results in
            DIFCnew = DIFCold * (1+dBin)^{-offset} = DIFCold * (1+1)^{-1} = 0.5*DIFCold
        """
        offsetWS = "offsets"
        prevCal = "previouscalibration"
        outputWS = "outputWS"
        testWS = "testresult"
        lenTest = 14
        dataX = range(lenTest)
        # create an offsets workspace
        CreateWorkspace(OutputWorkspace=offsetWS, DataX=dataX, DataY=[1.0 for i in range(lenTest)])
        # create previous calibration workspace as powers of 2 for easier division
        createDIFCTable(prevCal, dataX, [2 ** (i) for i in range(lenTest)])
        # create expected result workspace, one power of 2 smaller
        createDIFCTable(testWS, dataX, [2 ** (i - 1) for i in range(lenTest)])
        # run algo
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("OffsetsWorkspace", offsetWS)
        algo.setProperty("PreviousCalibration", prevCal)
        algo.setProperty("OutputWorkspace", outputWS)
        algo.setProperty("BinWidth", 1.0)
        assert algo.execute()
        assert CompareWorkspaces(Workspace1=testWS, Workspace2=outputWS)

    def test_random_values():
        """
        Test that the calculation works with random values
        """
        offsetWS = "offsets"
        prevCal = "previouscalibration"
        outputWS = "outputWS"
        testWS = "testresult"
        dBin = random.random()
        lenTest = 100
        dataX = range(lenTest)
        # create workspace of random offsets
        offsets = np.array([random.random() for i in range(lenTest)])
        CreateWorkspace(
            OutputWorkspace=offsetWS,
            DataX=dataX,
            DataY=offsets,
        )
        # create workspace of random previous calibrations
        dataYin = np.array([random.random() for i in range(lenTest)])
        createDIFCTable(prevCal, dataX, dataYin)
        # calculate expected values of calculation for comparison
        multFactor = np.power(np.ones(lenTest) + np.abs(dBin), -1 * offsets)
        dataYout = np.multiply(dataYin, multFactor)
        createDIFCTable(testWS, dataX, dataYout)
        # run algo
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("OffsetsWorkspace", offsetWS)
        algo.setProperty("PreviousCalibration", prevCal)
        algo.setProperty("OutputWorkspace", outputWS)
        algo.setProperty("BinWidth", dBin)
        assert algo.execute()
        assert CompareWorkspaces(Workspace1=testWS, Workspace2=outputWS)

    def test_dbin_abs_value():
        """
        Ensure that the calculation always uses absolute value of dBin
        by running the algorithm with a positive and negative dBin
        """
        offsetWS = "offsets"
        prevCal = "previouscalibration"
        outputWS1 = "outputWS1"
        outputWS2 = "outputWS2"
        lenTest = 10
        dBin = -0.5  # must pick dBin on order 1
        dataX = range(lenTest)
        # create an offsets workspace
        CreateWorkspace(
            OutputWorkspace=offsetWS,
            DataX=dataX,
            DataY=[1.0 for i in range(lenTest)],
        )
        # create a previous calibration workspace
        createDIFCTable(prevCal, dataX, [2**i for i in range(lenTest)])
        # run algo
        algo = ThisAlgo()
        algo.PyInit()  # use pyinit to appease codecov
        algo.setProperty("OffsetsWorkspace", offsetWS)
        algo.setProperty("PreviousCalibration", prevCal)
        # run once with positive (abs to ensure)
        algo.setProperty("OutputWorkspace", outputWS1)
        algo.setProperty("BinWidth", np.abs(dBin))
        algo.PyExec()  # use pyexec to appease codecov
        # run again with negative (-abs to ensure)
        algo.setProperty("OutputWorkspace", outputWS2)
        algo.setProperty("BinWidth", -np.abs(dBin))
        assert algo.execute()
        assert CompareWorkspaces(Workspace1=outputWS1, Workspace2=outputWS2)
