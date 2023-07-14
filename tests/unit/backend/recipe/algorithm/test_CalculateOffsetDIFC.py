import unittest.mock as mock

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients
    from snapred.backend.dao.RunConfig import RunConfig
    from snapred.backend.dao.state.FocusGroup import FocusGroup
    from snapred.backend.dao.state.InstrumentState import InstrumentState
    from snapred.backend.recipe.algorithm.CalculateOffsetDIFC import (
        CalculateOffsetDIFC as ThisAlgo,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    def set_spec(Class):
        return [k for k in Class.__annotations__.keys()]

    def mock_ingredients(runNumber, dBin):
        fakeRunConfig = RunConfig(runNumber=str(runNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/calibration/sampleInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 1567
        fakeInstrumentState.particleBounds.tof.maximum = 10186

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/calibration/sampleFocusGroup.json"))
        ntest = fakeFocusGroup.nHst
        fakeFocusGroup.dBin = [-abs(dBin)] * ntest
        fakeFocusGroup.dMax = [float(x) for x in range(100 * ntest, 101 * ntest)]
        fakeFocusGroup.dMin = [float(x) for x in range(ntest)]
        fakeFocusGroup.FWHM = [1] * ntest

        fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
        )

        return fakeIngredients

    def test_chop_ingredients():
        """Test that ingredients for algo are properly processed"""

        fakeDBin = abs(0.002)
        fakeRunNumber = "555"
        fakeIngredients = mock_ingredients(fakeRunNumber, fakeDBin)

        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(fakeIngredients)
        assert algo.runNumber == fakeRunNumber
        assert algo.TOFMin == fakeIngredients.instrumentState.particleBounds.tof.minimum
        assert algo.TOFMax == fakeIngredients.instrumentState.particleBounds.tof.maximum
        assert algo.overallDMin == max(fakeIngredients.focusGroup.dMin)
        assert algo.overallDMax == min(fakeIngredients.focusGroup.dMax)
        assert algo.dBin == -fakeDBin

    def test_init_properties():
        """Test that he properties of the algorithm can be initialized"""
        fakeDBin = abs(0.002)
        fakeRunNumber = "555"
        fakeIngredients = mock_ingredients(fakeRunNumber, fakeDBin)

        difcWS = f"_{fakeRunNumber}_difcs_test"

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("DiffractionCalibrationIngredients", fakeIngredients.json())
        algo.setProperty("CalibrationWorkspace", difcWS)
        assert algo.getProperty("DiffractionCalibrationIngredients").value == fakeIngredients.json()
        assert algo.getProperty("CalibrationWorkspace").value == difcWS

    # TODO: this test is not necessary, and is only here for:
    # 1. the principle that all methods should have independent tests
    # 2. demonstration of using CreateSampleWorkspace
    # 3. making codecov happier
    # feel free to remove
    def test_convert_and_rebin():
        """Test that units can be converted between TOF and d-spacing"""
        from mantid.simpleapi import (
            CompareWorkspaces,
            ConvertUnits,
            CreateSampleWorkspace,
            Rebin,
        )

        fakeDBin = -abs(0.002)
        fakeRunNumber = "555"
        fakeIngredients = mock_ingredients(fakeRunNumber, fakeDBin)

        wstof = "_test_tof_data"
        wsdsp = "_test_dsp_data"
        CreateSampleWorkspace(
            OutputWorkspace=wstof,
            WorkspaceType="Histogram",
            Function="Powder Diffraction",
            XUnit="TOF",
            InstrumentName="SNAP",
            NumBanks=1,
            BankPixelWidth=10,
        )

        # weak setup of algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(fakeIngredients)

        # try just rebinning the current workspace
        wstof_expected = "_test_tof_expected"
        Rebin(
            InputWorkspace=wstof,
            Params=f"{algo.TOFMin},{-abs(algo.TOFBin)},{algo.TOFMax}",
            OutputWorkspace=wstof_expected,
        )
        algo.convertUnitsAndRebin(wstof, wstof, target="TOF")
        algo.mantidSnapper.executeQueue()
        assert CompareWorkspaces(Workspace1=wstof, Workspace2=wstof_expected)

        # now try converting and rebinning workspace
        wsdsp_expected = "_test_dsp_expected"
        ConvertUnits(
            InputWorkspace=wstof_expected,
            OutputWorkspace=wsdsp_expected,
            Target="dSpacing",
        )
        Rebin(
            InputWorkspace=wsdsp_expected,
            Params=f"{algo.overallDMin},{-abs(algo.dBin)},{algo.overallDMax}",
            OutputWorkspace=wsdsp_expected,
        )
        algo.convertUnitsAndRebin(wstof, wsdsp)
        algo.mantidSnapper.executeQueue()
        assert CompareWorkspaces(Workspace1=wsdsp, Workspace2=wsdsp_expected)

    def test_reexecution_and_convergence():
        """Test that units can be converted between TOF and d-spacing"""
        from mantid.simpleapi import (
            CalculateDIFC,
            CreateSampleWorkspace,
            CreateWorkspace,
            LoadDetectorsGroupingFile,
            LoadInstrument,
            mtd,
        )

        fakeDBin = -abs(0.002)
        fakeRunNumber = "555"
        fakeIngredients = mock_ingredients(fakeRunNumber, fakeDBin)

        # weak setup of algorithm
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(fakeIngredients)
        algo.inputWStof = "_test_tof_data"
        algo.inputWSdsp = "_test_dsp_data"

        # prepare with test data
        CreateSampleWorkspace(
            OutputWorkspace=algo.inputWStof,
            WorkspaceType="Histogram",
            Function="Powder Diffraction",
            XUnit="TOF",
            Random=True,
        )
        algo.convertUnitsAndRebin(algo.inputWStof, algo.inputWStof)
        algo.convertUnitsAndRebin(algo.inputWStof, algo.inputWSdsp)

        # manually setup the grouping workspace
        algo.focusWSname = "_focusws_name_"
        CreateWorkspace(OutputWorkspace="idf", DataX=1, DataY=1)
        LoadInstrument(
            Workspace="idf",
            Filename=Resource.getPath("inputs/calibration/fakeSNAPLite.xml"),
            MonitorList="-2--1",
            RewriteSpectraMap=False,
        )
        LoadDetectorsGroupingFile(
            InputFile=Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml"),
            InputWorkspace="idf",
            OutputWorkspace=algo.focusWSname,
        )

        algo.focusWS = mtd[algo.focusWSname]
        assert "getGroupIDs" in dir(algo.focusWS)
        algo.groupIDs = algo.focusWS.getGroupIDs()

        # manually setup the initial DIFC workspace
        wsdifc = "_test_difc"
        CalculateDIFC(
            InputWorkspace=algo.inputWStof,
            OutputWorkspace=wsdifc,
        )

        # weak execution of algorithm
        data = algo.reexecute(wsdifc)
        assert data["medianOffset"] is not None
        assert data["medianOffset"] <= 2

        # TODO: all offsets are zero, so this test fails
        # find sample data where offsets *won't* all be zero
        # check that value converges
        # numIter = 5
        # for i in range(numIter):
        # previousOffset = data["medianOffset"]
        # data = algo.reexecute(wsdifc)
        # print(f"iteration {i} check {data['medianOffset']} < {previousOffset}")
        # assert data["medianOffset"] < previousOffset
