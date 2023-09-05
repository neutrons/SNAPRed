import json
import random
import unittest
import unittest.mock as mock
from typing import Dict, List

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.ConvertDiffCalLog import ConvertDiffCalLog  # noqa
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

# the algorithm to test
from snapred.backend.recipe.algorithm.VanadiumRawCorrection import (
    VanadiumRawCorrection as Algo,  # noqa: E402
)
from snapred.meta.Config import Resource

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


class TestVanadiumRawCorrection(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        # fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/calibration/sampleInstrumentState.json"))
        # fakeInstrumentState.particleBounds.tof.minimum = 10
        # fakeInstrumentState.particleBounds.tof.maximum = 1000

        # fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/calibration/sampleFocusGroup.json"))
        # ntest = fakeFocusGroup.nHst
        # fakeFocusGroup.dBin = [-abs(self.fakeDBin)] * ntest
        # fakeFocusGroup.dMax = [float(x) for x in range(100 * ntest, 101 * ntest)]
        # fakeFocusGroup.dMin = [float(x) for x in range(ntest)]
        # fakeFocusGroup.FWHM = [5 * random.random() for x in range(ntest)]
        # fakeFocusGroup.definition = Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml")

        # peakList = [
        #     DetectorPeak.parse_obj({"position": {"value": 2, "minimum": 2, "maximum": 3}}),
        #     DetectorPeak.parse_obj({"position": {"value": 5, "minimum": 4, "maximum": 6}}),
        # ]

        self.fakeIngredients = Ingredients.parse_raw(Resource.read("/inputs/reduction/fake_file.json"))
        self.fakeIngredients.runConfig = fakeRunConfig
        pass

    def mockRaidPantry(algo, wsName, filename):
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        # prepare with test data
        algo.mantidSnapper.CreateSampleWorkspace(
            "Make fake data for testing",
            OutputWorkspace=wsName,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=algo.TOFPars[0],
            Xmax=algo.TOFPars[2],
            BinWidth=algo.TOFPars[1],
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        algo.mantidSnapper.Rebin(
            "Rebin in log TOF",
            InputWorkspace=wsName,
            Params=algo.TOFPars,
            PreserveEvents=False,
            OutputWorkspace=wsName,
            BinningMode="Logarithmic",
        )
        algo.mantidSnapper.executeQueue()
        pass

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = Algo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.liteMode == self.fakeIngredients.reductionState.stateConfig.isLiteMode
        assert algo.vanadiumRunNumber == self.fakeIngredients.runConfig.runNumber
        assert (
            algo.vanadiumBackgroundRunNumber == self.fakeIngredients.reductionState.stateConfig.emptyInstrumentRunNumber
        )
        assert algo.TOFPars[0] == self.fakeIngredients.reductionState.stateConfig.tofMin
        assert algo.TOFPars[1] == self.fakeIngredients.reductionState.stateConfig.tofBin
        assert algo.TOFPars[2] == self.fakeIngredients.reductionState.stateConfig.tofMax
        assert algo.geomCalibFile == self.fakeIngredients.reductionState.stateConfig.geometryCalibrationFileName
        assert algo.rawVFile == self.fakeIngredients.reductionState.stateConfig.rawVanadiumCorrectionFileName

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        goodOutputWSName = "_test_raw_vanadium_corr"

        algo = Algo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()
        assert algo.getProperty("OutputWorkspace").value == "vanadiumrawcorr_out"
        algo.setProperty("OutputWorkspace", goodOutputWSName)
        assert algo.getProperty("OutputWorkspace").value == goodOutputWSName

    @mock.patch.object(Algo, "raidPantry", mockRaidPantry)
    @mock.patch.object(Algo, "restockPantry", lambda self, wsName, filename: None)
    @mock.patch(TheAlgorithmManager)
    def test_execute(self, mockAlgorithmManager):
        # """Test that the algorithm executes"""

        class mockGetIPTS(PythonAlgorithm):
            def PyInit(self):
                self.declareProperty("RunNumber", defaultValue="", direction=Direction.Input)
                self.declareProperty("Instrument", defaultValue="", direction=Direction.Input)
                self.declareProperty("Directory", defaultValue="nope!", direction=Direction.Output)
                self.setRethrows(True)

            def PyExec(self):
                self.setProperty("Directory", "nope!")

        def mockAlgorithmCreate(algoName: str):
            from mantid.api import AlgorithmManager

            if algoName == "GetIPTS":
                algo = mockGetIPTS()
                algo.initialize()
                return algo
            else:
                return AlgorithmManager.create(algoName)

        mockAlgorithmManager.create = mockAlgorithmCreate

        algo = Algo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("OutputWorkspace", "_test_workspace_rar_vanadium")
        assert algo.execute()

    # patch to make the offsets of sample data non-zero
    @mock.patch.object(Algo, "raidPantry", mockRaidPantry)
    def test_reexecution_and_convergence(self):
        # """Test that the algorithm can run, and that it will converge to an answer"""

        # algo = Algo()
        # algo.initialize()
        # algo.setProperty("Ingredients", self.fakeIngredients.json())
        # assert algo.execute()
        # assert algo.reexecute()

        # data = json.loads(algo.getProperty("data").value)
        # assert data["meanOffset"] is not None
        # assert data["meanOffset"] != 0
        # assert data["meanOffset"] <= 2

        # # check that value converges
        # numIter = 5
        # allOffsets = [data["meanOffset"]]
        # for i in range(numIter):
        #     algo.reexecute()
        #     data = json.loads(algo.getProperty("data").value)
        #     allOffsets.append(data["meanOffset"])
        #     assert allOffsets[-1] <= allOffsets[-2]
        pass

    def test_retrieve_from_pantry(self):
        # import os

        # from mantid.simpleapi import (
        #     CompareWorkspaces,
        #     CreateSampleWorkspace,
        #     LoadInstrument,
        #     SaveNexus,
        # )

        # algo = Algo()
        # algo.initialize()
        # algo.setProperty("Ingredients", self.fakeIngredients.json())
        # algo.chopIngredients(self.fakeIngredients)

        # # create a fake nexus file to load
        # fakeDataWorkspace = "_fake_sample_data"
        # fakeNexusFile = Resource.getPath("outputs/calibration/testInputData.nxs")
        # CreateSampleWorkspace(
        #     OutputWorkspace=fakeDataWorkspace,
        #     WorkspaceType="Event",
        #     Function="User Defined",
        #     UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
        #     Xmin=algo.TOFMin,
        #     Xmax=algo.TOFMax,
        #     BinWidth=0.1,
        #     XUnit="TOF",
        #     NumMonitors=1,
        #     NumBanks=4,  # must produce same number of pixels as fake instrument
        #     BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        #     Random=True,
        # )
        # LoadInstrument(
        #     Workspace=fakeDataWorkspace,
        #     Filename=Resource.getPath("inputs/calibration/fakeSNAPLite.xml"),
        #     InstrumentName="fakeSNAPLite",
        #     RewriteSpectraMap=False,
        # )
        # SaveNexus(
        #     InputWorkspace=fakeDataWorkspace,
        #     Filename=fakeNexusFile,
        # )
        # algo.rawDataPath = fakeNexusFile
        # algo.retrieveFromPantry()
        # os.remove(fakeNexusFile)
        # assert CompareWorkspaces(
        #     Workspace1=algo.inputWStof,
        #     Workspace2=fakeDataWorkspace,
        # )
        # assert len(algo.groupIDs) > 0
        # assert algo.groupIDs == list(algo.subgroupWorkspaceIndices.keys())
        pass


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
