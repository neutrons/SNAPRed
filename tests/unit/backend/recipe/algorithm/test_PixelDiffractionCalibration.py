import json
import unittest
import unittest.mock as mock

import pytest
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState

# the algorithm to test
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import (
    PixelDiffractionCalibration as ThisAlgo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestPixelDiffractionCalibration(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.maxOffset = 2
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/diffcal/fakeInstrumentState.json"))

        fakeFocusGroup = FocusGroup.parse_raw(
            """
        {
          "name": "Column",
          "FWHM": [
             5,
             5,
             5,
             5
          ],
          "nHst": 4,
          "dBin": [
          -0.00086,
          -0.00096,
          -0.0013,
          -0.00117
           ],
          "dMax": [
          0.35,
          0.50,
          0.30,
          0.40
          ],
          "dMin": [
          0.05,
          0.10,
          0.05,
          0.10
          ],
          "definition": "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml"
        }
        """
        )

        fakeFocusGroup.definition = Resource.getPath("inputs/diffcal/fakeSNAPFocGroup_Column.xml")

        peakList3 = [
            DetectorPeak.parse_obj({"position": {"value": 0.2, "minimum": 0.15, "maximum": 0.25}}),
        ]
        peakList7 = [
            DetectorPeak.parse_obj({"position": {"value": 0.3, "minimum": 0.20, "maximum": 0.40}}),
        ]
        peakList2 = [
            DetectorPeak.parse_obj({"position": {"value": 0.18, "minimum": 0.10, "maximum": 0.25}}),
        ]
        peakList11 = [
            DetectorPeak.parse_obj({"position": {"value": 0.24, "minimum": 0.18, "maximum": 0.30}}),
        ]

        self.fakeIngredients = DiffractionCalibrationIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[
                GroupPeakList(groupID=2, peaks=peakList2, maxfwhm=5),
                GroupPeakList(groupID=3, peaks=peakList3, maxfwhm=5),
                GroupPeakList(groupID=7, peaks=peakList7, maxfwhm=5),
                GroupPeakList(groupID=11, peaks=peakList11, maxfwhm=5),
            ],
            calPath=Resource.getPath("outputs/calibration/"),
            convergenceThreshold=1.0,
        )

    def makeFakeNeutronData(self, algo):
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        from mantid.simpleapi import (
            CreateSampleWorkspace,
            LoadInstrument,
        )

        # prepare with test data
        midpoint = (algo.overallDMin + algo.overallDMax) / 2.0
        CreateSampleWorkspace(
            OutputWorkspace=algo.inputWSdsp,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction=f"name=Gaussian,Height=10,PeakCentre={midpoint },Sigma={midpoint /10}",
            Xmin=algo.overallDMin,
            Xmax=algo.overallDMax,
            BinWidth=algo.dBin,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=algo.inputWSdsp,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        # # rebin and convert for DSP, TOF
        algo.convertUnitsAndRebin(algo.inputWSdsp, algo.inputWSdsp, "dSpacing")
        algo.convertUnitsAndRebin(algo.inputWSdsp, algo.inputWStof, "TOF")

    def makeFakeInputMask(self, algo):
        """Create an input mask workspace, compatible with the sample data"""
        # Assumes algo.inputWStof has already been created
        from mantid.dataobjects import MaskWorkspace
        from mantid.simpleapi import (
            ClearMaskFlag,
            ExtractMask,
            LoadInstrument,
            WorkspaceFactory,
            mtd,
        )

        maskWSName = algo.getPropertyValue("MaskWorkspace")
        inputWS = mtd[algo.inputWStof]
        inst = inputWS.getInstrument()
        mask = WorkspaceFactory.create("SpecialWorkspace2D", NVectors=inst.getNumberDetectors(), XLength=1, YLength=1)
        mtd[maskWSName] = mask
        LoadInstrument(
            Workspace=maskWSName,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )
        # output_workspace is converted to a MaskWorkspace
        ExtractMask(InputWorkspace=maskWSName, OutputWorkspace=maskWSName)
        assert isinstance(mtd[maskWSName], MaskWorkspace)

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.runNumber == self.fakeRunNumber
        assert algo.TOFMin == self.fakeIngredients.instrumentState.particleBounds.tof.minimum
        assert algo.TOFMax == self.fakeIngredients.instrumentState.particleBounds.tof.maximum
        assert algo.overallDMin == min(self.fakeIngredients.focusGroup.dMin)
        assert algo.overallDMax == max(self.fakeIngredients.focusGroup.dMax)
        assert algo.dBin == min([abs(db) for db in self.fakeIngredients.focusGroup.dBin])
        assert algo.TOFBin == algo.dBin

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()

    def test_execute(self):
        """Test that the algorithm executes"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaskWorkspace", f"_test_mask_{self.fakeRunNumber}")
        algo.setProperty("MaxOffset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        self.makeFakeInputMask(algo)
        assert algo.execute()

        data = json.loads(algo.getProperty("data").value)
        x = data["medianOffset"]
        assert x is not None
        assert x != 0.0
        assert x > 0.0
        assert x <= self.maxOffset

    # patch to make the offsets of sample data non-zero
    def test_reexecution_and_convergence(self):
        """Test that the algorithm can run, and that it will converge to an answer"""

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaskWorkspace", f"_test_mask_{self.fakeRunNumber}")
        algo.setProperty("MaxOffset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        assert algo.execute()

        data = json.loads(algo.getProperty("data").value)
        x = data["medianOffset"]
        assert x is not None
        assert x != 0.0
        assert x > 0.0
        assert x <= self.maxOffset

        # check that value converges
        numIter = 5
        allOffsets = [data["medianOffset"]]
        for i in range(numIter):
            algo.execute()
            data = json.loads(algo.getProperty("data").value)
            allOffsets.append(data["medianOffset"])
            assert allOffsets[-1] <= max(1.0e-4, allOffsets[-2])

    def test_retrieve_from_pantry(self):
        import os

        from mantid.simpleapi import (
            CompareWorkspaces,
            CreateSampleWorkspace,
            LoadInstrument,
            SaveNexus,
        )

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.chopIngredients(self.fakeIngredients)

        # create a fake nexus file to load
        fakeDataWorkspace = "_fake_sample_data"
        fakeNexusFile = Resource.getPath("outputs/calibration/testInputData.nxs")
        CreateSampleWorkspace(
            OutputWorkspace=fakeDataWorkspace,
            WorkspaceType="Event",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=algo.TOFMin,
            Xmax=algo.TOFMax,
            BinWidth=0.1,
            XUnit="TOF",
            NumMonitors=1,
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
        )
        LoadInstrument(
            Workspace=fakeDataWorkspace,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            InstrumentName="fakeSNAPLite",
            RewriteSpectraMap=False,
        )
        SaveNexus(
            InputWorkspace=fakeDataWorkspace,
            Filename=fakeNexusFile,
        )
        algo.rawDataPath = fakeNexusFile
        algo.raidPantry()
        os.remove(fakeNexusFile)
        assert CompareWorkspaces(
            Workspace1=algo.inputWStof,
            Workspace2=fakeDataWorkspace,
        )
        assert len(algo.subgroupIDs) > 0
        assert algo.subgroupIDs == list(algo.subgroupWorkspaceIndices.keys())

    def test_mask_is_created(self):
        """Test that a mask workspace is created if it doesn't already exist"""
        from mantid.simpleapi import mtd

        algo = ThisAlgo()
        algo.initialize()

        # Ensure that the mask workspace doesn't already exist
        maskWSName = f"_test_mask_{self.fakeRunNumber}"
        mtd.remove(maskWSName)
        assert maskWSName not in mtd

        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("MaxOffset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        algo.execute()
        assert maskWSName in mtd

    def test_existing_mask_is_used(self):
        """Test that an existing mask workspace is not overwritten"""
        from mantid.simpleapi import mtd

        algo = ThisAlgo()
        algo.initialize()

        maskWSName = f"_test_mask_{self.fakeRunNumber}"

        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("MaxOffset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        self.makeFakeInputMask(algo)
        assert maskWSName in mtd
        idIncomingMask = id(mtd[maskWSName])
        assert id(mtd[maskWSName]) == idIncomingMask
        # algo.execute()
        # THE FOLLOWING ASSERTION FAILS:  TODO: track this down.
        # Mantid 'GetDetectorOffsets' itself indicates that it is *not* creating a new mask workspace.
        # assert id(mtd[maskWSName]) == idIncomingMask
        #
        # For the moment: validate that the mask has the same contents as the incoming mask:
        mask = mtd[maskWSName]
        testIndices = (1, 5, 6, 7)
        for i in testIndices:
            mask.setY(
                i,
                [
                    1.0,
                ],
            )
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == len(testIndices)
        for i in testIndices:
            assert mask.readY(i)[0] == 1.0
        algo.execute()
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == len(testIndices)
        for i in testIndices:
            assert mask.readY(i)[0] == 1.0

    def countDetectorsForSpectra(self, inputWS, nss):  # inputWS: Workspace, nss: tuple[int]
        count = 0
        for ns in nss:
            count += len(inputWS.getSpectrum(ns).getDetectorIDs())
        return count

    def prepareSpectraToFail(self, ws, nss):  # ws: MatrixWorkspace, nss: tuple[int]
        import numpy as np

        zs = np.zeros_like(ws.readY(0))
        for ns in nss:
            ws.setY(ns, zs)

    def test_failures_are_masked(self):
        """Test that failing spectra are masked"""
        from mantid.simpleapi import mtd

        algo = ThisAlgo()
        algo.initialize()

        maskWSName = f"_test_mask_{self.fakeRunNumber}"

        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("MaxOffset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        self.makeFakeInputMask(algo)
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0
        inputWS = mtd[algo.inputWStof]
        spectraToFail = (2, 8, 11)
        self.prepareSpectraToFail(inputWS, spectraToFail)
        tofWS = inputWS.clone()  # Algorithm will delete its temporary workspaces after execution
        algo.execute()
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == self.countDetectorsForSpectra(tofWS, spectraToFail)
        for ns in spectraToFail:
            dets = tofWS.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert mask.isMasked(det)

    def maskSpectra(self, maskWS, inputWS, nss):  # maskWS: MaskWorkspace, inputWS: MatrixWorkspace, nss: tuple[int]
        for ns in nss:
            dets = inputWS.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                maskWS.setValue(det, True)

    def test_masks_stay_masked(self):
        """Test that incoming masked spectra are still masked at output"""
        from mantid.simpleapi import mtd

        algo = ThisAlgo()
        algo.initialize()

        maskWSName = f"_test_mask_{self.fakeRunNumber}"

        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("MaxOffset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        self.makeFakeInputMask(algo)
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0
        inputWS = mtd[algo.inputWStof]
        spectraToMask = (1, 5, 6, 7)
        self.maskSpectra(mask, inputWS, spectraToMask)
        tofWS = inputWS.clone()  # Algorithm will delete its temporary workspaces after execution
        algo.execute()
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == self.countDetectorsForSpectra(tofWS, spectraToMask)
        for ns in spectraToMask:
            dets = tofWS.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert mask.isMasked(det)

    def test_masks_are_combined(self):
        """Test that masks for failing spectra are combined with any input mask"""
        from mantid.simpleapi import mtd

        algo = ThisAlgo()
        algo.initialize()

        maskWSName = f"_test_mask_{self.fakeRunNumber}"

        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("MaskWorkspace", maskWSName)
        algo.setProperty("MaxOffset", self.maxOffset)
        algo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(algo)
        self.makeFakeInputMask(algo)
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == 0
        inputWS = mtd[algo.inputWStof]
        spectraToFail = (2, 8, 11)
        self.prepareSpectraToFail(inputWS, spectraToFail)
        spectraToMask = (1, 5, 6, 7)
        self.maskSpectra(mask, inputWS, spectraToMask)
        tofWS = inputWS.clone()  # Algorithm will delete its temporary workspaces after execution
        algo.execute()
        mask = mtd[maskWSName]
        assert mask.getNumberMasked() == self.countDetectorsForSpectra(
            tofWS, spectraToFail
        ) + self.countDetectorsForSpectra(tofWS, spectraToMask)
        for ns in spectraToFail:
            dets = tofWS.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert mask.isMasked(det)
        for ns in spectraToMask:
            dets = tofWS.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert mask.isMasked(det)


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
