# NOTE that the remote tests depend on files on the analysis filesystem:
# * if those files are changed or moved, it will cause test failure;
# * GOLDEN DATA must be IDENTICAL between local and remote tests.

import socket
import unittest
from pathlib import Path
from typing import Dict, List

import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    LoadDetectorsGroupingFile,
    LoadDiffCal,
    LoadEmptyInstrument,
    RenameWorkspace,
    mtd,
)
from pydantic import parse_file_as, parse_raw_as
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
    PixelGroupingParametersCalculationAlgorithm as ThisAlgo,
)
from snapred.meta.Config import Resource
from snapred.meta.redantic import write_model_list_pretty
from util.helpers import (
    createCompatibleMask,
    maskComponentByName,
)

GENERATE_GOLDEN_DATA = False
GOLDEN_DATA_DATE = "2024-05-20"  # date.today().isoformat()

# Override to run "as if" on analysis machine (but don't require access to SNS filesystem):
REMOTE_OVERRIDE = False
IS_ON_ANALYSIS_MACHINE = REMOTE_OVERRIDE or socket.gethostname().startswith("analysis")


class PixelGroupCalculation(unittest.TestCase):
    @classmethod
    def getInstrumentState(cls):
        calibrationPath = Resource.getPath("inputs/pixel_grouping/CalibrationParameters.json")
        return parse_file_as(Calibration, calibrationPath).instrumentState

    @classmethod
    def getInstrumentDefinitionFilePath(cls, isLiteInstrument):
        if IS_ON_ANALYSIS_MACHINE and not REMOTE_OVERRIDE:
            if isLiteInstrument:
                return "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
            else:
                return "/opt/anaconda/envs/mantid-dev/instrument/SNAP_Definition.xml"
        else:
            if isLiteInstrument:
                return Resource.getPath("inputs/pixel_grouping/SNAPLite_Definition.xml")
            else:
                return Resource.getPath("inputs/pixel_grouping/SNAP_Definition.xml")

    @classmethod
    def loadGroupingFile(cls, groupingWSName, groupingFilePath, instrumentWSName):
        if Path(groupingFilePath).suffix.upper() == ".HDF":
            LoadDiffCal(
                Filename=groupingFilePath,
                MakeGroupingWorkspace=True,
                MakeCalWorkspace=False,
                MakeMaskWorkspace=False,
                WorkspaceName=groupingWSName,
                InputWorkspace=instrumentWSName,
            )
            # Remove the "_group" suffix, which is added by LoadDiffCal to the output workspace name
            RenameWorkspace(
                "Renaming grouping workspace...",
                InputWorkspace=groupingWSName + "_group",
                OutputWorkspace=groupingWSName,
            )
        else:
            if Path(groupingFilePath).suffix.upper() != ".XML":
                raise RuntimeError(
                    f"not implemented: loading grouping file with extension {Path(groupingFilePath).suffix[1:]}"
                )
            LoadDetectorsGroupingFile(
                InputFile=groupingFilePath,
                InputWorkspace=instrumentWSName,
                OutputWorkspace=groupingWSName,
            )

    @classmethod
    def setUpClass(cls):
        cls.groceryService = GroceryService()

        # closest thing to an enum in python
        cls.isLite, cls.isFull, cls.isTest = (0, 1, 2)
        cls.all, cls.bank, cls.column, cls.natural = (3, 4, 5, 6)
        cls.unmasked, cls.westMasked, cls.eastMasked = (7, 8, 9)

        # state corresponding to local test instrument
        cls.localInstrumentState = InstrumentState.parse_raw(
            Resource.read("inputs/calibration/sampleInstrumentState.json")
        )
        cls.localIngredients = PixelGroupingIngredients(
            instrumentState=cls.localInstrumentState,
        )

        # the local test instrument file
        cls.localInstrumentFilename = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        # local grouping definition files
        cls.localGroupingFilename: Dict[int, str] = {
            cls.column: Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Column.xml"),
            cls.natural: Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
        }

        # a workspace holding the empty instrument
        cls.localInstrumentWorkspace: str = "test_instrument_idf"
        LoadEmptyInstrument(
            OutputWorkspace=cls.localInstrumentWorkspace,
            Filename=cls.localInstrumentFilename,
        )
        # Instrument workspace _not_ loaded by the grocery service => update its mutable instrument parameters.
        cls.groceryService.updateInstrumentParameters(
            cls.localInstrumentWorkspace, cls.localInstrumentState.detectorState
        )

        # grouping workspaces on local instrument
        cls.localGroupingWorkspace: Dict[int, str] = {
            cls.column: "test_grouping_workspace_test_column",
            cls.natural: "test_grouping_workspace_test_natural",
        }
        for x in [cls.column, cls.natural]:
            LoadDetectorsGroupingFile(
                InputFile=cls.localGroupingFilename[x],
                InputWorkspace=cls.localInstrumentWorkspace,
                OutputWorkspace=cls.localGroupingWorkspace[x],
            )

        # prepare mask workspaces with local instrument
        cls.localMaskWorkspace: Dict[int, str] = {
            cls.unmasked: "test_mask_unmasked",
            cls.westMasked: "test_mask_west",
            cls.eastMasked: "test_mask_east",
        }
        for x in [cls.unmasked, cls.westMasked, cls.eastMasked]:
            maskWSName = cls.localMaskWorkspace[x]
            createCompatibleMask(
                maskWSName=maskWSName,
                templateWSName=cls.localInstrumentWorkspace,
                instrumentFilePath=cls.localInstrumentFilename,
            )

            match x:
                case cls.eastMasked:
                    maskComponentByName(maskWSName=maskWSName, componentName="East")
                case cls.westMasked:
                    maskComponentByName(maskWSName=maskWSName, componentName="West")

        if IS_ON_ANALYSIS_MACHINE:
            # Instrument state is the same for both SNAP and SNAPLite instruments:
            cls.SNAPInstrumentState = cls.getInstrumentState()

            # load the SNAP instrument
            cls.SNAPInstrumentFilename = cls.getInstrumentDefinitionFilePath(isLiteInstrument=False)
            cls.SNAPInstrumentWorkspace = "SNAP_instrument_idf"
            LoadEmptyInstrument(
                OutputWorkspace=cls.SNAPInstrumentWorkspace,
                Filename=cls.SNAPInstrumentFilename,
            )
            # Instrument workspace _not_ loaded by the grocery service => update its mutable instrument parameters.
            cls.groceryService.updateInstrumentParameters(
                cls.SNAPInstrumentWorkspace, cls.SNAPInstrumentState.detectorState
            )

            # load the SNAPLite instrument
            cls.SNAPLiteInstrumentFilename = cls.getInstrumentDefinitionFilePath(isLiteInstrument=True)
            cls.SNAPLiteInstrumentWorkspace = "SNAPLite_instrument_idf"
            LoadEmptyInstrument(
                OutputWorkspace=cls.SNAPLiteInstrumentWorkspace,
                Filename=cls.SNAPLiteInstrumentFilename,
            )
            # Instrument workspace _not_ loaded by the grocery service => update its mutable instrument parameters.
            cls.groceryService.updateInstrumentParameters(
                cls.SNAPLiteInstrumentWorkspace, cls.SNAPInstrumentState.detectorState
            )

            # prepare loading of SNAP grouping workspaces
            cls.SNAPGroupingWorkspace = {
                cls.all: "test_grouping_workspace_SNAP_all",
                cls.bank: "test_grouping_workspace_SNAP_bank",
                cls.column: "test_grouping_workspace_SNAP_column",
            }

            if not REMOTE_OVERRIDE:
                pixelGroupPath = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/"
                cls.SNAPGroupingFilename = {
                    cls.all: f"{pixelGroupPath}SNAPFocGroup_All.xml",
                    cls.bank: f"{pixelGroupPath}SNAPFocGroup_Bank.xml",
                    cls.column: f"{pixelGroupPath}SNAPFocGroup_Column.xml",
                }
            else:
                pixelGroupPath = Resource.getPath("inputs/pixel_grouping/")
                cls.SNAPGroupingFilename = {
                    cls.all: f"{pixelGroupPath}SNAPFocGroup_All.hdf",
                    cls.bank: f"{pixelGroupPath}SNAPFocGroup_Bank.hdf",
                    cls.column: f"{pixelGroupPath}SNAPFocGroup_Column.hdf",
                }

            # prepare mask workspaces with SNAP instrument
            cls.SNAPMaskWorkspace: Dict[int, str] = {
                cls.unmasked: "test_mask_SNAP_unmasked",
                cls.westMasked: "test_mask_SNAP_west",
                cls.eastMasked: "test_mask_SNAP_east",
            }
            for x in [cls.unmasked, cls.westMasked, cls.eastMasked]:
                maskWSName = cls.SNAPMaskWorkspace[x]
                createCompatibleMask(
                    maskWSName=maskWSName,
                    templateWSName=cls.SNAPInstrumentWorkspace,
                    instrumentFilePath=cls.SNAPInstrumentFilename,
                )
                match x:
                    case cls.westMasked:
                        maskComponentByName(maskWSName=maskWSName, componentName="West")
                    case cls.eastMasked:
                        maskComponentByName(maskWSName=maskWSName, componentName="East")

            # prepare loading of SNAPLite grouping workspaces
            cls.SNAPLiteGroupingWorkspace = {
                cls.all: "test_grouping_workspace_SNAPLite_all",
                cls.bank: "test_grouping_workspace_SNAPLite_bank",
                cls.column: "test_grouping_workspace_SNAPLite_column",
            }
            cls.SNAPLiteGroupingFilename = {
                cls.all: f"{pixelGroupPath}SNAPFocGroup_All.lite.hdf",
                cls.bank: f"{pixelGroupPath}SNAPFocGroup_Bank.lite.hdf",
                cls.column: f"{pixelGroupPath}SNAPFocGroup_Column.lite.hdf",
            }

            # prepare mask workspaces with SNAPLite instrument
            cls.SNAPLiteMaskWorkspace: Dict[int, str] = {
                cls.unmasked: "test_mask_SNAPLite_unmasked",
                cls.westMasked: "test_mask_SNAPLite_west",
                cls.eastMasked: "test_mask_SNAPLite_east",
            }
            for x in [cls.unmasked, cls.westMasked, cls.eastMasked]:
                maskWSName = cls.SNAPLiteMaskWorkspace[x]
                createCompatibleMask(
                    maskWSName=maskWSName,
                    templateWSName=cls.SNAPLiteInstrumentWorkspace,
                    instrumentFilePath=cls.SNAPLiteInstrumentFilename,
                )
                match x:
                    case cls.westMasked:
                        maskComponentByName(maskWSName=maskWSName, componentName="West")
                    case cls.eastMasked:
                        maskComponentByName(maskWSName=maskWSName, componentName="East")

        else:
            # the SNAPLite tests are fast enough to run locally

            # Instrument state is the same for both SNAP and SNAPLite instruments:
            cls.SNAPInstrumentState = cls.getInstrumentState()

            # load the SNAPLite instrument
            cls.SNAPLiteInstrumentFilename = cls.getInstrumentDefinitionFilePath(isLiteInstrument=True)
            cls.SNAPLiteInstrumentWorkspace = "SNAPLite_intstrument_idf"
            LoadEmptyInstrument(
                OutputWorkspace=cls.SNAPLiteInstrumentWorkspace,
                Filename=cls.SNAPLiteInstrumentFilename,
            )
            # Instrument workspace not loaded by the grocery service => update its mutable instrument parameters.
            cls.groceryService.updateInstrumentParameters(
                cls.SNAPLiteInstrumentWorkspace, cls.SNAPInstrumentState.detectorState
            )

            cls.SNAPLiteGroupingWorkspace = {
                cls.all: "test_grouping_workspace_SNAPLite_all",
                cls.bank: "test_grouping_workspace_SNAPLite_bank",
                cls.column: "test_grouping_workspace_SNAPLite_column",
            }

            cls.SNAPLiteGroupingFilename: Dict[int, str] = {
                cls.all: Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_All.lite.hdf"),
                cls.bank: Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Bank.lite.hdf"),
                cls.column: Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.lite.hdf"),
            }

            for x in [cls.all, cls.bank, cls.column]:
                cls.loadGroupingFile(
                    cls.SNAPLiteGroupingWorkspace[x],
                    cls.SNAPLiteGroupingFilename[x],
                    cls.SNAPLiteInstrumentWorkspace,
                )

            # prepare mask workspaces with SNAPLite instrument
            cls.SNAPLiteMaskWorkspace: Dict[int, str] = {
                cls.unmasked: "test_mask_SNAPLite_unmasked",
                cls.westMasked: "test_mask_SNAPLite_west",
                cls.eastMasked: "test_mask_SNAPLite_east",
            }
            for x in [cls.unmasked, cls.westMasked, cls.eastMasked]:
                maskWSName = cls.SNAPLiteMaskWorkspace[x]
                createCompatibleMask(
                    maskWSName=maskWSName,
                    templateWSName=cls.SNAPLiteInstrumentWorkspace,
                    instrumentFilePath=cls.SNAPLiteInstrumentFilename,
                )
                match x:
                    case cls.westMasked:
                        maskComponentByName(maskWSName=maskWSName, componentName="West")
                    case cls.eastMasked:
                        maskComponentByName(maskWSName=maskWSName, componentName="East")

        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        # remove all workspaces
        for workspace in mtd.getObjectNames():
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")
        super().tearDownClass()

    ### BEGIN LOCAL TESTS OF PIXEL GROUPING CALCULATION

    def test_chop_ingredients(self):
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(self.localIngredients)
        notNones = ["tofMin", "tofMax", "delL", "deltaTOverT", "delTheta"]
        for notNone in notNones:
            assert getattr(algo, notNone) is not None
        assert algo.tofMin == self.localInstrumentState.particleBounds.tof.minimum
        assert algo.tofMax == self.localInstrumentState.particleBounds.tof.maximum
        assert algo.deltaTOverT == self.localInstrumentState.instrumentConfig.delTOverT
        assert algo.delLOverL == self.localInstrumentState.instrumentConfig.delLOverL
        assert algo.L == self.localInstrumentState.instrumentConfig.L1 + self.localInstrumentState.instrumentConfig.L2
        assert algo.delL == algo.L * algo.delLOverL
        assert algo.delTheta == self.localInstrumentState.instrumentConfig.delThWithGuide

    def run_test(self, instrumentState, groupingWorkspace, maskWorkspace, referenceParametersFile):
        pixelGroupingParams_calc = self.createPixelGroupingParameters(instrumentState, groupingWorkspace, maskWorkspace)
        self.compareToReference(pixelGroupingParams_calc, referenceParametersFile)

    def createPixelGroupingParameters(self, instrumentState, groupingWorkspace, maskWorkspace):
        """Test execution of PixelGroupingParametersCalculationAlgorithm"""

        ingredients = PixelGroupingIngredients(
            instrumentState=instrumentState,
        )

        # run the algorithm
        pixelGroupingAlgo = ThisAlgo()
        pixelGroupingAlgo.initialize()
        pixelGroupingAlgo.setProperty("Ingredients", ingredients.json())
        pixelGroupingAlgo.setProperty("GroupingWorkspace", groupingWorkspace)
        pixelGroupingAlgo.setProperty("MaskWorkspace", maskWorkspace)
        assert pixelGroupingAlgo.execute()

        # parse the algorithm output and create a list of PixelGroupingParameters
        pixelGroupingParams_calc = parse_raw_as(
            List[PixelGroupingParameters],
            pixelGroupingAlgo.getProperty("OutputParameters").value,
        )

        groupWS = mtd[groupingWorkspace]
        groupIDs = groupWS.getGroupIDs()
        for p in pixelGroupingParams_calc:
            assert isinstance(p, PixelGroupingParameters)
        assert len(pixelGroupingParams_calc) == len(groupIDs)
        for i in range(len(groupIDs)):
            assert groupIDs[i] == pixelGroupingParams_calc[i].groupID

        return pixelGroupingParams_calc

    def compareToReference(self, actual: List[PixelGroupingParameters], referenceParametersFile: str):
        if Path(referenceParametersFile).exists():
            # parse the reference file.
            with open(referenceParametersFile) as f:
                expected: List[PixelGroupingParameters] = parse_raw_as(
                    List[PixelGroupingParameters],
                    f.read(),
                )

            # compare calculated parameters with golden data
            assert len(expected) == len(actual)

            for index, param in enumerate(expected):
                assert param.isMasked is actual[index].isMasked
                assert pytest.approx(param.L2, 1.0e-6) == actual[index].L2
                assert pytest.approx(param.twoTheta, 1.0e-6) == actual[index].twoTheta
                assert pytest.approx(param.azimuth, 1.0e-6) == actual[index].azimuth
                assert pytest.approx(param.dResolution.minimum, 1.0e-6) == actual[index].dResolution.minimum
                assert pytest.approx(param.dResolution.maximum, 1.0e-6) == actual[index].dResolution.maximum
                assert pytest.approx(param.dRelativeResolution, 1.0e-6) == actual[index].dRelativeResolution

        elif GENERATE_GOLDEN_DATA:
            # GENERATE new GOLDEN data:
            write_model_list_pretty(actual, referenceParametersFile)

    # LOCAL TESTS ON TEST INSTRUMENT

    def test_local_testInstrument_column(self):
        groupingScheme = self.column
        pgp = self.createPixelGroupingParameters(
            instrumentState=self.localInstrumentState,
            groupingWorkspace=self.localGroupingWorkspace[groupingScheme],
            maskWorkspace=self.localMaskWorkspace[self.unmasked],
        )
        assert len(pgp) > 0

    def test_local_testInstrument_natural(self):
        groupingScheme = self.natural
        pgp = self.createPixelGroupingParameters(
            instrumentState=self.localInstrumentState,
            groupingWorkspace=self.localGroupingWorkspace[groupingScheme],
            maskWorkspace=self.localMaskWorkspace[self.unmasked],
        )
        assert len(pgp) > 0

    # LOCAL TESTS ON SNAPLITE

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_column(self):
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.lite.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_bank(self):
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.lite.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_all(self):
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.lite.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_column_west(self):
        # Test masking all detectors contributing to "west" component[s]
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.lite.west.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_bank_west(self):
        # Test masking all detectors contributing to "west" component[s]
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.lite.west.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_all_west(self):
        # Test masking all detectors contributing to "west" component[s]
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.lite.west.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_column_east(self):
        # Test masking all detectors contributing to "east" component[s]
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.lite.east.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_bank_east(self):
        # Test masking all detectors contributing to "east" component[s]
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.lite.east.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_all_east(self):
        # Test masking all detectors contributing to "east" component[s]
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.lite.east.{GOLDEN_DATA_DATE}.json"
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )

    # LOCAL NEGATIVE TESTS

    # TODO

    # BEGIN REMOTE TESTS OF PIXEL GROUPING CALCULATION

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_all_full(self):
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_bank_full(self):
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_column_full(self):
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_all_full_west(self):
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.west.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_bank_full_west(self):
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.west.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_column_full_west(self):
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.west.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_all_full_east(self):
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.east.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_bank_full_east(self):
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.east.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_column_full_east(self):
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.east.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPGroupingWorkspace[groupingScheme],
            self.SNAPGroupingFilename[groupingScheme],
            self.SNAPInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_all_lite(self):
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.lite.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_bank_lite(self):
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.lite.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_column_lite(self):
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.lite.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.unmasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_all_lite_west(self):
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.lite.west.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_bank_lite_west(self):
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.lite.west.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_column_lite_west(self):
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.lite.west.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.westMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_all_lite_east(self):
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/All_parameters.lite.east.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_bank_lite_east(self):
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Bank_parameters.lite.east.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_column_lite_east(self):
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath(
            f"outputs/pixel_grouping/golden_data/Column_parameters.lite.east.{GOLDEN_DATA_DATE}.json"
        )

        self.loadGroupingFile(
            self.SNAPLiteGroupingWorkspace[groupingScheme],
            self.SNAPLiteGroupingFilename[groupingScheme],
            self.SNAPLiteInstrumentWorkspace,
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            maskWorkspace=self.SNAPLiteMaskWorkspace[self.eastMasked],
            referenceParametersFile=referenceParametersFile,
        )
