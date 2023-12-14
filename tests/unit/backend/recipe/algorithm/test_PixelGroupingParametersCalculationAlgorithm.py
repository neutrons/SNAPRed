# NOTE the remote tests depend pn files in the analysis filesystem
# if those are changed or moved, it could cause unexpected test failure

import json
import socket
import unittest
import unittest.mock as mock
from typing import Dict, List

import pytest
from mantid.simpleapi import (
    CheckForSampleLogs,
    CreateWorkspace,
    DeleteWorkspace,
    DeleteWorkspaces,
    LoadDetectorsGroupingFile,
    LoadDiffCal,
    LoadEmptyInstrument,
    RenameWorkspace,
    mtd,
)
from pydantic import parse_file_as, parse_obj_as, parse_raw_as
from snapred.backend.dao.calibration.Calibration import Calibration

# for loading
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
    PixelGroupingParametersCalculationAlgorithm as ThisAlgo,
)
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe as FetchRx
from snapred.meta.Config import Resource

IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")


class PixelGroupCalculation(unittest.TestCase):
    @classmethod
    def getInstrumentState(cls):
        if IS_ON_ANALYSIS_MACHINE:
            calibrationPath = (
                "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/CalibrationParameters.json"
            )
        else:
            calibrationPath = Resource.getPath("inputs/pixel_grouping/CalibrationParameters.json")
        return parse_file_as(Calibration, calibrationPath).instrumentState

    @classmethod
    def getInstrumentDefinitionFilePath(cls, isLiteInstrument):
        if IS_ON_ANALYSIS_MACHINE:
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
    def setUpClass(cls):
        # closest thing to an enum in python
        cls.isLite, cls.isFull, cls.isTest = (0, 1, 2)
        cls.all, cls.bank, cls.column, cls.natural = (3, 4, 5, 6)

        # state corresponding to local test instrument
        cls.localInstrumentState = InstrumentState.parse_raw(
            Resource.read("inputs/calibration/sampleInstrumentState.json")
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

        # set up workspaces as instrument donors
        liteInstrument: str = "test_pgp_idf_lite"
        nativeInstrument: str = "test_pgp_idf_native"
        LoadEmptyInstrument(
            OutputWorkspace=liteInstrument,
            Filename=cls.getInstrumentDefinitionFilePath(True),
        )
        LoadEmptyInstrument(
            OutputWorkspace=nativeInstrument,
            Filename=cls.getInstrumentDefinitionFilePath(False),
        )

        if IS_ON_ANALYSIS_MACHINE:
            cls.referenceFileFolder = "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/"

            # load the SNAP instrument
            cls.SNAPInstrumentFilename = cls.getInstrumentDefinitionFilePath(isLiteInstrument=False)
            cls.SNAPInstrumentWorkspace = "SNAP_intstrument_idf"
            LoadEmptyInstrument(
                OutputWorkspace=cls.SNAPInstrumentWorkspace,
                Filename=cls.SNAPInstrumentFilename,
            )

            # load the SNAPLite instrument
            cls.SNAPLiteInstrumentFilename = cls.getInstrumentDefinitionFilePath(isLiteInstrument=True)
            cls.SNAPLiteInstrumentWorkspace = "SNAPLite_intstrument_idf"
            LoadEmptyInstrument(
                OutputWorkspace=cls.SNAPLiteInstrumentWorkspace,
                Filename=cls.SNAPLiteInstrumentFilename,
            )

            # prepare loading of SNAP grouping workspaces
            cls.SNAPGroupingWorkspace = {
                cls.all: "test_grouping_workspace_SNAP_all",
                cls.bank: "test_grouping_workspace_SNAP_bank",
                cls.column: "test_grouping_workspace_SNAP_column",
            }
            pixelGroupPath = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/"
            cls.SNAPGroupingFilename = {
                cls.all: f"{pixelGroupPath}SNAPFocGroup_All.xml",
                cls.bank: f"{pixelGroupPath}SNAPFocGroup_Bank.xml",
                cls.column: f"{pixelGroupPath}SNAPFocGroup_Column.xml",
            }

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

        else:
            # the SNAPLite tests are fast enough to run locally
            cls.SNAPLiteInstrumentFilename = cls.getInstrumentDefinitionFilePath(isLiteInstrument=True)
            cls.SNAPLiteGroupingFilename: Dict[int, str] = {
                cls.all: Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_All.lite.hdf"),
                cls.bank: Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Bank.lite.hdf"),
                cls.column: Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.lite.hdf"),
            }

            cls.SNAPLiteInstrumentWorkspace = "SNAPLite_intstrument_idf"
            LoadEmptyInstrument(
                OutputWorkspace=cls.SNAPLiteInstrumentWorkspace,
                Filename=cls.SNAPLiteInstrumentFilename,
            )

            cls.SNAPLiteGroupingWorkspace = {
                cls.all: "test_grouping_workspace_SNAPLite_all",
                cls.bank: "test_grouping_workspace_SNAPLite_bank",
                cls.column: "test_grouping_workspace_SNAPLite_column",
            }

            for x in [cls.all, cls.bank, cls.column]:
                LoadDiffCal(
                    Filename=cls.SNAPLiteGroupingFilename[x],
                    MakeGroupingWorkspace=True,
                    MakeCalWorkspace=False,
                    MakeMaskWorkspace=False,
                    WorkspaceName=cls.SNAPLiteGroupingWorkspace[x],
                    InputWorkspace=cls.SNAPLiteInstrumentWorkspace,
                )
                # Remove the "_group" suffix, which is added by LoadDiffCal to the output workspace name
                RenameWorkspace(
                    "Renaming grouping workspace...",
                    InputWorkspace=cls.SNAPLiteGroupingWorkspace[x] + "_group",
                    OutputWorkspace=cls.SNAPLiteGroupingWorkspace[x],
                )

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
        algo.chopIngredients(self.localInstrumentState)
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

    def test_loadNeededLogs(self):
        algo = ThisAlgo()
        algo.initialize()
        LoadEmptyInstrument(
            OutputWorkspace="test_logs",
            Filename=self.localInstrumentFilename,
        )
        algo.loadNeededLogs("test_logs", self.localInstrumentState)
        assert "" == CheckForSampleLogs(
            Workspace="test_logs",
            LogNames="det_arc1",
        )
        assert "" == CheckForSampleLogs(
            Workspace="test_logs",
            LogNames="det_arc2",
        )
        assert "" == CheckForSampleLogs(
            Workspace="test_logs",
            LogNames="det_lin1",
        )
        assert "" == CheckForSampleLogs(
            Workspace="test_logs",
            LogNames="det_lin2",
        )
        # TODO assert instrument was updated

    def run_test(self, instrumentState, groupingWorkspace, referenceParametersFile):
        pixelGroupingParams_calc = self.createPixelGroupingParameters(instrumentState, groupingWorkspace)
        self.compareToReference(pixelGroupingParams_calc, referenceParametersFile)

    def createPixelGroupingParameters(self, instrumentState, groupingWorkspace):
        """Test execution of PixelGroupingParametersCalculationAlgorithm"""

        # run the algorithm
        pixelGroupingAlgo = ThisAlgo()
        pixelGroupingAlgo.initialize()
        pixelGroupingAlgo.setProperty("InstrumentState", instrumentState.json())
        pixelGroupingAlgo.setProperty("GroupingWorkspace", groupingWorkspace)
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

    def compareToReference(self, pixelGroupingParams_calc, referenceParametersFile):
        # parse the reference file. Note, in the reference file each kind of parameter is grouped into its own list
        with open(referenceParametersFile) as f:
            pixelGroupingParams_ref = json.load(f)

        # compare calculated and reference parameters
        number_of_groupings_calc = len(pixelGroupingParams_calc)
        assert len(pixelGroupingParams_ref["twoTheta"]) == number_of_groupings_calc
        assert len(pixelGroupingParams_ref["dMin"]) == number_of_groupings_calc
        assert len(pixelGroupingParams_ref["dMax"]) == number_of_groupings_calc
        assert len(pixelGroupingParams_ref["delDOverD"]) == number_of_groupings_calc

        for index, param in enumerate(pixelGroupingParams_ref["twoTheta"]):
            assert abs(float(param) - pixelGroupingParams_calc[index].twoTheta) == 0

        for index, param in enumerate(pixelGroupingParams_ref["dMin"]):
            assert abs(float(param) - pixelGroupingParams_calc[index].dResolution.minimum) == 0

        for index, param in enumerate(pixelGroupingParams_ref["dMax"]):
            assert abs(float(param) - pixelGroupingParams_calc[index].dResolution.maximum) == 0

        for index, param in enumerate(pixelGroupingParams_ref["delDOverD"]):
            assert abs(float(param) - pixelGroupingParams_calc[index].dRelativeResolution) < 1.0e-3

    # LOCAL TESTS ON TEST INSTRUMENT

    def test_local_testInstrument_column(self):
        groupingScheme = self.column
        pgp = self.createPixelGroupingParameters(
            instrumentState=self.localInstrumentState,
            groupingWorkspace=self.localGroupingWorkspace[groupingScheme],
        )
        assert len(pgp) > 0

    def test_local_testInstrument_natural(self):
        groupingScheme = self.natural
        pgp = self.createPixelGroupingParameters(
            instrumentState=self.localInstrumentState,
            groupingWorkspace=self.localGroupingWorkspace[groupingScheme],
        )
        assert len(pgp) > 0

    # LOCAL TESTS ON SNAPLITE

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_column(self):
        groupingScheme = self.column
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Column_parameters.lite.json")

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_bank(self):
        groupingScheme = self.bank
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/Bank_parameters.lite.json")

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(IS_ON_ANALYSIS_MACHINE, reason="use remote version instead")
    def test_local_SNAPLite_all(self):
        groupingScheme = self.all
        referenceParametersFile = Resource.getPath("outputs/pixel_grouping/All_parameters.lite.json")

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )

    # LOCAL NEGATIVE TESTS

    # TODO

    # BEGIN REMOTE TESTS OF PIXEL GROUPING CALCULATION

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_all_full(self):
        groupingScheme = self.all
        referenceParametersFile = self.referenceFileFolder + "All_parameters_newCalc.json"

        LoadDetectorsGroupingFile(
            InputFile=self.SNAPGroupingFilename[groupingScheme],
            InputWorkspace=self.SNAPInstrumentWorkspace,
            OutputWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_bank_full(self):
        groupingScheme = self.bank
        referenceParametersFile = self.referenceFileFolder + "Bank_parameters_newCalc.json"

        LoadDetectorsGroupingFile(
            InputFile=self.SNAPGroupingFilename[groupingScheme],
            InputWorkspace=self.SNAPInstrumentWorkspace,
            OutputWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_column_full(self):
        groupingScheme = self.column
        referenceParametersFile = self.referenceFileFolder + "Column_parameters_newCalc.json"

        LoadDetectorsGroupingFile(
            InputFile=self.SNAPGroupingFilename[groupingScheme],
            InputWorkspace=self.SNAPInstrumentWorkspace,
            OutputWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_all_lite(self):
        groupingScheme = self.all
        referenceParametersFile = self.referenceFileFolder + "All_parameters_newCalc.lite.json"

        LoadDiffCal(
            Filename=self.SNAPLiteGroupingFilename[groupingScheme],
            MakeGroupingWorkspace=True,
            MakeCalWorkspace=False,
            MakeMaskWorkspace=False,
            WorkspaceName=self.SNAPLiteGroupingWorkspace[groupingScheme],
            InputWorkspace=self.SNAPLiteInstrumentWorkspace,
        )
        # Remove the "_group" suffix, which is added by LoadDiffCal to the output workspace name
        RenameWorkspace(
            InputWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme] + "_group",
            OutputWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_bank_lite(self):
        groupingScheme = self.bank
        referenceParametersFile = self.referenceFileFolder + "Bank_parameters_newCalc.lite.json"

        LoadDiffCal(
            Filename=self.SNAPLiteGroupingFilename[groupingScheme],
            MakeGroupingWorkspace=True,
            MakeCalWorkspace=False,
            MakeMaskWorkspace=False,
            WorkspaceName=self.SNAPLiteGroupingWorkspace[groupingScheme],
            InputWorkspace=self.SNAPLiteInstrumentWorkspace,
        )
        # Remove the "_group" suffix, which is added by LoadDiffCal to the output workspace name
        RenameWorkspace(
            InputWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme] + "_group",
            OutputWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_column_lite(self):
        groupingScheme = self.column
        referenceParametersFile = self.referenceFileFolder + "Column_parameters_newCalc.lite.json"

        LoadDiffCal(
            Filename=self.SNAPLiteGroupingFilename[groupingScheme],
            MakeGroupingWorkspace=True,
            MakeCalWorkspace=False,
            MakeMaskWorkspace=False,
            WorkspaceName=self.SNAPLiteGroupingWorkspace[groupingScheme],
            InputWorkspace=self.SNAPLiteInstrumentWorkspace,
        )
        # Remove the "_group" suffix, which is added by LoadDiffCal to the output workspace name
        RenameWorkspace(
            InputWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme] + "_group",
            OutputWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
        )

        self.run_test(
            instrumentState=self.getInstrumentState(),
            groupingWorkspace=self.SNAPLiteGroupingWorkspace[groupingScheme],
            referenceParametersFile=referenceParametersFile,
        )
