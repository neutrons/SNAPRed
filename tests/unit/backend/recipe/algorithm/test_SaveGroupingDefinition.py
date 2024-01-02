import os.path
import pathlib
import socket
import tempfile
import unittest
import unittest.mock as mock
from unittest.mock import ANY

import pytest
from mantid.simpleapi import (
    CloneWorkspace,
    CompareWorkspaces,
    CreateGroupingWorkspace,
    DeleteWorkspace,
    LoadDetectorsGroupingFile,
    LoadEmptyInstrument,
    LoadNexusProcessed,
    RenameWorkspace,
    mtd,
)
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition as LoadingAlgo
from snapred.backend.recipe.algorithm.SaveGroupingDefinition import SaveGroupingDefinition as SavingAlgo
from snapred.meta.Config import Resource

IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")


class TestSaveGroupingDefinition(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # file location for instrument definition
        cls.localInstrumentFilename = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        cls.localGroupingFilename = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")

        # names for instrument donor workspaces
        cls.localIDFWorkspace = "test_local_idf"

        # create instrument donor workspace
        LoadEmptyInstrument(
            OutputWorkspace=cls.localIDFWorkspace,
            Filename=cls.localInstrumentFilename,
        )

        # NOTE: the below do not need to match the actual grouping workspaces,
        #  and can be arbitary for these tests to still work
        cls.localReferenceWorkspace = {
            "Natural": "test_reference_grouping_natural",
            "Column": "test_reference_grouping_column",
        }
        CreateGroupingWorkspace(
            InputWorkspace=cls.localIDFWorkspace,
            OutputWorkspace=cls.localReferenceWorkspace["Column"],
            CustomGroupingString="0+2+4+5, 1+3+6+7, 8+9+12+13, 10+11+14+15",
        )
        CreateGroupingWorkspace(
            InputWorkspace=cls.localIDFWorkspace,
            OutputWorkspace=cls.localReferenceWorkspace["Natural"],
            CustomGroupingString="0+5+10+15, 1+7+8+13, 2+4+11+14, 3+6+9+12",
        )

    def setUp(self):
        """Common setup before each test"""
        self.columnGroupingWorkspace = "test_grouping_column"
        self.naturalGroupingWorkspace = "test_grouping_natural"
        CloneWorkspace(
            InputWorkspace=self.localReferenceWorkspace["Column"],
            Outputworkspace=self.columnGroupingWorkspace,
        )
        CloneWorkspace(
            InputWorkspace=self.localReferenceWorkspace["Natural"],
            Outputworkspace=self.naturalGroupingWorkspace,
        )

    def teardDown(self):
        """Common teardown after each test"""
        # remove all workspaces
        for workspace in [self.columnGroupingWorkspace, self.naturalGroupingWorkspace]:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    @classmethod
    def teardDownClass(cls):
        """Common teardown after each test"""
        # remove all workspaces
        for workspace in mtd.getObjectNames():
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    def getInstrumentDefinitionFilePath(isLocalTest, isLiteInstrument):
        if isLocalTest:
            if isLiteInstrument:
                return Resource.getPath("inputs/pixel_grouping/SNAPLite_Definition.xml")
            else:
                return Resource.getPath("inputs/pixel_grouping/SNAP_Definition.xml")
        else:
            if isLiteInstrument:
                return "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
            else:
                return "/opt/anaconda/envs/mantid-dev/instrument/SNAP_Definition.xml"

    ## VALIDATION CHECKS

    def test_fail_bad_grouping_ws(self):
        badGroupingWS = "junk"
        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        with pytest.raises(ValueError) as e:  # noqa: PT011
            savingAlgo.setPropertyValue("GroupingWorkspace", badGroupingWS)
        assert badGroupingWS in str(e.value)

    def test_fail_no_output_file(self):
        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        errs = savingAlgo.validateInputs()
        assert errs.get("OutputFilename") is not None
        # mke sure this fails fast with no other errors
        assert errs.get("GroupingFilename") is None
        assert errs.get("GroupingWorkspace") is None

    def test_fail_no_groupings(self):
        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        savingAlgo.setPropertyValue("OutputFilename", "test_bad_validation.hdf")
        errs = savingAlgo.validateInputs()
        assert errs.get("GroupingFilename") is not None
        assert errs.get("GroupingWorkspace") is not None

    def test_fail_two_groupings(self):
        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        savingAlgo.setPropertyValue("OutputFilename", "test_bad_validation.hdf")
        savingAlgo.setPropertyValue("GroupingWorkspace", self.naturalGroupingWorkspace)
        savingAlgo.setPropertyValue("GroupingFilename", self.localGroupingFilename)
        errs = savingAlgo.validateInputs()
        assert errs.get("GroupingFilename") is not None
        assert errs.get("GroupingWorkspace") is not None
        assert "both" in errs["GroupingFilename"].lower()

    def test_fail_bad_grouping_file(self):
        badGroupingFilename = "junk"
        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        savingAlgo.setPropertyValue("OutputFilename", "test_bad_validation.hdf")
        savingAlgo.setPropertyValue("GroupingFilename", badGroupingFilename)
        errs = savingAlgo.validateInputs()
        assert errs.get("GroupingFilename") is not None
        assert badGroupingFilename in errs["GroupingFilename"].lower()

    def test_fail_bad_grouping_extension(self):
        with tempfile.TemporaryDirectory(suffix=".junk") as badGroupingFilename:
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setPropertyValue("OutputFilename", "test_bad_validation")
            savingAlgo.setPropertyValue("GroupingFilename", badGroupingFilename)
            errs = savingAlgo.validateInputs()
            assert errs.get("GroupingFilename") is not None
            assert "extension" in errs["GroupingFilename"].lower()
            assert "does not exist" not in errs["GroupingFilename"].lower()

    def test_fail_bad_output_extension(self):
        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        savingAlgo.setPropertyValue("OutputFilename", "test_bad_validation.hdf")
        savingAlgo.setPropertyValue("GroupingWorkspace", self.naturalGroupingWorkspace)
        errs = savingAlgo.validateInputs()
        assert errs.get("OutputFilename") is None
        savingAlgo.setPropertyValue("OutputFilename", "test_bad_validation.junk")
        errs = savingAlgo.validateInputs()
        assert errs.get("OutputFilename") is not None
        assert "extension" in errs["OutputFilename"].lower()

    def test_fail_no_instrument_source(self):
        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        savingAlgo.setPropertyValue("OutputFilename", "test_bad_validation.hdf")
        savingAlgo.setPropertyValue("GroupingFilename", self.localGroupingFilename)
        errs = savingAlgo.validateInputs()
        assert errs.get("InstrumentDonor") is not None
        assert errs.get("InstrumentFilename") is not None
        assert errs.get("InstrumentName") is not None

    def test_fail_two_instrument_sources(self):
        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        savingAlgo.setPropertyValue("OutputFilename", "test_bad_validation.hdf")
        savingAlgo.setPropertyValue("GroupingFilename", self.localGroupingFilename)
        savingAlgo.setPropertyValue("InstrumentFilename", self.localInstrumentFilename)
        savingAlgo.setPropertyValue("InstrumentDonor", self.localIDFWorkspace)
        errs = savingAlgo.validateInputs()
        assert errs.get("InstrumentDonor") is not None
        assert errs.get("InstrumentFilename") is not None
        assert errs.get("InstrumentName") is None

    ## LOCAL CHECKS

    with tempfile.TemporaryDirectory() as tmp_dir:

        def do_test_local_from_workspace_test(self, workspaceName):
            outputFilename = f"from_workspace_test_{workspaceName}.hdf"

            outputFilePath = os.path.join(self.tmp_dir, outputFilename)
            # save the created workspace in the calibration format
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setProperty("GroupingWorkspace", workspaceName)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            assert savingAlgo.execute()

            assert os.path.exists(outputFilePath)

            # load the saved grouping definition as a workspace
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty("InstrumentDonor", self.localIDFWorkspace)
            loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
            assert loadingAlgo.execute()

            # retrieve the loaded workspace and compare it with the workspace created from the input grouping file
            assert CompareWorkspaces(loaded_ws_name, workspaceName)

        def test_local_from_groupingfile_test(self):
            groupingFile = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
            output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
            outputFilePath = os.path.join(self.tmp_dir, output_file_name)

            # will mock out the loader to simply rename the already-loaded workspaces
            def replaceInADS(msg, **kwargs):  # noqa: ARG001
                RenameWorkspace(
                    InputWorkspace=self.naturalGroupingWorkspace,
                    OutputWorkspace=kwargs["OutputWorkspace"],
                )

            # save the created workspace in the calibration format
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            # mock out the loader and dishwasher but make sure they are called
            savingAlgo.mantidSnapper.LoadGroupingDefinition = mock.Mock()
            savingAlgo.mantidSnapper.LoadGroupingDefinition.side_effect = lambda msg, **kwargs: replaceInADS(
                msg, **kwargs
            )
            savingAlgo.mantidSnapper.WashDishes = mock.Mock()
            savingAlgo.setProperty("GroupingFilename", groupingFile)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            savingAlgo.setProperty("InstrumentDonor", self.localIDFWorkspace)
            assert savingAlgo.execute()

            # assert that the mocked methods were called
            assert savingAlgo.mantidSnapper.LoadGroupingDefinition.called_once_with(
                ANY,
                GroupingFilename=groupingFile,
                OutputWorkspace=ANY,
                InstrumentDonor=self.localIDFWorkspace,
            )
            assert savingAlgo.mantidSnapper.WashDishes.called_once
            # assert the save went through
            assert os.path.exists(outputFilePath)

            # load the saved grouping definition as a workspace and compare
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty("InstrumentDonor", self.localIDFWorkspace)
            loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
            assert loadingAlgo.execute()
            assert CompareWorkspaces(loaded_ws_name, self.localReferenceWorkspace["Natural"])

    def test_local_from_workspace_test_column(self):
        self.do_test_local_from_workspace_test(self.columnGroupingWorkspace)
        # this last assert ensures saving does not alter the workspace
        assert CompareWorkspaces(self.columnGroupingWorkspace, self.localReferenceWorkspace["Column"])

    def test_local_from_workspace_test_natural(self):
        self.do_test_local_from_workspace_test(self.naturalGroupingWorkspace)
        # this last assert ensures saving does not alter the workspace
        assert CompareWorkspaces(self.columnGroupingWorkspace, self.localReferenceWorkspace["Natural"])

    ## REMOTE CHECKS WITH FULL INSTRUMENT

    with tempfile.TemporaryDirectory() as tmp_dir:

        @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
        def test_with_nxs_grouping_file(self):
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
            output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
            outputFilePath = os.path.join(self.tmp_dir, output_file_name)

            # save the input grouping file in the calibration format
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setProperty("GroupingFilename", groupingFile)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            savingAlgo.setProperty("InstrumentName", "SNAP")
            assert savingAlgo.execute()

            # load the saved grouping definition as a workspace
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty(
                "InstrumentFilename", self.getInstrumentDefinitionFilePath(isLocalTest=False, isLiteInstrument=True)
            )
            loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
            assert loadingAlgo.execute()

            # retrieve the loaded workspace and compare it with a workspace created from the input grouping file
            saved_and_loaded_ws = mtd[loaded_ws_name]

            lnp_ws_name = "lnp_ws_name"
            original_ws = LoadNexusProcessed(Filename=groupingFile, OutputWorkspace=lnp_ws_name)
            result, _ = CompareWorkspaces(original_ws, saved_and_loaded_ws)

            assert result

        @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
        def test_with_xml_grouping_file(self):
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.xml"
            output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
            outputFilePath = os.path.join(self.tmp_dir, output_file_name)

            # save the input grouping file in the calibration format
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setProperty("GroupingFilename", groupingFile)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            savingAlgo.setProperty("InstrumentName", "SNAP")
            assert savingAlgo.execute()

            # load the saved grouping definition as a workspace
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty(
                "InstrumentFilename", self.getInstrumentDefinitionFilePath(isLocalTest=False, isLiteInstrument=False)
            )
            loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
            assert loadingAlgo.execute()

            # retrieve the loaded workspace and compare it with a workspace created from the input grouping file
            saved_and_loaded_ws = mtd[loaded_ws_name]

            ldgf_ws_name = "ldgf_ws_name"
            original_ws = LoadDetectorsGroupingFile(InputFile=groupingFile, OutputWorkspace=ldgf_ws_name)

            result, _ = CompareWorkspaces(original_ws, saved_and_loaded_ws)
            assert result

        @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
        def test_with_grouping_workspace(self):
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
            output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
            outputFilePath = os.path.join(self.tmp_dir, output_file_name)

            # create a workspace from the input file
            lnp_ws_name = "lnp_ws_name"
            original_ws = LoadNexusProcessed(Filename=groupingFile, OutputWorkspace=lnp_ws_name)

            # save the created workspace in the calibration format
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setProperty("GroupingWorkspace", lnp_ws_name)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            savingAlgo.setProperty("InstrumentName", "SNAP")
            assert savingAlgo.execute()

            # load the saved grouping definition as a workspace
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty(
                "InstrumentFilename", self.getInstrumentDefinitionFilePath(isLocalTest=False, isLiteInstrument=True)
            )
            loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
            assert loadingAlgo.execute()

            # retrieve the loaded workspace and compare it with the workspace created from the input grouping file
            saved_and_loaded_ws = mtd[loaded_ws_name]
            result, _ = CompareWorkspaces(original_ws, saved_and_loaded_ws)
            assert result
