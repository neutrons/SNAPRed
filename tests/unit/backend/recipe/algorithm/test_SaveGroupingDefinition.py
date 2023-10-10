import os.path
import pathlib
import socket
import tempfile
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import CompareWorkspaces, DeleteWorkspace, LoadDetectorsGroupingFile, LoadNexusProcessed, mtd
    from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition as LoadingAlgo
    from snapred.backend.recipe.algorithm.SaveGroupingDefinition import SaveGroupingDefinition as SavingAlgo
    from snapred.meta.Config import Resource

    IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

    def setup():
        """Common setup before each test"""
        pass

    def teardown():
        """Common teardown after each test"""
        if not IS_ON_ANALYSIS_MACHINE:  # noqa: F821
            return
        # collect list of all workspaces
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    @pytest.fixture(autouse=True)
    def _setup_teardown():
        """Setup before each test, teardown after each test"""
        setup()
        yield
        teardown()

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

    with tempfile.TemporaryDirectory() as tmp_dir:

        @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
        def test_with_nxs_grouping_file():
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
            output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
            outputFilePath = os.path.join(tmp_dir, output_file_name)

            # save the input grouping file in the calibration format
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setProperty("GroupingFilename", groupingFile)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            assert savingAlgo.execute()

            # load the saved grouping definition as a workspace
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty(
                "InstrumentFilename", getInstrumentDefinitionFilePath(isLocalTest=False, isLiteInstrument=True)
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
        def test_with_xml_grouping_file():
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.xml"
            output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
            outputFilePath = os.path.join(tmp_dir, output_file_name)

            # save the input grouping file in the calibration format
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setProperty("GroupingFilename", groupingFile)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            assert savingAlgo.execute()

            # load the saved grouping definition as a workspace
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty(
                "InstrumentFilename", getInstrumentDefinitionFilePath(isLocalTest=False, isLiteInstrument=False)
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
        def test_with_grouping_workspace():
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
            output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
            outputFilePath = os.path.join(tmp_dir, output_file_name)

            # create a workspace from the input file
            lnp_ws_name = "lnp_ws_name"
            original_ws = LoadNexusProcessed(Filename=groupingFile, OutputWorkspace=lnp_ws_name)

            # save the created workspace in the calibration format
            outputFilePath = os.path.join(tmp_dir, output_file_name)
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setProperty("GroupingWorkspace", lnp_ws_name)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            assert savingAlgo.execute()

            # load the saved grouping definition as a workspace
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty(
                "InstrumentFilename", getInstrumentDefinitionFilePath(isLocalTest=False, isLiteInstrument=True)
            )
            loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
            assert loadingAlgo.execute()

            # retrieve the loaded workspace and compare it with the workspace created from the input grouping file
            saved_and_loaded_ws = mtd[loaded_ws_name]
            result, _ = CompareWorkspaces(original_ws, saved_and_loaded_ws)
            assert result

        def test_local_with_grouping_workspace():
            groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.xml")
            output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
            outputFilePath = os.path.join(tmp_dir, output_file_name)

            # create a workspace from the input file
            ldgf_ws_name = "lnp_ws_name"
            original_ws = LoadDetectorsGroupingFile(InputFile=groupingFile, OutputWorkspace=ldgf_ws_name)

            # save the created workspace in the calibration format
            outputFilePath = os.path.join(tmp_dir, output_file_name)
            savingAlgo = SavingAlgo()
            savingAlgo.initialize()
            savingAlgo.setProperty("GroupingWorkspace", ldgf_ws_name)
            savingAlgo.setProperty("OutputFilename", outputFilePath)
            savingAlgo.setProperty("InstrumentName", "SNAP")
            assert savingAlgo.execute()

            # load the saved grouping definition as a workspace
            loadingAlgo = LoadingAlgo()
            loadingAlgo.initialize()
            loaded_ws_name = "loaded_ws"
            loadingAlgo.setProperty("GroupingFilename", outputFilePath)
            loadingAlgo.setProperty(
                "InstrumentFilename", getInstrumentDefinitionFilePath(isLocalTest=True, isLiteInstrument=False)
            )
            loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
            assert loadingAlgo.execute()

            # retrieve the loaded workspace and compare it with the workspace created from the input grouping file
            saved_and_loaded_ws = mtd[loaded_ws_name]
            result, _ = CompareWorkspaces(original_ws, saved_and_loaded_ws)
            assert result

    def test_with_invalid_grouping_file():
        output_file_name = "GroupingDefinition.hdf"
        groupingFile = "junk"
        outputFilePath = os.path.join(tmp_dir, output_file_name)

        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        savingAlgo.setProperty("GroupingFilename", groupingFile)
        savingAlgo.setProperty("OutputFilename", outputFilePath)

        with pytest.raises(RuntimeError) as excinfo:
            savingAlgo.execute()
        assert "unsupported file name extension" in str(excinfo.value)

    def test_with_invalid_grouping_filename_extension():
        output_file_name = "GroupingDefinition.hdf"
        groupingFile = "junk.junk"
        outputFilePath = os.path.join(tmp_dir, output_file_name)

        savingAlgo = SavingAlgo()
        savingAlgo.initialize()
        savingAlgo.setProperty("GroupingFilename", groupingFile)
        savingAlgo.setProperty("OutputFilename", outputFilePath)

        with pytest.raises(RuntimeError) as excinfo:
            savingAlgo.execute()
        assert "unsupported file name extension" in str(excinfo.value)
