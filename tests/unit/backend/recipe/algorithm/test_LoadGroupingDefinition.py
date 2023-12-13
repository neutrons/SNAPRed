import os.path
import socket
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

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_with_nxs_grouping_file():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
        instrumentFile = getInstrumentDefinitionFilePath(isLocalTest=False, isLiteInstrument=True)

        # load the input grouping definition as a workspace
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loaded_ws_name = "loaded_ws"
        loadingAlgo.setProperty("GroupingFilename", groupingFile)
        loadingAlgo.setProperty("InstrumentFilename", instrumentFile)
        loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
        assert loadingAlgo.execute()

        # retrieve the loaded workspace and compare it with a workspace created from the input grouping file
        loaded_ws = mtd[loaded_ws_name]

        lnp_ws_name = "lnp_ws_name"
        original_ws = LoadNexusProcessed(Filename=groupingFile, OutputWorkspace=lnp_ws_name)
        result, _ = CompareWorkspaces(original_ws, loaded_ws)

        assert result

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_with_xml_grouping_file():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.xml"
        instrumentFile = getInstrumentDefinitionFilePath(isLocalTest=False, isLiteInstrument=False)

        # load the input grouping definition as a workspace
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loaded_ws_name = "loaded_ws"
        loadingAlgo.setProperty("GroupingFilename", groupingFile)
        loadingAlgo.setProperty("InstrumentFilename", instrumentFile)
        loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
        assert loadingAlgo.execute()

        # retrieve the loaded workspace and compare it with a workspace created from the input grouping file
        loaded_ws = mtd[loaded_ws_name]

        ldgf_ws_name = "ldgf_ws_name"
        original_ws = LoadDetectorsGroupingFile(InputFile=groupingFile, OutputWorkspace=ldgf_ws_name)

        result, _ = CompareWorkspaces(original_ws, loaded_ws)
        assert result

    def test_local_with_hdf_grouping_file():
        groupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.hdf")
        instrumentFile = getInstrumentDefinitionFilePath(isLocalTest=True, isLiteInstrument=False)

        # load the input grouping definition as a workspace
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loaded_ws_name = "loaded_ws"
        loadingAlgo.setProperty("GroupingFilename", groupingFile)
        loadingAlgo.setProperty("InstrumentFilename", instrumentFile)
        loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
        assert loadingAlgo.execute()

        # retrieve the loaded workspace and compare it with a workspace created by loading a reference grouping file
        loaded_ws = mtd[loaded_ws_name]

        refGroupingFile = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.xml")
        ref_ws_name = "ref_ws_name"
        ref_ws = LoadDetectorsGroupingFile(InputFile=refGroupingFile, OutputWorkspace=ref_ws_name)

        result, _ = CompareWorkspaces(ref_ws, loaded_ws)
        assert result

    def test_with_invalid_grouping_file_name():
        groupingFile = "junk"
        instrumentFile = getInstrumentDefinitionFilePath(isLocalTest=True, isLiteInstrument=True)

        # load the input grouping definition as a workspace
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loaded_ws_name = "loaded_ws"
        loadingAlgo.setProperty("GroupingFilename", groupingFile)
        loadingAlgo.setProperty("InstrumentFilename", instrumentFile)
        loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
        with pytest.raises(RuntimeError) as excinfo:
            loadingAlgo.execute()
        assert "unsupported file name extension" in str(excinfo.value)

    def test_with_invalid_grouping_file_name_extension():
        groupingFile = "abc.junk"
        instrumentFile = getInstrumentDefinitionFilePath(isLocalTest=True, isLiteInstrument=True)

        # load the input grouping definition as a workspace
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loaded_ws_name = "loaded_ws"
        loadingAlgo.setProperty("GroupingFilename", groupingFile)
        loadingAlgo.setProperty("InstrumentFilename", instrumentFile)
        loadingAlgo.setProperty("OutputWorkspace", loaded_ws_name)
        with pytest.raises(RuntimeError) as excinfo:
            loadingAlgo.execute()
        assert "unsupported file name extension" in str(excinfo.value)
