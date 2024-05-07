import os.path
import socket
import unittest
import unittest.mock as mock
from typing import Dict, Tuple

import pytest
from mantid.simpleapi import (
    DeleteWorkspace,
    LoadDetectorsGroupingFile,
    LoadDiffCal,
    LoadEmptyInstrument,
    LoadNexusProcessed,
    mtd,
)
from mantid.testing import assert_almost_equal as assert_wksp_almost_equal
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition as LoadingAlgo
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Resource
from util.helpers import workspacesEqual

IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")


class TestLoadGroupingDefinition(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # file location for instrument definition
        cls.localInstrumentFilename = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")

        # names for instrument donor workspaces
        cls.localIDFWorkspace = "test_local_idf"

        # file locations for local grouping files -- one of each class
        cls.localGroupingFile = {
            "xml": Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
            "hdf": Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.hdf"),
            "nxs": Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.nxs"),
        }

        LoadEmptyInstrument(
            OutputWorkspace=cls.localIDFWorkspace,
            Filename=cls.localInstrumentFilename,
        )

        # make a reference workspace for comparison
        cls.localReferenceWorkspace = "test_reference_workspace"

        LoadDetectorsGroupingFile(
            InputFile=cls.localGroupingFile["xml"],
            InputWorkspace=cls.localIDFWorkspace,
            OutputWorkspace=cls.localReferenceWorkspace,
        )

        # if on analysis, perform further tests with the real instrument
        if IS_ON_ANALYSIS_MACHINE:
            cls.isLite, cls.isFull = (0, 1)
            # locations for the filenames on analysis
            cls.remoteInstrumentFilename: Dict[int, str] = {
                cls.isLite: "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
                cls.isFull: "/opt/anaconda/envs/mantid-dev/instrument/SNAP_Definition.xml",
            }

            # file locations for remtoe grouping files
            pgdFolder = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/"
            cls.remoteGroupingFile = {
                (cls.isLite, "xml"): f"{pgdFolder}SNAPFocGrp_Column.lite.xml",
                (cls.isLite, "hdf"): f"{pgdFolder}SNAPFocGroup_Column.lite.hdf",
                (cls.isLite, "nxs"): f"{pgdFolder}SNAPFocGroup_Column.lite.nxs",
                (cls.isFull, "xml"): f"{pgdFolder}SNAPFocGroup_Column.xml",
                (cls.isFull, "hdf"): f"{pgdFolder}SNAPFocGroup_Column.hdf",
                (cls.isFull, "nxs"): None,  # TODO make an nxs grouping file
            }

            # workspaces containing full SNAP instrument
            cls.remoteIDFWorkspace: Dict[int, str] = {
                cls.isLite: "test_analysis_idf_lite",
                cls.isFull: "test_analysis_idf_full",
            }
            LoadEmptyInstrument(
                OutputWorkspace=cls.remoteIDFWorkspace[cls.isLite],
                Filename=cls.remoteInstrumentFilename[cls.isLite],
            )
            LoadEmptyInstrument(
                OutputWorkspace=cls.remoteIDFWorkspace[cls.isFull],
                Filename=cls.remoteInstrumentFilename[cls.isFull],
            )

            # make references workspaces for native and lite SNAP resolution
            cls.remoteReferenceGroupingFile = {
                cls.isFull: Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.xml"),
                cls.isLite: cls.remoteGroupingFile[(cls.isLite, "xml")],
            }
            cls.remoteReferenceWorkspace = {
                cls.isFull: "ref_ws_name_full",
                cls.isLite: "ref_ws_name_lite",
            }
            LoadDetectorsGroupingFile(
                InputFile=cls.remoteReferenceGroupingFile[cls.isFull],
                InputWorkspace=cls.remoteIDFWorkspace[cls.isFull],
                OutputWorkspace=cls.remoteReferenceWorkspace[cls.isFull],
            )
            LoadDetectorsGroupingFile(
                InputFile=cls.remoteReferenceGroupingFile[cls.isLite],
                InputWorkspace=cls.remoteIDFWorkspace[cls.isLite],
                OutputWorkspace=cls.remoteReferenceWorkspace[cls.isLite],
            )

        cls.callsForExtension = {
            "hdf": ["LoadDiffCal", "RenameWorkspace"],
            "xml": ["LoadEmptyInstrument", "LoadDetectorsGroupingFile", "WashDishes"],
            "nxs": ["LoadNexusProcessed"],
        }
        super().setUpClass()

    def setUp(self):
        """Common setup before each test"""
        pass

    def tearDown(self):
        for workspace in ["loaded_ws", "test_xml", "test_hdf", "test_nxs"]:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                pass

    @classmethod
    def tearDownClass(cls):
        """Common teardown after each test"""
        # remove all workspaces
        for workspace in mtd.getObjectNames():
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                pass
        super().tearDownClass()

    def test_chopIngredients(self):
        extensions = ["h5", "hd5", "hdf", "nxs", "nxs5", "xml"]
        stems = ["", "lite."]
        paths = ["/path/to/file/", "./file/"]

        algo = LoadingAlgo()
        algo.initialize()
        for ext in extensions:
            for stem in stems:
                for path in paths:
                    assert ext.upper() == algo.chopIngredients(path + "junk." + stem + ext)
        assert algo.instrumentSource == {}

        instrumentSources = {
            "InstrumentName": "imaginary instrument",
            "InstrumentDonor": self.localIDFWorkspace,
            "InstrumentFilename": self.localInstrumentFilename,
        }
        for prop, source in instrumentSources.items():
            algo.setPropertyValue(prop, source)
            algo.chopIngredients("x.xml")
            assert algo.instrumentSource == {prop: source}
            algo.setPropertyValue(prop, "")
        # edge case
        algo.setProperty("InstrumentDonor", self.localIDFWorkspace)
        algo.chopIngredients("x.hdf")
        assert algo.instrumentSource == {"InputWorkspace": self.localIDFWorkspace}

    def test_fail_with_invalid_grouping_file_name(self):
        groupingFile = "junk"

        # load the input grouping definition as a workspace
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        with pytest.raises(ValueError) as excinfo:  # noqa: PT011
            loadingAlgo.setProperty("GroupingFilename", groupingFile)
        assert "junk" in str(excinfo.value)
        assert "GroupingFilename" in str(excinfo.value)

    def test_fail_with_invalid_grouping_file_name_extension(self):
        groupingFileBad = Resource.getPath("/inputs/pixel_grouping/abc.junk")

        # load the input grouping definition as a workspace
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.setProperty("GroupingFilename", groupingFileBad)
        errors = loadingAlgo.validateInputs()
        assert "extension" in errors["GroupingFilename"]
        assert "junk".upper() in errors["GroupingFilename"]

    def test_with_valid_grouping_file_name(self):
        # load the input grouping definition as a workspace
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        try:
            loadingAlgo.setProperty("GroupingFilename", self.localGroupingFile["xml"])
        except ValueError as e:
            pytest.fail(str(e.value))

    def test_require_grouping_filename(self):
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        errors = loadingAlgo.validateInputs()
        assert errors != {}
        assert errors.get("GroupingFilename") is not None
        assert "grouping" in errors["GroupingFilename"]

    def test_require_output_workspace(self):
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.validateInputs = mock.Mock()
        loadingAlgo.setProperty("GroupingFilename", self.localGroupingFile["xml"])
        with pytest.raises(RuntimeError):
            loadingAlgo.execute()
        assert not loadingAlgo.validateInputs.called

    def test_fail_with_no_sources(self):
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.setProperty("GroupingFilename", self.localGroupingFile["xml"])
        errors = loadingAlgo.validateInputs()
        for prop in ["InstrumentFilename", "InstrumentName", "InstrumentDonor"]:
            assert "MUST specify" in errors[prop]

    def test_fail_with_two_sources(self):
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.setProperty("GroupingFilename", self.localGroupingFile["xml"])

        propertySourcePairs = [
            ("InstrumentFilename", self.localInstrumentFilename),
            ("InstrumentName", "fakeSNAP"),
            ("InstrumentDonor", self.localIDFWorkspace),
        ]
        for ij in [(0, 1), (0, 2), (1, 2)]:
            prop1, source1 = propertySourcePairs[ij[0]]
            prop2, source2 = propertySourcePairs[ij[1]]
            loadingAlgo.setPropertyValue(prop1, source1)
            loadingAlgo.setPropertyValue(prop2, source2)
            errors = loadingAlgo.validateInputs()
            assert "declare ONE" in errors[prop1]
            assert "declare ONE" in errors[prop2]
            loadingAlgo.setPropertyValue(prop1, "")
            loadingAlgo.setPropertyValue(prop2, "")

    # NOTE: MantidSnapper cannot be properly wrapped by Mock
    # In order to check if functions have been called through MantidSnapper
    # it is necessary to maintain a copy of its algorithm queue to inspect

    def do_test_load_with_instrument_file(self, ext: str):
        outputWorkspace = f"text_{ext}"
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.mantidSnapper.cleanup = mock.Mock()
        loadingAlgo.setProperty("GroupingFilename", self.localGroupingFile[ext])
        loadingAlgo.setProperty("InstrumentFilename", self.localInstrumentFilename)
        loadingAlgo.setProperty("OutputWorkspace", outputWorkspace)
        assert loadingAlgo.execute()
        assert mtd.doesExist(outputWorkspace)
        assert_wksp_almost_equal(outputWorkspace, self.localReferenceWorkspace)
        # check the function calls made
        calls = [call[0] for call in loadingAlgo.mantidSnapper._algorithmQueue]
        # check used correct cals
        for call in self.callsForExtension[ext]:
            assert call in calls

    # NOTE LOADING FROM NAME IS VERY SLOW
    def do_test_load_with_instrument_name(self, ext: str):
        outputWorkspace = "test_ext"
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.mantidSnapper.cleanup = mock.Mock()
        loadingAlgo.setProperty("GroupingFilename", self.localGroupingFile[ext])
        loadingAlgo.setProperty("InstrumentName", "SNAP")
        loadingAlgo.setProperty("OutputWorkspace", outputWorkspace)
        assert loadingAlgo.execute()
        assert mtd.doesExist(outputWorkspace)
        assert_wksp_almost_equal(outputWorkspace, self.localReferenceWorkspace)
        # check the function calls made
        calls = [call[0] for call in loadingAlgo.mantidSnapper._algorithmQueue]
        # check used correct cals
        for call in self.callsForExtension[ext]:
            assert call in calls

    def do_test_load_with_instrument_donor(self, ext: str):
        outputWorkspace = f"test_{ext}"
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.mantidSnapper.cleanup = mock.Mock()
        loadingAlgo.setProperty("GroupingFilename", self.localGroupingFile[ext])
        loadingAlgo.setProperty("InstrumentDonor", self.localIDFWorkspace)
        loadingAlgo.setProperty("OutputWorkspace", outputWorkspace)
        assert loadingAlgo.execute()
        assert mtd.doesExist(outputWorkspace)
        assert_wksp_almost_equal(outputWorkspace, self.localReferenceWorkspace)
        # check the function calls made
        calls = [call[0] for call in loadingAlgo.mantidSnapper._algorithmQueue]
        # check used correct cals
        if ext == "xml":
            correctCalls = ["LoadDetectorsGroupingFile"]
        else:
            correctCalls = self.callsForExtension[ext]
        for call in correctCalls:
            assert call in calls

    # test xml
    def test_load_from_xml_file_with_instrument_donor(self):
        self.do_test_load_with_instrument_donor("xml")

    def test_load_from_xml_file_with_instrument_file(self):
        self.do_test_load_with_instrument_file("xml")

    # NOTE commented out because slow
    # def test_load_from_xml_file_with_instrument_name(self):
    #     self.do_test_load_with_instrument_name("xml")

    # test hdf
    # TODO THIS IS BAD -- EWM 5043
    @pytest.mark.xfail(strict=True)
    def test_load_from_hdf_file_with_instrument_donor(self):
        self.do_test_load_with_instrument_donor("hdf")

    # TODO THIS IS BAD -- EWM 5043
    @pytest.mark.xfail(strict=True)
    def test_load_from_hdf_file_with_instrument_file(self):
        self.do_test_load_with_instrument_file("hdf")

    # NOTE commented out because slow
    # def test_load_from_hdf_file_with_instrument_name(self):
    #     self.do_test_load_with_instrument_name("hdf")

    # test nxs
    def test_load_from_nxs_file_with_instrument_donor(self):
        self.do_test_load_with_instrument_donor("nxs")

    def test_load_from_nxs_file_with_instrument_file(self):
        self.do_test_load_with_instrument_file("nxs")

    # NOTE commented out because slow
    # def test_load_from_nxs_file_with_instrument_name(self):
    #     self.do_test_load_with_instrument_name("nxs")

    ### BEGIN ANALYSIS TESTS

    # TODO whenever Mantid's LoadDiffCal is fixed, run the Lite tests in each below

    def do_test_load_with_instrument_file_remote(self, ext: str, useLite: int):
        outputWorkspace = f"text_{ext}"
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.mantidSnapper.cleanup = mock.Mock()
        loadingAlgo.setProperty("GroupingFilename", self.remoteGroupingFile[(useLite, ext)])
        loadingAlgo.setProperty("InstrumentFilename", self.remoteInstrumentFilename[useLite])
        loadingAlgo.setProperty("OutputWorkspace", outputWorkspace)
        assert loadingAlgo.execute()
        assert mtd.doesExist(outputWorkspace)
        assert_wksp_almost_equal(outputWorkspace, self.remoteReferenceWorkspace[useLite])
        # check the function calls made
        calls = [call[0] for call in loadingAlgo.mantidSnapper._algorithmQueue]
        # check used correct cals
        for call in self.callsForExtension[ext]:
            assert call in calls

    # NOTE only native SNAP can be loaded by name
    def do_test_load_with_instrument_name_remote(self, ext: str, useLite: int):
        outputWorkspace = f"test_{ext}"
        instrumentName = {
            self.isLite: "SNAPLite",
            self.isFull: "SNAP",
        }
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.mantidSnapper.cleanup = mock.Mock()
        loadingAlgo.setProperty("GroupingFilename", self.remoteGroupingFile[(useLite, ext)])
        loadingAlgo.setProperty("InstrumentName", instrumentName[useLite])
        loadingAlgo.setProperty("OutputWorkspace", outputWorkspace)
        assert loadingAlgo.execute()
        assert mtd.doesExist(outputWorkspace)
        assert_wksp_almost_equal(outputWorkspace, self.remoteReferenceWorkspace[useLite])
        # check the function calls made
        calls = [call[0] for call in loadingAlgo.mantidSnapper._algorithmQueue]
        # check used correct cals
        for call in self.callsForExtension[ext]:
            assert call in calls

    def do_test_load_with_instrument_donor_remote(self, ext: str, useLite: int):
        outputWorkspace = f"test_{ext}"
        loadingAlgo = LoadingAlgo()
        loadingAlgo.initialize()
        loadingAlgo.mantidSnapper.cleanup = mock.Mock()
        loadingAlgo.setProperty("GroupingFilename", self.remoteGroupingFile[(useLite, ext)])
        loadingAlgo.setProperty("InstrumentDonor", self.remoteIDFWorkspace[useLite])
        loadingAlgo.setProperty("OutputWorkspace", outputWorkspace)
        assert loadingAlgo.execute()
        assert mtd.doesExist(outputWorkspace)
        assert_wksp_almost_equal(outputWorkspace, self.remoteReferenceWorkspace[useLite])
        # check the function calls made
        calls = [call[0] for call in loadingAlgo.mantidSnapper._algorithmQueue]
        # check used correct cals
        if ext == "xml":
            correctCalls = ["LoadDetectorsGroupingFile"]
        else:
            correctCalls = self.callsForExtension[ext]
        for call in correctCalls:
            assert call in calls

    # remote test NXS
    # NOTE only lite mode groupings have NXS files
    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_nxs_with_instrument_file(self):
        self.do_test_load_with_instrument_file_remote("nxs", self.isLite)

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_nxs_with_instrument_name(self):
        self.do_test_load_with_instrument_name_remote("nxs", self.isLite)

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_nxs_with_instrument_donor(self):
        self.do_test_load_with_instrument_donor_remote("nxs", self.isLite)

    # remote test XML
    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_xml_with_instrument_file(self):
        self.do_test_load_with_instrument_file_remote("xml", self.isLite)
        self.do_test_load_with_instrument_file_remote("xml", self.isFull)

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_xml_with_instrument_name(self):
        self.do_test_load_with_instrument_name_remote("xml", self.isFull)

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_xml_with_instrument_donor(self):
        self.do_test_load_with_instrument_donor_remote("xml", self.isLite)
        self.do_test_load_with_instrument_donor_remote("xml", self.isFull)

    # remote test HDF
    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_hdf_with_instrument_file(self):
        self.do_test_load_with_instrument_file_remote("hdf", self.isLite)
        self.do_test_load_with_instrument_file_remote("hdf", self.isFull)

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_hdf_with_instrument_name(self):
        self.do_test_load_with_instrument_name_remote("hdf", self.isFull)

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_remote_load_hdf_with_instrument_donor(self):
        self.do_test_load_with_instrument_donor_remote("hdf", self.isLite)
        self.do_test_load_with_instrument_donor_remote("hdf", self.isFull)
