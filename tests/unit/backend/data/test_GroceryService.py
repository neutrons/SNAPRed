# ruff: noqa: E722, PT011, PT012, F811
import datetime
import inspect
import itertools
import json
import os
import shutil
import time

##
## In order to preserve the import order as much as possible, add test-related imports at the end.
##
import unittest
from pathlib import Path
from random import randint
from unittest import mock

import pytest
from mantid.dataobjects import MaskWorkspace
from mantid.kernel import V3D, Quat
from mantid.simpleapi import (
    CloneWorkspace,
    CreateSampleWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    ExtractMask,
    GenerateTableWorkspaceFromListOfDict,
    GroupWorkspaces,
    LoadEmptyInstrument,
    LoadInstrument,
    MaskDetectors,
    RenameWorkspace,
    RenameWorkspaces,
    SaveDiffCal,
    SaveNexus,
    SaveNexusProcessed,
    mtd,
)
from mantid.testing import assert_almost_equal as assert_wksp_almost_equal
from util.Config_helpers import Config_override
from util.dao import DAOFactory
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.instrument_helpers import addInstrumentLogs, getInstrumentLogDescriptors, mapFromSampleLogs
from util.kernel_helpers import tupleFromQuat, tupleFromV3D
from util.state_helpers import reduction_root_redirect, state_root_redirect
from util.WhateversInTheFridge import WhateversInTheFridge

import snapred.backend.recipe.algorithm  # noqa: F401
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem, LiveDataArgs
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.StateId import StateId
from snapred.backend.dao.WorkspaceMetadata import UNSET, DiffcalStateMetadata, WorkspaceMetadata
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config, Resource
from snapred.meta.InternalConstants import ReservedRunNumber
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType

ThisService = "snapred.backend.data.GroceryService."


class TestGroceryService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a mock data file which will be loaded in tests.
        This is created at the start of this test suite, then deleted at the end.
        """
        cls.runNumber = "555"
        cls.runNumber1 = "556"
        cls.useLiteMode = False
        cls.timestamp = time.time()
        cls.diffCalOutputName = (
            wng.diffCalOutput().runNumber(cls.runNumber1).unit(wng.Units.TOF).group(wng.Groups.UNFOC).build()
        )
        cls.sampleWSFilePath = Resource.getPath(f"inputs/test_{cls.runNumber}_groceryservice.nxs")
        cls.sampleTarWsFilePath = Resource.getPath(f"inputs/{cls.diffCalOutputName}.nxs.h5")

        cls.instrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        Config["instrument"]["native"]["definition"]["file"] = cls.instrumentFilePath

        cls.instrumentLiteFilePath = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
        Config["instrument"]["lite"]["definition"]["file"] = cls.instrumentLiteFilePath

        cls.fetchedWSname = "_fetched_grocery"
        cls.groupingScheme = "Native"
        # create some sample data
        cls.sampleWS = "_grocery_to_fetch"

        cls.rtolValue = 1.0e-10

        CreateSampleWorkspace(
            OutputWorkspace=cls.sampleWS, WorkspaceType="Event", XUnit="TOF", NumBanks=4, BankPixelWidth=2
        )
        LoadInstrument(
            Workspace=cls.sampleWS,
            Filename=cls.instrumentFilePath,
            RewriteSpectraMap=True,
        )

        # Clone a copy of the sample workspace, but without any instrument-parameter state.
        cls.sampleWSBareInstrument = mtd.unique_hidden_name()
        CloneWorkspace(OutputWorkspace=cls.sampleWSBareInstrument, InputWorkspace=cls.sampleWS)
        # Add a complete instrument-parameter state to the sample workspace.
        # This is now required of the instrument-donor workspace for `LoadGroupingDefinition`
        #   and `LoadCalibrationWorkspaces`.
        cls.detectorState = DAOFactory.real_detector_state
        addInstrumentLogs(cls.sampleWS, **getInstrumentLogDescriptors(cls.detectorState))

        SaveNexusProcessed(
            InputWorkspace=cls.sampleWS,
            Filename=cls.sampleWSFilePath,
        )

        SaveNexus(
            InputWorkspace=cls.sampleWS,
            Filename=cls.sampleTarWsFilePath,
        )

        assert os.path.exists(cls.sampleWSFilePath)

        cls.detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        cls.stateId1 = ObjectSHA.fromObject(StateId.fromDetectorState(cls.detectorState1)).hex
        cls.detectorState2 = DetectorState(arc=(7.0, 8.0), wav=9.0, freq=10.0, guideStat=2, lin=(11.0, 12.0))
        cls.stateId2 = ObjectSHA.fromObject(StateId.fromDetectorState(cls.detectorState2)).hex

        cls.difc_name = "diffract_consts"
        cls.sampleDiffCalFilePath = Resource.getPath(f"inputs/test_diffcal_{cls.runNumber}_groceryservice.h5")
        cls.samplePixelMaskFilePath = Resource.getPath(f"inputs/test_pixelmask_{cls.runNumber}_groceryservice.h5")
        cls.sampleTableWS = "_table_grocery_to_fetch"
        createCompatibleDiffCalTable(cls.sampleTableWS, cls.sampleWS)
        cls.sampleMaskWS = "_mask_grocery_to_fetch"
        createCompatibleMask(cls.sampleMaskWS, cls.sampleWS)

        SaveDiffCal(
            CalibrationWorkspace=cls.sampleTableWS,
            MaskWorkspace=cls.sampleMaskWS,
            Filename=cls.sampleDiffCalFilePath,
        )
        assert os.path.exists(cls.sampleDiffCalFilePath)

        # Save the mask by itself
        SaveDiffCal(
            MaskWorkspace=cls.sampleMaskWS,
            Filename=cls.samplePixelMaskFilePath,
        )
        assert os.path.exists(cls.samplePixelMaskFilePath)

        # cleanup at per-test teardown
        cls.excludeAtTeardown = [cls.sampleWS, cls.sampleTableWS, cls.sampleMaskWS]

        # cleanup during running tests:
        cls.exclude = cls.excludeAtTeardown + [cls.fetchedWSname]

    def setUp(self):
        # rather than the LocalDataService, just grab whatevers in the fridge
        self.fridge = WhateversInTheFridge()
        self.fridge.readDetectorState = mock.Mock(return_value=self.detectorState1)

        self.instance = GroceryService(dataService=self.fridge)
        self.stateId, _ = self.instance.dataService.generateStateId(self.runNumber)
        self.groupingItem = (
            GroceryListItem.builder()
            .fromRun(self.runNumber)
            .grouping(self.groupingScheme)
            .native()
            .source(InstrumentDonor=self.sampleWS)
            .build()
        )

        # For each test, ensure a random version is used.
        self.version = randint(1, 120)
        self.instance.dataService.latestVersion = self.version
        self.diffCalOutputName = (
            wng.diffCalOutput()
            .runNumber(self.runNumber1)
            .unit(wng.Units.TOF)
            .group(wng.Groups.UNFOC)
            .version(self.version)
            .build()
        )
        # self.versionOutputPath = Path(Resource.getPath(f"outputs/))
        return super().setUp()

    def mockIndexer(self, root=None, calType=None):
        mockIndexer = mock.Mock()
        mockIndexer.readRecord = mock.Mock(
            return_value=mock.MagicMock(version=self.version, runNumber=self.runNumber1, useLiteMode=False)
        )
        # tmpqw8kedh8/native/diffraction/v_0078/
        if root is not None:
            liteModeStr = "lite" if self.useLiteMode == "lite" else "native"
            versionPath = Path(f"{root}/{liteModeStr}/{calType}/v_{self.version:04d}")
            mockIndexer.versionPath = mock.Mock(return_value=versionPath)
        return mock.Mock(return_value=mockIndexer)

    def mockRunMetadata(self, runNumber: str, detectorState: DetectorState | None = None) -> RunMetadata:
        return RunMetadata.model_construct(
            runNumber=runNumber,
            detectorState=detectorState,
            # other fields...
        )

    def clearoutWorkspaces(self) -> None:
        """Delete the workspaces created by loading"""
        for ws in mtd.getObjectNames():
            if ws not in self.excludeAtTeardown and ws in mtd:
                DeleteWorkspace(ws)

    def tearDown(self):
        """At the end of each test, clear out the workspaces"""
        self.clearoutWorkspaces()
        del self.instance
        return super().tearDown()

    @classmethod
    def tearDownClass(cls):
        """
        At the end of the test suite, delete all workspaces
        and remove the test file.
        """
        for ws in mtd.getObjectNames():
            DeleteWorkspace(ws)
        os.remove(cls.sampleWSFilePath)
        os.remove(cls.sampleDiffCalFilePath)
        os.remove(cls.sampleTarWsFilePath)
        os.remove(cls.samplePixelMaskFilePath)
        return super().tearDownClass()

    @classmethod
    def create_dumb_workspace(cls, wsName):
        CreateWorkspace(
            OutputWorkspace=wsName,
            DataX=[1],
            DataY=[1],
        )

    # Check that string wsname_1 has a new token in comparison to wsname_2.
    # Note, the exact order of tokens and the location of the extra token within the string
    # are up to WorkspaceNameGenerator, which isn't the objective of this check method.
    def check_workspace_name_has_extra_token(self, wsname_1: str, wsname_2: str, extra_token: str) -> bool:
        delimiter = "_"
        tokens_1 = wsname_1.split(delimiter)
        tokens_2 = wsname_2.split(delimiter)
        diff_12 = [x for x in tokens_1 if x not in tokens_2]
        diff_21 = [x for x in tokens_2 if x not in tokens_1]
        return diff_12 == [extra_token] and diff_21 == []

    ## TESTS OF FILENAME METHODS

    def test_getIPTS(self):
        # ensure it is calling to the data service
        self.instance.getIPTS(self.runNumber)
        self.instance.dataService.getIPTS.assert_called_once_with(self.runNumber, Config["instrument.name"])
        self.instance.dataService.getIPTS.reset_mock()

        self.instance.getIPTS(self.runNumber, "CRACKLE")
        self.instance.dataService.getIPTS.assert_called_once_with(self.runNumber, "CRACKLE")

    def test_key_neutron(self):
        """ensure the key is a unique identifier for run number and lite mode"""
        # This tests both <neutron data> and <instrument donor> key tuples.
        runId1 = "555"
        runId2 = "556"
        lite = True
        native = False
        # two keys with different run numbers are different
        assert self.instance._key(runId1, lite) != self.instance._key(runId2, lite)
        # two keys with same run number but different resolution are different
        assert self.instance._key(runId1, lite) != self.instance._key(runId1, native)
        # two keys with different run number and resolution are different
        assert self.instance._key(runId1, lite) != self.instance._key(runId2, native)
        assert self.instance._key(runId1, native) != self.instance._key(runId2, lite)
        # it is shaped like itself
        assert self.instance._key(runId1, lite) == self.instance._key(runId1, lite)
        assert self.instance._key(runId1, native) == self.instance._key(runId1, native)

    def test_key_grouping(self):
        """ensure the key is a unique identifier for run number and lite mode"""
        runId1 = "2462"
        grouping1 = "column"
        runId2 = "2463"
        grouping2 = "bank"

        lite = True
        native = False
        # two keys with different groupings are different
        assert self.instance._key(grouping1, runId1, lite) != self.instance._key(grouping2, runId1, lite)
        # two keys with same grouping but different resolution are different
        assert self.instance._key(grouping1, runId1, lite) != self.instance._key(grouping1, runId1, native)
        # two keys with same grouping but different runNumber are different
        assert self.instance._key(grouping1, runId1, lite) != self.instance._key(grouping1, runId2, native)
        # two keys with different grouping  and resolution are different
        assert self.instance._key(grouping1, runId1, lite) != self.instance._key(grouping2, runId1, native)
        assert self.instance._key(grouping1, runId1, native) != self.instance._key(grouping2, runId1, lite)
        # it is shaped like itself
        assert self.instance._key(grouping1, runId1, lite) == self.instance._key(grouping1, runId1, lite)
        assert self.instance._key(grouping1, runId1, native) == self.instance._key(grouping1, runId1, native)

    def test_nexus_filename(self):
        """Test the creation of the nexus filename"""
        for useLiteMode in (False, True):
            self.instance._createNeutronFilePath(self.runNumber, useLiteMode)
            self.instance.dataService.createNeutronFilePath.assert_called_once_with(self.runNumber, useLiteMode)
            self.instance.dataService.createNeutronFilePath.reset_mock()

    def test_grouping_filename(self):
        """Test the creation of the grouping filename"""
        runNumber = "123"
        useLiteMode = False  # NOTE just use false due to grouping map file
        groupingMap = self.fridge.readGroupingMap(runNumber).getMap(useLiteMode)
        for key, value in groupingMap.items():
            res = self.instance._createGroupingFilename(runNumber, key, useLiteMode)
            assert res == value.definition

    def test_litedatamap_filename(self):
        """Test it will return name of Lite data map"""
        runNumber = GroceryListItem.RESERVED_NATIVE_RUNNUMBER
        res = self.instance._createGroupingFilename(runNumber, "Lite", False)
        assert res == str(Config["instrument.lite.map.file"])
        res2 = self.instance._createGroupingFilename(runNumber, "Lite", True)
        assert res2 == res

    def test_diffcal_output_filename(self):
        # Test name generation for diffraction-calibration table filename
        item = GroceryListItem(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            workspaceType="diffcal_output",
            diffCalVersion=self.version,
            unit=wng.Units.TOF,
            groupingScheme="some",
            state=self.stateId,
        )
        res = self.instance._createDiffCalOutputWorkspaceFilename(item)
        assert self.runNumber in res
        assert wnvf.formatVersion(self.version) in res
        assert "tof" in res
        assert "some" in res
        assert ".nxs.h5" in res

    def test_diffcal_table_filename_from_workspaceName(self):
        workspaceName = wng.diffCalTable().runNumber(self.runNumber).version(self.version).build()
        with mock.patch.object(self.instance, "_lookupDiffcalRunNumber", return_value=self.runNumber):
            res = self.instance._createDiffCalTableFilepathFromWsName(
                self.useLiteMode,
                self.version,
                workspaceName,
                self.stateId,
            )
            assert workspaceName in res
            assert self.runNumber in res
            assert wnvf.formatVersion(self.version) in res
            assert ".h5" in res

    def test_diffcal_table_filename_from_workspaceName_failure_name_mismatch(self):
        workspaceName = "bogus_name_"
        mockIndexer = mock.Mock()
        mockIndexer.readRecord = mock.Mock()
        mockDataService = mock.Mock()
        mockDataService.calibrationIndexer = mock.Mock(return_value=mockIndexer)
        with mock.patch.object(self.instance, "_lookupDiffcalRunNumber", return_value=self.runNumber):
            self.instance.dataService = mockDataService
            with pytest.raises(ValueError, match=r".*Workspace name .* does not match the expected *"):
                self.instance._createDiffCalTableFilepathFromWsName(
                    self.useLiteMode,
                    self.version,
                    workspaceName,
                    "stateId",
                )

    def test_diffcal_table_filename(self):
        # Test name generation for diffraction-calibration table filename
        res = self.instance._createDiffCalTableFilepath(self.runNumber, self.useLiteMode, self.version, self.stateId)
        assert self.difc_name in res
        assert self.runNumber in res
        assert wnvf.formatVersion(self.version) in res
        assert ".h5" in res

    def test_normalization_workspace_filename(self):
        # Test name generation for diffraction-calibration table filename
        self.instance.dataService.normalizationIndexer = self.mockIndexer("root", "normalization")
        res = self.instance._lookupNormalizationWorkspaceFilename(
            self.runNumber, self.useLiteMode, self.version, self.stateId
        )
        assert self.runNumber in res
        assert wnvf.formatVersion(self.version) in res
        assert ".nxs" in res

    ## TESTS OF WORKSPACE NAME METHODS

    def test_neutron_workspacename_plain(self):
        """Test the creation of a plain nexus workspace name"""
        res = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        fRunNumber = wnvf.formatRunNumber(self.runNumber)
        assert res == f"_tof_all_{fRunNumber}"
        # now use lite mode
        res = self.instance._createNeutronWorkspaceName(self.runNumber, True)
        assert res == f"_tof_all_lite_{fRunNumber}"

    def test_neutron_workspacename_raw(self):
        """Test the creation of a raw nexus workspace name"""
        plainName = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        rawName = self.instance._createRawNeutronWorkspaceName(self.runNumber, False)
        assert self.check_workspace_name_has_extra_token(rawName, plainName, "raw")

    def test_neutron_workspacename_copy(self):
        """Test the creation of a copy nexus workspace name"""
        fakeCopies = 117
        plainName = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        copyName = self.instance._createCopyNeutronWorkspaceName(self.runNumber, False, fakeCopies)
        assert self.check_workspace_name_has_extra_token(copyName, plainName, f"copy{fakeCopies}")

    def test_grouping_workspacename(self):
        """Test the creation of a grouping workspace name"""
        uniqueGroupingScheme = "Fruitcake"
        self.groupingItem.groupingScheme = uniqueGroupingScheme
        res = self.instance._createGroupingWorkspaceName(uniqueGroupingScheme, self.runNumber, False)
        assert uniqueGroupingScheme in res
        assert "lite" not in res.lower()
        res = self.instance._createGroupingWorkspaceName(uniqueGroupingScheme, self.runNumber, True)
        assert uniqueGroupingScheme in res
        assert "lite" in res.lower()

    def test_litedatamap_workspacename(self):
        """Test the creation of name for Lite data map"""
        res = self.instance._createGroupingWorkspaceName("Lite", self.runNumber, False)
        assert res == "lite_grouping_map"

    def test_diffcal_input_workspacename(self):
        # Test name generation for diffraction-calibration input workspace
        res = self.instance._createDiffCalInputWorkspaceName(self.runNumber)
        assert "tof" in res
        assert self.runNumber in res
        assert "raw" in res

    def test_diffcal_output_tof_workspacename(self):
        # Test name generation for diffraction-calibration focussed-data workspace
        item = GroceryListItem(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            diffCalVersion=self.version,
            unit=wng.Units.TOF,
            groupingScheme=wng.Groups.UNFOC,
            workspaceType="diffcal_output",
            state=self.stateId,
        )
        res = self.instance._createDiffCalOutputWorkspaceName(item)
        assert "tof" in res
        assert self.runNumber in res
        assert wnvf.formatVersion(self.version) in res

    def test_diffcal_output_dsp_workspacename(self):
        # Test name generation for diffraction-calibration focussed-data workspace
        item = GroceryListItem(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            diffCalVersion=self.version,
            unit=wng.Units.DSP,
            groupingScheme=wng.Groups.UNFOC,
            workspaceType="diffcal_output",
            state=self.stateId,
        )
        res = self.instance._createDiffCalOutputWorkspaceName(item)
        assert "dsp" in res
        assert self.runNumber in res
        assert wnvf.formatVersion(self.version) in res

    def test_diffcal_table_workspacename(self):
        # Test name generation for diffraction-calibration output table
        res = self.instance.createDiffCalTableWorkspaceName(self.runNumber, self.useLiteMode, self.version)
        assert self.difc_name in res
        assert self.runNumber in res
        assert wnvf.formatVersion(self.version) in res

    def test_diffcal_mask_workspacename(self):
        # Test name generation for diffraction-calibration output mask
        res = self.instance._createDiffCalMaskWorkspaceName(self.runNumber, self.useLiteMode, self.version)
        assert self.difc_name in res
        assert self.runNumber in res
        assert wnvf.formatVersion(self.version) in res
        assert "mask" in res

    def test_normalization_workspacename(self):
        # Test name generation for normalization workspaces
        res = self.instance._createNormalizationWorkspaceName(self.runNumber, self.useLiteMode, self.version, False)
        assert self.runNumber in res
        assert wnvf.formatVersion(self.version) in res

    ## TESTS OF ACCESS METHODS

    def test_workspaceDoesExist_false(self):
        wsname = "_test_ws_"
        # if there is no workspace, return false
        assert self.instance.workspaceDoesExist(wsname) is False

    def test_workspaceDoesExist_true(self):
        wsname = "_test_ws_"
        # create the workspace
        self.create_dumb_workspace(wsname)
        # if there is a workspace, return true
        assert self.instance.workspaceDoesExist(wsname) is True

    def test_getWorkspaceForName_exists(self):
        wsname = "_test_ws_"
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        assert self.instance.getWorkspaceForName(wsname)

    def test_getWorkspaceForName_does_not_exist(self):
        wsname = "_test_ws_"
        assert not mtd.doesExist(wsname)
        assert not self.instance.getWorkspaceForName(wsname)

    def test_getCloneOfWorkspace_fails(self):
        wsname1 = "_test_ws_"
        wsname2 = "_cloned_ws_"
        assert not mtd.doesExist(wsname1)
        assert not mtd.doesExist(wsname2)
        with pytest.raises(ValueError, match="was not found in the Analysis Data Service"):
            self.instance.getCloneOfWorkspace(wsname1, wsname2)

    def test_getCloneOfWorkspace_works(self):
        wsname1 = "_test_ws_"
        wsname2 = "_cloned_ws_"
        self.create_dumb_workspace(wsname1)
        assert mtd.doesExist(wsname1)
        assert not mtd.doesExist(wsname2)
        ws = self.instance.getCloneOfWorkspace(wsname1, wsname2)
        # We dont pass around workspace pointers, those are not reliable
        assert ws == wsname2
        assert mtd.doesExist(wsname2)
        assert_wksp_almost_equal(
            Workspace1=wsname1,
            Workspace2=wsname2,
            rtol=self.rtolValue,
        )

    def test_updateNeutronCacheFromADS_noop(self):
        """ws is not in cache and not in ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        assert self.instance._loadedRuns == {}
        assert not mtd.doesExist(wsname)
        self.instance._updateNeutronCacheFromADS(runId, useLiteMode)
        self.instance._createRawNeutronWorkspaceName.assert_called_once_with(runId, useLiteMode)
        assert self.instance._loadedRuns == {}
        assert not mtd.doesExist(wsname)

    def test_updateNeutronCacheFromADS_both(self):
        """ws is in cache and in ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedRuns[(runId, useLiteMode)] = 1
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        self.instance._updateNeutronCacheFromADS(runId, useLiteMode)
        self.instance._createRawNeutronWorkspaceName.assert_called_once_with(runId, useLiteMode)
        assert self.instance._loadedRuns == {(runId, useLiteMode): 1}

    def test_updateNeutronCacheFromADS_cache_no_ADS(self):
        """ws is in cache but not ADS"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedRuns[(runId, useLiteMode)] = 1
        assert not mtd.doesExist(wsname)
        self.instance._updateNeutronCacheFromADS(runId, useLiteMode)
        self.instance._createRawNeutronWorkspaceName.assert_called_once_with(runId, useLiteMode)
        assert self.instance._loadedRuns == {}

    def test_updateNeutronCacheFromADS_ADS_no_cache(self):
        """ws is in ADS and not in cache"""
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.create_dumb_workspace(wsname)
        assert self.instance._loadedRuns == {}
        assert mtd.doesExist(wsname)
        self.instance._updateNeutronCacheFromADS(runId, useLiteMode)
        self.instance._createRawNeutronWorkspaceName.assert_called_once_with(runId, useLiteMode)
        assert self.instance._loadedRuns == {(runId, useLiteMode): 0}

    def test_updateGroupingCacheFromADS_noop(self):
        """ws is not in cache and not in ADS"""
        groupingScheme = "column"
        stateId = self.stateId
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, stateId, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        assert self.instance._loadedGroupings == {}
        assert not mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        self.instance._createGroupingWorkspaceName.assert_not_called()
        assert self.instance._loadedGroupings == {}
        assert not mtd.doesExist(wsname)

    def test_updateGroupingCacheFromADS_both(self):
        """ws is in cache and in ADS"""
        groupingScheme = "column"
        stateId = self.stateId
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, stateId, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedGroupings[key] = wsname
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        self.instance._createGroupingWorkspaceName.assert_not_called()
        assert self.instance._loadedGroupings == {key: wsname}

    def test_updateGroupingCacheFromADS_cache_no_ADS(self):
        """ws is in cache but not ADS"""
        groupingScheme = "column"
        stateId = self.stateId
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, stateId, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedGroupings[key] = wsname
        assert not mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        self.instance._createGroupingWorkspaceName.assert_not_called()
        assert self.instance._loadedGroupings == {}

    def test_updateGroupingCacheFromADS_ADS_no_cache(self):
        """ws is in ADS and not in cache"""
        groupingScheme = "column"
        stateId = self.stateId
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, stateId, useLiteMode)
        self.instance._createGroupingWorkspaceName = mock.Mock(return_value=wsname)
        self.create_dumb_workspace(wsname)
        assert self.instance._loadedGroupings == {}
        assert mtd.doesExist(wsname)
        self.instance._updateGroupingCacheFromADS(key, wsname)
        self.instance._createGroupingWorkspaceName.assert_not_called()
        assert self.instance._loadedGroupings == {key: wsname}

    def test_updateInstrumentCacheFromADS_noop(self):
        """ws is not in cache and not in ADS"""
        runId = "555"
        stateId = self.stateId
        useLiteMode = True
        key = self.instance._key(stateId, useLiteMode)
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        assert self.instance._loadedInstruments == {}
        assert not mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode, key)
        assert self.instance._loadedInstruments == {}
        assert not mtd.doesExist(wsname)

    def test_updateInstrumentCacheFromADS_both(self):
        """ws is in cache and in ADS"""
        runId = "555"
        stateId = self.stateId
        useLiteMode = True
        key = self.instance._key(stateId, useLiteMode)
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedInstruments[key] = wsname
        self.create_dumb_workspace(wsname)
        assert mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode, key)
        assert self.instance._loadedInstruments == {key: wsname}

    def test_updateInstrumentCacheFromADS_cache_no_ADS(self):
        """ws is in cache but not ADS"""
        runId = "555"
        stateId = self.stateId
        useLiteMode = True
        key = self.instance._key(stateId, useLiteMode)
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedInstruments[key] = wsname
        assert not mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode, key)
        assert self.instance._loadedInstruments == {}

    def test_updateInstrumentCacheFromADS_ADS_no_cache(self):
        """ws is in ADS and not in cache"""
        runId = "555"
        stateId = self.stateId
        useLiteMode = True
        key = self.instance._key(stateId, useLiteMode)
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.create_dumb_workspace(wsname)
        assert self.instance._loadedInstruments == {}
        assert mtd.doesExist(wsname)
        self.instance._updateInstrumentCacheFromADS(runId, useLiteMode, key)
        assert self.instance._loadedInstruments == {key: wsname}

    def test_rebuildNeutronCache(self):
        """Test the correct behavior of the rebuildNeutronCache method"""
        runId = "555"
        useLiteMode = True
        key = (runId, useLiteMode)
        self.instance._loadedRuns[key] = 1
        self.instance.rebuildNeutronCache()
        assert self.instance._loadedRuns == {}

    def test_rebuildGroupingCache(self):
        """Test the correct behavior of the rebuildGroupingCache method"""
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        key = self.instance._key(groupingScheme, runId, useLiteMode)
        self.instance._loadedGroupings[key] = wsname
        self.instance.rebuildGroupingCache()
        assert self.instance._loadedGroupings == {}

    def test_rebuildInstrumentCache(self):
        """Test the correct behavior of the rebuildInstrumentCache method"""
        stateId = self.stateId
        useLiteMode = True
        key = self.instance._key(stateId, useLiteMode)
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        self.instance._loadedInstruments[key] = wsname
        self.instance.rebuildInstrumentCache()
        assert self.instance._loadedInstruments == {}

    def test_rebuildCache(self):
        """Test the correct behavior of the rebuildCache method"""
        groupingScheme = "column"
        runId = "555"
        useLiteMode = True
        wsname = "_test_ws_"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=wsname)
        key = (runId, useLiteMode)
        self.instance._loadedRuns[key] = 1
        self.instance._loadedGroupings[self.instance._key(groupingScheme, runId, useLiteMode)] = wsname
        self.instance._loadedInstruments[key] = wsname
        self.instance.rebuildCache()
        assert self.instance._loadedRuns == {}
        assert self.instance._loadedGroupings == {}
        assert self.instance._loadedInstruments == {}

    def test_clearLiveDataCache(self):
        self.instance._createRawNeutronWorkspaceName = mock.Mock(
            side_effect=lambda runNumber, useLiteMode: f"_test_ws_{runNumber}_{useLiteMode}"
        )
        self.instance.deleteWorkspaceUnconditional = mock.Mock()
        startKeys = [("23456", True), ("23457", False), ("23458", True)]
        startRunCache = {k: 0 for k in startKeys}
        for useLiteMode in (True, False):
            runNumbers = ("12345", "12346", "12347", "12348", "12349")
            keys = [(runNumber, useLiteMode) for runNumber in runNumbers]
            self.instance._loadedRuns = startRunCache.copy()
            self.instance._loadedRuns.update({k: 0 for k in keys})

            self.instance._liveDataKeys = keys
            self.instance.clearLiveDataCache()
            assert self.instance._liveDataKeys == []
            assert self.instance._loadedRuns == startRunCache
            for k in keys:
                self.instance.deleteWorkspaceUnconditional.assert_called_with(
                    self.instance._createRawNeutronWorkspaceName(*k)
                )

    def test_workspaceTagFunctions(self):
        wsname = mtd.unique_name(prefix="testWorkspaceTags_")
        tagValues = DiffcalStateMetadata.values()
        properties = list(WorkspaceMetadata.model_json_schema()["properties"].keys())
        logName = properties[0]

        # Make sure correct error is thrown if workspace does not exist
        with pytest.raises(RuntimeError) as e1:
            tagResult = self.instance.getWorkspaceTag(workspaceName=wsname, logname=logName)
        assert f"Workspace {wsname} does not exist" in str(e1.value)

        with pytest.raises(RuntimeError) as e2:
            self.instance.setWorkspaceTag(workspaceName=wsname, logname=logName, logvalue=tagValues[1])
        assert f"Workspace {wsname} does not exist" in str(e2.value)

        # Make sure default tag value is unset
        self.create_dumb_workspace(wsname)
        tagResult = self.instance.getWorkspaceTag(workspaceName=wsname, logname=logName)
        assert tagResult == UNSET

        # Set and update the tag with all possible tag values
        for tag in tagValues:
            self.instance.setWorkspaceTag(workspaceName=wsname, logname=logName, logvalue=tag)
            tagResult = self.instance.getWorkspaceTag(workspaceName=wsname, logname=logName)
            assert tagResult == tag

    def test_writeWorkspaceMetadataAsTags(self):
        self.instance.setWorkspaceTag = mock.Mock()
        wsName = "testWsName"
        metadata = WorkspaceMetadata(diffcalState=DiffcalStateMetadata.ALTERNATE, liteDataCompressionTolerance=0.1)
        self.instance.writeWorkspaceMetadataAsTags(wsName, metadata)
        assert self.instance.setWorkspaceTag.call_count == 2  # one for each metadata field set
        self.instance.setWorkspaceTag.assert_called_with(wsName, "liteDataCompressionTolerance", 0.1)

    ## TEST CLEANUP METHODS

    def test_deleteWorkspace(self):
        wsname = mtd.unique_name(prefix="_test_ws_")
        mockAlgo = mock.Mock()
        self.instance.mantidSnapper.WashDishes = mockAlgo
        # wash the dish
        self.instance.deleteWorkspace(wsname)
        mockAlgo.assert_called_once_with(mock.ANY, Workspace=wsname)

    def test_deleteWorkspaceUnconditional(self):
        wsname = mtd.unique_name(prefix="_test_ws_")
        mockAlgo = mock.Mock()
        self.instance.mantidSnapper.DeleteWorkspace = mockAlgo
        # if the workspace does not exist, then don't do anything
        self.instance.deleteWorkspaceUnconditional(wsname)
        mockAlgo.assert_not_called()
        # make the workspace
        self.create_dumb_workspace(wsname)
        # if the workspace does exist, then delete it
        self.instance.deleteWorkspaceUnconditional(wsname)
        mockAlgo.assert_called_once_with(mock.ANY, Workspace=wsname)

    ## TEST FETCH METHODS

    def test_fetchWorkspace(self):
        """Test the correct behavior of the fetchWorkspace method"""
        self.clearoutWorkspaces()
        res = self.instance.fetchWorkspace(self.sampleWSFilePath, self.fetchedWSname, loader="")
        assert res == {
            "result": True,
            "loader": "LoadNexusProcessed",
            "workspace": self.fetchedWSname,
        }
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
            rtol=self.rtolValue,
        )

        # make sure it won't load same workspace name again
        self.instance.grocer.executeRecipe = mock.Mock(return_value="bad")
        assert mtd.doesExist(self.fetchedWSname)
        res = self.instance.fetchWorkspace(self.sampleWSFilePath, self.fetchedWSname, loader="")
        assert res == {
            "result": True,
            "loader": "cached",
            "workspace": self.fetchedWSname,
        }
        self.instance.grocer.executeRecipe.assert_not_called()
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
            rtol=self.rtolValue,
        )

    @mock.patch.object(inspect.getmodule(MantidSnapper), "logger")
    def test_fetch_failed(self, mockLogger):
        # this is some file that it can't load
        mockFilename = Resource.getPath("inputs/crystalInfo/blank_file.cif")
        with pytest.raises(AlgorithmException, match=".*Failed to recognize this file as an ASCII file.*"):
            self.instance.fetchWorkspace(mockFilename, self.fetchedWSname, loader="")
        errorMessage = mockLogger.error.mock_calls[0].args[0]
        assert self.fetchedWSname in errorMessage
        assert mockFilename in errorMessage

    def test_fetch_false(self):
        # this is some file that it can't load
        self.instance.grocer.executeRecipe = mock.Mock(return_value={"result": False})
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchWorkspace(self.sampleWSFilePath, self.fetchedWSname, loader="")
        assert self.fetchedWSname in str(e.value)
        assert self.sampleWSFilePath in str(e.value)

    def test_fetch_dirty_nexus_native(self):
        """Test the correct behavior when fetching raw nexus data"""

        self.instance.convertToLiteMode = mock.Mock()
        self.instance.mantidSnapper = mock.MagicMock()
        self.instance.mantidSnapper.CloneWorkspace = mock.MagicMock(
            side_effect=lambda _, **kwargs: CloneWorkspace(**kwargs)
        )
        self.instance.mantidSnapper.RenameWorkspaces = mock.MagicMock(
            side_effect=lambda _, **kwargs: RenameWorkspaces(**kwargs)
        )
        self.instance.mantidSnapper.RenameWorkspace = mock.MagicMock(
            side_effect=lambda _, **kwargs: RenameWorkspace(**kwargs)
        )
        testCalibrationData = DAOFactory.calibrationParameters()
        self.instance.dataService.generateInstrumentState = mock.MagicMock(
            return_value=testCalibrationData.instrumentState
        )
        self.instance.mantidSnapper.mtd = mtd

        # mock out _createNeutronFilePath so that only a native version exists on disk
        nonExistentPath = Path("does/not/exist.nxs")
        assert not nonExistentPath.exists()
        self.instance._createNeutronFilePath = mock.Mock(
            side_effect=lambda _runNumber, useLiteMode: Path(self.sampleWSFilePath)
            if not useLiteMode
            else nonExistentPath
        )

        def generateTestItem(deep=False):  # noqa: ARG001
            return mock.Mock(
                spec=GroceryListItem,
                runNumber=self.runNumber,
                useLiteMode=False,
                state="stateId",
                loader="",
                liveDataArgs=None,
                diffCalVersion=None,
                diffCalFilePath=None,
            )

        testItem = generateTestItem()
        testItem.model_copy.side_effect = generateTestItem

        # ensure a clean ADS
        workspaceName = self.instance._createNeutronWorkspaceName(testItem.runNumber, testItem.useLiteMode)
        rawWorkspaceName = self.instance._createRawNeutronWorkspaceName(testItem.runNumber, testItem.useLiteMode)
        assert len(self.instance._loadedRuns) == 0
        assert not mtd.doesExist(workspaceName)
        assert not mtd.doesExist(rawWorkspaceName)

        # test that a nexus workspace can be loaded
        res = self.instance.fetchNeutronDataSingleUse(testItem)
        assert len(res) > 0
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceName
        assert len(self.instance._loadedRuns) == 0
        # assert the correct workspaces exist
        assert not mtd.doesExist(rawWorkspaceName)
        assert mtd.doesExist(workspaceName)

        # test the workspace readback matches
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
            rtol=self.rtolValue,
        )

        # test that it will use a raw workspace if one exists
        # first clear out the ADS
        self.clearoutWorkspaces()
        assert not mtd.doesExist(rawWorkspaceName)
        assert not mtd.doesExist(workspaceName)
        # create a clean version so a raw exists in cache
        res = self.instance.fetchNeutronDataCached(testItem)
        assert self.instance.mantidSnapper.CloneWorkspace.call_count == 1
        assert mtd.doesExist(rawWorkspaceName), (
            f"Raw workspace {rawWorkspaceName} not available in {mtd.getObjectNames()}"
        )
        assert not mtd.doesExist(workspaceName)
        assert len(self.instance._loadedRuns) == 1
        testKey = self.instance._key(testItem.runNumber, testItem.useLiteMode)
        assert self.instance._loadedRuns == {testKey: 1}
        # now load a dirty version, which should clone the raw
        res = self.instance.fetchNeutronDataSingleUse(testItem)
        assert mtd.doesExist(workspaceName)
        assert res["workspace"] == workspaceName
        assert res["loader"] == "cached"  # this indicates it used a cached value
        assert self.instance._loadedRuns == {testKey: 1}  # the number of copies did not increment
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=res["workspace"],
            rtol=self.rtolValue,
        )

        # test calling with Lite data, that it will call to lite service
        def generateTestItem(deep=False):  # noqa: ARG001
            m = mock.Mock(
                spec=GroceryListItem,
                runNumber=self.runNumber,
                useLiteMode=True,
                state="stateId",
                loader="",
                liveDataArgs=None,
                diffCalVersion=None,
                diffCalFilePath=None,
            )
            m.model_copy.side_effect = generateTestItem
            return m

        liteItem = generateTestItem()

        testKeyLite = self.instance._key(liteItem.runNumber, liteItem.useLiteMode)
        res = self.instance.fetchNeutronDataSingleUse(liteItem)
        assert self.instance._loadedRuns.get(testKeyLite) is None
        workspaceNameLite = self.instance._createNeutronWorkspaceName(liteItem.runNumber, liteItem.useLiteMode)
        self.instance.convertToLiteMode.assert_called_once_with(workspaceNameLite, export=True)
        assert mtd.doesExist(workspaceNameLite)

    def test_fetch_cached_native(self):
        """Test the correct behavior when fetching nexus data"""
        self.instance._createNeutronFilePath = mock.Mock()
        self.instance.dataService.hasLiveDataConnection = mock.Mock(return_value=False)
        self.instance.mantidSnapper = mock.MagicMock()
        self.instance.mantidSnapper.CloneWorkspace = mock.MagicMock(
            side_effect=lambda _, **kwargs: CloneWorkspace(**kwargs)
        )
        self.instance.mantidSnapper.RenameWorkspaces = mock.MagicMock(
            side_effect=lambda _, **kwargs: RenameWorkspaces(**kwargs)
        )
        self.instance.mantidSnapper.RenameWorkspace = mock.MagicMock(
            side_effect=lambda _, **kwargs: RenameWorkspace(**kwargs)
        )
        self.instance.mantidSnapper.mtd = mtd

        testCalibrationData = DAOFactory.calibrationParameters()
        self.instance.dataService.generateInstrumentState = mock.MagicMock(
            return_value=testCalibrationData.instrumentState
        )

        def generateTestItem(deep=False):  # noqa: ARG001
            return mock.Mock(
                spec=GroceryListItem,
                runNumber=self.runNumber,
                useLiteMode=False,
                loader="",
                state="stateId",
                liveDataArgs=None,
                diffCalVersion=None,
                diffCalFilePath=None,
            )

        testItem = generateTestItem()
        testItem.model_copy.side_effect = generateTestItem
        testKey = self.instance._key(testItem.runNumber, testItem.useLiteMode)

        workspaceNameRaw = self.instance._createRawNeutronWorkspaceName(testItem.runNumber, testItem.useLiteMode)
        workspaceNameCopy1 = self.instance._createCopyNeutronWorkspaceName(testItem.runNumber, testItem.useLiteMode, 1)
        workspaceNameCopy2 = self.instance._createCopyNeutronWorkspaceName(testItem.runNumber, testItem.useLiteMode, 2)

        # make sure the ADS is clean
        self.clearoutWorkspaces()
        for ws in [workspaceNameRaw, workspaceNameCopy1, workspaceNameCopy2]:
            assert not mtd.doesExist(ws)
        assert len(self.instance._loadedRuns) == 0

        # run with nothing in cache and bad filename -- it will fail
        self.instance._createNeutronFilePath = mock.Mock(return_value=Path("not/a/real/file.txt"))
        assert not self.instance._createNeutronFilePath.return_value.exists()
        with pytest.raises(
            RuntimeError, match=r".*is not present on disk, and no live-data connection is available.*"
        ) as e:
            self.instance.fetchNeutronDataCached(testItem)
        assert self.runNumber in str(e.value)

        # mock filename to point at test file
        self.instance._createNeutronFilePath.return_value = Path(self.sampleWSFilePath)

        # run with nothing loaded -- it will find the file and load it
        res = self.instance.fetchNeutronDataCached(testItem)
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceNameCopy1
        assert self.instance._loadedRuns == {testKey: 1}
        # assert the correct workspaces exist: a raw and a copy
        assert mtd.doesExist(workspaceNameRaw), (
            f"Raw workspace {workspaceNameRaw} not available in {mtd.getObjectNames()}"
        )
        assert mtd.doesExist(workspaceNameCopy1), (
            f"Copy workspace {workspaceNameCopy1} not available in {mtd.getObjectNames()}"
        )
        # test the workspace is correct
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=workspaceNameCopy1,
            rtol=self.rtolValue,
        )

        # run with a raw workspace in cache -- it will copy it
        res = self.instance.fetchNeutronDataCached(testItem)
        assert res["result"]
        assert res["loader"] == "cached"  # indicates no loader called
        assert res["workspace"] == workspaceNameCopy2
        assert self.instance._loadedRuns == {testKey: 2}
        # assert the correct workspaces exist: a raw and two copies
        assert mtd.doesExist(workspaceNameRaw)
        assert mtd.doesExist(workspaceNameCopy1)
        assert mtd.doesExist(workspaceNameCopy2)
        assert_wksp_almost_equal(
            Workspace1=self.sampleWS,
            Workspace2=workspaceNameCopy2,
            rtol=self.rtolValue,
        )

    def test_fetch_cached_lite(self):
        """
        Test the correct behavior when fetching nexus data in Lite mode.
        The cases where the data is cached or the file exists are tested above.
        This tests cases of using native-resolution data and auto-reducing.
        """
        self.instance.convertToLiteMode = mock.Mock()
        self.instance._createNeutronFilePath = mock.Mock()
        self.instance.dataService.hasLiveDataConnection = mock.Mock(return_value=False)
        self.instance.mantidSnapper = mock.MagicMock()
        self.instance.mantidSnapper.CloneWorkspace = mock.MagicMock(
            side_effect=lambda _, **kwargs: CloneWorkspace(**kwargs)
        )
        self.instance.mantidSnapper.RenameWorkspaces = mock.MagicMock(
            side_effect=lambda _, **kwargs: RenameWorkspaces(**kwargs)
        )
        self.instance.mantidSnapper.RenameWorkspace = mock.MagicMock(
            side_effect=lambda _, **kwargs: RenameWorkspace(**kwargs)
        )
        self.instance.mantidSnapper.mtd = mtd

        testCalibrationData = DAOFactory.calibrationParameters()
        self.instance.dataService.generateInstrumentState = mock.MagicMock(
            return_value=testCalibrationData.instrumentState
        )

        def generateTestItem(deep=False):  # noqa: ARG001
            m = mock.Mock(
                spec=GroceryListItem,
                runNumber=self.runNumber,
                useLiteMode=True,
                state="stateId",
                loader="",
                liveDataArgs=None,
                diffCalVersion=None,
                diffCalFilePath=None,
            )
            m.model_copy.side_effect = generateTestItem
            return m

        testItem = generateTestItem()

        testKey = self.instance._key(testItem.runNumber, testItem.useLiteMode)

        def generateTestItem(deep=False):  # noqa: ARG001
            return mock.Mock(
                spec=GroceryListItem,
                runNumber=self.runNumber,
                useLiteMode=False,
                state="stateId",
                loader="",
                liveDataArgs=None,
                diffCalVersion=None,
            )

        nativeItem = generateTestItem()
        nativeItem.model_copy.side_effect = generateTestItem

        nativeKey = self.instance._key(nativeItem.runNumber, nativeItem.useLiteMode)

        workspaceNameNativeRaw = self.instance._createRawNeutronWorkspaceName(
            nativeItem.runNumber, nativeItem.useLiteMode
        )
        workspaceNameLiteRaw = self.instance._createRawNeutronWorkspaceName(testItem.runNumber, testItem.useLiteMode)
        workspaceNameLiteCopy1 = self.instance._createCopyNeutronWorkspaceName(
            testItem.runNumber, testItem.useLiteMode, 1
        )

        # make sure the ADS is clean
        self.clearoutWorkspaces()
        for ws in [workspaceNameNativeRaw, workspaceNameLiteRaw, workspaceNameLiteCopy1]:
            assert not mtd.doesExist(ws)
        assert len(self.instance._loadedRuns) == 0

        # test that trying to load data from a fake file fails
        # will reach the final "else" statement
        fakeFilePath = Path("not/a/real/file.txt")
        self.instance._createNeutronFilePath.return_value = fakeFilePath
        assert not self.instance._createNeutronFilePath.return_value.exists()
        with pytest.raises(
            RuntimeError, match=r".*is not present on disk, and no live-data connection is available.*"
        ) as e:
            self.instance.fetchNeutronDataCached(testItem)
        assert self.runNumber in str(e.value)

        # mock filename -- create situation where Lite file does not exist, native does
        self.instance._createNeutronFilePath.side_effect = [
            fakeFilePath,  # called in the assert below
            fakeFilePath,  # called when testing for Lite on disk
            Path(self.sampleWSFilePath),  # called when testing for native on disk
        ]
        assert not self.instance._createNeutronFilePath(testItem.runNumber, testItem.useLiteMode).exists()

        # there is no lite file and nothing cached
        # load native resolution from file, then clone/reduce the native data
        testItem.useLiteMode = True
        res = self.instance.fetchNeutronDataCached(testItem)
        assert res["result"]
        assert res["loader"] == "LoadNexusProcessed"
        assert res["workspace"] == workspaceNameLiteCopy1
        for ws in [workspaceNameNativeRaw, workspaceNameLiteRaw, workspaceNameLiteCopy1]:
            assert mtd.doesExist(ws), f"Workspace {ws} does not exist in {mtd.getObjectNames()}"
        assert self.instance._loadedRuns == {testKey: 1, nativeKey: 0}
        # make sure it calls to convert to lite mode
        self.instance.convertToLiteMode.assert_called_once()
        self.instance.convertToLiteMode.reset_mock()

        # clear out the Lite workspaces from ADS and the cache
        self.instance._createNeutronFilePath.side_effect = [fakeFilePath, Path(self.sampleWSFilePath)]
        for ws in [workspaceNameLiteRaw, workspaceNameLiteCopy1]:
            DeleteWorkspace(ws)
            assert not mtd.doesExist(ws)
        del self.instance._loadedRuns[testKey]

        # there is no lite file and no cached lite workspace
        # but there is a cached native workspace
        # then clone/reduce the native workspace
        assert mtd.doesExist(workspaceNameNativeRaw)
        assert self.instance._loadedRuns == {nativeKey: 0}
        res = self.instance.fetchNeutronDataCached(testItem)
        assert res["result"]
        assert res["loader"] == "cached"
        assert res["workspace"] == workspaceNameLiteCopy1
        for ws in [workspaceNameNativeRaw, workspaceNameLiteRaw, workspaceNameLiteCopy1]:
            assert mtd.doesExist(ws)
        assert self.instance._loadedRuns == {testKey: 1, nativeKey: 0}
        # make sure it calls to convert to lite mode
        self.instance.convertToLiteMode.assert_called_once()

    @mock.patch.object(inspect.getmodule(GroceryService), "FileLoaderRegistry")
    def test_fetchNeutronDataSingleUse(self, mockFileLoaderRegistry):
        # Test all 16, non-live-data cases (including each setting of the `Config["nexus.dataFormat.event"]` flag).
        # Note: this test does not test "in cache" cases: those are treated elsewhere

        def mockIAlgorithm(name: str) -> mock.Mock:
            mock_ = mock.Mock()
            mock_.name.return_value = name
            return mock_

        for nexus_dataFormat_event in (False, True):
            # Treat the two possible settings of the `Config["nexus.dataFormat.event"]` flag.

            with Config_override("nexus.dataFormat.event", nexus_dataFormat_event):
                mockFileLoaderRegistry.Instance.return_value.chooseLoader.return_value = mockIAlgorithm(
                    "LoadEventNexus" if Config["nexus.dataFormat.event"] else "LoadNexusProcessed"
                )

                runNumber = self.runNumber

                for flags in itertools.product((False, True), repeat=4):
                    useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

                    instance = GroceryService()
                    liteModeFilePath = mock.MagicMock(spec=Path)
                    liteModeFilePath.__str__.return_value = "liteModeFilePath"

                    nativeModeFilePath = mock.MagicMock(spec=Path)
                    nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

                    testCalibrationData = DAOFactory.calibrationParameters()
                    instance.dataService.generateInstrumentState = mock.MagicMock(
                        return_value=testCalibrationData.instrumentState
                    )

                    # In order to avoid contaminating other tests, all mocks must be applied
                    #   as context managers.
                    with (
                        mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                        mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,
                        mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                        mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                        mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                        mock.patch.object(
                            instance, "_createRawNeutronWorkspaceName"
                        ) as mockCreateRawNeutronWorkspaceName,
                        mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                        mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                        mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                        mock.patch.object(instance, "mantidSnapper") as mockSnapper,  # noqa F841
                    ):
                        # Mocks for flow-control branching:
                        mockHasLiveDataConnection.return_value = False

                        if nativeInCache:
                            mockRunsCache[(runNumber, False)] = 0

                        liteModeFilePath.exists.return_value = liteOnDisk
                        nativeModeFilePath.exists.return_value = nativeOnDisk

                        mockCreateNeutronFilePath.side_effect = (
                            lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                        )

                        mockCreateNeutronWorkspaceName.side_effect = (
                            lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                            if useLiteMode
                            else mock.sentinel.nativeWorkspaceName
                        )

                        mockCreateRawNeutronWorkspaceName.side_effect = (
                            lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                            if useLiteMode
                            else mock.sentinel.nativeRawWorkspaceName
                        )

                        mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                        workspaceName = mockCreateNeutronWorkspaceName(runNumber, useLiteMode)

                        # Action-related mocks:
                        mockFetchGroceriesRecipe.executeRecipe.return_value = {
                            "result": True,
                            "loader": mock.sentinel.loader,
                            "workspace": workspaceName,
                        }

                        # BUILD the item argument:
                        item = GroceryListItem(
                            workspaceType="neutron",
                            runNumber=runNumber,
                            useLiteMode=useLiteMode,
                            loader="",
                            liveDataArgs=None,
                        )
                        # LOADER and LOADERARGS will be overridden on the `LoadEventNexus` call,
                        #   when `Config['nexus.dataFormat.event']` is set:
                        loader = ""
                        loaderArgs = "{}"
                        if Config["nexus.dataFormat.event"]:
                            loader = "LoadEventNexus"
                            loaderArgs = '{"NumberOfBins": 1}'

                        exceptionRaised = None
                        result = None
                        try:
                            result = instance.fetchNeutronDataSingleUse(item)
                        except RuntimeError as e:
                            print(f"EXCEPTION: RuntimeError: {e}")
                            exceptionRaised = e

                        match (useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk):
                            case (False, True, _, _):
                                # native mode and native in cache:
                                #   not tested _here_.
                                mockUpdateNeutronCache.assert_called_with(runNumber, False)
                                continue

                            case (True, _, True, _):
                                # lite mode and lite-mode exists on disk
                                assert result == mockFetchGroceriesRecipe.executeRecipe.return_value
                                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                                    str(liteModeFilePath), workspaceName, loader, loaderArgs=loaderArgs
                                )
                                mockConvertToLiteMode.assert_not_called()
                                mockUpdateNeutronCache.assert_called_with(runNumber, True)

                            case (True, True, _, _):
                                # lite mode and native is cached
                                assert result == {
                                    "fromNative": mock.sentinel.nativeWorkspaceName,
                                    "result": True,
                                    "loader": "cached",
                                    "workspace": workspaceName,
                                }
                                mockGetCloneOfWorkspace.assert_has_calls(
                                    [
                                        mock.call(
                                            mockCreateRawNeutronWorkspaceName(runNumber, False),
                                            mock.sentinel.nativeWorkspaceName,
                                        ),
                                        mock.call(mock.sentinel.nativeWorkspaceName, workspaceName),
                                    ]
                                )
                                mockConvertToLiteMode.assert_called_once_with(workspaceName, export=True)
                                mockUpdateNeutronCache.assert_has_calls(
                                    [mock.call(runNumber, True), mock.call(runNumber, False)]
                                )

                            case (True, _, _, True):
                                # lite mode and native exists on disk
                                assert result == mockFetchGroceriesRecipe.executeRecipe.return_value
                                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                                    str(nativeModeFilePath),
                                    mockCreateNeutronWorkspaceName(runNumber, False),
                                    loader,
                                    loaderArgs=loaderArgs,
                                )
                                mockConvertToLiteMode.assert_called_once_with(workspaceName, export=True)
                                mockUpdateNeutronCache.assert_has_calls(
                                    [mock.call(runNumber, True), mock.call(runNumber, False)]
                                )

                            case (False, _, _, True):
                                # native mode and native exists on disk
                                assert result == mockFetchGroceriesRecipe.executeRecipe.return_value
                                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                                    str(nativeModeFilePath), workspaceName, loader, loaderArgs=loaderArgs
                                )
                                mockConvertToLiteMode.assert_not_called()
                                mockUpdateNeutronCache.assert_called_with(runNumber, False)

                            case _:
                                # live data fallback: NOT tested here
                                assert result is None
                                mockFetchGroceriesRecipe.executeRecipe.assert_not_called()
                                mockGetCloneOfWorkspace.assert_not_called()
                                mockConvertToLiteMode.assert_not_called()
                                assert isinstance(exceptionRaised, RuntimeError)
                                assert (
                                    f"Neutron data for run '{runNumber}' is not present on disk, "
                                    + "and no live-data connection is available."
                                    == str(exceptionRaised)
                                )
                                mockUpdateNeutronCache.assert_called_with(runNumber, False)

    def test_fetchNeutronDataSingleUse_live_data(self):
        # Test live data, non-fallback cases.
        runNumber = self.runNumber

        for useLiteMode in (False, True):
            flags = useLiteMode, False, False, False
            useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

            instance = GroceryService()
            liteModeFilePath = mock.MagicMock(spec=Path)
            liteModeFilePath.__str__.return_value = "liteModeFilePath"

            nativeModeFilePath = mock.MagicMock(spec=Path)
            nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

            now_ = datetime.datetime.utcnow()
            duration = datetime.timedelta(seconds=42)
            expectedStartTime = now_ - duration

            expectedLoaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "PreserveEvents": True,
                "StartTime": expectedStartTime.isoformat(),
            }

            testCalibrationData = DAOFactory.calibrationParameters()
            instance.dataService.generateInstrumentState = mock.MagicMock(
                return_value=testCalibrationData.instrumentState
            )

            # In order to avoid contaminating other tests, all mocks must be applied
            #   as context managers.
            with (
                mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,  # noqa: F841
                mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                mock.patch.object(instance, "_createRawNeutronWorkspaceName") as mockCreateRawNeutronWorkspaceName,
                mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                mock.patch.object(instance, "deleteWorkspaceUnconditional") as mockDeleteWorkspace,  # noqa: F841
                mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                mock.patch(ThisService + "datetime", wraps=datetime.datetime) as mockDatetime,
                mock.patch(ThisService + "RunMetadata") as mockRunMetadata,
                mock.patch.object(instance, "mantidSnapper") as mockSnapper,
            ):
                mockDatetime.utcnow.return_value = now_

                # Mocks for flow-control branching:
                mockHasLiveDataConnection.return_value = True

                liteModeFilePath.exists.return_value = liteOnDisk
                nativeModeFilePath.exists.return_value = nativeOnDisk

                mockCreateNeutronFilePath.side_effect = (
                    lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                )

                mockCreateNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeWorkspaceName
                )

                mockCreateRawNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeRawWorkspaceName
                )

                mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                # Initial output ws from livedata will always be native before it gets converted to lite mode.
                workspaceName = mockCreateNeutronWorkspaceName(runNumber, False)

                # Mock `RunMetadata.fromRun` to return `RunMetadata` that has the correct run number.
                mockRunMetadata.fromRun = mock.Mock(return_value=self.mockRunMetadata(runNumber))
                mockRunMetadata.FROM_NOW_ISO8601 = RunMetadata.FROM_NOW_ISO8601
                mockRunMetadata.FROM_START_ISO8601 = RunMetadata.FROM_START_ISO8601

                def _valid_key(key: str, validKey: str):
                    if key != validKey:
                        raise KeyError(f"key '{key}', expecting '{validKey}'")
                    return True

                mockWs = mock.Mock()
                mockWs.getRun.return_value = mock.sentinel.run
                mockSnapper.mtd.__getitem__.side_effect = (
                    lambda wsName: mockWs if _valid_key(wsName, workspaceName) else None
                )

                # Action-related mocks:
                mockFetchGroceriesRecipe.executeRecipe.return_value = {
                    "result": True,
                    "loader": "LoadLiveDataInterval",
                    "workspace": workspaceName,
                }

                # BUILD the item argument:
                item = GroceryListItem(
                    workspaceType="neutron",
                    runNumber=runNumber,
                    useLiteMode=useLiteMode,
                    state="stateId",
                    loader="",
                    liveDataArgs=LiveDataArgs(duration=duration),
                )

                exceptionRaised = None
                result = None  # noqa: F841
                try:
                    result = instance.fetchNeutronDataSingleUse(item)  # noqa: F841
                except RuntimeError as e:
                    exceptionRaised = e

                assert exceptionRaised is None
                mockUpdateNeutronCache.assert_called_with(runNumber, False)
                mockHasLiveDataConnection.assert_called_once()
                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                    workspace=workspaceName,
                    loader="LoadLiveDataInterval",
                    loaderArgs=json.dumps(expectedLoaderArgs),
                )
                if useLiteMode:
                    workspaceLiteName = mockCreateNeutronWorkspaceName(runNumber, True)
                    mockConvertToLiteMode.assert_called_once_with(workspaceLiteName, export=False)
                else:
                    mockConvertToLiteMode.assert_not_called()

    def test_fetchNeutronDataSingleUse_live_data_transition(self):
        # Test live data, non-fallback cases.
        runNumber = self.runNumber

        for useLiteMode in (False, True):
            flags = useLiteMode, False, False, False
            useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

            instance = GroceryService()
            liteModeFilePath = mock.MagicMock(spec=Path)
            liteModeFilePath.__str__.return_value = "liteModeFilePath"

            nativeModeFilePath = mock.MagicMock(spec=Path)
            nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

            now_ = datetime.datetime.utcnow()
            duration = datetime.timedelta(seconds=42)
            expectedStartTime = now_ - duration

            expectedLoaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "PreserveEvents": True,
                "StartTime": expectedStartTime.isoformat(),
            }

            testCalibrationData = DAOFactory.calibrationParameters()
            instance.dataService.generateInstrumentState = mock.MagicMock(
                return_value=testCalibrationData.instrumentState
            )

            # In order to avoid contaminating other tests, all mocks must be applied
            #   as context managers.
            with (
                mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,  # noqa: F841
                mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                mock.patch.object(instance, "_createRawNeutronWorkspaceName") as mockCreateRawNeutronWorkspaceName,
                mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                mock.patch.object(instance, "deleteWorkspaceUnconditional") as mockDeleteWorkspace,
                mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                mock.patch(ThisService + "datetime", wraps=datetime.datetime) as mockDatetime,
                mock.patch(ThisService + "RunMetadata") as mockRunMetadata,
                mock.patch.object(instance, "mantidSnapper") as mockSnapper,
            ):
                mockDatetime.utcnow.return_value = now_

                # Mocks for flow-control branching:
                mockHasLiveDataConnection.return_value = True

                liteModeFilePath.exists.return_value = liteOnDisk
                nativeModeFilePath.exists.return_value = nativeOnDisk

                mockCreateNeutronFilePath.side_effect = (
                    lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                )

                mockCreateNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeWorkspaceName
                )

                mockCreateRawNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeRawWorkspaceName
                )

                mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                # Initial output ws from livedata will always be native before it gets converted to lite mode.
                workspaceName = mockCreateNeutronWorkspaceName(runNumber, False)

                # Mock `RunMetadata.fromRun` to return `RunMetadata` that DOES NOT HAVE the correct run number.
                mockRunMetadata.fromRun = mock.Mock(return_value=self.mockRunMetadata(str(int(runNumber) + 1)))
                mockRunMetadata.FROM_NOW_ISO8601 = RunMetadata.FROM_NOW_ISO8601
                mockRunMetadata.FROM_START_ISO8601 = RunMetadata.FROM_START_ISO8601

                def _valid_key(key: str, validKey: str):
                    assert key == validKey, f"key '{key}', expecting '{validKey}'"
                    return True

                mockWs = mock.Mock()
                mockWs.getRun.return_value = mock.sentinel.run
                mockSnapper.mtd.__getitem__.side_effect = (
                    lambda wsName: mockWs if _valid_key(wsName, workspaceName) else None
                )

                # Action-related mocks:
                mockFetchGroceriesRecipe.executeRecipe.return_value = {
                    "result": True,
                    "loader": "LoadLiveDataInterval",
                    "workspace": workspaceName,
                }

                # BUILD the item argument:
                item = GroceryListItem(
                    workspaceType="neutron",
                    runNumber=runNumber,
                    useLiteMode=useLiteMode,
                    loader="",
                    liveDataArgs=LiveDataArgs(duration=duration),
                )

                exceptionRaised = None
                result = None  # noqa: F841
                try:
                    result = instance.fetchNeutronDataSingleUse(item)  # noqa: F841
                except LiveDataState as e:  # noqa: BLE001
                    exceptionRaised = e

                assert (
                    exceptionRaised.model == LiveDataState.runStateTransition(str(int(runNumber) + 1), runNumber).model
                )
                mockUpdateNeutronCache.assert_called_with(runNumber, False)
                mockHasLiveDataConnection.assert_called_once()
                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                    workspace=workspaceName,
                    loader="LoadLiveDataInterval",
                    loaderArgs=json.dumps(expectedLoaderArgs),
                )
                mockConvertToLiteMode.assert_not_called()
                mockDeleteWorkspace.assert_called_once_with(workspaceName)

    def test_fetchNeutronDataSingleUse_live_data_fallback(self):
        # Test live data fallback cases.
        runNumber = self.runNumber

        for useLiteMode in (False, True):
            flags = useLiteMode, False, False, False
            useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

            instance = GroceryService()
            liteModeFilePath = mock.MagicMock(spec=Path)
            liteModeFilePath.__str__.return_value = "liteModeFilePath"

            nativeModeFilePath = mock.MagicMock(spec=Path)
            nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

            expectedLoaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "PreserveEvents": True,
                "StartTime": RunMetadata.FROM_START_ISO8601,
            }

            testCalibrationData = DAOFactory.calibrationParameters()
            instance.dataService.generateInstrumentState = mock.MagicMock(
                return_value=testCalibrationData.instrumentState
            )

            # In order to avoid contaminating other tests, all mocks must be applied
            #   as context managers.
            with (
                mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,  # noqa: F841
                mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                mock.patch.object(instance, "_createRawNeutronWorkspaceName") as mockCreateRawNeutronWorkspaceName,
                mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                mock.patch.object(instance, "deleteWorkspaceUnconditional") as mockDeleteWorkspace,  # noqa: F841
                mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                mock.patch(ThisService + "RunMetadata") as mockRunMetadata,
                mock.patch.object(instance, "mantidSnapper") as mockSnapper,
            ):
                # Mocks for flow-control branching:
                mockHasLiveDataConnection.return_value = True

                liteModeFilePath.exists.return_value = liteOnDisk
                nativeModeFilePath.exists.return_value = nativeOnDisk

                mockCreateNeutronFilePath.side_effect = (
                    lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                )

                mockCreateNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeWorkspaceName
                )

                mockCreateRawNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeRawWorkspaceName
                )

                mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                # Live data is always initially native until it is converted to lite mode.
                workspaceName = mockCreateNeutronWorkspaceName(runNumber, False)

                # Mock `RunMetadata.fromRun` to return `RunMetadata` that has the correct run number.
                mockRunMetadata.fromRun = mock.Mock(return_value=self.mockRunMetadata(runNumber))
                mockRunMetadata.FROM_NOW_ISO8601 = RunMetadata.FROM_NOW_ISO8601
                mockRunMetadata.FROM_START_ISO8601 = RunMetadata.FROM_START_ISO8601

                def _valid_key(key: str, validKey: str):
                    if key != validKey:
                        raise KeyError(f"key '{key}', expecting '{validKey}'")
                    return True

                mockWs = mock.Mock()
                mockWs.getRun.return_value = mock.sentinel.run
                mockSnapper.mtd.__getitem__.side_effect = (
                    lambda wsName: mockWs if _valid_key(wsName, workspaceName) else None
                )

                # Action-related mocks:
                mockFetchGroceriesRecipe.executeRecipe.return_value = {
                    "result": True,
                    "loader": "LoadLiveDataInterval",
                    "workspace": workspaceName,
                }

                # BUILD the item argument:
                item = GroceryListItem(
                    workspaceType="neutron", runNumber=runNumber, useLiteMode=useLiteMode, state="stateId", loader=""
                )

                exceptionRaised = None
                result = None  # noqa: F841
                try:
                    result = instance.fetchNeutronDataSingleUse(item)  # noqa: F841
                except RuntimeError as e:
                    exceptionRaised = e

                assert exceptionRaised is None
                mockUpdateNeutronCache.assert_called_with(runNumber, False)
                mockHasLiveDataConnection.assert_called_once()
                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                    workspace=workspaceName,
                    loader="LoadLiveDataInterval",
                    loaderArgs=json.dumps(expectedLoaderArgs),
                )
                if useLiteMode:
                    workspaceLiteName = mockCreateNeutronWorkspaceName(runNumber, True)
                    mockConvertToLiteMode.assert_called_once_with(workspaceLiteName, export=False)
                else:
                    mockConvertToLiteMode.assert_not_called()

    def test_fetchNeutronDataSingleUse_live_data_fallback_fails(self):
        # Test live data, non-fallback cases.
        runNumber = self.runNumber

        for useLiteMode in (False, True):
            flags = useLiteMode, False, False, False
            useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

            instance = GroceryService()
            liteModeFilePath = mock.MagicMock(spec=Path)
            liteModeFilePath.__str__.return_value = "liteModeFilePath"

            nativeModeFilePath = mock.MagicMock(spec=Path)
            nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

            expectedLoaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "PreserveEvents": True,
                "StartTime": RunMetadata.FROM_START_ISO8601,
            }

            testCalibrationData = DAOFactory.calibrationParameters()
            instance.dataService.generateInstrumentState = mock.MagicMock(
                return_value=testCalibrationData.instrumentState
            )

            # In order to avoid contaminating other tests, all mocks must be applied
            #   as context managers.
            with (
                mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,  # noqa: F841
                mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                mock.patch.object(instance, "_createRawNeutronWorkspaceName") as mockCreateRawNeutronWorkspaceName,
                mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                mock.patch.object(instance, "deleteWorkspaceUnconditional") as mockDeleteWorkspace,
                mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                mock.patch(ThisService + "RunMetadata") as mockRunMetadata,
                mock.patch.object(instance, "mantidSnapper") as mockSnapper,
            ):
                # Mocks for flow-control branching:
                mockHasLiveDataConnection.return_value = True

                liteModeFilePath.exists.return_value = liteOnDisk
                nativeModeFilePath.exists.return_value = nativeOnDisk

                mockCreateNeutronFilePath.side_effect = (
                    lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                )

                mockCreateNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeWorkspaceName
                )

                mockCreateRawNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeRawWorkspaceName
                )

                mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                # Live data is always initially native until it is converted to lite mode.
                workspaceName = mockCreateNeutronWorkspaceName(runNumber, False)

                # Mock `RunMetadata.fromRun` to return `RunMetadata` that DOES NOT HAVE the correct run number.
                mockRunMetadata.fromRun = mock.Mock(return_value=self.mockRunMetadata(str(int(runNumber) + 1)))
                mockRunMetadata.FROM_NOW_ISO8601 = RunMetadata.FROM_NOW_ISO8601
                mockRunMetadata.FROM_START_ISO8601 = RunMetadata.FROM_START_ISO8601

                def _valid_key(key: str, validKey: str):
                    if key != validKey:
                        raise KeyError(f"key '{key}', expecting '{validKey}'")
                    return True

                mockWs = mock.Mock()
                mockWs.getRun.return_value = mock.sentinel.run
                mockSnapper.mtd.__getitem__.side_effect = (
                    lambda wsName: mockWs if _valid_key(wsName, workspaceName) else None
                )

                # Action-related mocks:
                mockFetchGroceriesRecipe.executeRecipe.return_value = {
                    "result": True,
                    "loader": "LoadLiveDataInterval",
                    "workspace": workspaceName,
                }

                # BUILD the item argument:
                item = GroceryListItem(workspaceType="neutron", runNumber=runNumber, useLiteMode=useLiteMode, loader="")

                exceptionRaised = None
                result = None  # noqa: F841
                try:
                    result = instance.fetchNeutronDataSingleUse(item)  # noqa: F841
                except Exception as e:  # noqa: BLE001
                    exceptionRaised = e

                assert isinstance(exceptionRaised, RuntimeError)
                assert (
                    f"Neutron data for run '{runNumber}' is not present on disk, " + "nor is it the live-data run"
                    in str(exceptionRaised)
                )

                mockUpdateNeutronCache.assert_called_with(runNumber, False)
                mockHasLiveDataConnection.assert_called_once()
                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                    workspace=workspaceName,
                    loader="LoadLiveDataInterval",
                    loaderArgs=json.dumps(expectedLoaderArgs),
                )
                mockConvertToLiteMode.assert_not_called()
                mockDeleteWorkspace.assert_called_once_with(workspaceName)

    @mock.patch.object(inspect.getmodule(GroceryService), "FileLoaderRegistry")
    def test_fetchNeutronDataCached(self, mockFileLoaderRegistry):
        # Test all 16, non-live-data cases (including each setting of the `Config["nexus.dataFormat.event"]` flag).
        # Note: this test does not test "in cache" cases: those are treated elsewhere

        def mockIAlgorithm(name: str) -> mock.Mock:
            mock_ = mock.Mock()
            mock_.name.return_value = name
            return mock_

        for nexus_dataFormat_event in (False, True):
            # Treat the two possible settings of the `Config["nexus.dataFormat.event"]` flag.

            with Config_override("nexus.dataFormat.event", nexus_dataFormat_event):
                mockFileLoaderRegistry.Instance.return_value.chooseLoader.return_value = mockIAlgorithm(
                    "LoadEventNexus" if Config["nexus.dataFormat.event"] else "LoadNexusProcessed"
                )

                runNumber = self.runNumber

                for flags in itertools.product((False, True), repeat=4):
                    useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

                    instance = GroceryService()
                    liteModeFilePath = mock.MagicMock(spec=Path)
                    liteModeFilePath.__str__.return_value = "liteModeFilePath"

                    nativeModeFilePath = mock.MagicMock(spec=Path)
                    nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

                    testCalibrationData = DAOFactory.calibrationParameters()
                    instance.dataService.generateInstrumentState = mock.MagicMock(
                        return_value=testCalibrationData.instrumentState
                    )

                    # instance.dataService.readInstrumentConfig

                    # In order to avoid contaminating other tests, all mocks must be applied
                    #   as context managers.
                    with (
                        mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                        mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,
                        mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                        mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                        mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                        mock.patch.object(
                            instance, "_createRawNeutronWorkspaceName"
                        ) as mockCreateRawNeutronWorkspaceName,
                        mock.patch.object(
                            instance, "_createCopyNeutronWorkspaceName"
                        ) as mockCreateCopyNeutronWorkspaceName,
                        mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                        mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                        mock.patch.object(instance, "renameWorkspace") as mockRenameWorkspace,
                        mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                        mock.patch.object(instance, "mantidSnapper") as mockSnapper,  # noqa F841
                    ):
                        # Mocks for flow-control branching:
                        mockHasLiveDataConnection.return_value = False

                        if nativeInCache:
                            mockRunsCache[(runNumber, False)] = 0

                        liteModeFilePath.exists.return_value = liteOnDisk
                        nativeModeFilePath.exists.return_value = nativeOnDisk

                        mockCreateNeutronFilePath.side_effect = (
                            lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                        )

                        mockCreateNeutronWorkspaceName.side_effect = (
                            lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                            if useLiteMode
                            else mock.sentinel.nativeWorkspaceName
                        )

                        mockCreateRawNeutronWorkspaceName.side_effect = (
                            lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                            if useLiteMode
                            else mock.sentinel.nativeRawWorkspaceName
                        )

                        mockCreateCopyNeutronWorkspaceName.side_effect = (
                            lambda _runNumber, useLiteMode, _copyNumber: mock.sentinel.liteCopyWorkspaceName
                            if useLiteMode
                            else mock.sentinel.nativeCopyWorkspaceName
                        )

                        mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                        mockRenameWorkspace.side_effect = lambda _, newName: newName

                        workspaceName = mockCreateNeutronWorkspaceName(runNumber, useLiteMode)

                        # Action-related mocks:
                        mockFetchGroceriesRecipe.executeRecipe.return_value = {
                            "result": True,
                            "loader": mock.sentinel.loader,
                            "workspace": workspaceName,
                        }

                        # BUILD the item argument:
                        item = GroceryListItem(
                            workspaceType="neutron",
                            runNumber=runNumber,
                            useLiteMode=useLiteMode,
                            loader="",
                            liveDataArgs=None,
                        )
                        # LOADER and LOADERARGS will be overridden on the `LoadEventNexus` call,
                        #   when `Config['nexus.dataFormat.event']` is set:
                        loader = ""
                        loaderArgs = "{}"
                        if Config["nexus.dataFormat.event"]:
                            loader = "LoadEventNexus"
                            loaderArgs = '{"NumberOfBins": 1}'

                        exceptionRaised = None
                        result = None  # noqa: F841
                        try:
                            result = instance.fetchNeutronDataCached(item)  # noqa: F841
                        except RuntimeError as e:
                            exceptionRaised = e

                        match (useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk):
                            case (False, True, _, _):
                                # native mode and native in cache:
                                #   not tested _here_.
                                continue

                            case (True, _, True, _):
                                # lite mode and lite-mode exists on disk
                                assert result == mockFetchGroceriesRecipe.executeRecipe.return_value
                                assert instance._loadedRuns[(runNumber, useLiteMode)] == 1
                                liteWorkspaceName = mockCreateNeutronWorkspaceName(runNumber, True)
                                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                                    str(liteModeFilePath), liteWorkspaceName, loader, loaderArgs=loaderArgs
                                )
                                mockConvertToLiteMode.assert_not_called()
                                mockUpdateNeutronCache.assert_has_calls([mock.call(runNumber, True)])

                            case (True, True, _, _):
                                # lite mode and native is cached
                                assert instance._loadedRuns[(runNumber, useLiteMode)] == 1
                                assert instance._loadedRuns[(runNumber, False)] == 0
                                nativeWorkspaceName = mockCreateNeutronWorkspaceName(runNumber, False)
                                liteWorkspaceName = mockCreateNeutronWorkspaceName(runNumber, True)
                                mockGetCloneOfWorkspace.assert_any_call(nativeWorkspaceName, liteWorkspaceName)
                                mockConvertToLiteMode.assert_called_once_with(liteWorkspaceName, export=True)
                                mockUpdateNeutronCache.assert_has_calls(
                                    [mock.call(runNumber, True), mock.call(runNumber, False)]
                                )

                            case (True, _, _, True):
                                # lite mode and native exists on disk
                                assert result == mockFetchGroceriesRecipe.executeRecipe.return_value
                                assert instance._loadedRuns[(runNumber, False)] == 0
                                assert instance._loadedRuns[(runNumber, useLiteMode)] == 1
                                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                                    str(nativeModeFilePath),
                                    mockCreateNeutronWorkspaceName(runNumber, False),
                                    loader,
                                    loaderArgs=loaderArgs,
                                )
                                liteWorkspaceName = mockCreateNeutronWorkspaceName(runNumber, True)
                                mockConvertToLiteMode.assert_called_once_with(liteWorkspaceName, export=True)
                                mockUpdateNeutronCache.assert_has_calls(
                                    [mock.call(runNumber, True), mock.call(runNumber, False)]
                                )

                            case (False, _, _, True):
                                # native mode and native exists on disk
                                assert result == mockFetchGroceriesRecipe.executeRecipe.return_value
                                assert instance._loadedRuns[(runNumber, useLiteMode)] == (1 if not useLiteMode else 0)
                                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                                    str(nativeModeFilePath), workspaceName, loader, loaderArgs=loaderArgs
                                )
                                mockConvertToLiteMode.assert_not_called()
                                mockUpdateNeutronCache.assert_has_calls([mock.call(runNumber, False)])

                            case _:
                                # live data fallback: NOT tested here
                                assert result is None
                                mockFetchGroceriesRecipe.executeRecipe.assert_not_called()
                                mockGetCloneOfWorkspace.assert_not_called()
                                mockConvertToLiteMode.assert_not_called()
                                assert isinstance(exceptionRaised, RuntimeError)
                                assert (
                                    f"Neutron data for run '{runNumber}' is not present on disk, "
                                    + "and no live-data connection is available."
                                    == str(exceptionRaised)
                                )

    def test_fetchNeutronDataCached_live_data(self):
        # Test live data, non-fallback cases.
        runNumber = self.runNumber

        for useLiteMode in (False, True):
            flags = useLiteMode, False, False, False
            useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

            instance = GroceryService()
            liteModeFilePath = mock.MagicMock(spec=Path)
            liteModeFilePath.__str__.return_value = "liteModeFilePath"

            nativeModeFilePath = mock.MagicMock(spec=Path)
            nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

            now_ = datetime.datetime.utcnow()
            duration = datetime.timedelta(seconds=42)
            expectedStartTime = now_ - duration

            expectedLoaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "PreserveEvents": True,
                "StartTime": expectedStartTime.isoformat(),
            }

            testCalibrationData = DAOFactory.calibrationParameters()
            instance.dataService.generateInstrumentState = mock.MagicMock(
                return_value=testCalibrationData.instrumentState
            )

            # In order to avoid contaminating other tests, all mocks must be applied
            #   as context managers.
            with (
                mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,  # noqa: F841
                mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                mock.patch.object(instance, "_createRawNeutronWorkspaceName") as mockCreateRawNeutronWorkspaceName,
                mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                mock.patch.object(instance, "deleteWorkspaceUnconditional") as mockDeleteWorkspace,  # noqa: F841
                mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                mock.patch(ThisService + "datetime", wraps=datetime.datetime) as mockDatetime,
                mock.patch(ThisService + "RunMetadata") as mockRunMetadata,
                mock.patch.object(instance, "mantidSnapper") as mockSnapper,
            ):
                mockDatetime.utcnow.return_value = now_

                # Mocks for flow-control branching:
                mockHasLiveDataConnection.return_value = True

                liteModeFilePath.exists.return_value = liteOnDisk
                nativeModeFilePath.exists.return_value = nativeOnDisk

                mockCreateNeutronFilePath.side_effect = (
                    lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                )

                mockCreateNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeWorkspaceName
                )

                mockCreateRawNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeRawWorkspaceName
                )

                mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                # Initial output ws from livedata will always be native before it gets converted to lite mode.
                workspaceName = mockCreateNeutronWorkspaceName(runNumber, False)

                # Mock `RunMetadata.fromRun` to return `RunMetadata` that has the correct run number.
                mockRunMetadata.fromRun = mock.Mock(return_value=self.mockRunMetadata(runNumber))
                mockRunMetadata.FROM_NOW_ISO8601 = RunMetadata.FROM_NOW_ISO8601
                mockRunMetadata.FROM_START_ISO8601 = RunMetadata.FROM_START_ISO8601

                def _valid_key(key: str, validKey: str):
                    if key != validKey:
                        raise KeyError(f"key '{key}', expecting '{validKey}'")
                    return True

                mockWs = mock.Mock()
                mockWs.getRun.return_value = mock.sentinel.run
                mockSnapper.mtd.__getitem__.side_effect = (
                    lambda wsName: mockWs if _valid_key(wsName, workspaceName) else None
                )

                # Action-related mocks:
                mockFetchGroceriesRecipe.executeRecipe.return_value = {
                    "result": True,
                    "loader": "LoadLiveDataInterval",
                    "workspace": workspaceName,
                }

                # BUILD the item argument:
                item = GroceryListItem(
                    workspaceType="neutron",
                    runNumber=runNumber,
                    useLiteMode=useLiteMode,
                    loader="",
                    liveDataArgs=LiveDataArgs(duration=duration),
                    state="stateId",
                )

                exceptionRaised = None
                result = None  # noqa: F841
                try:
                    result = instance.fetchNeutronDataCached(item)  # noqa: F841
                except RuntimeError as e:
                    exceptionRaised = e

                assert exceptionRaised is None
                assert instance._loadedRuns[(runNumber, useLiteMode)] == 1
                # If live data is used, the run number is stored in the cache as native
                assert (runNumber, False) in instance._liveDataKeys

                if not useLiteMode:
                    mockUpdateNeutronCache.assert_called_once_with(runNumber, False)
                else:
                    mockUpdateNeutronCache.assert_has_calls([mock.call(runNumber, True), mock.call(runNumber, False)])
                mockHasLiveDataConnection.assert_called_once()
                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                    workspace=workspaceName,
                    loader="LoadLiveDataInterval",
                    loaderArgs=json.dumps(expectedLoaderArgs),
                )
                mockRunMetadata.fromRun.assert_called_once_with(mock.sentinel.run)

                if useLiteMode:
                    assert instance._loadedRuns[(runNumber, False)] == 0
                    assert (runNumber, False) in instance._liveDataKeys
                    workspaceLiteName = mockCreateNeutronWorkspaceName(runNumber, True)
                    mockConvertToLiteMode.assert_called_once_with(workspaceLiteName, export=False)
                else:
                    mockConvertToLiteMode.assert_not_called()

    def test_fetchNeutronDataCached_live_data_transition(self):
        # Test live data, non-fallback cases.
        runNumber = self.runNumber

        for useLiteMode in (False, True):
            flags = useLiteMode, False, False, False
            useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

            instance = GroceryService()
            liteModeFilePath = mock.MagicMock(spec=Path)
            liteModeFilePath.__str__.return_value = "liteModeFilePath"

            nativeModeFilePath = mock.MagicMock(spec=Path)
            nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

            now_ = datetime.datetime.utcnow()
            duration = datetime.timedelta(seconds=42)
            expectedStartTime = now_ - duration

            expectedLoaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "PreserveEvents": True,
                "StartTime": expectedStartTime.isoformat(),
            }

            # In order to avoid contaminating other tests, all mocks must be applied
            #   as context managers.
            with (
                mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,  # noqa: F841
                mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                mock.patch.object(instance, "_createRawNeutronWorkspaceName") as mockCreateRawNeutronWorkspaceName,
                mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                mock.patch.object(instance, "deleteWorkspaceUnconditional") as mockDeleteWorkspace,
                mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                mock.patch(ThisService + "datetime", wraps=datetime.datetime) as mockDatetime,
                mock.patch(ThisService + "RunMetadata") as mockRunMetadata,
                mock.patch.object(instance, "mantidSnapper") as mockSnapper,
            ):
                mockDatetime.utcnow.return_value = now_

                # Mocks for flow-control branching:
                mockHasLiveDataConnection.return_value = True

                liteModeFilePath.exists.return_value = liteOnDisk
                nativeModeFilePath.exists.return_value = nativeOnDisk

                mockCreateNeutronFilePath.side_effect = (
                    lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                )

                mockCreateNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeWorkspaceName
                )

                mockCreateRawNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeRawWorkspaceName
                )

                mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                # Live data is always initially native until it is converted to lite mode.
                workspaceName = mockCreateNeutronWorkspaceName(runNumber, False)

                # Mock `RunMetadata.fromRun` to return `RunMetadata` that DOES NOT HAVE the correct run number.
                mockRunMetadata.fromRun = mock.Mock(return_value=self.mockRunMetadata(str(int(runNumber) + 1)))
                mockRunMetadata.FROM_NOW_ISO8601 = RunMetadata.FROM_NOW_ISO8601
                mockRunMetadata.FROM_START_ISO8601 = RunMetadata.FROM_START_ISO8601

                def _valid_key(key: str, validKey: str):
                    if key != validKey:
                        raise KeyError(f"key '{key}', expecting '{validKey}'")
                    return True

                mockWs = mock.Mock()
                mockWs.getRun.return_value = mock.sentinel.run
                mockSnapper.mtd.__getitem__.side_effect = (
                    lambda wsName: mockWs if _valid_key(wsName, workspaceName) else None
                )

                # Action-related mocks:
                mockFetchGroceriesRecipe.executeRecipe.return_value = {
                    "result": True,
                    "loader": "LoadLiveDataInterval",
                    "workspace": workspaceName,
                }

                # BUILD the item argument:
                item = GroceryListItem(
                    workspaceType="neutron",
                    runNumber=runNumber,
                    useLiteMode=useLiteMode,
                    loader="",
                    liveDataArgs=LiveDataArgs(duration=duration),
                )

                exceptionRaised = None
                result = None  # noqa: F841
                try:
                    result = instance.fetchNeutronDataCached(item)  # noqa: F841
                except LiveDataState as e:  # noqa: BLE001
                    exceptionRaised = e

                assert (
                    exceptionRaised.model == LiveDataState.runStateTransition(str(int(runNumber) + 1), runNumber).model
                )
                if not useLiteMode:
                    mockUpdateNeutronCache.assert_called_once_with(runNumber, False)
                else:
                    mockUpdateNeutronCache.assert_has_calls([mock.call(runNumber, True), mock.call(runNumber, False)])
                mockHasLiveDataConnection.assert_called_once()
                # it gets loaded into the native workspace first, then cache renames it to raw
                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                    workspace=workspaceName,
                    loader="LoadLiveDataInterval",
                    loaderArgs=json.dumps(expectedLoaderArgs),
                )
                mockConvertToLiteMode.assert_not_called()
                mockDeleteWorkspace.assert_called_once_with(workspaceName)

    def test_fetchNeutronDataCached_live_data_fallback(self):
        # Test live data fallback cases.
        runNumber = self.runNumber

        for useLiteMode in (False, True):
            flags = useLiteMode, False, False, False
            useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

            instance = GroceryService()
            liteModeFilePath = mock.MagicMock(spec=Path)
            liteModeFilePath.__str__.return_value = "liteModeFilePath"

            nativeModeFilePath = mock.MagicMock(spec=Path)
            nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

            expectedLoaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "PreserveEvents": True,
                "StartTime": RunMetadata.FROM_START_ISO8601,
            }

            # In order to avoid contaminating other tests, all mocks must be applied
            #   as context managers.
            with (
                mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,  # noqa: F841
                mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                mock.patch.object(instance, "_createRawNeutronWorkspaceName") as mockCreateRawNeutronWorkspaceName,
                mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                mock.patch.object(instance, "deleteWorkspaceUnconditional") as mockDeleteWorkspace,  # noqa: F841
                mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                mock.patch.object(instance, "_processNeutronDataCopy") as mockProcessNeutronDataCopy,  # noqa: F841
                mock.patch(ThisService + "RunMetadata") as mockRunMetadata,
                mock.patch.object(instance, "mantidSnapper") as mockSnapper,
            ):
                # Mocks for flow-control branching:
                mockHasLiveDataConnection.return_value = True

                liteModeFilePath.exists.return_value = liteOnDisk
                nativeModeFilePath.exists.return_value = nativeOnDisk

                mockCreateNeutronFilePath.side_effect = (
                    lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                )

                mockCreateNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeWorkspaceName
                )

                mockCreateRawNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeRawWorkspaceName
                )

                mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                # Live data is always initially native until it is converted to lite mode.
                workspaceName = mockCreateNeutronWorkspaceName(runNumber, False)

                # Mock `RunMetadata.fromRun` to return `RunMetadata` that has the correct run number.
                mockRunMetadata.fromRun = mock.Mock(return_value=self.mockRunMetadata(runNumber))
                mockRunMetadata.FROM_NOW_ISO8601 = RunMetadata.FROM_NOW_ISO8601
                mockRunMetadata.FROM_START_ISO8601 = RunMetadata.FROM_START_ISO8601

                def _valid_key(key: str, validKey: str):
                    if key != validKey:
                        raise KeyError(f"key '{key}', expecting '{validKey}'")
                    return True

                mockWs = mock.Mock()
                mockWs.getRun.return_value = mock.sentinel.run
                mockSnapper.mtd.__getitem__.side_effect = (
                    lambda wsName: mockWs if _valid_key(wsName, workspaceName) else None
                )

                # Action-related mocks:
                mockFetchGroceriesRecipe.executeRecipe.return_value = {
                    "result": True,
                    "loader": "LoadLiveDataInterval",
                    "workspace": workspaceName,
                }

                # BUILD the item argument:
                item = GroceryListItem(
                    workspaceType="neutron", runNumber=runNumber, useLiteMode=useLiteMode, state="stateId", loader=""
                )

                exceptionRaised = None
                result = None  # noqa: F841
                try:
                    result = instance.fetchNeutronDataCached(item)  # noqa: F841
                except RuntimeError as e:
                    exceptionRaised = e

                assert exceptionRaised is None

                assert instance._loadedRuns[(runNumber, useLiteMode)] == 1
                # live data is always native until it is converted to lite mode
                # NOTE: How does cacheing a livedata run work?  is that even possible?
                assert (runNumber, False) in instance._liveDataKeys

                if not useLiteMode:
                    mockUpdateNeutronCache.assert_called_once_with(runNumber, False)
                else:
                    mockUpdateNeutronCache.assert_has_calls([mock.call(runNumber, True), mock.call(runNumber, False)])

                mockHasLiveDataConnection.assert_called_once()
                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                    workspace=workspaceName,
                    loader="LoadLiveDataInterval",
                    loaderArgs=json.dumps(expectedLoaderArgs),
                )
                if useLiteMode:
                    assert instance._loadedRuns[(runNumber, False)] == 0
                    assert (runNumber, False) in instance._liveDataKeys
                    workspaceLiteName = mockCreateNeutronWorkspaceName(runNumber, True)
                    mockConvertToLiteMode.assert_called_once_with(workspaceLiteName, export=False)
                else:
                    mockConvertToLiteMode.assert_not_called()
                mockProcessNeutronDataCopy.assert_called_once_with(item, result["workspace"])

    def test_fetchNeutronDataCached_live_data_fallback_fails(self):
        # Test live data, non-fallback cases.
        runNumber = self.runNumber

        for useLiteMode in (False, True):
            flags = useLiteMode, False, False, False
            useLiteMode, nativeInCache, liteOnDisk, nativeOnDisk = flags

            instance = GroceryService()
            liteModeFilePath = mock.MagicMock(spec=Path)
            liteModeFilePath.__str__.return_value = "liteModeFilePath"

            nativeModeFilePath = mock.MagicMock(spec=Path)
            nativeModeFilePath.__str__.return_value = "nativeModeFilePath"

            expectedLoaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "PreserveEvents": True,
                "StartTime": RunMetadata.FROM_START_ISO8601,
            }

            # In order to avoid contaminating other tests, all mocks must be applied
            #   as context managers.
            with (
                mock.patch.object(instance, "_updateNeutronCacheFromADS") as mockUpdateNeutronCache,
                mock.patch.dict(instance._loadedRuns, clear=True) as mockRunsCache,  # noqa: F841
                mock.patch.object(instance, "_createNeutronFilePath") as mockCreateNeutronFilePath,
                mock.patch.object(instance, "grocer") as mockFetchGroceriesRecipe,
                mock.patch.object(instance, "_createNeutronWorkspaceName") as mockCreateNeutronWorkspaceName,
                mock.patch.object(instance, "_createRawNeutronWorkspaceName") as mockCreateRawNeutronWorkspaceName,
                mock.patch.object(instance.dataService, "hasLiveDataConnection") as mockHasLiveDataConnection,
                mock.patch.object(instance, "getCloneOfWorkspace") as mockGetCloneOfWorkspace,
                mock.patch.object(instance, "deleteWorkspaceUnconditional") as mockDeleteWorkspace,
                mock.patch.object(instance, "convertToLiteMode") as mockConvertToLiteMode,
                mock.patch(ThisService + "RunMetadata") as mockRunMetadata,
                mock.patch.object(instance, "mantidSnapper") as mockSnapper,
            ):
                # Mocks for flow-control branching:
                mockHasLiveDataConnection.return_value = True

                liteModeFilePath.exists.return_value = liteOnDisk
                nativeModeFilePath.exists.return_value = nativeOnDisk

                mockCreateNeutronFilePath.side_effect = (
                    lambda _runNumber, useLiteMode: liteModeFilePath if useLiteMode else nativeModeFilePath
                )

                mockCreateNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeWorkspaceName
                )

                mockCreateRawNeutronWorkspaceName.side_effect = (
                    lambda _runNumber, useLiteMode: mock.sentinel.liteRawWorkspaceName
                    if useLiteMode
                    else mock.sentinel.nativeRawWorkspaceName
                )

                mockGetCloneOfWorkspace.return_value = mock.sentinel.clonedWorkspace

                # Live data is always initially native until it is converted to lite mode.
                workspaceName = mockCreateNeutronWorkspaceName(runNumber, False)

                # Mock `RunMetadata.fromRun` to return `RunMetadata` that DOES NOT HAVE the correct run number.
                mockRunMetadata.fromRun = mock.Mock(return_value=self.mockRunMetadata(str(int(runNumber) + 1)))
                mockRunMetadata.FROM_NOW_ISO8601 = RunMetadata.FROM_NOW_ISO8601
                mockRunMetadata.FROM_START_ISO8601 = RunMetadata.FROM_START_ISO8601

                def _valid_key(key: str, validKey: str):
                    if key != validKey:
                        raise KeyError(f"key '{key}', expecting '{validKey}'")
                    return True

                mockWs = mock.Mock()
                mockWs.getRun.return_value = mock.sentinel.run
                mockSnapper.mtd.__getitem__.side_effect = (
                    lambda wsName: mockWs if _valid_key(wsName, workspaceName) else None
                )

                # Action-related mocks:
                mockFetchGroceriesRecipe.executeRecipe.return_value = {
                    "result": True,
                    "loader": "LoadLiveDataInterval",
                    "workspace": workspaceName,
                }

                # BUILD the item argument:
                item = GroceryListItem(workspaceType="neutron", runNumber=runNumber, useLiteMode=useLiteMode, loader="")

                exceptionRaised = None
                result = None  # noqa: F841
                try:
                    result = instance.fetchNeutronDataCached(item)  # noqa: F841
                except RuntimeError as e:  # noqa: BLE001
                    exceptionRaised = e

                assert (
                    f"Neutron data for run '{runNumber}' is not present on disk, " + "nor is it the live-data run"
                    in str(exceptionRaised)
                )

                if not useLiteMode:
                    mockUpdateNeutronCache.assert_called_once_with(runNumber, False)
                else:
                    mockUpdateNeutronCache.assert_has_calls([mock.call(runNumber, True), mock.call(runNumber, False)])
                mockHasLiveDataConnection.assert_called_once()
                mockFetchGroceriesRecipe.executeRecipe.assert_called_once_with(
                    workspace=workspaceName,
                    loader="LoadLiveDataInterval",
                    loaderArgs=json.dumps(expectedLoaderArgs),
                )
                mockConvertToLiteMode.assert_not_called()
                mockDeleteWorkspace.assert_called_once_with(workspaceName)

    def test_fetch_grouping(self):
        groupFilepath = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")
        self.instance._createGroupingFilename = mock.Mock(return_value=groupFilepath)
        testItem = (self.groupingItem.groupingScheme, self.runNumber, self.groupingItem.useLiteMode)
        testKey = (self.groupingItem.groupingScheme, self.stateId, self.groupingItem.useLiteMode)

        # call once and load
        groupingWorkspaceName = self.instance._createGroupingWorkspaceName(*testItem)
        groupKey = self.instance._key(*testKey)
        res = self.instance.fetchGroupingDefinition(self.groupingItem)
        assert res["result"]
        assert res["loader"] == "LoadGroupingDefinition"
        assert res["workspace"] == groupingWorkspaceName
        assert self.instance._loadedGroupings == {groupKey: groupingWorkspaceName}

        # call again and no load
        res = self.instance.fetchGroupingDefinition(self.groupingItem)
        assert res["result"]
        assert res["loader"] == "cached"
        assert res["workspace"] == groupingWorkspaceName
        assert self.instance._loadedGroupings == {groupKey: groupingWorkspaceName}

    def test_failed_fetch_grouping(self):
        # this is some file that it can't load
        fakeFilepath = Resource.getPath("inputs/crystalInfo/blank_file.cif")
        self.instance._createGroupingFilename = mock.Mock(return_value=fakeFilepath)
        with pytest.raises(AlgorithmException, match=".*Grouping file extension CIF is not supported.*"):
            self.instance.fetchGroupingDefinition(self.groupingItem)

    @mock.patch(ThisService + "GroceryListItem")
    def test_fetch_lite_data_map(self, mockGroceryList):
        """
        This is a special case of the fetch grouping method.
        """
        groupMapFilepath = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        # self.instance._createGroupingWorkspaceName = mock.Mock(return_value="lite_map")
        self.instance._createGroupingFilename = mock.Mock(return_value=groupMapFilepath)
        # have to subvert the validation methods in grocerylistitem
        mockLiteMapGroceryItem = GroceryListItem(
            workspaceType="grouping",
            runNumber=ReservedRunNumber.NATIVE,
            groupingScheme="Lite",
        )
        mockLiteMapGroceryItem.instrumentSource = self.instrumentFilePath
        mockGroceryList.builder.return_value.grouping.return_value.build.return_value = mockLiteMapGroceryItem
        stateId, _ = self.instance.dataService.generateStateId(mockLiteMapGroceryItem.runNumber)

        # call once and load
        testItem = ("Lite", ReservedRunNumber.NATIVE, False)
        testKey = ("Lite", stateId, False)
        groupingWorkspaceName = self.instance._createGroupingWorkspaceName(*testItem)
        groupKey = self.instance._key(*testKey)

        res = self.instance.fetchLiteDataMap()
        assert res == groupingWorkspaceName
        assert self.instance._loadedGroupings == {groupKey: groupingWorkspaceName}

    def test_fetch_grocery_list(self):
        # expected workspaces
        cleanWorkspace = mock.Mock()
        dirtyWorkspace = mock.Mock()
        groupWorkspace = mock.Mock()

        self.instance.fetchNeutronDataCached = mock.Mock(return_value={"result": True, "workspace": cleanWorkspace})
        self.instance.fetchNeutronDataSingleUse = mock.Mock(return_value={"result": True, "workspace": dirtyWorkspace})
        self.instance.fetchGroupingDefinition = mock.Mock(return_value={"result": True, "workspace": groupWorkspace})

        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).add()
        clerk.native().neutron(self.runNumber).dirty().add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).source(InstrumentDonor=self.sampleWS).add()
        groceryList = clerk.buildList()

        res = self.instance.fetchGroceryList(groceryList)

        assert res == [cleanWorkspace, dirtyWorkspace, groupWorkspace]
        for i, fetchMethod in enumerate(
            [self.instance.fetchNeutronDataCached, self.instance.fetchNeutronDataSingleUse]
        ):
            fetchMethod.assert_called_once_with(groceryList[i])
        self.instance.fetchGroupingDefinition.assert_called_once_with(groceryList[2])

    def test_fetch_grocery_list_fails(self):
        self.instance.fetchNeutronDataSingleUse = mock.Mock(return_value={"result": False, "workspace": "unimportant"})
        groceryList = GroceryListItem.builder().native().neutron(self.runNumber).dirty().buildList()
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchGroceryList(groceryList)
        print(str(e.value))

    def test_fetch_grocery_list_diffcal_fails(self):
        groceryList = GroceryListItem.builder().native().diffcal(self.runNumber).buildList()
        with pytest.raises(
            RuntimeError,
            match="not implemented: no path available to fetch diffcal",
        ):
            self.instance.fetchGroceryList(groceryList)

    def test_fetch_grocery_list_diffcal_output(self):
        # Test of workspace type "diffcal_output" as `Input` argument in the `GroceryList`
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.calibrationIndexer = self.mockIndexer(tmpRoot.path(), "diffraction")
            groceryList = (
                GroceryListItem.builder()
                .native()
                .diffcal_output("stateId", self.version)
                .unit(wng.Units.TOF)
                .group(wng.Groups.UNFOC)
                .buildList()
            )
            item = groceryList[0]
            item.runNumber = self.runNumber1
            diffCalOutputFilename = self.instance._createDiffCalOutputWorkspaceFilename(item)
            SaveNexus(
                InputWorkspace=self.sampleWS,
                Filename=self.sampleTarWsFilePath,
            )
            tmpRoot.addFileAs(self.sampleTarWsFilePath, diffCalOutputFilename)
            assert Path(diffCalOutputFilename).exists()

            assert not mtd.doesExist(self.diffCalOutputName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == self.diffCalOutputName
            assert mtd.doesExist(self.diffCalOutputName)

    def test_fetch_grocery_list_diffcal_output_cached(self):
        # Test of workspace type "diffcal_output" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        groceryList = (
            GroceryListItem.builder()
            .native()
            .diffcal_output(self.runNumber1, self.version)
            .unit(wng.Units.TOF)
            .group(wng.Groups.UNFOC)
            .buildList()
        )
        diffCalOutputName = (
            wng.diffCalOutput()
            .unit(wng.Units.TOF)
            .group(wng.Groups.UNFOC)
            .runNumber(self.runNumber1)
            .version(self.version)
            .build()
        )
        CloneWorkspace(
            InputWorkspace=self.sampleWS,
            OutputWorkspace=diffCalOutputName,
        )

        self.instance.dataService.calibrationIndexer = self.mockIndexer("root", "diffraction")

        assert mtd.doesExist(diffCalOutputName)
        testTitle = "I'm a little teapot"
        mtd[diffCalOutputName].setTitle(testTitle)
        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == diffCalOutputName
        assert mtd.doesExist(diffCalOutputName)
        assert mtd[diffCalOutputName].getTitle() == testTitle

    def test_fetch_grocery_list_diffcal_table(self):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.calibrationIndexer = self.mockIndexer(tmpRoot.path(), "diffraction")
            groceryList = (
                GroceryListItem.builder().native().diffcal_table("stateId", self.version, self.runNumber1).buildList()
            )
            # independently construct the pathname, move file to there, assert exists
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).version(self.version).build()
            self.instance._lookupDiffCalWorkspaceNames = mock.Mock(return_value=(diffCalTableName, diffCalMaskName))
            diffCalTableFilename = self.instance._createDiffCalTableFilepath(
                diffcalRunNumber=groceryList[0].runNumber,
                useLiteMode=groceryList[0].useLiteMode,
                version=self.version,
                state="stateId",
            )
            tmpRoot.addFileAs(self.sampleDiffCalFilePath, diffCalTableFilename)
            assert Path(diffCalTableFilename).exists()
            # fetch the workspace
            assert not mtd.doesExist(diffCalTableName)

            with Config_override("instrument.native.pixelResolution", 16):
                items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalTableName
            assert mtd.doesExist(diffCalTableName)

    def test_validateCalibrationMask(self):
        # load a diffcal mask and validate it
        item = mock.Mock(useLiteMode=False)
        with (
            mock.patch.object(self.instance, "mantidSnapper") as mockSnapper,
            Config_override("instrument.native.pixelResolution", 16),
        ):
            mockWs = mock.Mock()
            mockWs.getNumberHistograms.return_value = 16
            mockSnapper.mtd.__getitem__.return_value = mockWs
            self.instance._validateCalibrationMask(item, "someMask")
            mockWs.getNumberHistograms.return_value = 1337
            with pytest.raises(
                RuntimeError,
                match="Expected 16 histograms for the native resolution.",
            ):
                self.instance._validateCalibrationMask(item, "someMask")

    def test_validateCalibrationTable(self):
        # load a diffcal table and validate it
        testCalibrationTable = self.generateTestDiffcalTable()
        item = GroceryListItem.builder().native().diffcal_table(self.runNumber1, self.version).buildList()[0]
        with Config_override("instrument.native.pixelResolution", 16):
            self.instance._validateCalibrationTable(item, testCalibrationTable)

    def test_validateCalibrationTable_fail_workspaceExists(self):
        item = GroceryListItem.builder().native().diffcal_table(self.runNumber1, self.version).buildList()[0]
        with pytest.raises(
            RuntimeError,
            match="does not exist in the ADS.",
        ):
            self.instance._validateCalibrationTable(item, "does not exist")

    def test_validateCalibrationTable(self):
        # load a diffcal table and validate it
        testCalibrationTable = self.generateTestDiffcalTable()
        item = GroceryListItem.builder().native().diffcal_table(self.runNumber1, self.version).buildList()[0]
        with pytest.raises(
            RuntimeError,
            match="Expected 1179648 rows for the native resolution.",
        ):
            self.instance._validateCalibrationTable(item, testCalibrationTable)

    def test_validateCalibrationTable_fail_invalidDifcValues(self):
        # load a diffcal table and validate it
        testCalibrationTable = self.generateTestDiffcalTable()
        mtd[testCalibrationTable].removeColumn("difc")
        mtd[testCalibrationTable].addColumn("float", "difc")
        item = GroceryListItem.builder().native().diffcal_table(self.runNumber1, self.version).buildList()[0]
        with Config_override("instrument.native.pixelResolution", 16):
            with pytest.raises(
                RuntimeError,
                match="All 'difc' values must be positive floats.",
            ):
                self.instance._validateCalibrationTable(item, testCalibrationTable)

    def test_fetch_grocery_list_diffcal_table_cached(self):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        self.instance.grocer = mock.Mock()
        groceryList = GroceryListItem.builder().native().diffcal_table(self.runNumber1, self.version).buildList()

        diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
        diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).version(self.version).build()
        self.instance.dataService.calibrationIndexer = self.mockIndexer(calType="calibration")
        self.instance._createDiffCalTableFilepathFromWsName = mock.Mock(return_value="does/not/matter/is/cached")
        self.instance._lookupDiffCalWorkspaceNames = mock.Mock(return_value=(diffCalTableName, diffCalMaskName))

        # Both table and mask workspace must be resident in the ADS, otherwise a reload will be triggered.
        CloneWorkspace(
            InputWorkspace=self.sampleTableWS,
            OutputWorkspace=diffCalTableName,
        )
        assert mtd.doesExist(diffCalTableName)
        CloneWorkspace(
            InputWorkspace=self.sampleMaskWS,
            OutputWorkspace=diffCalMaskName,
        )
        assert mtd.doesExist(diffCalMaskName)

        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == diffCalTableName
        self.instance.grocer.executeRecipe.assert_not_called()

    def test_fetch_grocery_list_diffcal_table_cached_no_mask(self):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`:
        #   the table workspace is already in the ADS but the corresponding mask workspace is absent.
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.calibrationIndexer = self.mockIndexer(tmpRoot.path(), "diffraction")
            groceryList = (
                GroceryListItem.builder().native().diffcal_table("stateId", self.version, self.runNumber1).buildList()
            )
            # independently construct the pathname, move file to there, assert exists
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).version(self.version).build()
            self.instance._lookupDiffCalWorkspaceNames = mock.Mock(return_value=(diffCalTableName, diffCalMaskName))
            diffCalTableFilename = self.instance._createDiffCalTableFilepath(
                diffcalRunNumber=groceryList[0].runNumber,
                useLiteMode=groceryList[0].useLiteMode,
                version=self.version,
                state="stateId",
            )
            tmpRoot.addFileAs(self.sampleDiffCalFilePath, diffCalTableFilename)
            assert Path(diffCalTableFilename).exists()

            # When both table and mask workspaces exist for the given calibration:
            #   both must be resident in the ADS, otherwise a reload will be triggered.
            CloneWorkspace(
                InputWorkspace=self.sampleTableWS,
                OutputWorkspace=diffCalTableName,
            )
            assert mtd.doesExist(diffCalTableName)
            assert not mtd.doesExist(diffCalMaskName)
            with Config_override("instrument.native.pixelResolution", 16):
                wss = self.instance.fetchGroceryList(groceryList)  # noqa: F841
            assert mtd.doesExist(diffCalTableName)
            assert mtd.doesExist(diffCalMaskName)

    def test_fetch_grocery_list_diffcal_table_loads_mask(self):
        # Test of workspace type "diffcal_table" as `Input` argument in the `GroceryList`:
        #   * corresponding mask workspace is also loaded from the hdf5-format file.
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.calibrationIndexer = self.mockIndexer(tmpRoot.path(), "diffraction")
            groceryList = (
                GroceryListItem.builder().native().diffcal_table("stateId", self.version, self.runNumber1).buildList()
            )
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).version(self.version).build()
            self.instance._lookupDiffCalWorkspaceNames = mock.Mock(return_value=(diffCalTableName, diffCalMaskName))
            diffCalTableFilename = self.instance._createDiffCalTableFilepath(
                diffcalRunNumber=groceryList[0].runNumber,
                useLiteMode=groceryList[0].useLiteMode,
                version=self.version,
                state="stateId",
            )
            tmpRoot.addFileAs(self.sampleDiffCalFilePath, diffCalTableFilename)
            assert Path(diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalTableName)
            assert not mtd.doesExist(diffCalMaskName)
            with Config_override("instrument.native.pixelResolution", 16):
                items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalTableName
            assert mtd.doesExist(diffCalTableName)
            assert mtd.doesExist(diffCalMaskName)

    def test_fetch_grocery_list_diffcal_mask(self):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.calibrationIndexer = self.mockIndexer(tmpRoot.path(), "diffraction")
            groceryList = (
                GroceryListItem.builder().native().diffcal_mask("stateId", self.version, self.runNumber1).buildList()
            )
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).version(self.version).build()

            # DiffCal filename is constructed from the table name
            diffCalTableFilename = self.instance._createDiffCalTableFilepath(
                diffcalRunNumber=groceryList[0].runNumber,
                useLiteMode=groceryList[0].useLiteMode,
                version=groceryList[0].diffCalVersion,
                state="stateId",
            )
            tmpRoot.addFileAs(self.sampleDiffCalFilePath, diffCalTableFilename)
            assert Path(diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalMaskName)
            self.instance._lookupDiffCalWorkspaceNames = mock.Mock(return_value=(diffCalTableName, diffCalMaskName))
            with Config_override("instrument.native.pixelResolution", 16):
                items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalMaskName
            assert mtd.doesExist(diffCalMaskName)

    def test_fetch_grocery_list_diffcal_mask_no_mask(self):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`:
        #   a mask may not exist corresponding to a given calibration -- this is not an error.
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.calibrationIndexer = self.mockIndexer(tmpRoot.path(), "diffraction")
            groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber1, self.version).buildList()
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
            diffCalMaskName = None
            self.instance._lookupDiffCalWorkspaceNames = mock.Mock(return_value=(diffCalTableName, diffCalMaskName))
            self.instance.fetchCalibrationWorkspaces = mock.Mock(return_value=(diffCalTableName, None))
            with Config_override("instrument.native.pixelResolution", 16):
                items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == ""

    def test_fetch_grocery_list_diffcal_mask_cached(self):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber1, self.version).buildList()
        diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
        diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).version(self.version).build()
        self.instance._lookupDiffCalWorkspaceNames = mock.Mock(return_value=(diffCalTableName, diffCalMaskName))
        self.instance.dataService.calibrationIndexer = self.mockIndexer("root", "diffraction")
        CloneWorkspace(
            InputWorkspace=self.sampleMaskWS,
            OutputWorkspace=diffCalMaskName,
        )
        assert mtd.doesExist(diffCalMaskName)
        testTitle = "I'm a little teapot"
        mtd[diffCalMaskName].setTitle(testTitle)

        # Reloading is triggered based on whether or not the corresponding table workspace is in the ADS.
        diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
        CloneWorkspace(
            InputWorkspace=self.sampleTableWS,
            OutputWorkspace=diffCalTableName,
        )
        assert mtd.doesExist(diffCalTableName)

        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == diffCalMaskName
        assert mtd.doesExist(diffCalMaskName)
        assert mtd[diffCalMaskName].getTitle() == testTitle

    def test_fetch_grocery_list_diffcal_mask_loads_table(self):
        # Test of workspace type "diffcal_mask" as `Input` argument in the `GroceryList`:
        #   * corresponding table workspace is also loaded from the hdf5-format file.

        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.calibrationIndexer = self.mockIndexer(tmpRoot.path(), "diffraction")
            groceryList = (
                GroceryListItem.builder().native().diffcal_mask("stateId", self.version, self.runNumber1).buildList()
            )
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
            diffCalMaskName = wng.diffCalMask().runNumber(self.runNumber1).version(self.version).build()

            # DiffCal filename is constructed from the table name
            diffCalTableName = wng.diffCalTable().runNumber(self.runNumber1).version(self.version).build()
            self.instance._lookupDiffCalWorkspaceNames = mock.Mock(return_value=(diffCalTableName, diffCalMaskName))
            diffCalTableFilename = self.instance._createDiffCalTableFilepath(
                diffcalRunNumber=groceryList[0].runNumber,
                useLiteMode=groceryList[0].useLiteMode,
                version=groceryList[0].diffCalVersion,
                state="stateId",
            )
            tmpRoot.addFileAs(self.sampleDiffCalFilePath, diffCalTableFilename)
            assert Path(diffCalTableFilename).exists()

            assert not mtd.doesExist(diffCalMaskName)
            assert not mtd.doesExist(diffCalTableName)
            with Config_override("instrument.native.pixelResolution", 16):
                items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == diffCalMaskName
            assert mtd.doesExist(diffCalMaskName)
            assert mtd.doesExist(diffCalTableName)

    @mock.patch(ThisService + "logger")
    def test_fetch_grocery_list_normalization(self, mockLogger):
        # Test of workspace type "normalization" as `Input` argument in the `GroceryList`
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.normalizationIndexer = self.mockIndexer(tmpRoot.path(), "normalization")
            groceryList = (
                GroceryListItem.builder().lite().normalization(self.runNumber1, "statId", self.version).buildList()
            )
            self.instance._processNeutronDataCopy = mock.Mock()
            self.instance.updateInstrument = mock.Mock()
            self.instance.dataService.generateInstrumentState = mock.Mock(
                return_value=DAOFactory.default_instrument_state
            )
            # normalization filename is constructed
            normalizationWorkspaceName = (
                wng.rawVanadium().runNumber(self.runNumber1).version(self.version).hidden(True).build()
            )
            normalizationFilename = self.instance._lookupNormalizationWorkspaceFilename(
                normcalRunNumber=groceryList[0].runNumber,
                useLiteMode=groceryList[0].useLiteMode,
                version=self.version,
                state="stateId",
            )
            tmpRoot.addFileAs(self.sampleWSFilePath, normalizationFilename)
            assert Path(normalizationFilename).exists()

            assert not mtd.doesExist(normalizationWorkspaceName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == normalizationWorkspaceName
            assert mtd.doesExist(normalizationWorkspaceName)
            self.instance._processNeutronDataCopy.assert_called_once_with(groceryList[0], normalizationWorkspaceName)
            self.instance.updateInstrument.assert_called_once_with(normalizationWorkspaceName)

            mockLogger.info.assert_any_call(
                f"Fetching normalization workspace for run {self.runNumber1}, version {self.version}"
            )

    @mock.patch(ThisService + "logger")
    def test_fetch_grocery_list_normalization_bust_cache(self, mockLogger):
        # Test of workspace type "normalization" as `Input` argument in the `GroceryList`
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
        self.instance._processNeutronDataCopy = mock.Mock()
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.normalizationIndexer = self.mockIndexer(tmpRoot.path(), "normalization")
            self.instance.dataService.generateInstrumentState = mock.Mock(
                return_value=DAOFactory.default_instrument_state
            )

            version2 = self.version + 1
            groceryList = (
                GroceryListItem.builder().native().normalization(self.runNumber1, "stateId", self.version).buildList()
            )
            groceryList2 = (
                GroceryListItem.builder().native().normalization(self.runNumber1, "stateId", version2).buildList()
            )

            # normalization filename is constructed
            normalizationWorkspaceName = (
                wng.rawVanadium().runNumber(self.runNumber1).version(self.version).hidden(True).build()
            )
            normalizationFilename = self.instance._lookupNormalizationWorkspaceFilename(
                normcalRunNumber=groceryList[0].runNumber,
                useLiteMode=groceryList[0].useLiteMode,
                version=self.version,
                state="stateID",
            )
            tmpRoot.addFileAs(self.sampleWSFilePath, normalizationFilename)
            assert Path(normalizationFilename).exists()

            normalizationWorkspaceName2 = (
                wng.rawVanadium().runNumber(self.runNumber1).version(version2).hidden(True).build()
            )
            normalizationFilename2 = self.instance._lookupNormalizationWorkspaceFilename(
                normcalRunNumber=groceryList2[0].runNumber,
                useLiteMode=groceryList2[0].useLiteMode,
                version=version2,
                state="stateId",
            )
            tmpRoot.addFileAs(self.sampleWSFilePath, normalizationFilename2)
            assert Path(normalizationFilename2).exists()

            assert not mtd.doesExist(normalizationWorkspaceName)
            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == normalizationWorkspaceName
            assert mtd.doesExist(normalizationWorkspaceName)
            self.instance._processNeutronDataCopy.assert_called_once()
            self.instance._processNeutronDataCopy.reset_mock()

            assert not mtd.doesExist(normalizationWorkspaceName2)
            items = self.instance.fetchGroceryList(groceryList2)
            assert mtd.doesExist(normalizationWorkspaceName2)
            assert not mtd.doesExist(normalizationWorkspaceName)
            assert items[0] == normalizationWorkspaceName2
            self.instance._processNeutronDataCopy.assert_called_once()

            mockLogger.info.assert_any_call(
                f"Fetching normalization workspace for run {self.runNumber1}, version {self.version}"
            )

    def test_fetch_grocery_list_normalization_cached(self):
        # Test of workspace type "normalization" as `Input` argument in the `GroceryList`:
        #   workspace already in ADS
        self.instance.grocer = mock.Mock()
        self.instance.dataService.normalizationIndexer = self.mockIndexer()
        self.instance._processNeutronDataCopy = mock.Mock()
        groceryList = (
            GroceryListItem.builder().native().normalization(self.runNumber1, "stateId", self.version).buildList()
        )
        normalizationWorkspaceName = (
            wng.rawVanadium().runNumber(self.runNumber1).version(self.version).hidden(True).build()
        )
        CloneWorkspace(
            InputWorkspace=self.sampleWS,
            OutputWorkspace=normalizationWorkspaceName,
        )
        assert mtd.doesExist(normalizationWorkspaceName)
        testTitle = "I'm a little teapot"
        mtd[normalizationWorkspaceName].setTitle(testTitle)

        items = self.instance.fetchGroceryList(groceryList)
        assert items[0] == normalizationWorkspaceName
        assert mtd.doesExist(normalizationWorkspaceName)
        assert mtd[normalizationWorkspaceName].getTitle() == testTitle
        assert self.instance.grocer.executeRecipe.call_count == 0
        self.instance._processNeutronDataCopy.assert_not_called()

    def test_fetch_grocery_list_normalization_not_found_unreachable_code(self):
        # Test of workspace type "normalization": no normalization exists
        with state_root_redirect(self.instance.dataService) as tmpRoot:
            self.instance.dataService.normalizationIndexer = self.mockIndexer(tmpRoot.path(), "normalization")
            self.instance.dataService.normalizationIndexer.latestApplicableVersion = mock.Mock(return_value=None)

            # Bypass "version" validation, to trigger "unreachable code" section:
            groceryList = (
                GroceryListItem.builder().native().normalization(self.runNumber1, "stateId", self.version).buildList()
            )
            groceryList[0].normCalVersion = None

            with pytest.raises(RuntimeError, match="Could not find any Normalizations associated with run.*"):
                self.instance.fetchGroceryList(groceryList)

    def test_fetch_grocery_list_reductionPixelMask(self):
        # Test of workspace type "reduction_pixel_mask" as `Input` argument in the `GroceryList`
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)

        stateId = "ab8704b0bc2a2342"
        with reduction_root_redirect(self.instance.dataService, stateId=stateId):
            groceryList = (
                GroceryListItem.builder()
                .reduction_pixel_mask(self.runNumber1, self.timestamp)
                .useLiteMode(self.useLiteMode)
                .buildList()
            )

            # reduction pixelmask filename
            tmpPath = self.instance.dataService._constructReductionDataPath(
                self.runNumber1, self.useLiteMode, self.timestamp
            )
            tmpPath.mkdir(parents=True)

            maskWorkspaceName = wng.reductionPixelMask().runNumber(self.runNumber1).timestamp(self.timestamp).build()
            maskWorkspaceFilename = maskWorkspaceName + ".h5"
            shutil.copy2(self.samplePixelMaskFilePath, tmpPath / maskWorkspaceFilename)
            assert (tmpPath / maskWorkspaceFilename).exists()
            assert not mtd.doesExist(maskWorkspaceName)

            items = self.instance.fetchGroceryList(groceryList)
            assert items[0] == maskWorkspaceName
            assert mtd.doesExist(maskWorkspaceName)
            assert isinstance(mtd[maskWorkspaceName], MaskWorkspace)

    def test_fetch_grocery_list_unknown_type(self):
        groceryList = GroceryListItem.builder().native().diffcal_mask(self.runNumber, self.version).buildList()
        groceryList[0].workspaceType = "banana"
        with pytest.raises(
            RuntimeError,
            match="unrecognized 'workspaceType': 'banana'",
        ):
            self.instance.fetchGroceryList(groceryList)

    def test_fetch_grocery_list_with_source(self):
        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = mock.Mock()

        self.instance.fetchNeutronDataCached = mock.Mock(return_value={"result": True, "workspace": cleanWorkspace})
        self.instance.fetchGroupingDefinition = mock.Mock(return_value={"result": True, "workspace": groupWorkspace})

        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).source(
            InstrumentDonor=cleanWorkspace
        ).add()
        groceryList = clerk.buildList()

        inputItem, groupingItemWithSource = groceryList

        res = self.instance.fetchGroceryList(groceryList)
        assert res == [cleanWorkspace, groupWorkspace]
        self.instance.fetchGroupingDefinition.assert_called_with(groupingItemWithSource)
        self.instance.fetchNeutronDataCached.assert_called_with(inputItem)

    def test_updateInstrumentParameters(self):
        wsName = mtd.unique_hidden_name()
        CloneWorkspace(
            InputWorkspace=self.sampleWSBareInstrument,
            OutputWorkspace=wsName,
        )
        assert mtd.doesExist(wsName)

        # Verify that there is a _zero_ relative location prior to updating instrument location parameters
        ws = mtd[wsName]
        # Exact float comparison is intended: these should match
        assert ws.getInstrument().getComponentByName("West").getRelativeRot() == Quat(1.0, 0.0, 0.0, 0.0)
        assert ws.getInstrument().getComponentByName("West").getRelativePos() == V3D(0.0, 0.0, 0.0)
        assert ws.getInstrument().getComponentByName("East").getRelativeRot() == Quat(1.0, 0.0, 0.0, 0.0)
        assert ws.getInstrument().getComponentByName("East").getRelativePos() == V3D(0.0, 0.0, 0.0)

        self.instance.updateInstrumentParameters(wsName, self.detectorState1)

        # Verify that all of the log-derived parameters have been added
        requiredLogs = (
            "det_arc1",
            "det_arc2",
            "BL3:Chop:Skf1:WavelengthUserReq",
            "BL3:Det:TH:BL:Frequency",
            "BL3:Mot:OpticsPos:Pos",
            "det_lin1",
            "det_lin2",
        )
        sampleLogs = mapFromSampleLogs(wsName, requiredLogs)

        # Exact float comparison is intended: these should match
        # Exact float comparison is intended: these should match
        assert sampleLogs["det_arc1"] == self.detectorState1.arc[0]
        assert sampleLogs["det_arc2"] == self.detectorState1.arc[1]
        assert sampleLogs["BL3:Chop:Skf1:WavelengthUserReq"] == self.detectorState1.wav
        assert sampleLogs["BL3:Det:TH:BL:Frequency"] == self.detectorState1.freq
        assert sampleLogs["BL3:Mot:OpticsPos:Pos"] == self.detectorState1.guideStat
        assert sampleLogs["det_lin1"] == self.detectorState1.lin[0]
        assert sampleLogs["det_lin2"] == self.detectorState1.lin[1]

        # Verify that the relative location changes correctly after updating the instrument location parameters
        assert pytest.approx(tupleFromQuat(ws.getInstrument().getComponentByName("West").getRelativeRot()), 1.0e-6) == (
            -0.008726535498373997,
            0.0,
            0.9999619230641713,
            0.0,
        )
        assert pytest.approx(tupleFromV3D(ws.getInstrument().getComponentByName("West").getRelativePos()), 1.0e-6) == (
            0.09598823540505931,
            0.0,
            5.499162323360152,
        )
        assert pytest.approx(tupleFromQuat(ws.getInstrument().getComponentByName("East").getRelativeRot()), 1.0e-6) == (
            -0.017452406437283477,
            0.0,
            0.9998476951563913,
            0.0,
        )
        assert pytest.approx(tupleFromV3D(ws.getInstrument().getComponentByName("East").getRelativePos()), 1.0e-6) == (
            0.2268467285662563,
            0.0,
            6.496040375624123,
        )

    def test_fetch_instrument_donor_neutron_data(self):
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=self.sampleWS)
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {(self.runNumber, self.useLiteMode): 0}
        self.instance._loadedInstruments = {}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
        assert testWS == self.sampleWS

    def test_fetch_instrument_donor_neutron_data_is_cached(self):
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=self.sampleWS)
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {(self.runNumber, self.useLiteMode): 0}
        self.instance._loadedInstruments = {}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)  # noqa: F841
        assert self.instance._loadedInstruments == {(self.stateId, self.useLiteMode): self.sampleWS}

    def test_fetch_instrument_donor_cached(self):
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {}
        self.instance._loadedInstruments = {(self.stateId, self.useLiteMode): self.sampleWS}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
        assert testWS == self.sampleWS

    def test_fetch_instrument_donor_empty_instrument(self):
        self.instance.dataService.generateStateId = mock.Mock(return_value=(self.stateId2, self.detectorState2))
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {}
        self.instance._loadedInstruments = {}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
        assert mtd.doesExist(testWS)
        assert not testWS == self.sampleWS

        # Verify that the instrument workspace has log-derived parameters from `self.detectorState2`.
        requiredLogs = (
            "det_arc1",
            "det_arc2",
            "BL3:Chop:Skf1:WavelengthUserReq",
            "BL3:Det:TH:BL:Frequency",
            "BL3:Mot:OpticsPos:Pos",
            "det_lin1",
            "det_lin2",
        )
        sampleLogs = mapFromSampleLogs(testWS, requiredLogs)

        # Exact float comparison is intended: these should match
        assert sampleLogs["det_arc1"] == self.detectorState2.arc[0]
        assert sampleLogs["det_arc2"] == self.detectorState2.arc[1]
        assert sampleLogs["BL3:Chop:Skf1:WavelengthUserReq"] == self.detectorState2.wav
        assert sampleLogs["BL3:Det:TH:BL:Frequency"] == self.detectorState2.freq
        assert sampleLogs["BL3:Mot:OpticsPos:Pos"] == self.detectorState2.guideStat
        assert sampleLogs["det_lin1"] == self.detectorState2.lin[0]
        assert sampleLogs["det_lin2"] == self.detectorState2.lin[1]

    def test_fetch_instrument_donor_empty_instrument_is_cached(self):
        self.instance.dataService.generateStateId = mock.Mock(return_value=(self.stateId2, self.detectorState2))
        self.instance._updateNeutronCacheFromADS = mock.Mock()
        self.instance._updateInstrumentCacheFromADS = mock.Mock()
        self.instance._loadedRuns = {}
        self.instance._loadedInstruments = {}
        testWS = self.instance._fetchInstrumentDonor(self.runNumber, self.useLiteMode)
        assert self.instance._loadedInstruments == {(self.stateId2, self.useLiteMode): testWS}

    def test_update_instrument_cache_from_ADS_to_cache(self):
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=self.sampleWS)
        self.instance._loadedInstruments = {}
        key = self.instance._key(self.stateId, self.useLiteMode)
        self.instance._updateInstrumentCacheFromADS(self.runNumber, self.useLiteMode, key)
        assert self.instance._loadedInstruments == {(self.stateId, self.useLiteMode): self.sampleWS}

    def test_update_instrument_cache_from_ADS_cache_del(self):
        testWSName = "not_any_workspace"
        self.instance._createRawNeutronWorkspaceName = mock.Mock(return_value=testWSName)
        key = self.instance._key(self.stateId, self.useLiteMode)
        self.instance._loadedInstruments = {key: testWSName}
        self.instance._updateInstrumentCacheFromADS(self.runNumber, self.useLiteMode, key)
        assert self.instance._loadedInstruments == {}

    def test_fetchCompatibleMask(self):
        # Add an actual instrument state to the sample workspace
        instrumentWs = mtd.unique_hidden_name()
        CloneWorkspace(
            InputWorkspace=self.sampleWS,
            OutputWorkspace=instrumentWs,
        )
        assert mtd.doesExist(instrumentWs)

        self.instance.updateInstrumentParameters(instrumentWs, self.detectorState1)
        stateId, _ = self.instance.dataService.stateIdFromWorkspace(instrumentWs)

        with mock.patch.object(self.instance, "_fetchInstrumentDonor", mock.Mock(return_value=instrumentWs)):
            maskWsName = mtd.unique_hidden_name()
            self.instance.fetchCompatiblePixelMask(maskWsName, self.runNumber, self.useLiteMode)

            # Mask shall have a number of spectra equal to the number of non-monitor pixels
            #   in the template workspace's instrument.
            pixelCount = mtd[instrumentWs].getInstrument().getNumberDetectors(True)
            assert mtd[maskWsName].getNumberHistograms() == pixelCount

            # Mask shall have the same instrument state as the template workspace.
            maskStateId, _ = self.instance.dataService.stateIdFromWorkspace(maskWsName)
            assert maskStateId == stateId

            # Mask shall be blank.
            assert mtd[maskWsName].getNumberMasked() == 0

    def test_unique_hidden_name(self):
        testWSName = "not_any_workspace"
        self.instance.mantidSnapper.mtd.unique_hidden_name = mock.Mock(return_value=testWSName)
        wsName = self.instance.uniqueHiddenName()
        assert wsName == testWSName

    def test_fetch_grocery_dict(self):
        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = "groupme"
        self.instance.fetchGroceryList = mock.Mock(return_value=[cleanWorkspace, groupWorkspace])

        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).name("InputWorkspace").add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).name("GroupingWorkspace").add()
        groceryDict = clerk.buildDict()

        res = self.instance.fetchGroceryDict(groceryDict)
        assert res == {"InputWorkspace": cleanWorkspace, "GroupingWorkspace": groupWorkspace}

        assert self.instance.fetchGroceryList.call_count == 1

        # <dict>.values() compare as "NOT IMPLEMENTED", which evaluates to False
        #   => so we need to extract and compare the call args as lists.
        assert list(self.instance.fetchGroceryList.call_args[0][0]) == list(groceryDict.values())

    def test_fetch_grocery_dict_with_kwargs(self):
        # expected workspaces
        cleanWorkspace = "unimportant"
        groupWorkspace = "groupme"
        otherWorkspace = "somethingelse"
        self.instance.fetchGroceryList = mock.Mock(return_value=[cleanWorkspace, groupWorkspace])

        clerk = GroceryListItem.builder()
        clerk.native().neutron(self.runNumber).name("InputWorkspace").add()
        clerk.native().fromRun(self.runNumber).grouping(self.groupingScheme).name("GroupingWorkspace").add()
        groceryDict = clerk.buildDict()

        res = self.instance.fetchGroceryDict(groceryDict, OtherWorkspace=otherWorkspace)
        assert res == {
            "InputWorkspace": cleanWorkspace,
            "GroupingWorkspace": groupWorkspace,
            "OtherWorkspace": otherWorkspace,
        }

        assert self.instance.fetchGroceryList.call_count == 1

        # <dict>.values() compare as "NOT IMPLEMENTED", which evaluates to False
        #   => so we need to extract and compare the call args as lists.
        assert list(self.instance.fetchGroceryList.call_args[0][0]) == list(groceryDict.values())

    def generateTestDiffcalTable(self, refTable=None):
        ## Create the reference table
        if refTable is None:
            refTable = mtd.unique_name(prefix="_table_")
        # these values found from mantid workbench, CalculateDIFC with test instrument
        known_difc = [
            2434.197617645125,
            1561.1814075314217,
            2741.860927442078,
            2058.4132086720383,
            2741.860927442078,
            2434.197617645125,
            2058.4132086720383,
            1561.1814075314217,
            1561.1814075314217,
            2058.4132086720383,
            2434.197617645125,
            2741.860927442078,
            2058.4132086720383,
            1561.1814075314217,
            2741.860927442078,
            2434.197617645125,
        ]
        known_list_of_dict = [
            {"detid": int(i), "difc": known_difc[i], "difa": 0.0, "tzero": 0.0} for i in range(len(known_difc))
        ]
        GenerateTableWorkspaceFromListOfDict(
            OutputWorkspace=refTable,
            ListOfDict=json.dumps(known_list_of_dict),
        )
        return refTable

    def test_fetch_default_diffcal_table(self):
        """
        Use the test instrument to create a default DIFC table from the method.
        Compare to known values, found independently in mantid workbench.
        """
        runNumber = "123"
        useLiteMode = True

        ## Create the reference table
        refTable = self.generateTestDiffcalTable()

        ## Create the default diffcal table
        idfWS = mtd.unique_name(prefix="_idf_")
        LoadEmptyInstrument(
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
            OutputWorkspace=idfWS,
        )
        self.instance._fetchInstrumentDonor = mock.Mock(return_value=idfWS)
        ws = self.instance.fetchDefaultDiffCalTable(runNumber, useLiteMode, self.version)

        # make sure the correct workspace name is generated
        assert ws == self.instance.createDiffCalTableWorkspaceName("default", useLiteMode, self.version)
        ## Compare the two diffcal tables to ensure equality
        table1 = mtd[ws]
        table2 = mtd[refTable]
        assert table1.rowCount() == table2.rowCount()
        for i in range(table1.rowCount()):
            print(list(table1.row(i).values()), list(table2.row(i).values()))
            assert list(table1.row(i).values()) == list(table2.row(i).values())

        ## Ensure graceful failure if the workspace is not created
        self.instance.workspaceDoesExist = mock.Mock(return_value=False)
        with pytest.raises(RuntimeError) as e:
            self.instance.fetchDefaultDiffCalTable(runNumber, useLiteMode, self.version)
        assert runNumber in str(e)

    @mock.patch("snapred.backend.service.LiteDataService.LiteDataService")
    def test_make_lite(self, mockLiteDataService):
        # This test makes sure that `GroceryService.convertToLiteMode` appropriately calls
        #   `LiteDataService`.
        # The circular reference from `LiteDataService` back to `GroceryService.fetchLiteDataMap`
        #   (which should almost certainly be a service-to-service `InterfaceController` request)
        #   should be checked in `test_LiteDataService`, _not_ here.

        mockLiteDataService().createLiteData = mock.Mock()
        mockLiteDataService().createLiteData.return_value = ("ws", 0.004)

        # reset calls
        mockLiteDataService.reset_mock()

        workspacename = self.instance._createNeutronWorkspaceName(self.runNumber, False)
        self.instance.convertToLiteMode(workspacename)

        # assert that the lite data service was created and called
        mockLiteDataService.assert_called_once()
        mockLiteDataService.return_value.createLiteData.assert_called_once_with(
            workspacename, workspacename, export=True
        )

        with Config_override("constants.LiteDataCreationAlgo.toggleCompressionTolerance", True):
            mockLiteDataService.reset_mock()
            self.instance.writeWorkspaceMetadataAsTags = mock.Mock()

            expected = WorkspaceMetadata(liteDataCompressionTolerance=0.004)

            self.instance.convertToLiteMode(workspacename)

            self.instance.writeWorkspaceMetadataAsTags.assert_called_once_with(workspacename, expected)

    def test_getCachedWorkspaces(self):
        rawWsName = self.instance._createRawNeutronWorkspaceName("556854", False)
        self.instance._loadedRuns = {("556854", False): 0}
        self.instance._loadedGroupings = {("column", "556854", True): "d"}
        self.instance._loadedInstruments = {("556854", False): rawWsName, ("556855", True): "hello"}
        assert set(self.instance.getCachedWorkspaces()) == set([rawWsName, "d", "hello"])

    def test_getCachedWorkspaces_empty(self):
        self.instance._loadedRuns = {}
        self.instance._loadedGroupings = {}
        self.instance._loadedInstruments = {}

        assert self.instance.getCachedWorkspaces() == []

    def test_renameWorkspace(self):
        oldName = "old"
        newName = "new"
        self.create_dumb_workspace(oldName)
        assert mtd.doesExist(oldName)
        self.instance.renameWorkspace(oldName, newName)
        assert not mtd.doesExist(oldName)
        assert mtd.doesExist(newName)

    def test_renameWorkspaces(self):
        oldNames = ["old1", "old2"]
        newNames = ["new1", "new2"]
        for oldName in oldNames:
            self.create_dumb_workspace(oldName)
            assert mtd.doesExist(oldName)
        self.instance.renameWorkspaces(oldNames, newNames)
        for oldName in oldNames:
            assert not mtd.doesExist(oldName)
        for newName in newNames:
            assert mtd.doesExist(newName)

    def test_clearADS(self):
        rawWsName = self.instance._createRawNeutronWorkspaceName(0, "a")
        self.instance._loadedRuns = {(0, "a"): rawWsName}
        self.instance._loadedGroupings = {(1, "c"): "d"}

        with mock.patch.object(self.instance, "rebuildCache") as mockRebuildCache:
            self.create_dumb_workspace(rawWsName)  # in the cache
            self.create_dumb_workspace("b")  # not in the cache
            self.instance.clearADS(exclude=self.exclude)  # default => don't clear cache
            assert not mtd.doesExist("b")
            assert mtd.doesExist(rawWsName)

            assert mtd.doesExist(rawWsName)
            self.instance.clearADS(exclude=self.exclude, clearCache=True)
            assert not mtd.doesExist(rawWsName)
            mockRebuildCache.assert_called()

    def test_clearADS_group(self):
        # create a workspace that will be removed
        dumbws = mtd.unique_name(prefix="_dumb_")
        self.create_dumb_workspace(dumbws)
        assert mtd.doesExist(dumbws)

        # create a workspace group
        groupws = mtd.unique_name(prefix="_groupws_")
        subws1 = mtd.unique_name(prefix="a")
        subws2 = mtd.unique_name(prefix="b")
        self.create_dumb_workspace(subws1)
        self.create_dumb_workspace(subws2)
        GroupWorkspaces(
            OutputWorkspace=groupws,
            InputWorkspaces=[subws1, subws2],
        )
        assert mtd.doesExist(groupws)
        assert mtd.doesExist(subws1)
        assert mtd.doesExist(subws2)
        assert mtd[groupws].isGroup()
        assert set(mtd[groupws].getNames()) == {subws1, subws2}

        # exclude the group from clearing
        exclude = self.exclude.copy()
        exclude.append(groupws)

        # clear the ADS with this excluded
        self.instance.clearADS(exclude=exclude)

        # ensure the workspace group and its sub still exist
        assert mtd.doesExist(groupws)
        assert mtd.doesExist(subws1)
        assert mtd.doesExist(subws2)
        # assert the workspace not in the exclude is removed
        assert not mtd.doesExist(dumbws)

        # delete these specially
        mtd.remove(groupws)

    def test_getResidentWorkspaces(self):
        with mock.patch.object(self.instance, "getCachedWorkspaces") as mockGetCachedWorkspaces:
            expected = list(set(mtd.getObjectNames()))  # note: re-ordering is required
            assert len(expected)

            actual = self.instance.getResidentWorkspaces(excludeCache=False)
            mockGetCachedWorkspaces.assert_not_called()
            assert actual == expected

    def test_getResidentWorkspaces_exclude_cache(self):
        rawWsName = self.instance._createRawNeutronWorkspaceName(0, "a")
        self.instance._loadedRuns = {(0, "a"): rawWsName}
        expected = list(set(mtd.getObjectNames()).difference([rawWsName]))
        assert len(expected)

        actual = self.instance.getResidentWorkspaces(excludeCache=True)
        assert actual == expected

    def test__lookupDiffCalWorkspaceNames_no_record(self):
        runNumber = 123
        version = 1
        mockIndexer = mock.Mock()
        mockIndexer.readRecord = mock.Mock(return_value=None)
        mockDataService = mock.Mock()
        mockDataService.calibrationIndexer = mock.Mock(return_value=mockIndexer)
        self.instance.dataService = mockDataService

        with pytest.raises(RuntimeError, match=r".*Could not find calibration record*"):
            self.instance._lookupDiffCalWorkspaceNames(runNumber, True, "stateId", version)

    def test__lookupDiffCalWorkspaceNames_nonInt_version(self):
        runNumber = 123
        version = "v1"
        mockRecord = mock.Mock()
        mockRecord.version = 1
        mockRecord.workspaces = {
            WorkspaceType.DIFFCAL_TABLE: ["diffcal_table"],
            WorkspaceType.DIFFCAL_MASK: ["diffcal_mask"],
        }
        mockIndexer = mock.Mock()
        mockIndexer.latestApplicableVersion = mock.Mock(return_value=1)
        mockIndexer.readRecord = mock.Mock(return_value=mockRecord)
        mockDataService = mock.Mock()
        mockDataService.calibrationIndexer = mock.Mock(return_value=mockIndexer)
        self.instance.dataService = mockDataService

        self.instance._lookupDiffCalWorkspaceNames(runNumber, True, "stateId", version)

        mockIndexer.latestApplicableVersion.assert_called_once_with(runNumber)

    def test__lookupDiffCalWorkspaceNames_missing_table_workspace_in_record(self):
        runNumber = 123
        version = "v1"
        mockRecord = mock.Mock()
        mockRecord.version = 1
        mockRecord.workspaces = {"does_not_exist": ["diffcal_table"], WorkspaceType.DIFFCAL_MASK: ["diffcal_mask"]}
        mockIndexer = mock.Mock()
        mockIndexer.readRecord = mock.Mock(return_value=mockRecord)
        mockDataService = mock.Mock()
        mockDataService.calibrationIndexer = mock.Mock(return_value=mockIndexer)
        self.instance.dataService = mockDataService

        with pytest.raises(RuntimeError, match=r".*Could not find any table workspaces in calibration record*"):
            self.instance._lookupDiffCalWorkspaceNames(runNumber, True, "stateId", version)

    def test__lookupDiffCalWorkspaceNames_missing_mask_workspace_in_record(self):
        # In a calibration record, the mask workspace is optional.

        runNumber = 123
        version = "v1"
        mockRecord = mock.Mock()
        mockRecord.version = 1
        mockRecord.workspaces = {WorkspaceType.DIFFCAL_TABLE: ["diffcal_table"], "does_not_exist": ["lah_dee_dah"]}
        mockIndexer = mock.Mock()
        mockIndexer.readRecord = mock.Mock(return_value=mockRecord)
        mockDataService = mock.Mock()
        mockDataService.calibrationIndexer = mock.Mock(return_value=mockIndexer)
        self.instance.dataService = mockDataService

        tableWorkspaceName, maskWorkspaceName = self.instance._lookupDiffCalWorkspaceNames(
            runNumber, True, "stateId", version
        )
        assert tableWorkspaceName == mockRecord.workspaces[WorkspaceType.DIFFCAL_TABLE][0]
        assert maskWorkspaceName is None

    def test_checkPixelMask(self):
        # raises an error if workspace not in ADS
        nonexistent = mtd.unique_name(prefix="_mask_check_")
        assert not mtd.doesExist(nonexistent)
        assert not self.instance.checkPixelMask(nonexistent)

        # raises an error if the workspace is not a MaskWorkspace
        notamask = mtd.unique_name(prefix="_mask_check_")
        CreateSampleWorkspace(
            OutputWorkspace=notamask,
            NumBanks=1,
            BankPixelWidth=1,
        )
        assert mtd.doesExist(notamask)
        assert not isinstance(mtd[notamask], MaskWorkspace)
        assert not self.instance.checkPixelMask(notamask)

        # return False if nothing is masked
        emptymask = mtd.unique_name(prefix="_mask_check_")
        ExtractMask(InputWorkspace=notamask, OutputWorkspace=emptymask)
        assert isinstance(mtd[emptymask], MaskWorkspace)
        assert mtd[emptymask].getNumberMasked() == 0
        assert not self.instance.checkPixelMask(emptymask)

        # return True if something is masked
        nonemptymask = mtd.unique_name(prefix="_mask_check_")
        MaskDetectors(Workspace=notamask, WorkspaceIndexList=[0])
        ExtractMask(InputWorkspace=notamask, OutputWorkspace=nonemptymask)
        assert isinstance(mtd[nonemptymask], MaskWorkspace)
        assert mtd[nonemptymask].getNumberMasked() != 0
        assert self.instance.checkPixelMask(nonemptymask)

    def test_fetchDiffCalForSample_altPath(self):
        item = GroceryListItem(
            workspaceType="diffcal_table",
            diffCalFilePath=Path("some/path/to/diffcal_table.h5"),
            state="stateId",
            useLiteMode=True,
            diffCalVersion=0,
        )
        with mock.patch.object(
            self.instance,
            "_loadCalibrationFile",
        ):
            tableName, maskName = self.instance.fetchDiffCalForSample(item)
            assert tableName == "diffcal_table"
            assert maskName == "diffcal_table_mask"

    def test_processNeutronDataCopy(self):
        # confirm that appropriate algorthims are applied to the workspace
        testCalibrationData = DAOFactory.calibrationParameters()
        instance = GroceryService()
        with (
            mock.patch.object(instance, "mantidSnapper") as _,
            mock.patch.object(instance, "dataService") as _,
        ):
            instance.mantidSnapper = mock.Mock()
            instance.dataService = mock.Mock()
            instance.dataService.generateInstrumentState = mock.Mock(return_value=DAOFactory.default_instrument_state)
            instance.fetchDiffCalForSample = mock.Mock(return_value=("diffcal_table", "diffcal_mask"))

            item = GroceryListItem(
                workspaceType="neutron", runNumber="123", useLiteMode=True, loader="", diffCalVersion="3"
            )
            instance._processNeutronDataCopy(item, "wsName")

            assert instance.mantidSnapper.CropWorkspace.call_count == 1
            assert instance.mantidSnapper.CropWorkspace.call_args[1]["OutputWorkspace"] == "wsName"
            assert (
                instance.mantidSnapper.CropWorkspace.call_args[1]["XMin"]
                == testCalibrationData.instrumentState.particleBounds.tof.minimum
            )
            assert (
                instance.mantidSnapper.CropWorkspace.call_args[1]["XMax"]
                == testCalibrationData.instrumentState.particleBounds.tof.maximum
            )

            assert instance.mantidSnapper.RemovePromptPulse.call_count == 1
            assert instance.mantidSnapper.RemovePromptPulse.call_args[1]["OutputWorkspace"] == "wsName"
            assert (
                instance.mantidSnapper.RemovePromptPulse.call_args[1]["Width"]
                == testCalibrationData.instrumentState.instrumentConfig.width
            )
            assert (
                instance.mantidSnapper.RemovePromptPulse.call_args[1]["Frequency"]
                == testCalibrationData.instrumentState.instrumentConfig.frequency
            )

            assert instance.mantidSnapper.ApplyDiffCal.call_count == 1
            assert instance.mantidSnapper.ApplyDiffCal.call_args[1]["InstrumentWorkspace"] == "wsName"
            assert instance.mantidSnapper.ApplyDiffCal.call_args[1]["CalibrationWorkspace"] == "diffcal_table"

            # reset mocks
            instance.mantidSnapper.reset_mock()

            with Config_override("mantid.workspace.normalizeByBeamMonitor", True):
                instance.fetchMonitorWorkspace = mock.Mock(return_value="monitor")
                instance.writeWorkspaceMetadataAsTags = mock.Mock()
                instance.deleteWorkspaceUnconditional = mock.Mock()
                instance.mantidSnapper.mtd = mock.MagicMock()
                instance.mantidSnapper.mtd["monitor"].getSpectrum(0).getNumberEvents.return_value = 1000

                instance._processNeutronDataCopy(item, "wsName")
                assert instance.mantidSnapper.CropWorkspace.call_count == 2
                assert instance.mantidSnapper.RemovePromptPulse.call_count == 2

                assert instance.fetchMonitorWorkspace.call_count == 1
                assert instance.fetchMonitorWorkspace.call_args[0][0] == item

                assert instance.writeWorkspaceMetadataAsTags.call_count == 1
                assert instance.writeWorkspaceMetadataAsTags.call_args[0][0] == "wsName"
                assert instance.writeWorkspaceMetadataAsTags.call_args[0][1].dict().get("normalizeByMonitorID") == 0
                assert (
                    instance.writeWorkspaceMetadataAsTags.call_args[0][1].dict().get("normalizeByMonitorFactor") == 1000
                )

                assert instance.deleteWorkspaceUnconditional.call_count == 1
                assert instance.deleteWorkspaceUnconditional.call_args[0][0] == "monitor"

    def test_fetchMonitorWorkspace(self):
        item = GroceryListItem(workspaceType="neutron", runNumber="123", useLiteMode=True, loader="")
        instance = GroceryService()
        instance.dataService.generateInstrumentState = mock.Mock(
            return_value=DAOFactory.calibrationParameters().instrumentState
        )
        instance._createNeutronFilePath = mock.Mock(return_value="neutron")
        with (
            mock.patch.object(instance, "_createMonitorWorkspaceName", wraps=instance._createMonitorWorkspaceName) as _,
            mock.patch.object(instance, "_createNeutronFilePath", wraps=instance._createNeutronFilePath) as _,
            mock.patch.object(instance, "grocer") as _,
        ):
            name = instance.fetchMonitorWorkspace(item)

            assert name == wng.monitor().runNumber("123").build()
            assert instance._createMonitorWorkspaceName.call_count == 1
            assert instance._createMonitorWorkspaceName.call_args[0][0] == item.runNumber

            assert instance._createNeutronFilePath.call_count == 1
            assert instance._createNeutronFilePath.call_args[0][0] == item.runNumber
            assert not instance._createNeutronFilePath.call_args[0][1]

            assert instance.grocer.executeRecipe.call_count == 1
            assert instance.grocer.executeRecipe.call_args[0][0] == str(
                self.instance._createNeutronFilePath(item.runNumber, False)
            )

    def test_updateInstrument(self):
        with (
            mock.patch.object(self.instance, "getWorkspaceForName") as mockWorkspaceFunc,
            mock.patch.object(self.instance, "mantidSnapper") as mockSnapper,
        ):
            mockWorkspace = mock.Mock()
            mockWorkspaceFunc.return_value = mockWorkspace
            mockWorkspace.getInstrument().getName.return_value = "snap"
            mockWorkspace.getNumberHistograms.return_value = Config["instrument.lite.pixelResolution"]

            mockSnapper.mtd = {"wsName": mockWorkspaceFunc}

            self.instance.updateInstrument("wsName")

            assert mockWorkspaceFunc().getInstrument().getName.call_count == 1
            assert self.instance.mantidSnapper.executeQueue.call_count == 1
            assert self.instance.mantidSnapper.LoadInstrument.call_count == 1
            self.instance.mantidSnapper.LoadInstrument.assert_called_once_with(
                "Replacing instrument definition with Lite instrument",
                Workspace="wsName",
                Filename=Config["instrument.lite.definition.file"],
                RewriteSpectraMap=False,
            )
