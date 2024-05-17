# ruff: noqa: E402

import importlib
import json
import logging
import os
import shutil
import socket
import tempfile
import unittest.mock as mock
from pathlib import Path
from random import randint, shuffle
from typing import List

import pytest
from mantid.api import ITableWorkspace, MatrixWorkspace
from mantid.dataobjects import MaskWorkspace
from mantid.simpleapi import (
    CloneWorkspace,
    CreateGroupingWorkspace,
    CreateSampleWorkspace,
    LoadEmptyInstrument,
    LoadInstrument,
    mtd,
)
from pydantic import parse_raw_as
from snapred.backend.dao import StateConfig
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as WNG
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.redantic import write_model_pretty
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.WhateversInTheFridge import WhateversInTheFridge, RedirectStateRoot
from util.InstaEats import InstaEats


VERSION_START = Config["version.calibration.start"]

fakeInstrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
reductionIngredientsPath = Resource.getPath("inputs/calibration/ReductionIngredients.json")
reductionIngredients = ReductionIngredients.parse_file(reductionIngredientsPath)


class ImitationDataService(LocalDataService):
    stateId = "notarealstate000"

    def _generateStateId(self, *x, **y):
        return self.stateId, 0
    
    def getIPTS(self, *x, **y):
        return Resource.getPath("inputs/testInstrument/IPTS-456/")
    
    def readDetectorState(self, runId: str):
        return DetectorState.construct(wav=1.0)
    
    def _defaultGroupingMapPath(self) -> Path:
        return Path(Resource.getPath("inputs/testInstrument/groupingMap.json"))
    
    def _writeDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool):
        version = self.VERSION_START
        grocer = InstaEats()
        filename = Path(grocer._createDiffcalTableWorkspaceName("default", useLiteMode, str(version)) + ".h5")
        outWS = grocer.fetchDefaultDiffCalTable(runNumber, useLiteMode, version)
        calibrationDataPath = self._constructCalibrationDataPath(runNumber, useLiteMode, version)
        self.writeDiffCalWorkspaces(calibrationDataPath, filename, outWS)



def test_calibration_versioning():
    runNumber = "123"
    useLiteMode = True    
    lds = ImitationDataService()
    dfs = DataFactoryService(lookupService=lds)
    des = DataExportService(dataService=lds)
    with RedirectStateRoot(lds, stateId="notarealstate000") as tmpRoot:
        # start with unitialized state
        stateId, _ = dfs.lookupService._generateStateId(runNumber)
        assert stateId == tmpRoot.stateId
        difcalPath = dfs.lookupService._constructCalibrationStatePath(stateId, useLiteMode)
        normalizationPath = dfs.lookupService._constructNormalizationStatePath(stateId, useLiteMode)
        assert not Path(difcalPath).exists()
        assert not Path(normalizationPath).exists()
        assert not dfs.checkCalibrationStateExists(runNumber)
        with pytest.raises(RecoverableException):
            dfs.getCalibrationState(runNumber, useLiteMode)

        # assert there is no calibration index
        assert [] == dfs.getCalibrationIndex(runNumber, useLiteMode)

        # assert there are no versions
        versionIndex = lds._getVersionFromCalibrationIndex(runNumber, useLiteMode)
        versionFile = lds._getLatestCalibrationVersionNumber(stateId, useLiteMode)
        assert versionIndex is None
        assert versionFile == VERSION_START

        # now initialize state
        des.initializeState(runNumber, useLiteMode, "teststate")

        # assert initialize state creates the diffcal path
        # assert the v0 directory is created
        assert Path(difcalPath).exists()
        version = dfs.lookupService._getLatestCalibrationVersionNumber(stateId, useLiteMode)
        difcalV0 = dfs.lookupService._constructCalibrationDataPath(runNumber, useLiteMode, version)
        print(difcalV0)
        assert Path(difcalV0).exists()

        # assert there is a state
        assert dfs.checkCalibrationStateExists(runNumber)

        # assert there are versions
        versionIndex = lds._getVersionFromCalibrationIndex(runNumber, useLiteMode)
        versionFile = lds._getLatestCalibrationVersionNumber(tmpRoot.stateId, useLiteMode)
        assert versionIndex == VERSION_START
        assert versionFile == VERSION_START

        # 


        # # get the state ID
        # stateID = dfs.constructStateId(runNumber)
        # assert not Path(tmpDir/stateID).exists()
        
        # # create a state
        # # lds.getIPTS = mock.Mock(return_value="")
        # des.initializeState(runNumber, useLiteMode, "test state")
        # assert dfs.checkCalibrationStateExists(runNumber)
        # assert Path(tmpDir/stateID).exists()

