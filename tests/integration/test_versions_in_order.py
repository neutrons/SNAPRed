# ruff: noqa: E402, ARG002

"""

This is a test of verisoning inside calibration from the API layer.
Calls are made through the API to endpoints inside the calibration service,
down to the underlying data layer.

The data layer has been redirected to point inside a temporary file.
There is nothing mocked, and files are actually being read / written.
The data layer objects are SUBCLASSES rather than mocks, meaning thet are actually objects
of the type indicated, and most of their methods are inherited from the parent classes.  Only
those methods necessary to generate correct data have been overriden.

This will run the entire diffraction calculation recipe on synthetic data inside the test instrument ("POP")
and then save it, first at the starting version (0), then at a specified version (7)
All versioned output is checked to make sure the correct versions are written.

If this runs, we can have good confidence that calls to the calibration service can run
all the way to the data layer in the way expected.

"""

import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest import TestCase

from mantid.simpleapi import (
    CalculateDiffCalTable,
    CloneWorkspace,
    LoadEmptyInstrument,
    mtd,
)
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT, VERSION_START
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.service.SousChef import SousChef
from snapred.meta.Config import Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.redantic import parse_file_as, write_model_pretty
from util.dao import DAOFactory
from util.diffraction_calibration_synthetic_data import SyntheticData

dataSynthesizer = SyntheticData()


class ImitationDataService(LocalDataService):
    """

    This subclass of LocalDataService redirects some of the behavior to point to test resources,
    to perform a simpler calculation on the test instrument ("POP")

    """

    stateId = "abc123padto16xxx"

    def __init__(self):
        super().__init__()
        self._outputPath = Path(Resource.getPath("outputs/version_test"))
        self._outputPath.mkdir(exist_ok=True)
        self._stateRoot = self._outputPath / self.stateId
        if self._stateRoot.exists():
            shutil.rmtree(self._stateRoot)
            assert not self._stateRoot.exists()
        # add the default grouping map for state init to find
        write_model_pretty(DAOFactory.groupingMap_POP(), self._defaultGroupingMapPath())

    def __del__(self):
        if self._outputPath.exists():
            shutil.rmtree(self._outputPath)

    def readCifFilePath(self, sampleId: str):
        return Resource.getPath("inputs/crystalInfo/example.cif")

    def getIPTS(self, *x, **y):
        # if this is not overriden, it creates hundreds of headaches
        return Resource.getPath("inputs/testInstrument/IPTS-456/")

    def generateStateId(self, runId: str) -> Tuple[str, str]:
        return (self.stateId, "gibberish")

    def constructCalibrationStateRoot(self, stateId) -> Path:
        return Path(self._stateRoot)

    def readCalibrationState(self, runId: str, useLiteMode: bool):
        return DAOFactory.calibrationParameters(runId, useLiteMode)

    def readDetectorState(self, runId: str):
        return DetectorState(
            arc=(1, 2),
            wav=1.1,
            freq=1.2,
            guideStat=1,
            lin=(1, 2),
        )

    def _defaultGroupingMapPath(self) -> Path:
        return self._outputPath / "defaultGroupingMap.json"

    def _writeDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool):
        """
        Note this replicates the original in every respect, except using the ImitationGroceryService
        """
        version = VERSION_DEFAULT
        grocer = ImitationGroceryService()
        outWS = grocer.fetchDefaultDiffCalTable(runNumber, useLiteMode, version)
        filename = Path(outWS + ".h5")
        calibrationDataPath = self.calibrationIndexer(runNumber, useLiteMode).versionPath(version)
        self.writeDiffCalWorkspaces(calibrationDataPath, filename, outWS)

    # TODO delete this method override when SaveNexusProcessed has been fixed
    def writeWorkspace(self, path: Path, filename: Path, workspace: str, append=False):
        """
        NOTE there is a bug inside SaveNexusProcessed which causes a segfault with about 50% probability
        The problem is within mantid and will require a separate ticket to fix.
        This override is a temporary fix for the test to work.
        """
        with open(path / filename, "w") as f:
            f.write("x")


class ImitationGroceryService(GroceryService):
    """
    This makes use of the SyntheticData class, and overloads the fetch methods
    of the GroceryService to instead return the synthetic data generated by SyntheticData
    """

    def __init__(self):
        # create workspaces that will be returned as though they were loaded
        super().__init__()

        # instrument file
        self.instrumentWS = mtd.unique_name(prefix="idf_")
        LoadEmptyInstrument(
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
            OutputWorkspace=self.instrumentWS,
        )

        # Grab the workspaces created by SyntheticData
        # return these whenever a workspace of the indicated type is requested.
        self.rawdataWS = mtd.unique_name(prefix="raw_")
        self.groupingWS = mtd.unique_name(prefix="group_")
        self.maskWS = mtd.unique_name(prefix="mask_")
        dataSynthesizer.generateWorkspaces(self.rawdataWS, self.groupingWS, self.maskWS)

        # Create a diffcal table based on the instrument geometry.
        self.diffcalTableWS = mtd.unique_name(prefix="tab_")
        CalculateDiffCalTable(
            InputWorkspace=self.instrumentWS,
            CalibrationTable=self.diffcalTableWS,
        )

    def fetchWorkspace(self, filePath: str, name: str, loader: str = "") -> Dict[str, Any]:
        CloneWorkspace(InputWorkspace=self.instrumentWS, Outputworkspace=name)
        return {"result": True, "loader": "LoadNexus", "workspace": name}

    def fetchGroupingDefinition(self, item: Any) -> Dict[str, Any]:
        workspaceName = self._createGroupingWorkspaceName(item.groupingScheme, item.runNumber, item.useLiteMode)
        CloneWorkspace(InputWorkspace=self.groupingWS, OutputWorkspace=workspaceName)
        return {
            "result": True,
            "loader": "LoadGroupingDefinition",
            "workspace": workspaceName,
        }

    def fetchNeutronDataSingleUse(self, runNumber: str, useLiteMode: bool, loader: str = "") -> Dict[str, Any]:
        return self.fetchNeutronDataCached(runNumber, useLiteMode, loader)

    def fetchNeutronDataCached(self, runNumber: str, useLiteMode: bool, loader: str = "") -> Dict[str, Any]:
        workspaceName = self._createNeutronWorkspaceName(runNumber, useLiteMode)
        CloneWorkspace(InputWorkspace=self.rawdataWS, OutputWorkspace=workspaceName)
        return {
            "result": True,
            "loader": "LoadNexus",
            "workspace": workspaceName,
        }

    def fetchCalibrationWorkspaces(self, item: Any) -> Dict[str, Any]:
        runNumber, version, useLiteMode = item.runNumber, item.version, item.useLiteMode
        tableWorkspaceName = self._createDiffcalTableWorkspaceName(runNumber, useLiteMode, version)
        maskWorkspaceName = self._createDiffcalMaskWorkspaceName(runNumber, useLiteMode, version)

        CloneWorkspace(InputWorkspace=self.diffcalTableWS, OutputWorkspace=tableWorkspaceName)
        CloneWorkspace(InputWorkspace=self.maskWS, OutputWorkspace=maskWorkspaceName)
        return {
            "result": True,
            "loader": "LoadCalibrationWorkspaces",
            "workspace": tableWorkspaceName,
        }

    def fetchDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool, version: int | Any) -> str:
        tableWorkspaceName = self._createDiffcalTableWorkspaceName("default", useLiteMode, version)
        CloneWorkspace(InputWorkspace=self.diffcalTableWS, OutputWorkspace=tableWorkspaceName)
        return tableWorkspaceName


class ImitationSousChef(SousChef):
    """
    This overrides the SousChef so that it returns the set of Diffraction
    Calibration Ingredients that were used by SyntheticData in the data
    generation process.
    """

    def prepDiffractionCalibrationIngredients(self, ingredients: Any) -> Any:
        return dataSynthesizer.ingredients


###############################################################################
###        THE TEST ITSELF                                                  ###
###############################################################################


class TestVersioning(TestCase):
    def setUp(self):
        # NOTE the act of importing InterfaceController will cause test_APIService
        # to fail.  The exact reason is unknown.  Guarding the import inside this
        # setup function prevents that test from failing.
        from snapred.backend.api.InterfaceController import InterfaceController

        self.runNumber = "54321"
        self.useLiteMode = False  # NOTE using test instrument, focus on native

        # create a calibration service
        # ensure that all of its data services are insync
        # this is necessary because `Singleton` is disabled in testing
        self.localDataService = ImitationDataService()
        groceryService = ImitationGroceryService()
        groceryService.dataService = self.localDataService
        sousChef = ImitationSousChef()
        sousChef.groceryService = groceryService
        self.instance = CalibrationService()
        self.instance.groceryService = groceryService
        self.instance.sousChef = sousChef
        self.instance.dataExportService.dataService = self.localDataService
        self.instance.dataFactoryService.lookupService = self.localDataService
        self.instance.sousChef.dataFactoryService = self.instance.dataFactoryService

        self.stateId = self.localDataService.generateStateId(self.runNumber)
        self.stateRoot = self.localDataService.constructCalibrationStateRoot(self.stateId)

        # grab the associated Indexer
        self.indexer = self.localDataService.calibrationIndexer(self.runNumber, self.useLiteMode)

        # create an InterfaceController
        self.api = InterfaceController()
        self.old_self = self.api.serviceFactory.getService
        self.api.serviceFactory.getService = lambda x: self.instance  # noqa: ARG005

    def tearDown(self):
        self.api.serviceFactory.getService = self.old_self

    def test_calibration_versioning(self):
        """
        After setting up the data services with the appropriate imitations,
        this will initialize state and perform checks that the state initialized as planned.
        This includes checking that default versions were created.

        It will then run calibration, save it, then check again that everything was
        saved as intended, including version numbers.

        It will then run calibration again, and this time save it to a particular version,
        and make sure everything saved as intended, including version numbers.

        NOTE issues with writeWorkspace and SaveNexusProcessed
        can result in segmentation faults.  I am not sure why these are happening.

        """

        # start with unitialized state -- ensure there is no state there
        assert not self.state_exists()

        # initialize state and ensure it is there
        payload = {
            "runId": self.runNumber,
            "useLiteMode": self.useLiteMode,
            "humanReadableName": "initialize the state",
        }
        request = SNAPRequest(path="calibration/initializeState", payload=json.dumps(payload))
        response = self.api.executeRequest(request)
        assert response.code <= ResponseCode.MAX_OK
        assert self.state_exists()

        # ensure the new state has grouping map, calibration state, and default diffcal table
        diffCalTableName = wng.diffCalTable().runNumber("default").version(VERSION_DEFAULT).build()
        assert self.localDataService._groupingMapPath(self.stateId).exists()
        versionDir = wnvf.pathVersion(VERSION_DEFAULT)
        assert Path(self.stateRoot, "lite", "diffraction", versionDir, "CalibrationParameters.json").exists()
        assert Path(self.stateRoot, "native", "diffraction", versionDir, "CalibrationParameters.json").exists()
        assert Path(self.stateRoot, "lite", "diffraction", versionDir, diffCalTableName + ".h5").exists()
        assert Path(self.stateRoot, "native", "diffraction", versionDir, diffCalTableName + ".h5").exists()

        # assert there is no calibration index
        assert [] == self.get_index()

        # assert the current diffcal version is the default, and the next is the start
        assert self.indexer.currentVersion() == VERSION_DEFAULT
        assert self.indexer.latestApplicableVersion(self.runNumber) == VERSION_DEFAULT
        assert self.indexer.nextVersion() == VERSION_START

        # run diffraction calibration for the first time, and save
        res = self.run_diffcal()
        self.save_diffcal(res, version=None)

        # ensure things saved correctly
        self.assert_diffcal_saved(VERSION_START)
        assert len(self.get_index()) == 1

        # run diffraction calibration for a second time, and save
        res = self.run_diffcal()
        self.save_diffcal(res, version=None)
        self.assert_diffcal_saved(VERSION_START + 1)
        assert len(self.get_index()) == 2

        # now save at version 7
        version = 7
        res = self.run_diffcal()
        self.save_diffcal(res, version=version)
        self.assert_diffcal_saved(version)
        assert len(self.get_index()) == 3

        # now save at next version -- will be 8
        version = 8
        res = self.run_diffcal()
        self.save_diffcal(res, version=None)  # NOTE using None points it to next version
        self.assert_diffcal_saved(version)
        assert len(self.get_index()) == 4

        shutil.rmtree(self.localDataService._outputPath)

    ### HELPER METHODS FOR ABOVE TEST ###

    def state_exists(self) -> bool:
        # send a request through interface controller to check if state exists
        payload = {"runId": self.runNumber, "useLiteMode": self.useLiteMode}
        request = SNAPRequest(path="calibration/hasState", payload=json.dumps(payload))
        response = self.api.executeRequest(request)
        assert response.code <= ResponseCode.MAX_OK
        assert response.data == self.stateRoot.exists()
        return response.data

    def get_index(self) -> List[IndexEntry]:
        # send a request through interface controller to retrieve calibration index
        payload = {"run": {"runNumber": self.runNumber, "useLiteMode": self.useLiteMode}}
        request = SNAPRequest(path="calibration/index", payload=json.dumps(payload))
        response = self.api.executeRequest(request)
        assert response.code <= ResponseCode.MAX_OK
        return response.data

    def run_diffcal(self):
        # send a request through interface controller to run the diffcal calculation
        groupingMap = self.localDataService.readGroupingMap(self.runNumber)
        groupingMap = groupingMap.getMap(self.useLiteMode)
        payload = {
            "runNumber": self.runNumber,
            "useLiteMode": self.useLiteMode,
            "focusGroup": groupingMap["Natural"].model_dump(),
            "calibrantSamplePath": Resource.getPath("inputs/calibrantSamples/Silicon_NIST_640D_001.json"),
            "fwhmMultipliers": {"left": 0.5, "right": 0.5},
            "removeBackground": True,
        }
        request = SNAPRequest(path="calibration/diffraction", payload=json.dumps(payload))
        response = self.api.executeRequest(request)
        assert response.code <= ResponseCode.MAX_OK
        return response.data

    def save_diffcal(self, res, version=None):
        # send a request through interface controller to save the diffcal results
        # needs the list of output workspaces, and may take an optional version
        # create an export request using an existing record as a basis
        workspaces = {
            wngt.DIFFCAL_OUTPUT: [res["outputWorkspace"]],
            wngt.DIFFCAL_DIAG: [res["diagnosticWorkspace"]],
            wngt.DIFFCAL_TABLE: [res["calibrationTable"]],
            wngt.DIFFCAL_MASK: [res["maskWorkspace"]],
        }
        params = DAOFactory.calibrationParameters()
        params.creationDate = 0  # the creation data cannot be parsed by JSON, so set to something else
        createRecordRequest = {
            "runNumber": self.runNumber,
            "useLiteMode": self.useLiteMode,
            "version": version,
            "calculationParameters": params.model_dump(),
            "crystalInfo": DAOFactory.default_xtal_info.model_dump(),
            "pixelGroups": [x.model_dump() for x in DAOFactory.pixelGroups()],
            "focusGroupCalibrationMetrics": DAOFactory.focusGroupCalibrationMetric_Column.model_dump(),
            "workspaces": workspaces,
        }
        createIndexEntryRequest = {
            "runNumber": self.runNumber,
            "useLiteMode": self.useLiteMode,
            "version": version,
            "comments": "",
            "author": "",
            "appliesTo": f">={self.runNumber}",
        }
        payload = {
            "createIndexEntryRequest": createIndexEntryRequest,
            "createRecordRequest": createRecordRequest,
        }
        request = SNAPRequest(path="calibration/save", payload=json.dumps(payload))
        response = self.api.executeRequest(request)
        assert response.code <= ResponseCode.MAX_OK
        return response.data

    def assert_diffcal_saved(self, version):
        # perform various checks to ensure everything saved correctly
        assert self.indexer.versionPath(version).exists()
        assert self.indexer.recordPath(version).exists()
        assert self.indexer.parametersPath(version).exists()
        savedRecord = parse_file_as(CalibrationRecord, self.indexer.recordPath(version))
        assert savedRecord.version == version
        assert savedRecord.calculationParameters.version == version
        # make sure all workspaces exist
        workspaces = savedRecord.workspaces
        assert (self.indexer.versionPath(version) / (workspaces[wngt.DIFFCAL_OUTPUT][0] + ".nxs.h5")).exists()
        assert (self.indexer.versionPath(version) / (workspaces[wngt.DIFFCAL_DIAG][0] + ".nxs.h5")).exists()
        assert (self.indexer.versionPath(version) / (workspaces[wngt.DIFFCAL_TABLE][0] + ".h5")).exists()
        # assert this version is in the index
        index = self.indexer.readIndex()
        assert index[version].version == version
        index = parse_file_as(List[IndexEntry], self.indexer.indexPath())
        assert index[-1].version == version
        # verify versions on indexer are as expected
        assert self.indexer.currentVersion() == version
        assert self.indexer.latestApplicableVersion(self.runNumber) == version
        assert self.indexer.nextVersion() == version + 1
        # load the previous calibration and verify equality
        runConfig = {"runNumber": self.runNumber, "useLiteMode": self.useLiteMode}
        request = SNAPRequest(path="calibration/load", payload=json.dumps(runConfig))
        response = self.api.executeRequest(request)
        assert response.code <= ResponseCode.MAX_OK
        loadedRecord = response.data
        assert loadedRecord.version == version
        assert loadedRecord == savedRecord
