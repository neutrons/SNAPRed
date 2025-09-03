# ruff: noqa: E402, ARG002
import json
import tempfile
import time
import unittest
from copy import deepcopy
from pathlib import Path
from random import randint
from typing import Dict, List
from unittest import mock

import pytest
from mantid.api import MatrixWorkspace
from mantid.simpleapi import (
    CloneWorkspace,
    CreateSingleValuedWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    GroupWorkspaces,
    LoadInstrument,
    mtd,
)
from util.Config_helpers import Config_override
from util.dao import DAOFactory
from util.helpers import (
    createCompatibleDiffCalTable,
    createCompatibleMask,
)
from util.Pydantic_util import assertEqualModel, assertEqualModelList
from util.SculleryBoy import SculleryBoy
from util.state_helpers import state_root_redirect

from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.request import (
    CalculateResidualRequest,
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    CalibrationLoadAssessmentRequest,
    CalibrationWritePermissionsRequest,
    CreateCalibrationRecordRequest,
    DiffractionCalibrationRequest,
    FarmFreshIngredients,
    FocusSpectraRequest,
    HasStateRequest,
    InitializeStateRequest,
    OverrideRequest,
    SimpleDiffCalRequest,
)
from snapred.backend.dao.request.CalibrationLockRequest import CalibrationLockRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state import PixelGroup
from snapred.backend.dao.state.CalibrantSample import CalibrantSample
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.recipe.CalculateDiffCalResidualRecipe import CalculateDiffCalServing
from snapred.backend.recipe.GroupDiffCalRecipe import GroupDiffCalRecipe
from snapred.backend.recipe.PixelDiffCalRecipe import PixelDiffCalRecipe, PixelDiffCalServing
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.service.SousChef import SousChef
from snapred.meta.builder.GroceryListBuilder import GroceryListBuilder
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import NameBuilder, WorkspaceName, WorkspaceType
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.redantic import parse_file_as

thisService = "snapred.backend.service.CalibrationService."


class TestCalibrationServiceExports:
    @pytest.fixture
    def setUp(self):
        # setup
        modulesPatch = mock.patch.dict(
            "sys.modules",
            {
                "snapred.backend.data.DataExportService": mock.Mock(),
                "snapred.backend.data.DataFactoryService": mock.Mock(),
                "snapred.backend.log": mock.Mock(),
                "snapred.backend.log.logger": mock.Mock(),
            },
        )
        modulesPatch.start()
        yield

        # teardown
        modulesPatch.stop()

    # test export calibration
    def test_exportCalibrationIndex(self, setUp):
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry.return_value = "expected"
        calibrationService.saveCalibrationToIndex(
            IndexEntry(runNumber="1", useLiteMode=True, comments="", author="", version=1),
        )
        assert calibrationService.dataExportService.exportCalibrationIndexEntry.called
        savedEntry = calibrationService.dataExportService.exportCalibrationIndexEntry.call_args.args[0]
        assert savedEntry.appliesTo == ">=1"
        assert savedEntry.timestamp is not None

    def test_save(self, setUp):
        workspace = mtd.unique_name(prefix="_dsp_")
        CreateSingleValuedWorkspace(OutputWorkspace=workspace)
        workspaces = {wngt.DIFFCAL_OUTPUT: [workspace]}
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationRecord = mock.Mock()
        calibrationService.dataExportService.exportCalibrationWorkspaces = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.createCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.calibrationExists = mock.Mock(return_value=False)
        calibrationService.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))
        calibrationService.dataFactoryService.createCalibrationRecord = mock.Mock(
            return_value=mock.Mock(
                runNumber="012345",
                focusGroupCalibrationMetrics=mock.Mock(focusGroupName="group"),
                workspaces=workspaces,
            )
        )
        calibrationService.save(mock.Mock())
        assert calibrationService.dataExportService.exportCalibrationRecord.called
        savedEntry = calibrationService.dataExportService.exportCalibrationRecord.call_args.args[0]
        assert savedEntry.parameters is not None

    def test_save_unexpected_units(self, setUp):
        workspace = mtd.unique_name(prefix="_tof_")
        CreateSingleValuedWorkspace(OutputWorkspace=workspace)
        workspaces = {wngt.DIFFCAL_OUTPUT: [workspace]}
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationRecord = mock.Mock()
        calibrationService.dataExportService.exportCalibrationWorkspaces = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.createCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.calibrationExists = mock.Mock(return_value=False)
        calibrationService.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))
        calibrationService.dataFactoryService.createCalibrationRecord = mock.Mock(
            return_value=mock.Mock(
                runNumber="012345",
                focusGroupCalibrationMetrics=mock.Mock(focusGroupName="group"),
                workspaces=workspaces,
            )
        )
        with pytest.raises(RuntimeError, match=r".*without a units token in its name.*"):
            calibrationService.save(mock.Mock())

    def test_save_unexpected_type(self, setUp):
        workspace = mtd.unique_name(prefix="_dsp_")
        CreateSingleValuedWorkspace(OutputWorkspace=workspace)
        workspaces = {wngt.REDUCTION_OUTPUT: [workspace]}
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationRecord = mock.Mock()
        calibrationService.dataExportService.exportCalibrationWorkspaces = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.createCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.calibrationExists = mock.Mock(return_value=False)
        calibrationService.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))
        calibrationService.dataFactoryService.createCalibrationRecord = mock.Mock(
            return_value=mock.Mock(
                runNumber="012345",
                focusGroupCalibrationMetrics=mock.Mock(focusGroupName="group"),
                workspaces=workspaces,
            )
        )
        with pytest.raises(RuntimeError, match=r".*Unexpected output type.*"):
            calibrationService.save(mock.Mock())

    def test_load(self, setUp):
        calibrationService = CalibrationService()
        calibrationService.dataFactoryService.getCalibrationRecord = mock.Mock(return_value="passed")
        calibrationService.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))
        res = calibrationService.load(mock.Mock())
        assert res == calibrationService.dataFactoryService.getCalibrationRecord.return_value

    def test_getCalibrationIndex(self, setUp):
        calibrationService = CalibrationService()
        calibrationService.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))
        calibrationService.dataFactoryService.getCalibrationIndex = mock.Mock(
            return_value=IndexEntry(runNumber="1", useLiteMode=True, comments="", author="", version=1)
        )
        calibrationService.getCalibrationIndex(mock.MagicMock(run=mock.MagicMock(runNumber="123")))
        assert calibrationService.dataFactoryService.getCalibrationIndex.called


class TestCalibrationServiceMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runNumber = "555"
        cls.version = "1"
        cls.timestamp = time.time()

        cls.SNAPInstrumentFilePath = Config["instrument"]["native"]["definition"]["file"]
        cls.instrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        Config["instrument"]["native"]["definition"]["file"] = cls.instrumentFilePath

        cls.SNAPLiteInstrumentFilePath = Config["instrument"]["lite"]["definition"]["file"]
        cls.instrumentLiteFilePath = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
        Config["instrument"]["lite"]["definition"]["file"] = cls.instrumentLiteFilePath

        # create some sample data
        cls.sampleWS = "_sample_data"
        cls.sampleDiagnosticWS = "_sample_diag_"
        cls.sampleTableWS = "_sample_diffcal_table"
        cls.sampleMaskWS = "_sample_diffcal_mask"

        CreateWorkspace(
            OutputWorkspace=cls.sampleWS,
            DataX=[0.5, 1.5] * 16,
            DataY=[3] * 16,
            NSpec=16,
        )
        LoadInstrument(
            Filename=cls.instrumentFilePath,
            Workspace=cls.sampleWS,
            RewriteSpectraMap=True,
        )
        ws = mtd.unique_name()
        CreateWorkspace(
            OutputWorkspace=ws,
            DataX=[0.5, 1.5] * 16,
            DataY=[3] * 16,
            NSpec=16,
        )
        LoadInstrument(
            Filename=cls.instrumentFilePath,
            Workspace=ws,
            RewriteSpectraMap=True,
        )
        GroupWorkspaces(
            InputWorkspaces=[ws],
            OutputWorkspace=cls.sampleDiagnosticWS,
        )
        createCompatibleDiffCalTable(cls.sampleTableWS, cls.sampleWS)
        createCompatibleMask(cls.sampleMaskWS, cls.sampleWS)

        # cleanup at per-test teardown
        cls.excludeAtTeardown = [cls.sampleWS, cls.sampleTableWS, cls.sampleDiagnosticWS, ws]

    def setUp(self):
        createCompatibleMask(self.sampleMaskWS, self.sampleWS)
        self.localDataService = LocalDataService()
        self.instance = CalibrationService()
        # it is necessary to coordinate these to all point to the same data service for redirect to work
        self.instance.dataExportService.dataService = self.localDataService
        self.instance.dataFactoryService.lookupService = self.localDataService
        self.instance.groceryService.dataService = self.localDataService
        self.outputNameFormat = "{}_calibration_reduction_result"
        # Mock the _calculatePixelGroupingParameters method to return a predefined value
        self.instance._calculatePixelGroupingParameters = lambda calib, focus_def, lite_mode, nBins: {  # noqa: ARG005
            "parameters": f"params for {focus_def}",
            "tof": f"tof for {focus_def}",
        }

    def clearoutWorkspaces(self) -> None:
        # Delete the workspaces created by loading
        for ws in mtd.getObjectNames():
            if ws not in self.excludeAtTeardown:
                DeleteWorkspace(ws)

    def tearDown(self) -> None:
        # At the end of each test, clear out the workspaces
        self.clearoutWorkspaces()
        return super().tearDown()

    def create_fake_diffcal_files(self, path: Path, workspaces: Dict[WorkspaceType, List[WorkspaceName]], version: str):
        tableWSName = None
        maskWSName = None
        # Note: this form allows testing of partial-write behavior:
        #   e.g. no _mask_ or _table_ workspace in the 'workspaces' dict.
        for key, wsNames in workspaces.items():
            for wsName in wsNames:
                match key:
                    case wngt.DIFFCAL_TABLE:
                        tableWSName = wsName
                    case wngt.DIFFCAL_MASK:
                        maskWSName = wsName
                    case wngt.DIFFCAL_DIAG:
                        filename = Path(wsName + ".nxs.h5")
                        self.instance.dataExportService.exportWorkspace(path, filename, self.sampleDiagnosticWS)
                    case _:
                        filename = Path(wsName + ".nxs.h5")
                        self.instance.dataExportService.exportWorkspace(path, filename, self.sampleWS)

        # Write the table and/or mask
        if tableWSName or maskWSName:
            filename = Path(tableWSName + ".h5")
            self.instance.dataExportService.dataService.writeDiffCalWorkspaces(
                path,
                filename,
                tableWorkspaceName=self.sampleTableWS if tableWSName else "",
                maskWorkspaceName=self.sampleMaskWS if maskWSName else "",
            )

    @mock.patch(thisService + "CalibrationMetricExtractionRecipe")
    @mock.patch(thisService + "FocusGroupMetric")
    def test_collectMetrics(
        self,
        FocusGroupMetric,
        CalibrationMetricExtractionRecipe,
    ):
        # mock the inputs to the function
        focussedData = mock.MagicMock(spec_set=str)
        focusGroup = mock.MagicMock(spec=FocusGroup)
        focusGroup.name = "group1"
        pixelGroup = mock.MagicMock(spec=PixelGroup, json=lambda: "I'm a little teapot")

        # mock external method calls
        self.instance.parseCalibrationMetricList = mock.Mock(
            side_effect=lambda y: y,
        )
        mockMetric = mock.Mock()
        CalibrationMetricExtractionRecipe.return_value.executeRecipe.return_value = mockMetric

        # Call the method to test
        metric = self.instance._collectMetrics(focussedData, focusGroup, pixelGroup)

        # Assert correct behavior
        CalibrationMetricExtractionRecipe.return_value.executeRecipe.assert_called_once_with(
            InputWorkspace=focussedData,
            PixelGroup=pixelGroup.json(),
        )
        self.instance.parseCalibrationMetricList.assert_called_once_with(mockMetric)
        FocusGroupMetric.assert_called_once_with(
            focusGroupName=focusGroup.name,
            calibrationMetric=mockMetric,  # NOTE this is because of above lambda
        )
        assert metric == FocusGroupMetric.return_value

    @mock.patch(thisService + "FarmFreshIngredients")
    @mock.patch(thisService + "FitMultiplePeaksRecipe")
    def test_assessQuality(self, FitMultiplePeaksRecipe, FarmFreshIngredients):
        # Mock input data
        timestamp = time.time()
        mockFarmFreshIngredients = mock.Mock(
            runNumber="12345",
            useLiteMode=True,
            timestamp=timestamp,
        )
        FarmFreshIngredients.return_value = mockFarmFreshIngredients
        fakeMetrics = DAOFactory.focusGroupCalibrationMetric_Column
        expectedRecord = DAOFactory.calibrationRecord()
        expectedWorkspaces = [
            wng.diffCalTimedMetric().runNumber(expectedRecord.runNumber).timestamp(timestamp).metricName(metric).build()
            for metric in ["sigma", "strain"]
        ]

        # Mock the necessary method calls
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="good/cif/path")
        self.instance.dataFactoryService.createCalibrationRecord = mock.Mock(return_value=expectedRecord)
        self.instance.dataExportService.getUniqueTimestamp = mock.Mock(return_value=timestamp)
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))
        self.instance.dataFactoryService.getNextCalibrationVersion = mock.Mock(return_value=expectedRecord.version)
        self.instance._collectMetrics = mock.Mock(return_value=fakeMetrics)
        workspaces = {
            wngt.DIFFCAL_OUTPUT: [self.sampleWS],
            wngt.DIFFCAL_TABLE: [self.sampleTableWS],
            wngt.DIFFCAL_MASK: [self.sampleMaskWS],
        }
        # Call the method to test
        request = CalibrationAssessmentRequest(
            workspaces=workspaces,
            run=RunConfig(runNumber=expectedRecord.runNumber),
            useLiteMode=True,
            focusGroup={"name": fakeMetrics.focusGroupName, "definition": ""},
            calibrantSamplePath="egg/muffin/biscuit.pastry",
            peakFunction="Gaussian",
            crystalDMin=0,
            crystalDMax=10,
            nBinsAcrossPeakWidth=0,
            maxChiSq=100.0,
        )

        with (
            mock.patch.object(
                self.instance.sousChef, "prepCalibration", return_value=expectedRecord.calculationParameters
            ),
            mock.patch.object(self.instance.sousChef, "prepPixelGroup", return_value=expectedRecord.pixelGroups[0]),
        ):
            response = self.instance.assessQuality(request)

        # Assert correct method calls
        FitMultiplePeaksRecipe.return_value.executeRecipe.assert_called_once_with(
            InputWorkspace=request.workspaces[wngt.DIFFCAL_OUTPUT][0],
            DetectorPeaks=self.instance.sousChef.prepDetectorPeaks(
                FarmFreshIngredients.return_value,
            ),
        )
        self.instance.dataFactoryService.getCifFilePath.assert_called_once_with("biscuit")

        # Assert the result is as expected
        assert response.version == expectedRecord.version
        assertEqualModel(response.calculationParameters, expectedRecord.calculationParameters)
        assertEqualModel(response.crystalInfo, expectedRecord.crystalInfo)
        assertEqualModelList(response.pixelGroups, expectedRecord.pixelGroups)
        assert response.focusGroupCalibrationMetrics == fakeMetrics
        assert response.workspaces == workspaces
        assert response.metricWorkspaces == expectedWorkspaces

        # Assert expected calibration metric workspaces have been generated
        for wsName in expectedWorkspaces:
            assert self.instance.dataFactoryService.workspaceDoesExist(wsName), (
                f"{wsName} missing, existing workspaces: {mtd.getObjectNames()}"
            )

    def test_save_respects_version(self):
        version = 1
        record = DAOFactory.calibrationRecord(version=1)
        record.workspaces = {
            wngt.DIFFCAL_OUTPUT: ["_dsp_diffoc_057514"],
            wngt.DIFFCAL_DIAG: ["_diagnostic_diffoc_057514"],
            wngt.DIFFCAL_TABLE: ["_diffract_consts_057514"],
            wngt.DIFFCAL_MASK: ["_diffract_consts_mask_057514"],
        }
        CloneWorkspace(InputWorkspace=self.sampleWS, OutputWorkspace=record.workspaces[wngt.DIFFCAL_OUTPUT][0])
        CloneWorkspace(InputWorkspace=self.sampleWS, OutputWorkspace=record.workspaces[wngt.DIFFCAL_DIAG][0])
        CloneWorkspace(InputWorkspace=self.sampleTableWS, Outputworkspace=record.workspaces[wngt.DIFFCAL_TABLE][0])
        CloneWorkspace(InputWorkspace=self.sampleMaskWS, OutputWorkspace=record.workspaces[wngt.DIFFCAL_MASK][0])
        version = record.version
        """
        # This form additionally tests usage of `@FromString`, which should probably be deprecated.
        request = {
            "createIndexEntryRequest": {
                "runNumber": record.runNumber,
                "useLiteMode": record.useLiteMode,
                "version": record.version,
                "appliesTo": f">={record.runNumber}",
                "author": "",
                "comments": "",
                "timestamp": time.time(),
            },
            "createRecordRequest": record.model_dump(),
        }
        """
        recordDump = record.model_dump()
        del recordDump["snapredVersion"]
        del recordDump["snapwrapVersion"]
        request = CalibrationExportRequest(
            createRecordRequest=CreateCalibrationRecordRequest(**recordDump),
        )

        with state_root_redirect(self.localDataService) as tmpRoot:
            self.instance.save(request)
            savedRecord = parse_file_as(
                CalibrationRecord, tmpRoot.path() / "lite/diffraction/v_0001/CalibrationRecord.json"
            )
        assert savedRecord.version == version
        for wss in savedRecord.workspaces.values():
            for ws in wss:
                assert "v0001" in ws

    def test_load_quality_assessment_no_calibration_record_exception(self):
        with state_root_redirect(self.localDataService):
            mockRequest = mock.Mock(
                runId=self.runNumber,
                useLiteMode=True,
                version=self.version,
                checkExistent=False,
            )
            self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(
                side_effect=FileNotFoundError("No record found")
            )
            with pytest.raises(FileNotFoundError):  # noqa: PT011
                self.instance.loadQualityAssessment(mockRequest)

    @mock.patch(thisService + "GenerateCalibrationMetricsWorkspaceRecipe")
    @mock.patch(thisService + "CalibrationMetricsWorkspaceIngredients")
    def test_load_quality_assessment_no_calibration_metrics_exception(
        self,
        mockCalibrationMetricsWorkspaceIngredients,
        mockGenerateCalibrationMetricsWorkspaceRecipe,
    ):
        mockRequest = mock.Mock(runId=self.runNumber, version=self.version, checkExistent=False)
        calibrationRecord = DAOFactory.calibrationRecord(runNumber="57514", version=1)
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))
        # Clear the input metrics list
        calibrationRecord.focusGroupCalibrationMetrics.calibrationMetric = []
        mockCalibrationMetricsWorkspaceIngredients.return_value = mock.Mock(
            calibrationRecord=calibrationRecord,
            timestamp=self.timestamp,
        )
        # Mock the recipe to raise the expected exception
        mockGenerateCalibrationMetricsWorkspaceRecipe.return_value.executeRecipe.side_effect = Exception(
            "input table is empty"
        )
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibrationRecord)
        with pytest.raises(Exception, match=r".*input table is empty.*"):
            self.instance.loadQualityAssessment(mockRequest)

    def test_load_quality_assessment_check_existent_metrics(self):
        path = Resource.getPath("outputs")
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpDir:
            calibRecord = DAOFactory.calibrationRecord()
            self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)
            self.instance.dataExportService.getUniqueTimestamp = mock.Mock(return_value=None)
            self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.instance.dataFactoryService.getCalibrationDataPath = mock.Mock(return_value=tmpDir)
            self.instance.groceryService.dataService.readCalibrationRecord = mock.Mock()
            self.instance.groceryService.fetchCalibrationWorkspaces = mock.Mock()
            self.instance.groceryService.createDiffCalTableWorkspaceName = mock.Mock()
            self.instance.groceryService.fetchGroceryDict = mock.Mock()
            self.create_fake_diffcal_files(Path(tmpDir), calibRecord.workspaces, calibRecord.version)

            self.instance.groceryService._getCalibrationDataPath = mock.Mock(return_value=tmpDir)
            self.instance.groceryService._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
            mockRequest = CalibrationLoadAssessmentRequest(
                runId=calibRecord.runNumber,
                useLiteMode=calibRecord.useLiteMode,
                version=calibRecord.version,
                checkExistent=False,
            )

            # Load the assessment workspaces:
            self.instance.loadQualityAssessment(mockRequest)

            # Delete any existing _data_ workspaces:
            for wss in calibRecord.workspaces.values():
                for ws in wss:
                    if mtd.doesExist(ws):
                        DeleteWorkspace(Workspace=ws)

            mockRequest = mock.Mock(runId=calibRecord.runNumber, version=calibRecord.version, checkExistent=True)
            with pytest.raises(RuntimeError) as excinfo:  # noqa: PT011
                self.instance.loadQualityAssessment(mockRequest)
            assert "is already loaded" in str(excinfo.value)

    def test_load_quality_assessment_check_existent_data(self):
        version = randint(2, 120)
        record = DAOFactory.calibrationRecord(version=version)
        parameters = DAOFactory.calibrationParameters(version=version)
        index = []
        with state_root_redirect(self.localDataService) as tmpRoot:
            indexer = self.localDataService.calibrationIndexer(record.useLiteMode, "stateId")
            indexPath = indexer.indexPath()
            recordFilepath = indexer.recordPath(version)
            tmpRoot.saveObjectAt(record, recordFilepath)
            tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))
            indexPath.write_text(json.dumps(index, indent=2))

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.create_fake_diffcal_files(recordFilepath.parent, record.workspaces, version)

            mockRequest = mock.Mock(
                runId=record.runNumber,
                useLiteMode=record.useLiteMode,
                version=version,
                checkExistent=False,
            )
            self.instance.groceryService._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
            self.instance.groceryService._validateWorkspaceInstrument = mock.Mock(return_value=True)

            with Config_override("instrument.lite.pixelResolution", 16):
                # Load the assessment workspaces:
                self.instance.loadQualityAssessment(mockRequest)

            # Delete any existing _metric_ workspaces:
            for ws in mtd.getObjectNames():
                if "sigma" in ws or "strain" in ws:
                    DeleteWorkspace(Workspace=ws)

            mockRequest.checkExistent = True
            with pytest.raises(RuntimeError) as excinfo:  # noqa: PT011
                self.instance.loadQualityAssessment(mockRequest)
            assert "is already loaded" in str(excinfo.value)

    def test_load_quality_assessment(self):
        version = randint(2, 120)
        record = DAOFactory.calibrationRecord(version=version)
        parameters = DAOFactory.calibrationParameters(version=version)
        index = []
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=record)
        self.instance.dataExportService.getUniqueTimestamp = mock.Mock(return_value=None)
        with state_root_redirect(self.localDataService) as tmpRoot:
            indexer = self.localDataService.calibrationIndexer(record.useLiteMode, "stateId")
            recordFilepath = indexer.recordPath(version)
            indexPath = indexer.indexPath()
            tmpRoot.saveObjectAt(record, recordFilepath)
            tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))
            indexPath.write_text(json.dumps(index, indent=2))

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.create_fake_diffcal_files(recordFilepath.parent, record.workspaces, version)

            # Call the method to test. Use a mocked run and a mocked version
            mockRequest = mock.Mock(
                runId=record.runNumber,
                useLiteMode=record.useLiteMode,
                version=version,
                checkExistent=False,
            )
            self.instance.groceryService._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
            self.instance.groceryService._validateWorkspaceInstrument = mock.Mock(return_value=True)

            with Config_override("instrument.lite.pixelResolution", 16):
                self.instance.loadQualityAssessment(mockRequest)

            # Assert the expected calibration metric workspaces have been generated
            for metric in ["sigma", "strain"]:
                ws_name = (
                    wng.diffCalMetric()
                    .metricName(metric)
                    .runNumber(record.runNumber)
                    .version(record.version)
                    .metricName(metric)
                    .build()
                )
                assert self.instance.dataFactoryService.workspaceDoesExist(ws_name), (
                    f"{ws_name} missing, existing workspaces: {mtd.getObjectNames()}"
                )

            # Assert all "persistent" workspaces have been loaded
            for wsNames in record.workspaces.values():
                for wsName in wsNames:
                    assert self.instance.dataFactoryService.workspaceDoesExist(wsName), (
                        f"{wsName} missing, existing workspaces: {mtd.getObjectNames()}"
                    )

    def test_load_quality_assessment_no_units(self):
        calibRecord = DAOFactory.calibrationRecord(runNumber="57514", version=1)
        calibRecord.workspaces[wngt.DIFFCAL_OUTPUT] = ["_diffoc_057514_v0001"]  # NOTE no unit token
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("stateId", None))

        # Call the method to test. Use a mocked run and a mocked version
        mockRequest = mock.Mock(runId=calibRecord.runNumber, version=calibRecord.version, checkExistent=False)
        with pytest.raises(RuntimeError, match=r".*without a units token in its name.*"):
            self.instance.loadQualityAssessment(mockRequest)

    def test_load_quality_assessment_dsp_and_diag(self):
        calibRecord = DAOFactory.calibrationRecord(
            workspaces={
                wngt.DIFFCAL_OUTPUT: ["_dsp_diffoc_057514"],
                wngt.DIFFCAL_DIAG: ["_diagnostic_diffoc_057514"],
                wngt.DIFFCAL_TABLE: ["_diffract_consts_057514"],
                wngt.DIFFCAL_MASK: ["_diffract_consts_mask_057514"],
            }
        )
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))

        mockFetchGroceryDict = mock.Mock(return_value={})
        self.instance.groceryService.fetchGroceryDict = mockFetchGroceryDict

        mockRequest = mock.Mock(
            spec=CalibrationLoadAssessmentRequest,
            runId=calibRecord.runNumber,
            useLiteMode=calibRecord.useLiteMode,
            version=calibRecord.version,
            checkExistent=False,
        )
        self.instance.loadQualityAssessment(mockRequest)
        calledWithDict = mockFetchGroceryDict.call_args[0][0]
        assert calledWithDict["diffCalOutput_0000"].unit == wng.Units.DSP
        assert calledWithDict["diffCalDiagnostic_0000"].unit == wng.Units.DIAG

    def test_load_quality_assessment_unexpected_type(self):
        calibRecord = DAOFactory.calibrationRecord()
        calibRecord.workspaces = {
            wngt.DIFFCAL_OUTPUT: ["_dsp_diffoc_057514"],
            wngt.DIFFCAL_TABLE: ["_diffract_consts_057514"],
            wngt.DIFFCAL_MASK: ["_diffract_consts_mask_057514"],
            wngt.RAW_VANADIUM: ["_unexpected_workspace_type"],
        }
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))

        mockRequest = mock.Mock(
            spec=CalibrationLoadAssessmentRequest,
            runId=calibRecord.runNumber,
            useLiteMode=calibRecord.useLiteMode,
            version=calibRecord.version,
            checkExistent=False,
        )
        with pytest.raises(RuntimeError, match=r".*not implemented: unable to load unexpected.*"):
            self.instance.loadQualityAssessment(mockRequest)

    def test__calibration_N_ref(self):
        inputFileSize = 409600
        mockInputPath = mock.Mock(
            spec=Path,
            exists=mock.Mock(return_value=True),
            stat=mock.Mock(return_value=mock.Mock(st_size=inputFileSize)),
        )
        self.instance.groceryService.createNeutronFilePath = mock.Mock(return_value=mockInputPath)

        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=False,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
        )

        expected = float(mockInputPath.stat.return_value.st_size)
        assert self.instance._calibration_N_ref(request) == expected
        self.instance.groceryService.createNeutronFilePath.assert_called_once_with(
            request.runNumber, request.useLiteMode
        )
        mockInputPath.exists.assert_called_once()
        mockInputPath.stat.assert_called_once()

        # input path doesn't exist: returns `None`
        self.instance.groceryService.createNeutronFilePath.reset_mock()
        mockInputPath.reset_mock()
        mockInputPath.exists.return_value = False
        assert self.instance._calibration_N_ref(request) is None
        self.instance.groceryService.createNeutronFilePath.assert_called_once_with(
            request.runNumber, request.useLiteMode
        )
        mockInputPath.exists.assert_called_once()
        mockInputPath.stat.assert_not_called()

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    def test_prepDiffractionCalibrationIngredients(self, FarmFreshIngredients):
        mockFF = mock.Mock()
        FarmFreshIngredients.return_value = mockFF
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="bundt/cake.egg")
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))

        # self.instance.sousChef = SculleryBoy() #
        self.instance.sousChef = mock.Mock(spec_set=SousChef)

        # Call the method with the provided parameters
        request = mock.Mock(calibrantSamplePath="bundt/cake_egg.py")
        res = self.instance.prepDiffractionCalibrationIngredients(request)

        # Perform assertions to check the result and method calls
        assert FarmFreshIngredients.call_count == 1
        self.instance.dataFactoryService.getCifFilePath.assert_called_once_with("cake_egg")
        assert res == self.instance.sousChef.prepDiffractionCalibrationIngredients(mockFF)

    def test_fetchDiffractionCalibrationGroceries(self):
        self.instance.groceryClerk = mock.Mock(spec=GroceryListBuilder)
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("StateId", None))

        runNumber = "12345"
        useLiteMode = True
        focusGroup = FocusGroup(name="group1", definition="/any/path")
        diffcalOutputName = wng.diffCalOutput().unit(wng.Units.DSP).runNumber(runNumber).group(focusGroup.name).build()
        diagnosticWorkspaceName = (
            wng.diffCalOutput().unit(wng.Units.DIAG).runNumber(runNumber).group(focusGroup.name).build()
        )
        calibrationTableName = wng.diffCalTable().runNumber(runNumber).build()
        calibrationMaskName = wng.diffCalMask().runNumber(runNumber).build()

        self.instance.groceryService.fetchGroceryDict = mock.Mock(
            return_value={
                "grocery1": "orange",
                "outputWorkspace": diffcalOutputName,
                "diagnosticWorkspace": diagnosticWorkspaceName,
                "calibrationTable": calibrationTableName,
                "maskWorkspace": calibrationMaskName,
            }
        )

        # Call the method with the provided parameters
        request = mock.Mock(
            spec=DiffractionCalibrationRequest,
            runNumber=runNumber,
            useLiteMode=useLiteMode,
            focusGroup=focusGroup,
            startingTableVersion=0,
        )
        result = self.instance.fetchDiffractionCalibrationGroceries(request)

        assert self.instance.groceryClerk.buildDict.call_count == 1
        self.instance.groceryService.fetchGroceryDict.assert_called_once_with(
            self.instance.groceryClerk.buildDict.return_value,
            outputWorkspace=diffcalOutputName,
            diagnosticWorkspace=diagnosticWorkspaceName,
            calibrationTable=calibrationTableName,
            maskWorkspace=calibrationMaskName,
        )
        assert result == self.instance.groceryService.fetchGroceryDict.return_value

    @mock.patch(thisService + "SimpleDiffCalRequest", spec_set=SimpleDiffCalRequest)
    def test_diffractionCalibration_calls_others(self, SimpleDiffCalRequest):
        # mock out external functionalties
        SimpleDiffCalRequest.return_value = SimpleDiffCalRequest.model_construct(groceries={"previousCalibration": ""})
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="bundt/cake.egg")
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value=mock.sentinel.stateroot)
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=True)
        self.instance.prepDiffractionCalibrationIngredients = mock.Mock(return_value=mock.sentinel.ingredients)
        self.instance.fetchDiffractionCalibrationGroceries = mock.Mock(return_value=mock.sentinel.groceries)
        mockPixelRxServing = mock.Mock(
            result=True,
            calibrationTable=mock.sentinel.oldCalTable,
            medianOffsets=mock.sentinel.offsets,
        )
        mockGroupRxServing = mock.Mock(
            result=True,
            calibrationTable=mock.sentinel.newCalTable,
            diagnosticWorkspace=mock.sentinel.diagnostic,
            outputWorkspace=mock.sentinel.output,
            maskWorkspace=mock.sentinel.mask,
        )
        self.instance.pixelCalibration = mock.Mock(return_value=mockPixelRxServing)
        self.instance.groupCalibration = mock.Mock(return_value=mockGroupRxServing)

        # Call the method with the provided parameters
        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=False,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
        )
        result = self.instance.diffractionCalibration(request)

        # Perform assertions to check the result and method calls
        self.instance.dataExportService.getCalibrationStateRoot.assert_called_once_with(request.runNumber)
        self.instance.dataExportService.checkWritePermissions.assert_called_once_with(mock.sentinel.stateroot)
        self.instance.prepDiffractionCalibrationIngredients.assert_called_once_with(request)
        self.instance.fetchDiffractionCalibrationGroceries.assert_called_once_with(request)
        SimpleDiffCalRequest.assert_called_once_with(
            ingredients=mock.sentinel.ingredients,
            groceries=mock.sentinel.groceries,
        )
        self.instance.pixelCalibration.assert_called_once_with(SimpleDiffCalRequest())
        self.instance.groupCalibration.assert_called_once_with(SimpleDiffCalRequest())
        assert result == {
            "calibrationTable": mock.sentinel.newCalTable,
            "diagnosticWorkspace": mock.sentinel.diagnostic,
            "outputWorkspace": mock.sentinel.output,
            "maskWorkspace": mock.sentinel.mask,
            "steps": mock.sentinel.offsets,
            "result": True,
        }

    @mock.patch(thisService + "SimpleDiffCalRequest")
    def test_diffractionCalibration_pixel_fail(self, mockSimpleDiffCalRequest):
        mockSimpleDiffCalRequest.return_value = SimpleDiffCalRequest.model_construct(
            groceries={"previousCalibration": ""}
        )
        self.instance.validateRequest = mock.Mock()
        mockPixelRxServing = mock.Mock(
            result=False,
            calibrationTable=mock.sentinel.oldCalTable,
            medianOffsets=mock.sentinel.offsets,
        )
        self.instance.prepDiffractionCalibrationIngredients = mock.Mock()
        self.instance.fetchDiffractionCalibrationGroceries = mock.Mock()
        self.instance.pixelCalibration = mock.Mock(return_value=mockPixelRxServing)

        # Call the method with the provided parameters
        request = DiffractionCalibrationRequest.model_construct()
        with pytest.raises(RuntimeError) as e:
            self.instance.diffractionCalibration(request)
        assert str(e.value) == "Pixel Calibration failed"

    @mock.patch(thisService + "SimpleDiffCalRequest")
    def test_diffractionCalibration_group_fail(self, mockSimpleDiffCalRequest):
        mockSimpleDiffCalRequest.return_value = SimpleDiffCalRequest.model_construct(
            groceries={"previousCalibration": ""}
        )
        self.instance.validateRequest = mock.Mock()
        mockPixelRxServing = mock.Mock(
            result=True,
            calibrationTable=mock.sentinel.oldCalTable,
            medianOffsets=mock.sentinel.offsets,
        )
        mockGroupRxServing = mock.Mock(
            result=False,
            calibrationTable=mock.sentinel.newCalTable,
            diagnosticWorkspace=mock.sentinel.diagnostic,
            outputWorkspace=mock.sentinel.output,
            maskWorkspace=mock.sentinel.mask,
        )
        self.instance.prepDiffractionCalibrationIngredients = mock.Mock()
        self.instance.fetchDiffractionCalibrationGroceries = mock.Mock()
        self.instance.pixelCalibration = mock.Mock(return_value=mockPixelRxServing)
        self.instance.groupCalibration = mock.Mock(return_value=mockGroupRxServing)

        # Call the method with the provided parameters
        request = DiffractionCalibrationRequest.model_construct()
        with pytest.raises(RuntimeError) as e:
            self.instance.diffractionCalibration(request)
        assert str(e.value) == "Group Calibration failed"

    def test__calibration_substep_N_ref(self):
        inputWorkspaceMemorySize = 409600
        mockInputWs = mock.Mock(
            spec=MatrixWorkspace,
            getMemorySize=mock.Mock(return_value=inputWorkspaceMemorySize),
        )
        mockMtd = mock.MagicMock(
            doesExist=mock.Mock(return_value=True), __getitem__=mock.Mock(return_value=mockInputWs)
        )
        self.instance.mantidSnapper = mock.Mock(mtd=mockMtd)

        request = SimpleDiffCalRequest.model_construct(
            ingredients=mock.sentinel.ingredients, groceries={"inputWorkspace": mock.sentinel.inputWs}
        )

        expected = float(mockInputWs.getMemorySize.return_value)
        assert self.instance._calibration_substep_N_ref(request) == expected
        mockMtd.doesExist.assert_called_once_with(mock.sentinel.inputWs)
        mockMtd.__getitem__.assert_called_once_with(mock.sentinel.inputWs)
        mockInputWs.getMemorySize.assert_called_once()

        # input workspace doesn't exist: returns `None`
        mockMtd.reset_mock()
        mockInputWs.reset_mock()
        mockMtd.doesExist.return_value = False
        assert self.instance._calibration_substep_N_ref(request) is None
        mockMtd.doesExist.assert_called_once_with(mock.sentinel.inputWs)
        mockMtd.__getitem__.assert_not_called()
        mockInputWs.getMemorySize.assert_not_called()

    @mock.patch(thisService + "PixelDiffCalRecipe", spec_set=PixelDiffCalRecipe)
    def test_pixelCalibration_success(self, PixelRx):
        mock.sentinel.result = PixelDiffCalServing.model_construct(
            maskWorkspace=self.sampleMaskWS,
        )
        PixelRx().cook.return_value = mock.sentinel.result

        # Call the method with the provided parameters
        request = SimpleDiffCalRequest.model_construct(
            ingredients=mock.sentinel.ingredients,
            groceries=mock.sentinel.groceries,
        )
        result = self.instance.pixelCalibration(request)

        # Perform assertions to check the result and method calls
        assert result == mock.sentinel.result
        PixelRx().cook.assert_called_once_with(
            mock.sentinel.ingredients,
            mock.sentinel.groceries,
        )

    @mock.patch(thisService + "PixelDiffCalRecipe", spec_set=PixelDiffCalRecipe)
    def test_pixelCalibration_with_bad_masking(self, PixelRx):
        # muck up the mask workspace
        maskWS = mtd[self.sampleMaskWS]
        numHistograms = maskWS.getNumberHistograms()
        for pixel in range(numHistograms):
            maskWS.setY(pixel, [1.0])

        # mock out the return
        mock.sentinel.result = PixelDiffCalServing.model_construct(
            maskWorkspace=self.sampleMaskWS,
        )
        PixelRx().cook.return_value = mock.sentinel.result

        # Call the method with the provided parameters
        request = SimpleDiffCalRequest.model_construct(
            ingredients=mock.sentinel.ingredients,
            groceries=mock.sentinel.groceries,
        )
        with pytest.raises(Exception, match=r".*pixels failed calibration*"):
            self.instance.pixelCalibration(request)

    @mock.patch(thisService + "GroupDiffCalRecipe", spec_set=GroupDiffCalRecipe)
    def test_groupCalibration(self, GroupRx):
        # mock out the return
        GroupRx().cook.return_value = mock.sentinel.result

        # Call the method with the provided parameters
        request = SimpleDiffCalRequest.model_construct(
            ingredients=mock.sentinel.ingredients,
            groceries=mock.sentinel.groceries,
        )
        result = self.instance.groupCalibration(request)

        assert result == mock.sentinel.result
        GroupRx().cook.assert_called_once_with(
            mock.sentinel.ingredients,
            mock.sentinel.groceries,
        )

    def test_validateRequest(self):
        # test that `validateRequest` calls `validateWritePermissions`
        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=False,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
        )
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=request.runNumber, continueFlags=request.continueFlags
        )
        with mock.patch.object(self.instance, "validateWritePermissions") as mockValidateWritePermissions:
            self.instance.validateRequest(request)
            mockValidateWritePermissions.assert_called_once_with(permissionsRequest)

    def test_validateWritePermissions(self):
        # test that `validateWritePermissions` throws no exceptions
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value=Path("lah/dee/dah"))
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=True)
        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=False,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
        )
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=request.runNumber, continueFlags=request.continueFlags
        )
        self.instance.validateWritePermissions(permissionsRequest)
        self.instance.dataExportService.getCalibrationStateRoot.assert_called_once_with(request.runNumber)
        self.instance.dataExportService.checkWritePermissions.assert_called_once_with(
            self.instance.dataExportService.getCalibrationStateRoot.return_value
        )

    def test_validateWritePermissions_no_permissions(self):
        # test that `validateWritePermissions` raises `ContinueWarning`
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value=Path("lah/dee/dah"))
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=False)
        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=False,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
        )
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=request.runNumber, continueFlags=request.continueFlags
        )
        with pytest.raises(ContinueWarning, match=r".*you don't have permissions to write.*"):
            self.instance.validateWritePermissions(permissionsRequest)

        self.instance.dataExportService.generateUserRootFolder = mock.Mock(return_value=Path("user/root/folder"))
        permissionsRequest.continueFlags = ContinueWarning.Type.CALIBRATION_HOME_WRITE_PERMISSION
        self.instance.validateWritePermissions(permissionsRequest)

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @mock.patch(thisService + "GroupDiffCalRecipe", spec_set=GroupDiffCalRecipe)
    @mock.patch(thisService + "PixelDiffCalRecipe", spec_set=PixelDiffCalRecipe)
    def test_diffractionCalibration_with_bad_masking(
        self,
        PixelRx,
        GroupRx,
        FarmFreshIngredients,
    ):
        FarmFreshIngredients.return_value = mock.Mock(runNumber="123")
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="bundt/cake.egg")
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("stateId", None))
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value="lah/dee/dah")
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=True)
        self.instance.sousChef = SculleryBoy()

        PixelRx().cook.return_value = mock.Mock(result=True, calibrationTable="fake")
        GroupRx().cook.return_value = mock.Mock(result=True)

        maskWS = mtd[self.sampleMaskWS]
        numHistograms = maskWS.getNumberHistograms()
        for pixel in range(numHistograms):
            maskWS.setY(pixel, [1.0])
        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroceryDict = mock.Mock(return_value={"maskWorkspace": self.sampleMaskWS})
        self.instance.groceryService.getWorkspaceForName = mock.Mock(return_value=mtd[self.sampleMaskWS])

        # Call the method with the provided parameters
        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=False,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
        )
        with pytest.raises(Exception, match=r".*pixels failed calibration*"):
            self.instance.diffractionCalibration(request)
        GroupRx().cook.assert_not_called()

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @mock.patch(thisService + "FocusSpectraRecipe")
    @mock.patch(thisService + "ConvertUnitsRecipe")
    def test_focusSpectra_not_exist(self, ConvertUnitsRecipe, FocusSpectraRecipe, FarmFreshIngredients):
        self.instance.sousChef = SculleryBoy()

        ConvertUnitsRecipe().executeRecipe.return_value = mock.Mock()
        FocusSpectraRecipe().executeRecipe.return_value = mock.Mock()

        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroupingDefinition = mock.Mock(return_value={"workspace": "orange"})
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("stateId", None))

        runNumber = "12345"
        groupingName = "group1"
        focusedWorkspace = (
            wng.run().runNumber(runNumber).group(groupingName).unit(wng.Units.DSP).auxiliary("F-dc").build()
        )
        assert not mtd.doesExist(focusedWorkspace)
        groupingWorkspace = self.instance.groceryService.fetchGroupingDefinition.return_value["workspace"]

        request = mock.Mock(
            spec=FocusSpectraRequest,
            runNumber=runNumber,
            useLiteMode=True,
            focusGroup=FocusGroup(name=groupingName, definition="path/to/grouping"),
            preserveEvents=False,
            inputWorkspace=focusedWorkspace,
            groupingWorkspace=groupingWorkspace,
        )

        # Call the method with the provided parameters
        res = self.instance.focusSpectra(request)

        # Perform assertions to check the result and method calls

        groupingItem = self.instance.groceryClerk.build.return_value
        assert FarmFreshIngredients.call_count == 1
        self.instance.groceryService.fetchGroupingDefinition.assert_called_once_with(groupingItem)
        FocusSpectraRecipe().executeRecipe.assert_called_once_with(
            InputWorkspace=focusedWorkspace,
            GroupingWorkspace=groupingWorkspace,
            OutputWorkspace=focusedWorkspace,
            PixelGroup=self.instance.sousChef.prepPixelGroup(FarmFreshIngredients()),
            PreserveEvents=request.preserveEvents,
        )
        assert res == (focusedWorkspace, groupingWorkspace)

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @mock.patch(thisService + "FocusSpectraRecipe")
    def test_focusSpectra_exists(self, FocusSpectraRecipe, FarmFreshIngredients):
        self.instance.sousChef = SculleryBoy()

        request = mock.Mock(runNumber="12345", focusGroup=mock.Mock(name="group1"))
        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroupingDefinition = mock.Mock(return_value={"workspace": "orange"})
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("stateId", None))

        # create the focused wprkspace in the ADS
        focusedWorkspace = (
            wng.run()
            .runNumber(request.runNumber)
            .group(request.focusGroup.name)
            .unit(wng.Units.DSP)
            .auxiliary("F-dc")
            .build()
        )
        CreateSingleValuedWorkspace(OutputWorkspace=focusedWorkspace)

        # Call the method with the provided parameters
        res = self.instance.focusSpectra(request)

        # assert the correct setup calls were made
        groupingItem = self.instance.groceryClerk.build.return_value
        groupingWorkspace = self.instance.groceryService.fetchGroupingDefinition.return_value["workspace"]
        assert FarmFreshIngredients.call_count == 1
        self.instance.groceryService.fetchGroupingDefinition.assert_called_once_with(groupingItem)

        # assert that the recipe is not called and the correct workspaces are returned
        FocusSpectraRecipe().executeRecipe.assert_not_called()
        assert res == (focusedWorkspace, groupingWorkspace)

    @mock.patch(thisService + "FitMultiplePeaksRecipe")
    def test_fitPeaks(self, FitMultiplePeaksRecipe):
        request = mock.Mock()
        res = self.instance.fitPeaks(request)
        assert res == FitMultiplePeaksRecipe.return_value.executeRecipe.return_value

    def test_matchRuns(self):
        self.instance.dataFactoryService.getLatestApplicableCalibrationVersion = mock.Mock(
            side_effect=[mock.sentinel.version1, mock.sentinel.version2, mock.sentinel.version3],
        )
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("stateId", None))
        request = mock.Mock(runNumbers=[mock.sentinel.run1, mock.sentinel.run2], useLiteMode=True)
        response = self.instance.matchRunsToCalibrationVersions(request)
        assert response == {
            mock.sentinel.run1: mock.sentinel.version1,
            mock.sentinel.run2: mock.sentinel.version2,
        }

    def test_fetchRuns(self):
        mockCalibrations = {
            mock.sentinel.run1: mock.sentinel.version1,
            mock.sentinel.run2: mock.sentinel.version2,
            mock.sentinel.run3: mock.sentinel.version2,
        }
        mockGroceries = [
            mock.sentinel.grocery1_table,
            mock.sentinel.grocery1_mask,
            mock.sentinel.grocery2_table,
            mock.sentinel.grocery2_mask,
            mock.sentinel.grocery2_table,
            mock.sentinel.grocery2_mask,
        ]
        self.instance.matchRunsToCalibrationVersions = mock.Mock(return_value=mockCalibrations)
        self.instance.dataFactoryService.constructStateId = mock.Mock(return_value=("stateId", None))
        self.instance.groceryService.fetchGroceryList = mock.Mock(return_value=mockGroceries)
        self.instance.groceryClerk = mock.Mock()

        with mock.patch.object(NameBuilder, "build", autospec=True) as mockNameBuilder_build:
            mockNameBuilder_build.side_effect = lambda self_: tuple(self_.props.values())

            request = mock.Mock(
                runNumbers=[mock.sentinel.run1, mock.sentinel.run2, mock.sentinel.run3], useLiteMode=True
            )
            workspaces, cal = self.instance.fetchMatchingCalibrations(request)
            assert workspaces == {
                mock.sentinel.grocery1_table,  # diffcal-table #1
                mock.sentinel.grocery2_table,  # diffcal-table #2
                mock.sentinel.grocery1_mask,  # diffcal-mask #1
                mock.sentinel.grocery2_mask,  # diffcal-mask #2
            }
            assert cal == mockCalibrations

    def test_initializeState(self):
        testCalibration = DAOFactory.calibrationParameters()
        mockInitializeState = mock.Mock(return_value=testCalibration.instrumentState)
        self.instance.dataExportService.initializeState = mockInitializeState
        request = InitializeStateRequest(
            runId=testCalibration.seedRun,
            humanReadableName="friendly name",
            useLiteMode=True,
        )
        self.instance.initializeState(request)
        mockInitializeState.assert_called_once_with(request.runId, request.useLiteMode, request.humanReadableName)

    def test_getState(self):
        testCalibration = DAOFactory.calibrationParameters()
        testConfig = StateConfig.model_construct()
        testConfig.calibration = testCalibration
        states = [deepcopy(testConfig), deepcopy(testConfig), deepcopy(testConfig)]
        states[0].calibration.instrumentState.id = "0000000000000000"
        states[1].calibration.instrumentState.id = "1111111111111111"
        states[2].calibration.instrumentState.id = "2222222222222222"
        runs = [RunConfig(runNumber="0"), RunConfig(runNumber="1"), RunConfig(runNumber="2")]
        mockDataFactoryService = mock.Mock()
        mockDataFactoryService.getStateConfig.side_effect = states
        self.instance.dataFactoryService = mockDataFactoryService
        actual = self.instance.getState(runs)
        assert actual == states

    def test_hasState(self):
        mockCheckCalibrationStateExists = mock.Mock(return_value=True)
        self.instance.dataFactoryService.checkCalibrationStateExists = mockCheckCalibrationStateExists
        request = HasStateRequest(
            runId="50000",
            useLiteMode=True,
        )
        self.instance.hasState(request)
        mockCheckCalibrationStateExists.assert_called_once_with("50000")

        badRequest = HasStateRequest(
            runId="5",
            useLiteMode=True,
        )
        with pytest.raises(ValueError, match="Invalid run number: 5"):
            self.instance.hasState(badRequest)

    @mock.patch("snapred.backend.service.CalibrationService.CalculateDiffCalResidualRecipe")
    def test_calculateResidual(self, MockCalculateDiffCalResidualRecipe):
        # Arrange: set up mock request and mock recipe
        mockRequest = mock.Mock(
            spec=CalculateResidualRequest,
            inputWorkspace="inputWS",
            outputWorkspace="outputWS",
            fitPeaksDiagnosticWorkspace="fitPeaksDiagWS",
        )

        # Mock the recipe instance and its return value
        mockRecipeInstance = MockCalculateDiffCalResidualRecipe.return_value
        mockRecipeInstance.cook.return_value = CalculateDiffCalServing(outputWorkspace="outputWS")

        # Act: call calculateResidual with the mock request
        result = self.instance.calculateResidual(mockRequest)

        # Assert: check that the recipe was instantiated and cook was called with the correct ingredients
        MockCalculateDiffCalResidualRecipe.assert_called_once_with()
        mockRecipeInstance.cook.assert_called_once_with(
            None,
            mockRequest.model_dump(),
        )

        # Assert that the result contains the expected output workspace
        self.assertEqual(result.outputWorkspace, "outputWS")  # noqa: PT009

    def test_parseCalibrationMetricList(self):
        fakeMetrics = CalibrationMetric(
            sigmaAverage=0.0,
            sigmaStandardDeviation=0.0,
            strainAverage=0.0,
            strainStandardDeviation=0.0,
            twoThetaAverage=0.0,
        )
        source = [fakeMetrics.model_dump()]
        assert [fakeMetrics] == self.instance.parseCalibrationMetricList(json.dumps(source))

    def test_handleOverrides(self):
        service = self.instance
        service.dataFactoryService.getCalibrantSample = mock.Mock()

        sampleWithOverrides = CalibrantSample(
            name="diamond",
            unique_id="0001",
            geometry=Geometry(shape="Cylinder", radius=1.0, height=1.0, center=[0, 0, 0]),
            material=Material(massDensity=2.0, chemicalFormula="C"),
            overrides={"CrystalDMin": 0.3, "CrystalDMax": 99.5, "PeakFunction": "Gaussian"},
        )

        sampleNoOverrides = CalibrantSample(
            name="graphite",
            unique_id="0002",
            geometry=Geometry(shape="Cylinder", radius=0.5, height=0.5, center=[0, 0, 0]),
            material=Material(massDensity=1.8, chemicalFormula="C"),
            overrides=None,
        )

        service.dataFactoryService.getCalibrantSample.return_value = sampleWithOverrides
        request = OverrideRequest(calibrantSamplePath="dummy/path/to/calibrant.json")
        result = service.handleOverrides(request)
        self.assertEqual(  # noqa: PT009
            result,
            sampleWithOverrides.overrides,
            "Expected handleOverrides to return the sample's overrides dictionary when present.",
        )

        service.dataFactoryService.getCalibrantSample.return_value = sampleNoOverrides
        requestNoOvr = OverrideRequest(calibrantSamplePath="dummy/path/no_overrides.json")
        resultNoOvr = service.handleOverrides(requestNoOvr)
        self.assertIsNone(  # noqa: PT009
            resultNoOvr, "Expected handleOverrides to return None when calibrantSample.overrides is None."
        )


from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import deleteWorkspaceNoThrow


class TestDiffractionCalibration(unittest.TestCase):
    """
    NOTE This test was taken from tests of the former DiffractionCalibrationRecipe.
    It performs a full calculation of pixel and group calibration using synthetic data
    on the POP test instrument.
    """

    def setUp(self):
        self.syntheticInputs = SyntheticData()
        self.fakeIngredients = self.syntheticInputs.ingredients

        self.fakeRawData = "_test_diffcal_rx"
        self.fakeGroupingWorkspace = "_test_diffcal_rx_grouping"
        self.fakeDiagnosticWorkspace = "_test_diffcal_rx_diagnostic"
        self.fakeOutputWorkspace = "_test_diffcal_rx_dsp_output"
        self.fakeTableWorkspace = "_test_diffcal_rx_table"
        self.fakeMaskWorkspace = "_test_diffcal_rx_mask"
        self.syntheticInputs.generateWorkspaces(self.fakeRawData, self.fakeGroupingWorkspace, self.fakeMaskWorkspace)

        self.groceryList = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "outputWorkspace": self.fakeOutputWorkspace,
            "diagnosticWorkspace": self.fakeDiagnosticWorkspace,
            "calibrationTable": self.fakeTableWorkspace,
            "maskWorkspace": self.fakeMaskWorkspace,
        }
        self.service = CalibrationService()

    def tearDown(self) -> None:
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for ws in workspaces:
            deleteWorkspaceNoThrow(ws)
        return super().tearDown

    @mock.patch(thisService + "SimpleDiffCalRequest")
    def test_execute_diffcal(self, mockSimpleDiffCalRequest):
        self.service.validateRequest = mock.Mock()
        self.service.prepDiffractionCalibrationIngredients = mock.Mock()
        self.service.fetchDiffractionCalibrationGroceries = mock.Mock()

        rawWS = "_test_diffcal_rx_data"
        groupingWS = "_test_diffcal_grouping"
        maskWS = "_test_diffcal_mask"
        self.syntheticInputs.generateWorkspaces(rawWS, groupingWS, maskWS)

        self.groceryList["inputWorkspace"] = rawWS
        self.groceryList["groupingWorkspace"] = groupingWS
        self.groceryList["maskWorkspace"] = maskWS
        mockSimpleDiffCalRequest.return_value = SimpleDiffCalRequest(
            ingredients=self.fakeIngredients,
            groceries=self.groceryList,
        )

        try:
            res = self.service.diffractionCalibration(mock.Mock())
        except ValueError:
            print(res)
        assert res["result"]

        assert res["maskWorkspace"]
        mask = mtd[res["maskWorkspace"]]
        assert mask.getNumberMasked() == 0
        assert res["steps"][-1] <= self.fakeIngredients.convergenceThreshold

    def test_obtainLock(self):
        runNumber = "123456"
        useLiteMode = True
        request = CalibrationLockRequest(
            runNumber=runNumber,
            useLiteMode=useLiteMode,
        )
        with mock.patch.object(self.service.dataExportService.dataService, "generateStateId") as mockGenerateStateId:
            stateHash = "1a2b3c4d5e6f7a8b"
            mockGenerateStateId.return_value = (stateHash, None)
            lock = self.service.obtainLock(request)
            lockfileContents = lock.lockFilePath.read_text()
            assert f"/{stateHash}/lite/diffraction" in lockfileContents
            assert mockGenerateStateId.called
            assert lock is not None
        lock.release()
