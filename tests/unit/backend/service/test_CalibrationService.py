# ruff: noqa: E402, ARG002
import json
import tempfile
import time
import unittest
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from random import randint
from typing import Dict, List
from unittest import mock

import pytest
from mantid.simpleapi import (
    CloneWorkspace,
    CreateSingleValuedWorkspace,
    CreateWorkspace,
    DeleteWorkspace,
    GroupWorkspaces,
    LoadInstrument,
    mtd,
)
from snapred.backend.dao.ingredients import CalibrationMetricsWorkspaceIngredients
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    CalibrationLoadAssessmentRequest,
    CalibrationWritePermissionsRequest,
    CreateCalibrationRecordRequest,
    CreateIndexEntryRequest,
    DiffractionCalibrationRequest,
    InitializeStateRequest,
)
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.meta.builder.GroceryListBuilder import GroceryListBuilder
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.redantic import parse_file_as
from util.dao import DAOFactory
from util.state_helpers import state_root_redirect

# Mock out of scope modules before importing DataExportService

localMock = mock.Mock()

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.data.DataExportService": mock.Mock(),
        "snapred.backend.data.DataFactoryService": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
    from snapred.backend.dao.indexing.IndexEntry import IndexEntry
    from snapred.backend.dao.request.CalibrationAssessmentRequest import CalibrationAssessmentRequest
    from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
    from snapred.backend.dao.request.HasStateRequest import HasStateRequest
    from snapred.backend.dao.state import PixelGroup
    from snapred.backend.dao.state.FocusGroup import FocusGroup
    from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe
    from snapred.backend.service.CalibrationService import CalibrationService
    from snapred.backend.service.SousChef import SousChef
    from snapred.meta.Config import Resource
    from util.helpers import (
        createCompatibleDiffCalTable,
        createCompatibleMask,
    )
    from util.SculleryBoy import SculleryBoy

    thisService = "snapred.backend.service.CalibrationService."

    # test export calibration
    def test_exportCalibrationIndex():
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry.return_value = "expected"
        calibrationService.saveCalibrationToIndex(
            IndexEntry(runNumber="1", useLiteMode=True, comments="", author=""),
        )
        assert calibrationService.dataExportService.exportCalibrationIndexEntry.called
        savedEntry = calibrationService.dataExportService.exportCalibrationIndexEntry.call_args.args[0]
        assert savedEntry.appliesTo == ">=1"
        assert savedEntry.timestamp is not None

    def test_exportCalibrationIndex_no_timestamp():
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry.return_value = "expected"
        calibrationService.dataExportService.getUniqueTimestamp = mock.Mock(return_value=123.123)
        entry = IndexEntry(runNumber="1", useLiteMode=True, comments="", author="")
        entry.timestamp = None
        calibrationService.saveCalibrationToIndex(entry)
        assert calibrationService.dataExportService.exportCalibrationIndexEntry.called
        savedEntry = calibrationService.dataExportService.exportCalibrationIndexEntry.call_args.args[0]
        assert savedEntry.timestamp == calibrationService.dataExportService.getUniqueTimestamp.return_value

    def test_save():
        workspace = mtd.unique_name(prefix="_dsp_")
        CreateSingleValuedWorkspace(OutputWorkspace=workspace)
        workspaces = {wngt.DIFFCAL_OUTPUT: [workspace]}
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationRecord = mock.Mock()
        calibrationService.dataExportService.exportCalibrationWorkspaces = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.createCalibrationIndexEntry = mock.Mock()
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

    def test_save_unexpected_units():
        workspace = mtd.unique_name(prefix="_tof_")
        CreateSingleValuedWorkspace(OutputWorkspace=workspace)
        workspaces = {wngt.DIFFCAL_OUTPUT: [workspace]}
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationRecord = mock.Mock()
        calibrationService.dataExportService.exportCalibrationWorkspaces = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.createCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.createCalibrationRecord = mock.Mock(
            return_value=mock.Mock(
                runNumber="012345",
                focusGroupCalibrationMetrics=mock.Mock(focusGroupName="group"),
                workspaces=workspaces,
            )
        )
        with pytest.raises(RuntimeError, match=r".*without a units token in its name.*"):
            calibrationService.save(mock.Mock())

    def test_save_unexpected_type():
        workspace = mtd.unique_name(prefix="_dsp_")
        CreateSingleValuedWorkspace(OutputWorkspace=workspace)
        workspaces = {wngt.REDUCTION_OUTPUT: [workspace]}
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationRecord = mock.Mock()
        calibrationService.dataExportService.exportCalibrationWorkspaces = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.createCalibrationIndexEntry = mock.Mock()
        calibrationService.dataFactoryService.createCalibrationRecord = mock.Mock(
            return_value=mock.Mock(
                runNumber="012345",
                focusGroupCalibrationMetrics=mock.Mock(focusGroupName="group"),
                workspaces=workspaces,
            )
        )
        with pytest.raises(RuntimeError, match=r".*Unexpected output type.*"):
            calibrationService.save(mock.Mock())

    def test_load():
        calibrationService = CalibrationService()
        calibrationService.dataFactoryService.getCalibrationRecord = mock.Mock(return_value="passed")
        res = calibrationService.load(mock.Mock())
        assert res == calibrationService.dataFactoryService.getCalibrationRecord.return_value

    def test_getCalibrationIndex():
        calibrationService = CalibrationService()
        calibrationService.dataFactoryService.getCalibrationIndex = mock.Mock(
            return_value=IndexEntry(runNumber="1", useLiteMode=True, comments="", author="")
        )
        calibrationService.getCalibrationIndex(mock.MagicMock(run=mock.MagicMock(runNumber="123")))
        assert calibrationService.dataFactoryService.getCalibrationIndex.called


from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.service.CalibrationService import CalibrationService  # noqa: F811


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
        cls.excludeAtTeardown = [cls.sampleWS, cls.sampleTableWS, cls.sampleMaskWS, cls.sampleDiagnosticWS, ws]

    def setUp(self):
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
    def test_assessQuality(
        self,
        FitMultiplePeaksRecipe,
        FarmFreshIngredients,
    ):
        # Mock input data
        timestamp = time.time()
        mockFarmFreshIngredients = mock.Mock(
            spec=FarmFreshIngredients,
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
        self.instance._collectMetrics = mock.Mock(return_value=fakeMetrics)

        # Call the method to test
        request = CalibrationAssessmentRequest(
            workspaces={
                wngt.DIFFCAL_OUTPUT: [self.sampleWS],
                wngt.DIFFCAL_TABLE: [self.sampleTableWS],
                wngt.DIFFCAL_MASK: [self.sampleMaskWS],
            },
            run=RunConfig(runNumber=self.runNumber),
            useLiteMode=True,
            focusGroup={"name": fakeMetrics.focusGroupName, "definition": ""},
            calibrantSamplePath="egg/muffin/biscuit.pastry",
            peakFunction="Gaussian",
            crystalDMin=0,
            crystalDMax=10,
            nBinsAcrossPeakWidth=0,
            maxChiSq=100.0,
        )
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
        assert response.model_dump() == {
            "record": expectedRecord.model_dump(),
            "metricWorkspaces": expectedWorkspaces,
        }

        # Assert expected calibration metric workspaces have been generated
        for wsName in expectedWorkspaces:
            assert self.instance.dataFactoryService.workspaceDoesExist(wsName)

    def test_save_respects_version(self):
        version = 1
        record = DAOFactory.calibrationRecord(version=1)
        record.calculationParameters.creationDate = datetime.today()
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
        request = CalibrationExportRequest(
            createIndexEntryRequest=CreateIndexEntryRequest(
                runNumber=record.runNumber,
                useLiteMode=record.useLiteMode,
                version=record.version,
                appliesTo=f">={record.runNumber}",
                author="",
                comments="",
                timestamp=time.time(),
            ),
            createRecordRequest=CreateCalibrationRecordRequest(**record.model_dump()),
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
            with pytest.raises(ValueError) as excinfo:  # noqa: PT011
                self.instance.loadQualityAssessment(mockRequest)
            assert str(mockRequest.runId) in str(excinfo.value)
            assert str(mockRequest.version) in str(excinfo.value)

    @mock.patch(thisService + "CalibrationMetricsWorkspaceIngredients")
    def test_load_quality_assessment_no_calibration_metrics_exception(
        self,
        mockCalibrationMetricsWorkspaceIngredients,
    ):
        mockRequest = mock.Mock(runId=self.runNumber, version=self.version, checkExistent=False)
        calibrationRecord = DAOFactory.calibrationRecord(runNumber="57514", version=1)
        # Clear the input metrics list
        calibrationRecord.focusGroupCalibrationMetrics.calibrationMetric = []
        mockCalibrationMetricsWorkspaceIngredients.return_value = mock.Mock(
            spec=CalibrationMetricsWorkspaceIngredients,
            calibrationRecord=calibrationRecord,
            timestamp=self.timestamp,
        )
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibrationRecord)
        with pytest.raises(Exception, match=r".*input table is empty.*"):
            self.instance.loadQualityAssessment(mockRequest)

    def test_load_quality_assessment_check_existent_metrics(self):
        path = Resource.getPath("outputs")
        with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpDir:
            calibRecord = DAOFactory.calibrationRecord()
            self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.instance.dataFactoryService.getCalibrationDataPath = mock.Mock(return_value=tmpDir)
            self.instance.groceryService.dataService.readCalibrationRecord = mock.Mock()
            self.instance.groceryService.fetchCalibrationWorkspaces = mock.Mock()
            self.instance.groceryService._createDiffcalTableWorkspaceName = mock.Mock()
            self.instance.groceryService.fetchGroceryDict = mock.Mock()
            self.create_fake_diffcal_files(Path(tmpDir), calibRecord.workspaces, calibRecord.version)

            self.instance.groceryService._getCalibrationDataPath = mock.Mock(return_value=tmpDir)
            self.instance.groceryService._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)
            mockRequest = mock.Mock(
                spec=CalibrationLoadAssessmentRequest,
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
        with state_root_redirect(self.localDataService) as tmpRoot:
            indexer = self.localDataService.calibrationIndexer(record.runNumber, record.useLiteMode)
            recordFilepath = indexer.recordPath(version)
            tmpRoot.saveObjectAt(record, recordFilepath)
            tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.create_fake_diffcal_files(recordFilepath.parent, record.workspaces, version)

            mockRequest = mock.Mock(
                runId=record.runNumber,
                useLiteMode=record.useLiteMode,
                version=version,
                checkExistent=False,
            )
            self.instance.groceryService._fetchInstrumentDonor = mock.Mock(return_value=self.sampleWS)

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
        with state_root_redirect(self.localDataService) as tmpRoot:
            indexer = self.localDataService.calibrationIndexer(record.runNumber, record.useLiteMode)
            recordFilepath = indexer.recordPath(version)
            tmpRoot.saveObjectAt(record, recordFilepath)
            tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))

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
                assert self.instance.dataFactoryService.workspaceDoesExist(ws_name)

            # Assert all "persistent" workspaces have been loaded
            for wsNames in record.workspaces.values():
                for wsName in wsNames:
                    assert self.instance.dataFactoryService.workspaceDoesExist(wsName)

    def test_load_quality_assessment_no_units(self):
        calibRecord = DAOFactory.calibrationRecord(runNumber="57514", version=1)
        calibRecord.workspaces[wngt.DIFFCAL_OUTPUT] = ["_diffoc_057514_v0001"]  # NOTE no unit token
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)

        # Call the method to test. Use a mocked run and a mocked version
        mockRequest = mock.Mock(runId=calibRecord.runNumber, version=calibRecord.version, checkExistent=False)
        with pytest.raises(RuntimeError, match=r".*without a units token in its name.*"):
            self.instance.loadQualityAssessment(mockRequest)

    def test_load_quality_assessment_dsp_and_diag(self):
        calibRecord = DAOFactory.calibrationRecord(
            workspaces={
                "diffCalOutput": ["_dsp_diffoc_057514"],
                "diffCalDiagnostic": ["_diagnostic_diffoc_057514"],
                "diffCalTable": ["_diffract_consts_057514"],
                "diffCalMask": ["_diffract_consts_mask_057514"],
            }
        )
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)

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
            "diffCalOutput": ["_dsp_diffoc_057514"],
            "diffCalTable": ["_diffract_consts_057514"],
            "diffCalMask": ["_diffract_consts_mask_057514"],
            "rawVanadium": ["_unexpected_workspace_type"],
        }
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)

        mockRequest = mock.Mock(
            spec=CalibrationLoadAssessmentRequest,
            runId=calibRecord.runNumber,
            useLiteMode=calibRecord.useLiteMode,
            version=calibRecord.version,
            checkExistent=False,
        )
        with pytest.raises(RuntimeError, match=r".*not implemented: unable to load unexpected.*"):
            self.instance.loadQualityAssessment(mockRequest)

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    def test_prepDiffractionCalibrationIngredients(self, FarmFreshIngredients):
        mockFF = mock.Mock()
        FarmFreshIngredients.return_value = mockFF
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="bundt/cake.egg")

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

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @mock.patch(thisService + "DiffractionCalibrationRecipe", spec_set=DiffractionCalibrationRecipe)
    def test_diffractionCalibration(
        self,
        DiffractionCalibrationRecipe,
        FarmFreshIngredients,
    ):
        FarmFreshIngredients.return_value = mock.Mock(runNumber="123")
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="bundt/cake.egg")
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value="lah/dee/dah")
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=True)
        self.instance.sousChef = SculleryBoy()

        DiffractionCalibrationRecipe().executeRecipe.return_value = {"calibrationTable": "fake"}

        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroceryDict = mock.Mock(return_value={"maskWorkspace": self.sampleMaskWS})

        # Call the method with the provided parameters
        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=True,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
        )
        result = self.instance.diffractionCalibration(request)

        # Perform assertions to check the result and method calls
        assert FarmFreshIngredients.call_count == 1
        prepared_ingredients = self.instance.sousChef.prepDiffractionCalibrationIngredients(FarmFreshIngredients())
        prepared_ingredients.removeBackground = request.removeBackground
        DiffractionCalibrationRecipe().executeRecipe.assert_called_once_with(
            prepared_ingredients,
            self.instance.groceryService.fetchGroceryDict.return_value,
        )
        assert result == {"calibrationTable": "fake"}
        self.instance.dataExportService.getCalibrationStateRoot.assert_called_once_with(request.runNumber)
        self.instance.dataExportService.checkWritePermissions.assert_called_once_with(
            self.instance.dataExportService.getCalibrationStateRoot.return_value
        )

    def test_validateRequest(self):
        # test that `validateRequest` calls `validateWritePermissions`
        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=True,
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
            removeBackground=True,
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
            removeBackground=True,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
        )
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=request.runNumber, continueFlags=request.continueFlags
        )
        with pytest.raises(RuntimeError, match=r".*you don't have permissions to write.*"):
            self.instance.validateWritePermissions(permissionsRequest)

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @mock.patch(thisService + "DiffractionCalibrationRecipe", spec_set=DiffractionCalibrationRecipe)
    def test_diffractionCalibration_with_bad_masking(
        self,
        DiffractionCalibrationRecipe,
        FarmFreshIngredients,
    ):
        FarmFreshIngredients.return_value = mock.Mock(runNumber="123")
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="bundt/cake.egg")
        self.instance.dataExportService.getCalibrationStateRoot = mock.Mock(return_value="lah/dee/dah")
        self.instance.dataExportService.checkWritePermissions = mock.Mock(return_value=True)
        self.instance.sousChef = SculleryBoy()

        DiffractionCalibrationRecipe().executeRecipe.return_value = {"calibrationTable": "fake"}

        numHistograms = mtd[self.sampleMaskWS].getNumberHistograms()
        for pixel in range(numHistograms):
            mtd[self.sampleMaskWS].setY(pixel, [1.0])
        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroceryDict = mock.Mock(return_value={"maskWorkspace": self.sampleMaskWS})

        # Call the method with the provided parameters
        # Call the method with the provided parameters
        request = DiffractionCalibrationRequest(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
            removeBackground=True,
            focusGroup=FocusGroup(name="all", definition="path/to/all"),
            continueFlags=ContinueWarning.Type.UNSET,
            skipPixelCalibration=False,
        )
        with pytest.raises(Exception, match=r".*pixels failed calibration*"):
            self.instance.diffractionCalibration(request)

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @mock.patch(thisService + "FocusSpectraRecipe")
    def test_focusSpectra_not_exist(
        self,
        FocusSpectraRecipe,
        FarmFreshIngredients,
    ):
        self.instance.sousChef = SculleryBoy()

        FocusSpectraRecipe().executeRecipe.return_value = mock.Mock()

        request = mock.Mock(runNumber="12345", focusGroup=mock.Mock(name="group1"))
        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroupingDefinition = mock.Mock(return_value={"workspace": "orange"})

        focusedWorkspace = (
            wng.run()
            .runNumber(request.runNumber)
            .group(request.focusGroup.name)
            .unit(wng.Units.DSP)
            .auxiliary("F-dc")
            .build()
        )
        assert not mtd.doesExist(focusedWorkspace)

        # Call the method with the provided parameters
        res = self.instance.focusSpectra(request)

        # Perform assertions to check the result and method calls

        groupingItem = self.instance.groceryClerk.build.return_value
        groupingWorkspace = self.instance.groceryService.fetchGroupingDefinition.return_value["workspace"]
        assert FarmFreshIngredients.call_count == 1
        self.instance.groceryService.fetchGroupingDefinition.assert_called_once_with(groupingItem)
        FocusSpectraRecipe().executeRecipe.assert_called_once_with(
            InputWorkspace=request.inputWorkspace,
            GroupingWorkspace=groupingWorkspace,
            Ingredients=self.instance.sousChef.prepPixelGroup(FarmFreshIngredients()),
            OutputWorkspace=focusedWorkspace,
        )
        assert res == (focusedWorkspace, groupingWorkspace)

    @mock.patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @mock.patch(thisService + "FocusSpectraRecipe")
    def test_focusSpectra_exists(self, FocusSpectraRecipe, FarmFreshIngredients):
        self.instance.sousChef = SculleryBoy()

        request = mock.Mock(runNumber="12345", focusGroup=mock.Mock(name="group1"))
        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroupingDefinition = mock.Mock(return_value={"workspace": "orange"})

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
        testConfig = StateConfig.construct()
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
        assert not self.instance.hasState(badRequest)

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
