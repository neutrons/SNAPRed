# ruff: noqa: E402, ARG002
import json
import tempfile
import time
import unittest
import unittest.mock as mock
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest
from mantid.simpleapi import (
    CloneWorkspace,
    CreateSingleValuedWorkspace,
    DeleteWorkspace,
    GroupWorkspaces,
    LoadEmptyInstrument,
    mtd,
)
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.ingredients import CalibrationMetricsWorkspaceIngredients
from snapred.backend.dao.request.CalibrationExportRequest import CalibrationExportRequest
from snapred.backend.dao.request.CreateCalibrationRecordRequest import CreateCalibrationRecordRequest
from snapred.backend.dao.request.CreateIndexEntryRequest import CreateIndexEntryRequest
from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.redantic import parse_file_as
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
    from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
    from snapred.backend.dao.indexing.IndexEntry import IndexEntry
    from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
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

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/calibration/ReductionIngredients.json", "r") as f:
            return ReductionIngredients.model_validate_json(f.read())

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
        calibrationService.getCalibrationIndex(MagicMock(run=MagicMock(runNumber="123")))
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

        LoadEmptyInstrument(
            Filename=cls.instrumentFilePath,
            OutputWorkspace=cls.sampleWS,
        )
        ws = mtd.unique_name()
        LoadEmptyInstrument(
            Filename=cls.instrumentFilePath,
            OutputWorkspace=ws,
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
                        filename = Path(wsName + ".tar")
                        self.instance.dataExportService.exportRaggedWorkspace(path, filename, self.sampleWS)

        # Write the table and/or mask
        if tableWSName or maskWSName:
            filename = Path(tableWSName + ".h5")
            self.instance.dataExportService.dataService.writeDiffCalWorkspaces(
                path,
                filename,
                tableWorkspaceName=self.sampleTableWS if tableWSName else "",
                maskWorkspaceName=self.sampleMaskWS if maskWSName else "",
            )

    @patch(thisService + "CalibrationMetricExtractionRecipe")
    @patch(thisService + "FocusGroupMetric")
    def test_collectMetrics(
        self,
        FocusGroupMetric,
        CalibrationMetricExtractionRecipe,
    ):
        # mock the inputs to the function
        focussedData = MagicMock(spec_set=str)
        focusGroup = MagicMock(spec=FocusGroup)
        focusGroup.name = "group1"
        pixelGroup = MagicMock(spec_set=PixelGroup)

        # mock external method calls
        self.instance.parseCalibrationMetricList = mock.Mock(
            side_effect=lambda y: y,
        )
        mockMetric = mock.Mock()
        CalibrationMetricExtractionRecipe.return_value.executeRecipe.return_value = mockMetric

        # Call the method to test
        metric = self.instance._collectMetrics(focussedData, focusGroup, pixelGroup)

        # Assert correct behavior
        assert CalibrationMetricExtractionRecipe.called_once_with(
            InputWorkspace=focussedData,
            PixelGroup=pixelGroup,
        )
        assert self.instance.parseCalibrationMetricList.called_once_with(mockMetric)
        assert FocusGroupMetric.called_once_with(
            focusGroupName=focusGroup.name,
            calibrationMetric=mockMetric,  # NOTE this is because of above lambda
        )
        assert metric == FocusGroupMetric.return_value

    @patch(
        thisService + "CalibrationAssessmentResponse",
        return_value=MagicMock(mockId="mock_calibration_assessment_response"),
    )
    @patch(thisService + "CreateCalibrationRecordRequest")
    @patch(thisService + "FitMultiplePeaksRecipe")
    @patch(thisService + "FarmFreshIngredients")
    @patch(
        thisService + "CalibrationMetricsWorkspaceIngredients",
        return_value=MagicMock(
            calibrationRecord=CalibrationRecord.model_validate_json(
                Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
            ),
            timestamp=123.123,
        ),
    )
    def test_assessQuality(
        self,
        CalibrationMetricsWorkspaceIngredients,
        FarmFreshIngredients,
        FitMultiplePeaksRecipe,
        CreateCalibrationRecordRequest,
        CalibrationAssessmentResponse,
    ):
        # Mock input data
        fakeFocusGroup = FocusGroup(name="egg", definition="muffin")
        fakeMetrics = CalibrationMetric(
            sigmaAverage=0.0,
            sigmaStandardDeviation=0.0,
            strainAverage=0.0,
            strainStandardDeviation=0.0,
            twoThetaAverage=0.0,
        )
        fakeMetrics = FocusGroupMetric(focusGroupName=fakeFocusGroup.name, calibrationMetric=[fakeMetrics])

        # Mock the necessary method calls
        self.instance.sousChef = SculleryBoy()
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value="good/cif/path")
        self.instance.dataFactoryService.createCalibrationRecord = MagicMock()
        self.instance._collectMetrics = MagicMock(return_value=fakeMetrics)

        FarmFreshIngredients.return_value.get.return_value = True

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
            peakIntensityThreshold=0,
            nBinsAcrossPeakWidth=0,
            maxChiSq=100.0,
        )
        response = self.instance.assessQuality(request)

        # Assert correct method calls
        assert FarmFreshIngredients.call_count == 1
        assert FitMultiplePeaksRecipe.called_once_with(self.instance.sousChef.prepPeakIngredients({}))
        assert self.instance.dataFactoryService.getCifFilePath.called_once_with("biscuit")

        # Assert the result is as expected
        assert response == CalibrationAssessmentResponse.return_value

        # Assert expected calibration metric workspaces have been generated
        for metric in ["sigma", "strain"]:
            wsName = wng.diffCalTimedMetric().runNumber("57514").timestamp(123.123).metricName(metric).build()
            assert self.instance.dataFactoryService.workspaceDoesExist(wsName)

    def test_save_respects_version(self):
        record = parse_file_as(CalibrationRecord, Resource.getPath("inputs/calibration/CalibrationRecord_v0001.json"))
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
            pass
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
            mockRequest = MagicMock(
                runId=self.runNumber,
                useLiteMode=True,
                version=self.version,
                checkExistent=False,
            )
            with pytest.raises(ValueError) as excinfo:  # noqa: PT011
                self.instance.loadQualityAssessment(mockRequest)
            assert str(mockRequest.runId) in str(excinfo.value)
            assert str(mockRequest.version) in str(excinfo.value)

    @patch(thisService + "CalibrationMetricsWorkspaceIngredients")
    def test_load_quality_assessment_no_calibration_metrics_exception(
        self,
        mockCalibrationMetricsWorkspaceIngredients,
    ):
        mockRequest = mock.Mock(runId=self.runNumber, version=self.version, checkExistent=False)
        calibrationRecord = CalibrationRecord.model_validate_json(
            Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
        )

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
            calibRecord = CalibrationRecord.model_validate_json(
                Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
            )
            self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=calibRecord)

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.instance.dataFactoryService.getCalibrationDataPath = MagicMock(return_value=tmpDir)
            self.instance.groceryService.dataService.readCalibrationRecord = MagicMock()
            self.instance.groceryService.fetchCalibrationWorkspaces = MagicMock()
            self.instance.groceryService._createDiffcalTableWorkspaceName = MagicMock()
            self.instance.groceryService.fetchGroceryDict = MagicMock()
            self.create_fake_diffcal_files(Path(tmpDir), calibRecord.workspaces, calibRecord.version)

            mockRequest = MagicMock(runId=calibRecord.runNumber, version=calibRecord.version, checkExistent=False)
            self.instance.groceryService._getCalibrationDataPath = MagicMock(return_value=tmpDir)
            self.instance.groceryService._fetchInstrumentDonor = MagicMock(return_value=self.sampleWS)

            # Load the assessment workspaces:
            self.instance.loadQualityAssessment(mockRequest)

            # Delete any existing _data_ workspaces:
            for wss in calibRecord.workspaces.values():
                for ws in wss:
                    if mtd.doesExist(ws):
                        DeleteWorkspace(Workspace=ws)

            mockRequest = MagicMock(runId=calibRecord.runNumber, version=calibRecord.version, checkExistent=True)
            with pytest.raises(RuntimeError) as excinfo:  # noqa: PT011
                self.instance.loadQualityAssessment(mockRequest)
            assert "is already loaded" in str(excinfo.value)

    def test_load_quality_assessment_check_existent_data(self):
        inputFilepath = Resource.getPath("inputs/calibration/CalibrationRecord_v0001.json")
        paramsFilepath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
        with state_root_redirect(self.localDataService) as tmpRoot:
            calibRecord = parse_file_as(CalibrationRecord, inputFilepath)
            indexer = self.localDataService.calibrationIndexer(calibRecord.runNumber, calibRecord.useLiteMode)
            recordFilepath = indexer.recordPath(1)
            tmpRoot.addFileAs(inputFilepath, recordFilepath)
            tmpRoot.addFileAs(paramsFilepath, indexer.parametersPath(1))

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.create_fake_diffcal_files(recordFilepath.parent, calibRecord.workspaces, calibRecord.version)

            mockRequest = mock.Mock(
                runId=calibRecord.runNumber,
                useLiteMode=calibRecord.useLiteMode,
                version=calibRecord.version,
                checkExistent=False,
            )
            self.instance.groceryService._fetchInstrumentDonor = MagicMock(return_value=self.sampleWS)

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
        inputFilepath = Resource.getPath("inputs/calibration/CalibrationRecord_v0001.json")
        paramsFilepath = Resource.getPath("inputs/calibration/CalibrationParameters.json")
        with state_root_redirect(self.localDataService) as tmpRoot:
            calibRecord = parse_file_as(CalibrationRecord, inputFilepath)
            indexer = self.localDataService.calibrationIndexer(calibRecord.runNumber, calibRecord.useLiteMode)
            recordFilepath = indexer.recordPath(1)
            tmpRoot.addFileAs(inputFilepath, recordFilepath)
            tmpRoot.addFileAs(paramsFilepath, indexer.parametersPath(calibRecord.version))

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.create_fake_diffcal_files(recordFilepath.parent, calibRecord.workspaces, calibRecord.version)

            # Call the method to test. Use a mocked run and a mocked version
            mockRequest = MagicMock(
                runId=calibRecord.runNumber,
                useLiteMode=calibRecord.useLiteMode,
                version=calibRecord.version,
                checkExistent=False,
            )
            self.instance.groceryService._fetchInstrumentDonor = MagicMock(return_value=self.sampleWS)

            self.instance.loadQualityAssessment(mockRequest)

            # Assert the expected calibration metric workspaces have been generated
            for metric in ["sigma", "strain"]:
                ws_name = (
                    wng.diffCalMetric()
                    .metricName(metric)
                    .runNumber(calibRecord.runNumber)
                    .version(calibRecord.version)
                    .metricName(metric)
                    .build()
                )
                assert self.instance.dataFactoryService.workspaceDoesExist(ws_name)

            # Assert all "persistent" workspaces have been loaded
            for wsNames in calibRecord.workspaces.values():
                for wsName in wsNames:
                    assert self.instance.dataFactoryService.workspaceDoesExist(wsName)

    def test_load_quality_assessment_no_units(self):
        calibRecord = CalibrationRecord.model_validate_json(
            Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
        )
        calibRecord.workspaces[wngt.DIFFCAL_OUTPUT] = ["_diffoc_057514_v0001"]  # NOTE no unit token
        self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=calibRecord)

        # Call the method to test. Use a mocked run and a mocked version
        mockRequest = MagicMock(runId=calibRecord.runNumber, version=calibRecord.version, checkExistent=False)
        with pytest.raises(RuntimeError, match=r".*without a units token in its name.*"):
            self.instance.loadQualityAssessment(mockRequest)

    def test_load_quality_assessment_dsp_and_diag(self):
        calibRecord = CalibrationRecord.model_validate_json(
            Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
        )
        calibRecord.workspaces = {
            "diffCalOutput": ["_dsp_diffoc_057514"],
            "diffCalDiagnostic": ["_diagnostic_diffoc_057514"],
            "diffCalTable": ["_diffract_consts_057514"],
            "diffCalMask": ["_diffract_consts_mask_057514"],
        }
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=calibRecord)

        mockFetchGroceryDict = mock.Mock(return_value={})
        self.instance.groceryService.fetchGroceryDict = mockFetchGroceryDict

        # Call the method to test. Use a mocked run and a mocked version
        mockRequest = MagicMock(runId=calibRecord.runNumber, version=calibRecord.version, checkExistent=False)
        self.instance.loadQualityAssessment(mockRequest)
        calledWithDict = mockFetchGroceryDict.call_args[0][0]
        assert calledWithDict["diffCalOutput_0000"].unit == wng.Units.DSP
        assert calledWithDict["diffCalDiagnostic_0000"].unit == wng.Units.DIAG

    def test_load_quality_assessment_unexpected_type(self):
        calibRecord = CalibrationRecord.model_validate_json(
            Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
        )
        calibRecord.workspaces = {
            "diffCalOutput": ["_dsp_diffoc_057514"],
            "diffCalTable": ["_diffract_consts_057514"],
            "diffCalMask": ["_diffract_consts_mask_057514"],
            "rawVanadium": ["_unexpected_workspace_type"],
        }
        self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=calibRecord)

        # Call the method to test. Use a mocked run and a mocked version
        mockRequest = MagicMock(runId=calibRecord.runNumber, version=calibRecord.version, checkExistent=False)
        with pytest.raises(RuntimeError, match=r".*not implemented: unable to load unexpected.*"):
            self.instance.loadQualityAssessment(mockRequest)

    @patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
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
        assert self.instance.dataFactoryService.getCifFilePath.called_once_with("cake_egg")
        assert res == self.instance.sousChef.prepDiffractionCalibrationIngredients(mockFF)

    def test_fetchDiffractionCalibrationGroceries(self):
        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroceryDict = mock.Mock(return_value={"grocery1": "orange"})

        # Call the method with the provided parameters
        request = mock.Mock(runNumber="12345", focusGroup=mock.Mock(name="group1"))
        res = self.instance.fetchDiffractionCalibrationGroceries(request)

        # Perform assertions to check the result and method calls
        assert self.instance.groceryClerk.buildDict.call_count == 1
        assert self.instance.groceryService.fetchGroceryDict.called_once_with(
            self.instance.groceryClerk.buildDict.return_value
        )
        assert res == self.instance.groceryService.fetchGroceryDict.return_value

    @patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @patch(thisService + "DiffractionCalibrationRecipe", spec_set=DiffractionCalibrationRecipe)
    def test_diffractionCalibration(
        self,
        DiffractionCalibrationRecipe,
        FarmFreshIngredients,
    ):
        FarmFreshIngredients.return_value = mock.Mock(runNumber="123")
        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="bundt/cake.egg")
        self.instance.sousChef = SculleryBoy()

        DiffractionCalibrationRecipe().executeRecipe.return_value = {"calibrationTable": "fake"}

        self.instance.groceryClerk = mock.Mock()
        self.instance.groceryService.fetchGroceryDict = mock.Mock(return_value={"grocery1": "orange"})

        # Call the method with the provided parameters
        request = mock.Mock(
            runNumber="123",
            useLiteMode=True,
            calibrantSamplePath="bundt/cake_egg.py",
        )
        res = self.instance.diffractionCalibration(request)

        # Perform assertions to check the result and method calls
        assert FarmFreshIngredients.call_count == 1
        assert DiffractionCalibrationRecipe().executeRecipe.called_once_with(
            self.instance.sousChef.prepDiffractionCalibrationIngredients(FarmFreshIngredients()),
            self.instance.groceryService.fetchGroceryDict.return_value,
        )
        assert res == {"calibrationTable": "fake"}

    @patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @patch(thisService + "FocusSpectraRecipe")
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
        # assert self.instance.sousChef.prepPixelGroup.called_once_with(FarmFreshIngredients.return_value)
        assert self.instance.groceryService.fetchGroupingDefinition.called_once_with(groupingItem)
        assert FocusSpectraRecipe().executeRecipe.called_once_with(
            InputWorkspace=request.inputWorkspace,
            GroupingWorkspace=groupingWorkspace,
            Ingredients=self.instance.sousChef.prepPixelGroup(FarmFreshIngredients()),
            OutputWorkspace=focusedWorkspace,
        )
        assert res == (focusedWorkspace, groupingWorkspace)

    @patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @patch(thisService + "FocusSpectraRecipe")
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
        assert self.instance.groceryService.fetchGroupingDefinition.called_once_with(groupingItem)

        # assert that the recipe is not called and the correct workspaces are returned
        assert FocusSpectraRecipe().executeRecipe.call_count == 0
        assert res == (focusedWorkspace, groupingWorkspace)

    @patch(thisService + "FitMultiplePeaksRecipe")
    def test_fitPeaks(self, FitMultiplePeaksRecipe):
        request = mock.Mock()
        res = self.instance.fitPeaks(request)
        assert res == FitMultiplePeaksRecipe.return_value.executeRecipe.return_value

    def test_initializeState(self):
        testCalibration = Calibration.model_validate_json(
            Resource.read("inputs/calibration/CalibrationParameters.json")
        )
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
        testCalibration = Calibration.model_validate_json(
            Resource.read("inputs/calibration/CalibrationParameters.json")
        )
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
