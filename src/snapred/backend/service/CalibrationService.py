import json
import time
from datetime import date
from functools import lru_cache
from typing import List, Tuple

from pydantic import parse_raw_as

from snapred.backend.dao import GroupPeakList, RunConfig
from snapred.backend.dao.calibration import (
    CalibrationIndexEntry,
    CalibrationMetric,
    CalibrationRecord,
    FocusGroupMetric,
)
from snapred.backend.dao.ingredients import (
    CalibrationMetricsWorkspaceIngredients,
    DiffractionCalibrationIngredients,
    FitMultiplePeaksIngredients,
    GroceryListItem,
    PixelGroupingIngredients,
)
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    DiffractionCalibrationRequest,
    FarmFreshIngredients,
    InitializeStateRequest,
)
from snapred.backend.dao.state import FocusGroup, InstrumentState, PixelGroup
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe
from snapred.backend.recipe.GenerateCalibrationMetricsWorkspaceRecipe import GenerateCalibrationMetricsWorkspaceRecipe
from snapred.backend.recipe.GenericRecipe import (
    CalibrationMetricExtractionRecipe,
    DetectorPeakPredictorRecipe,
    FitMultiplePeaksRecipe,
    GenerateTableWorkspaceFromListOfDictRecipe,
)
from snapred.backend.recipe.GroupWorkspaceIterator import GroupWorkspaceIterator
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


@Singleton
class CalibrationService(Service):
    dataFactoryService: "DataFactoryService"
    dataExportService: "DataExportService"
    MILLISECONDS_PER_SECOND = Config["constants.millisecondsPerSecond"]

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.groceryClerk = GroceryListItem.builder()
        self.sousChef = SousChef()
        self.registerPath("reduction", self.fakeMethod)
        self.registerPath("save", self.save)
        self.registerPath("load", self.load)
        self.registerPath("initializeState", self.initializeState)
        self.registerPath("calculatePixelGroupingParameters", self.fakeMethod)
        self.registerPath("hasState", self.hasState)
        self.registerPath("checkDataExists", self.fakeMethod)
        self.registerPath("assessment", self.assessQuality)
        self.registerPath("quality", self.readQuality)
        self.registerPath("retrievePixelGroupingParams", self.fakeMethod)
        self.registerPath("diffraction", self.diffractionCalibration)
        return

    @staticmethod
    def name():
        return "calibration"

    @FromString
    def fakeMethod(self):
        # NOTE this is not a real method
        # it's here to be used in the registered paths above, for the moment
        # when possible this and its registered paths should be deleted
        return {}

    @FromString
    def diffractionCalibration(self, request: DiffractionCalibrationRequest):
        # ingredients
        cifPath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            # fiddly-bits
            peakIntensityThreshold=request.peakIntensityThreshold,
            convergenceThreshold=request.convergenceThreshold,
            nBinsAcrossPeakWidth=request.nBinsAcrossPeakWidth,
        )
        ingredients = self.sousChef.prepDiffractionCalibrationIngredients(farmFresh)

        # groceries
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("groupingWorkspace").grouping(request.focusGroup.name).useLiteMode(
            request.useLiteMode
        ).fromPrev().add()
        groceries = self.groceryService.fetchGroceryDict(self.groceryClerk.buildDict())

        # now have all ingredients and groceries, run recipe
        return DiffractionCalibrationRecipe().executeRecipe(ingredients, groceries)

    @FromString
    def save(self, request: CalibrationExportRequest):
        entry = request.calibrationIndexEntry
        calibrationRecord = request.calibrationRecord
        calibrationRecord = self.dataExportService.exportCalibrationRecord(calibrationRecord)
        entry.version = calibrationRecord.version
        self.saveCalibrationToIndex(entry)

    @FromString
    def load(self, run: RunConfig):
        runId = run.runNumber
        return self.dataFactoryService.getCalibrationRecord(runId)

    @FromString
    def saveCalibrationToIndex(self, entry: CalibrationIndexEntry):
        if entry.appliesTo is None:
            entry.appliesTo = ">" + entry.runNumber
        if entry.timestamp is None:
            entry.timestamp = int(round(time.time() * self.MILLISECONDS_PER_SECOND))
        logger.info("Saving calibration index entry for Run Number {}".format(entry.runNumber))
        self.dataExportService.exportCalibrationIndexEntry(entry)

    @FromString
    def initializeState(self, request: InitializeStateRequest):
        return self.dataExportService.initializeState(request.runId, request.humanReadableName)

    @FromString
    def getState(self, runs: List[RunConfig]):
        states = []
        for run in runs:
            state = self.dataFactoryService.getStateConfig(run.runNumber)
            states.append(state)
        return states

    def hasState(self, runId: str):
        calibrationFile = self.dataFactoryService.checkCalibrationStateExists(runId)
        if calibrationFile:
            return True
        else:
            return False

    def _getInstrumentDefinitionFilename(self, useLiteMode: bool):
        if useLiteMode is True:
            return Config["instrument.lite.definition.file"]
        elif useLiteMode is False:
            return Config["instrument.native.definition.file"]

    # TODO make the inputs here actually work
    def _collectMetrics(self, focussedData, focusGroup, pixelGroup):
        metric = parse_raw_as(
            List[CalibrationMetric],
            CalibrationMetricExtractionRecipe().executeRecipe(
                InputWorkspace=focussedData,
                PixelGroup=pixelGroup.json(),
            ),
        )
        return FocusGroupMetric(focusGroup=focusGroup, calibrationMetric=metric)

    @FromString
    def readQuality(self, runId: str, version: str):
        calibrationRecord = self.dataFactoryService.getCalibrationRecord(runId, version)
        if calibrationRecord is None:
            raise ValueError(f"No calibration record found for run {runId}, version {version}.")
        GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(calibrationRecord=calibrationRecord)
        )
        for ws_name in calibrationRecord.workspaceNames:
            self.dataFactoryService.loadCalibrationDataWorkspace(
                calibrationRecord.runNumber, calibrationRecord.version, ws_name
            )

    @FromString
    def assessQuality(self, request: CalibrationAssessmentRequest):
        # NOTE the previous structure of this method implied it was meant to loop over a list.
        # However, its actual implementation did not actually loop over a list.
        # I removed most of the parts that implied a loop or list.
        # This can be easily refactored to a loop structure when needed
        cifPath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        farmFresh = FarmFreshIngredients(
            runNumber=request.run.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
        )
        peakIngredients = self.sousChef.prepPeakIngredients(farmFresh)
        # TODO in future, the ingredients for FitMultiplePeaks will be PeakIngredients
        fitIngredients = FitMultiplePeaksIngredients(
            instrumentState=peakIngredients.instrumentState,
            crystalInfo=peakIngredients.crystalInfo,
            inputWorkspace=request.workspace,
        )

        # TODO: We Need to Fit the Data
        fitResults = FitMultiplePeaksRecipe().executeRecipe(FitMultiplePeaksIngredients=fitIngredients)
        metrics = self._collectMetrics(fitResults, request.focusGroup, peakIngredients.pixelGroup)

        record = CalibrationRecord(
            runNumber=request.run.runNumber,
            crystalInfo=peakIngredients.crystalInfo,
            calibrationFittingIngredients=self.sousChef.prepCalibration(request.run.runNumber),
            pixelGroups=[peakIngredients.pixelGroup],
            focusGroupCalibrationMetrics=metrics,
            workspaceNames=[request.workspace],
        )

        timestamp = int(round(time.time() * self.MILLISECONDS_PER_SECOND))
        GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(calibrationRecord=record, timestamp=timestamp)
        )

        return record
