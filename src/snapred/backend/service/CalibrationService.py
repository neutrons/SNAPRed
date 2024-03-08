import json
import time
from typing import Dict, List

from pydantic import parse_file_as, parse_raw_as

from snapred.backend.dao import RunConfig
from snapred.backend.dao.calibration import (
    CalibrationIndexEntry,
    CalibrationMetric,
    CalibrationRecord,
    FocusGroupMetric,
)
from snapred.backend.dao.ingredients import (
    CalibrationMetricsWorkspaceIngredients,
    DiffractionCalibrationIngredients,
    GroceryListItem,
)
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    CalibrationIndexRequest,
    CalibrationLoadAssessmentRequest,
    DiffractionCalibrationRequest,
    FarmFreshIngredients,
    FocusSpectraRequest,
    InitializeStateRequest,
)
from snapred.backend.dao.response.CalibrationAssessmentResponse import CalibrationAssessmentResponse
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe
from snapred.backend.recipe.GenerateCalibrationMetricsWorkspaceRecipe import GenerateCalibrationMetricsWorkspaceRecipe
from snapred.backend.recipe.GenericRecipe import (
    CalibrationMetricExtractionRecipe,
    FitMultiplePeaksRecipe,
    FocusSpectraRecipe,
    GenerateTableWorkspaceFromListOfDictRecipe,
)
from snapred.backend.recipe.GroupWorkspaceIterator import GroupWorkspaceIterator
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
    MINIMUM_PEAKS_PER_GROUP = Config["calibration.diffraction.minimumPeaksPerGroup"]

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.groceryClerk = GroceryListItem.builder()
        self.sousChef = SousChef()
        self.registerPath("reduction", self.fakeMethod)
        self.registerPath("ingredients", self.prepDiffractionCalibrationIngredients)
        self.registerPath("groceries", self.fetchDiffractionCalibrationGroceries)
        self.registerPath("focus", self.focusSpectra)
        self.registerPath("save", self.save)
        self.registerPath("load", self.load)
        self.registerPath("initializeState", self.initializeState)
        self.registerPath("calculatePixelGroupingParameters", self.fakeMethod)
        self.registerPath("hasState", self.hasState)
        self.registerPath("checkDataExists", self.fakeMethod)
        self.registerPath("assessment", self.assessQuality)
        self.registerPath("loadQualityAssessment", self.loadQualityAssessment)
        self.registerPath("index", self.getCalibrationIndex)
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
        raise NotImplementedError("You tried to access an invalid path in the calibration service.")

    @FromString
    def prepDiffractionCalibrationIngredients(
        self, request: DiffractionCalibrationRequest
    ) -> DiffractionCalibrationIngredients:
        # fetch the ingredients needed to focus and plot the peaks
        cifPath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            # fiddly-bits
            peakFunction=request.peakFunction,
            crystalDBounds={"minimum": request.crystalDMin, "maximum": request.crystalDMax},
            peakIntensityThreshold=request.peakIntensityThreshold,
            convergenceThreshold=request.convergenceThreshold,
            nBinsAcrossPeakWidth=request.nBinsAcrossPeakWidth,
        )
        return self.sousChef.prepDiffractionCalibrationIngredients(farmFresh)

    @FromString
    def fetchDiffractionCalibrationGroceries(self, request: DiffractionCalibrationRequest) -> Dict[str, str]:
        # groceries
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("groupingWorkspace").fromRun(request.runNumber).grouping(
            request.focusGroup.name
        ).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.specialOrder().name("outputWorkspace").diffcal_output(request.runNumber).useLiteMode(
            request.useLiteMode
        ).add()
        self.groceryClerk.specialOrder().name("calibrationTable").diffcal_table(request.runNumber).useLiteMode(
            request.useLiteMode
        ).add()
        self.groceryClerk.specialOrder().name("maskWorkspace").diffcal_mask(request.runNumber).useLiteMode(
            request.useLiteMode
        ).add()

        return self.groceryService.fetchGroceryDict(self.groceryClerk.buildDict())

    @FromString
    def diffractionCalibration(self, request: DiffractionCalibrationRequest):
        # ingredients
        ingredients = self.prepDiffractionCalibrationIngredients(request)
        # groceries
        groceries = self.fetchDiffractionCalibrationGroceries(request)

        # now have all ingredients and groceries, run recipe
        return DiffractionCalibrationRecipe().executeRecipe(ingredients, groceries)

    @FromString
    def focusSpectra(self, request: FocusSpectraRequest):
        # prep the ingredients -- a pixel group
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
        )
        ingredients = self.sousChef.prepPixelGroup(farmFresh)
        # fetch the grouping workspace
        self.groceryClerk.grouping(request.focusGroup.name).fromRun(request.runNumber).useLiteMode(request.useLiteMode)
        groupingWorkspace = self.groceryService.fetchGroupingDefinition(self.groceryClerk.build())["workspace"]
        # now focus
        focusedWorkspace = (
            wng.run().runNumber(request.runNumber).group(request.focusGroup.name).auxiliary("F-dc").build()
        )
        if not self.groceryService.workspaceDoesExist(focusedWorkspace):
            FocusSpectraRecipe().executeRecipe(
                InputWorkspace=request.inputWorkspace,
                GroupingWorkspace=groupingWorkspace,
                Ingredients=ingredients,
                OutputWorkspace=focusedWorkspace,
            )
        return focusedWorkspace, groupingWorkspace

    @FromString
    def save(self, request: CalibrationExportRequest):
        entry = request.calibrationIndexEntry
        calibrationRecord = request.calibrationRecord
        calibrationRecord = self.dataExportService.exportCalibrationRecord(calibrationRecord)
        calibrationRecord = self.dataExportService.exportCalibrationWorkspaces(calibrationRecord)
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
        return FocusGroupMetric(focusGroupName=focusGroup.name, calibrationMetric=metric)

    @FromString
    def getCalibrationIndex(self, request: CalibrationIndexRequest):
        run = request.run
        calibrationIndex = self.dataFactoryService.getCalibrationIndex(run.runNumber)
        return calibrationIndex

    @FromString
    def loadQualityAssessment(self, request: CalibrationLoadAssessmentRequest):
        runId = request.runId
        version = request.version

        calibrationRecord = self.dataFactoryService.getCalibrationRecord(runId, version)
        if calibrationRecord is None:
            errorTxt = f"No calibration record found for run {runId}, version {version}."
            logger.error(errorTxt)
            raise ValueError(errorTxt)

        # check if any of the workspaces already exist
        if request.checkExistent:
            wkspaceExists = False
            for metricName in ["sigma", "strain"]:
                ws_name = (
                    wng.diffCalMetrics()
                    .runNumber(request.runId)
                    .version(request.version)
                    .metricName(metricName)
                    .build()
                )
                if self.dataFactoryService.workspaceDoesExist(ws_name):
                    wkspaceExists = True
                    break
            if not wkspaceExists:
                for ws_name in calibrationRecord.workspaceNames:
                    if self.dataFactoryService.workspaceDoesExist(ws_name):
                        wkspaceExists = True
                        break
            if wkspaceExists:
                errorTxt = (
                    f"Calibration assessment for Run {runId} Version {version} "
                    f"is already loaded: see workspace {ws_name}."
                )
                logger.error(errorTxt)
                raise ValueError(errorTxt)

        # generate metrics workspaces
        GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(calibrationRecord=calibrationRecord)
        )

        # load persistent workspaces
        for ws_name in calibrationRecord.workspaceNames:
            self.dataFactoryService.getCalibrationDataWorkspace(
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
        ingredients = self.sousChef.prepPeakIngredients(farmFresh)

        # TODO: We Need to Fit the Data
        fitResults = FitMultiplePeaksRecipe().executeRecipe(
            InputWorkspace=request.workspace, DetectorPeakIngredients=ingredients
        )
        metrics = self._collectMetrics(fitResults, request.focusGroup, ingredients.pixelGroup)

        record = CalibrationRecord(
            runNumber=request.run.runNumber,
            crystalInfo=ingredients.crystalInfo,
            calibrationFittingIngredients=self.sousChef.prepCalibration(request.run.runNumber),
            pixelGroups=[ingredients.pixelGroup],
            focusGroupCalibrationMetrics=metrics,
            workspaceNames=[request.workspace],
        )

        timestamp = int(round(time.time() * self.MILLISECONDS_PER_SECOND))
        metricWorkspaces = GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(calibrationRecord=record, timestamp=timestamp)
        )

        return CalibrationAssessmentResponse(record=record, metricWorkspaces=metricWorkspaces)
