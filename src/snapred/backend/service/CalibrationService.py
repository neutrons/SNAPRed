import json
import time
from datetime import date
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
    GroceryListItem,
    NormalizationCalibrationIngredients,
    PeakIngredients,
    PixelGroupingIngredients,
)
from snapred.backend.dao.normalization import (
    Normalization,
    NormalizationIndexEntry,
    NormalizationRecord,
)
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    DiffractionCalibrationRequest,
    InitializeStateRequest,
    NormalizationCalibrationRequest,
    NormalizationExportRequest,
)
from snapred.backend.dao.state import FocusGroup, InstrumentState, PixelGroup
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
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
        self.registerPath("reduction", self.reduction)
        self.registerPath("save", self.save)
        self.registerPath("load", self.load)
        self.registerPath("initializeState", self.initializeState)
        self.registerPath("calculatePixelGroupingParameters", self.calculatePixelGroupingParameters)
        self.registerPath("hasState", self.hasState)
        self.registerPath("checkDataExists", self.calculatePixelGroupingParameters)
        self.registerPath("assessment", self.assessQuality)
        self.registerPath("quality", self.readQuality)
        self.registerPath("retrievePixelGroupingParams", self.retrievePixelGroupingParams)
        self.registerPath("diffraction", self.diffractionCalibration)
        return

    @staticmethod
    def name():
        return "calibration"

    @FromString
    def reduction(self, runs: List[RunConfig]):  #  noqa: ARG002
        # TODO this is apparently dead code -- remove
        return {}

    # TODO when pixelGroup fully removed from instrumentState
    # then remove its calculation here
    # also remove useLiteMode and nBinsAcrossPeakWidth as inputs
    # further remove these inputs when used in below methods:
    # - diffractionCalibration
    # - assessQuality
    # - normalization
    # Must also remove L385 from associated unit test
    def _generateFocusGroupAndInstrumentState(
        self,
        runNumber,
        definition: str,
        calibration=None,
    ) -> Tuple[FocusGroup, InstrumentState]:
        if calibration is None:
            calibration = self.dataFactoryService.getCalibrationState(runNumber)
        instrumentState = calibration.instrumentState
        focusGroup = FocusGroup(
            name=definition.split("/")[-1],
            definition=definition,
        )
        return (focusGroup, instrumentState)

    @FromString
    def diffractionCalibration(self, request: DiffractionCalibrationRequest):
        # shopping list
        # 1. full runconfig
        runConfig = self.dataFactoryService.getRunConfig(request.runNumber)
        runConfig.useLiteMode = request.useLiteMode
        # 2. instrument state
        # 3. focus group
        # get the pixel grouping parameters and load them into the focus group
        # TODO: This may be pending a refactor and a closer look,
        # based on my convos it should be a correct translation
        focusGroup, instrumentState = self._generateFocusGroupAndInstrumentState(
            request.runNumber,
            request.focusGroupPath,
        )
        data = self._calculatePixelGroupingParameters(
            instrumentState,
            focusGroup.definition,
            request.useLiteMode,
            request.nBinsAcrossPeakWidth,
        )
        pixelGroup = PixelGroup(
            focusGroup=focusGroup,
            pixelGroupingParameters=data["parameters"],
            timeOfFlight=data["tof"],
            nBinsAcrossPeakWidth=request.nBinsAcrossPeakWidth,
        )
        # 4. grouped peak list
        # need to calculate these using DetectorPeakPredictor
        # 4a. InstrumentState
        # 4b. CrystalInfo
        cifFilePath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        crystalInfo = CrystallographicInfoService().ingest(cifFilePath)["crystalInfo"]
        # 4c. PeakIntensityThreshold
        detectorPeakIngredients = PeakIngredients(
            instrumentState=instrumentState,
            crystalInfo=crystalInfo,
            pixelGroup=pixelGroup,
            peakIntensityThreshold=request.peakIntensityThreshold,
        )
        detectorPeaks = DetectorPeakPredictorRecipe().executeRecipe(
            Ingredients=detectorPeakIngredients,
        )
        detectorPeaks = parse_raw_as(List[GroupPeakList], detectorPeaks)
        # 5. cal path
        # this is just the state folder/calibration folder used solely for saving the calibration
        # set it to tmp because we dont know if we want to keep it yet
        # TODO: The algo really shouldnt be saving data unless it has to
        # TODO: this cal path needs to be exposed in DataFactoryService or DataExportService
        # 6. convergence threshold
        convergenceThreshold = request.convergenceThreshold
        ingredients = DiffractionCalibrationIngredients(
            runConfig=runConfig,
            groupedPeakLists=detectorPeaks,
            convergenceThreshold=convergenceThreshold,
            pixelGroup=pixelGroup,
            maxOffset=request.maximumOffset,
        )
        focusFile = request.focusGroupPath.split("/")[-1]
        focusName = focusFile.split(".")[0]
        focusScheme = focusName.split("_")[-1]

        # 7. the neutron data and a grouping workspace
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("groupingWorkspace").grouping(focusScheme).useLiteMode(
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

    @FromString
    def calculatePixelGroupingParameters(
        self,
        runs: List[RunConfig],
        groupingFile: str,
        useLiteMode: bool,
        export: bool = True,
        nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"],
    ):
        for run in runs:
            calibrationState = self.dataFactoryService.getCalibrationState(run.runNumber)
            try:
                data = self._calculatePixelGroupingParameters(
                    calibrationState.instrumentState,
                    groupingFile,
                    useLiteMode,
                    nBinsAcrossPeakWidth,
                )
                if export is True:
                    self.dataExportService.exportCalibrationState(runId=run.runNumber, calibration=calibrationState)
            except:
                raise
        return data

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

    def _calculatePixelGroupingParameters(
        self,
        instrumentState: InstrumentState,
        groupingFile: str,
        useLiteMode: bool,
        nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"],
    ):
        groupingIngredients = PixelGroupingIngredients(
            instrumentState=instrumentState,
            nBinsAcrossPeakWidth=nBinsAcrossPeakWidth,
        )

        # TODO replace this with grouping scheme passed instead as the parameter
        #  Doing so requires updating the UI to display focus group names instead of files
        groupingScheme = groupingFile.split("/")[-1].split(".")[0].split("_")[-1]
        getGrouping = (
            self.groceryClerk.grouping(groupingScheme)
            .useLiteMode(useLiteMode)
            .source(InstrumentFilename=self._getInstrumentDefinitionFilename(useLiteMode))
            .buildList()
        )
        groupingWorkspace = self.groceryService.fetchGroceryList(getGrouping)[0]

        try:
            data = PixelGroupingParametersCalculationRecipe().executeRecipe(groupingIngredients, groupingWorkspace)
        except:
            raise
        return data

    def collectPixelGroups(self, focusGroups, pixelGroupingParams, tofParams, nBinsAcrossPeakWidth) -> List[PixelGroup]:
        pixelGroups = []
        for focusGroup, pixelGroupingParam, tof in zip(focusGroups, pixelGroupingParams, tofParams):
            pixelGroups.append(
                PixelGroup(
                    focusGroupName=focusGroup,
                    pixelGroupingParameters=pixelGroupingParam,
                    timeOfFlight=tof,
                    nBinsAcrossPeakWidth=nBinsAcrossPeakWidth,
                )
            )
        return pixelGroups

    def _getPixelGroupingParams(
        self,
        instrumentState: InstrumentState,
        focusGroups: List[FocusGroup],
        useLiteMode: bool,
        nBinsAcrossPeakWidth: int,
    ):
        pixelGroupingParams = []
        for focusGroup in focusGroups:
            data = self._calculatePixelGroupingParameters(
                instrumentState,
                focusGroup.definition,
                useLiteMode,
                nBinsAcrossPeakWidth,
            )
            pixelGroupingParams.append(data["parameters"])
        return pixelGroupingParams

    def _collectMetrics(self, focussedData, focusGroup, pixelGroupingParam):
        metric = parse_raw_as(
            List[CalibrationMetric],
            CalibrationMetricExtractionRecipe().executeRecipe(
                InputWorkspace=focussedData,
                PixelGroupingParameter=json.dumps([pgp.dict() for pgp in pixelGroupingParam]),
            ),
        )
        return FocusGroupMetric(focusGroupName=focusGroup.name, calibrationMetric=metric)

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
        # TODO this needs work
        # it seems like this is expeced to run in a loop over grouping schemas,
        # but instead is only running on a single grouping schema with one pixelGroup
        focussedData = request.workspace
        calibration = self.dataFactoryService.getCalibrationState(request.run.runNumber)
        focusGroup, instrumentState = self._generateFocusGroupAndInstrumentState(
            request.run.runNumber,
            request.focusGroupPath,
            request.useLiteMode,  # TODO delete
            request.nBinsAcrossPeakWidth,  # TODO delete
            calibration,
        )
        data = self._calculatePixelGroupingParameters(
            instrumentState,
            focusGroup.definition,
            request.useLiteMode,
            request.nBinsAcrossPeakWidth,
        )
        pixelGroupingParam = data["parameters"]
        timeOfFlightParam = data["tof"]
        pixelGroup = PixelGroup(
            instrumentState=instrumentState,
            focusGroup=focusGroup,
            pixelGroupingParameters=pixelGroupingParam,
            timeOfFlight=timeOfFlightParam,
            nBinsAcrossPeakWidth=request.nBinsAcrossPeakWidth,
        )
        cifFilePath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        crystalInfo = CrystallographicInfoService().ingest(cifFilePath)["crystalInfo"]
        # TODO: We Need to Fit the Data
        fitIngredients = PeakIngredients(
            instrumentState=instrumentState,
            crystalInfo=crystalInfo,
            pixelGroup=pixelGroup,
            peakIntensityThreshold=request.peakIntensityThreshold,
        )
        fitResults = FitMultiplePeaksRecipe().executeRecipe(
            InputWorkspace=focussedData,
            DetectorPeakIngredients=fitIngredients,
            PeakType=request.peakType,
        )
        metrics = self._collectMetrics(fitResults, focusGroup, pixelGroupingParam)

        record = CalibrationRecord(
            runNumber=request.run.runNumber,
            crystalInfo=crystalInfo,
            calibrationFittingIngredients=calibration,
            pixelGroups=[pixelGroup],
            focusGroupCalibrationMetrics=metrics,
            workspaceNames=[focussedData],
        )

        timestamp = int(round(time.time() * self.MILLISECONDS_PER_SECOND))
        GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(calibrationRecord=record, timestamp=timestamp)
        )

        return record

    @FromString
    def retrievePixelGroupingParams(self, runID: str, useLiteMode: bool = True):
        calibration = self.dataFactoryService.getCalibrationState(runID)
        focusGroups = self.dataFactoryService.getFocusGroups(runID)  # TODO: fix to work with new group map work
        pixelGroupingParams = self._getPixelGroupingParams(calibration.instrumentState, focusGroups, useLiteMode)
        return pixelGroupingParams
