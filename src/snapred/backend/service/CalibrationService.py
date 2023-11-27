import json
import os
import time
from typing import List

from pydantic import parse_raw_as

from snapred.backend.dao import GroupPeakList, RunConfig
from snapred.backend.dao.calibration import (
    CalibrationIndexEntry,
    CalibrationMetric,
    CalibrationRecord,
    FocusGroupMetric,
)
from snapred.backend.dao.ingredients import (
    DiffractionCalibrationIngredients,
    FitCalibrationWorkspaceIngredients,
    FitMultiplePeaksIngredients,
    GroceryListItem,
    PixelGroupingIngredients,
    SmoothDataExcludingPeaksIngredients,
)
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    DiffractionCalibrationRequest,
    InitializeStateRequest,
)
from snapred.backend.dao.state import FocusGroup, FocusGroupParameters
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe
from snapred.backend.recipe.GenericRecipe import (
    CalibrationMetricExtractionRecipe,
    CalibrationReductionRecipe,
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
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


@Singleton
class CalibrationService(Service):
    dataFactoryService: "DataFactoryService"
    dataExportService: "DataExportService"

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.registerPath("reduction", self.reduction)
        self.registerPath("save", self.save)
        self.registerPath("load", self.load)
        self.registerPath("initializeState", self.initializeState)
        self.registerPath("calculatePixelGroupingParameters", self.calculatePixelGroupingParameters)
        self.registerPath("hasState", self.hasState)
        self.registerPath("checkDataExists", self.calculatePixelGroupingParameters)
        self.registerPath("assessment", self.assessQuality)
        self.registerPath("retrievePixelGroupingParams", self.retrievePixelGroupingParams)
        self.registerPath("diffraction", self.diffractionCalibration)
        return

    @staticmethod
    def name():
        return "calibration"

    @FromString
    def reduction(self, runs: List[RunConfig]):
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of their state
        for run in runs:
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
            try:
                CalibrationReductionRecipe().executeRecipe(ReductionIngredients=reductionIngredients)
            except:
                raise
        return {}

    def _generateFocusGroupAndInstrumentState(
        self,
        runNumber,
        definition,
        nBinsAcrossPeakWidth=Config["calibration.diffraction.nBinsAcrossPeakWidth"],
        calibration=None,
    ):
        if calibration is None:
            calibration = self.dataFactoryService.getCalibrationState(runNumber)
        instrumentState = calibration.instrumentState
        pixelGroupingParams = self._calculatePixelGroupingParameters(instrumentState, definition)["parameters"]
        instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParams
        return (
            FocusGroup(
                FWHM=[pgp.twoTheta for pgp in pixelGroupingParams],  # TODO: Remove or extract out a level up
                name=definition.split("/")[-1],
                definition=definition,
                nHst=len(pixelGroupingParams),
                dMin=[pgp.dResolution.minimum for pgp in pixelGroupingParams],
                dMax=[pgp.dResolution.maximum for pgp in pixelGroupingParams],
                dBin=[pgp.dRelativeResolution / nBinsAcrossPeakWidth for pgp in pixelGroupingParams],
            ),
            instrumentState,
        )

    @FromString
    def diffractionCalibration(self, request: DiffractionCalibrationRequest):
        # shopping list
        # 1. full runconfig
        runConfig = self.dataFactoryService.getRunConfig(request.runNumber)
        runConfig.isLite = request.useLiteMode
        # 2. instrument state
        # 3. focus group
        # get the pixel grouping parameters and load them into the focus group
        nBinsAcrossPeakWidth = request.nBinsAcrossPeakWidth
        # TODO: This may be pending a refactor and a closer look,
        # based on my convos it should be a correct translation
        focusGroup, instrumentState = self._generateFocusGroupAndInstrumentState(
            request.runNumber,
            request.focusGroupPath,
            nBinsAcrossPeakWidth,
        )
        # 4. grouped peak list
        # need to calculate these using DetectorPeakPredictor
        # 4a. InstrumentState
        # 4b. CrystalInfo
        cifFilePath = request.cifPath
        crystalInfo = CrystallographicInfoService().ingest(cifFilePath)["crystalInfo"]
        # 4c. PeakIntensityThreshold
        peakIntensityThreshold = request.peakIntensityThreshold
        detectorPeaks = DetectorPeakPredictorRecipe().executeRecipe(
            InstrumentState=instrumentState,
            CrystalInfo=crystalInfo,
            PeakIntensityFractionThreshold=peakIntensityThreshold,
        )
        detectorPeaks = parse_raw_as(List[GroupPeakList], detectorPeaks)
        # 5. cal path
        # this is just the state folder/calibration folder used solely for saving the calibration
        # set it to tmp because we dont know if we want to keep it yet
        # TODO: The algo really shouldnt be saving data unless it has to
        # TODO: this cal path needs to be exposed in DataFactoryService or DataExportService
        calpath = "~/tmp/"
        # 6. convergence threshold
        convergenceThreshold = request.convergenceThreshold
        ingredients = DiffractionCalibrationIngredients(
            runConfig=runConfig,
            instrumentState=instrumentState,
            focusGroup=focusGroup,
            groupedPeakLists=detectorPeaks,
            calPath=calpath,
            convergenceThreshold=convergenceThreshold,
        )
        focusFile = request.focusGroupPath.split("/")[-1]
        focusName = focusFile.split(".")[0]
        focusScheme = focusName.split("_")[-1]

        # get the needed input data
        groceryList = [
            GroceryListItem(
                workspaceType="nexus",
                runConfig=runConfig,
                loader="LoadEventNexus",
            ),
            GroceryListItem(
                workspaceType="grouping",
                groupingScheme=focusScheme,
                isLite=runConfig.isLite,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource="prev",
            ),
        ]
        workspaceList = FetchGroceriesRecipe().executeRecipe(groceryList)["workspaces"]
        groceries = {
            "inputWorkspace": workspaceList[0],
            "groupingWorkspace": workspaceList[1],
        }
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
            entry.timestamp = int(round(time.time() * 1000))
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
    def calculatePixelGroupingParameters(self, runs: List[RunConfig], groupingFile: str, export: bool = True):
        for run in runs:
            calibrationState = self.dataFactoryService.getCalibrationState(run.runNumber)
            try:
                data = self._calculatePixelGroupingParameters(calibrationState.instrumentState, groupingFile)
                calibrationState.instrumentState.pixelGroupingInstrumentParameters = data["parameters"]
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

    def _calculatePixelGroupingParameters(self, instrumentState, groupingFile: str):
        groupingIngredients = PixelGroupingIngredients(
            instrumentState=instrumentState,
            instrumentDefinitionFile=Config["instrument.lite.definition.file"],
            groupingFile=groupingFile,
        )
        try:
            data = PixelGroupingParametersCalculationRecipe().executeRecipe(groupingIngredients)
        except:
            raise
        return data

    def collectFocusGroupParameters(self, focusGroups, pixelGroupingParams):
        focusGroupParameters = []
        for focusGroup, pixelGroupingParam in zip(focusGroups, pixelGroupingParams):
            focusGroupParameters.append(
                FocusGroupParameters(
                    focusGroupName=focusGroup.name,
                    pixelGroupingParameters=pixelGroupingParam,
                )
            )
        return focusGroupParameters

    def _loadFocusedData(self, runId):
        outputNameFormat = Config["calibration.reduction.output.format"]
        focussedData = self.dataFactoryService.getWorkspaceForName(outputNameFormat.format(runId))
        if focussedData is None:
            raise ValueError(f"No focussed data found for run {runId}, Please run Calibration Reduction on this Data.")
        else:
            focussedData = outputNameFormat.format(runId)
        return focussedData

    def _getPixelGroupingParams(self, instrumentState, focusGroups):
        pixelGroupingParams = []
        for focusGroup in focusGroups:
            pixelGroupingParams.append(
                self._calculatePixelGroupingParameters(instrumentState, focusGroup.definition)["parameters"]
            )
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
    def assessQuality(self, request: CalibrationAssessmentRequest):
        run = request.run

        focussedData = request.workspace
        calibration = self.dataFactoryService.getCalibrationState(run.runNumber)
        focusGroup, instrumentState = self._generateFocusGroupAndInstrumentState(
            run.runNumber, request.focusGroupPath, request.nBinsAcrossPeakWidth, calibration
        )
        pixelGroupingParam = self._calculatePixelGroupingParameters(instrumentState, focusGroup.definition)[
            "parameters"
        ]
        cifFilePath = self.dataFactoryService.getCifFilePath(request.cifPath.split("/")[-1].split(".")[0])
        crystalInfo = CrystallographicInfoService().ingest(cifFilePath)["crystalInfo"]
        # TODO: We Need to Fitt the Data
        fitIngredients = FitMultiplePeaksIngredients(
            instrumentState=instrumentState, crystalInfo=crystalInfo, inputWorkspace=focussedData
        )
        fitResults = FitMultiplePeaksRecipe().executeRecipe(FitMultiplePeaksIngredients=fitIngredients)
        metrics = self._collectMetrics(fitResults, focusGroup, pixelGroupingParam)
        prevCalibration = self.dataFactoryService.getCalibrationRecord(run.runNumber)
        timestamp = int(round(time.time() * 1000))
        GenerateTableWorkspaceFromListOfDictRecipe().executeRecipe(
            ListOfDict=list_to_raw(metrics.calibrationMetric),
            OutputWorkspace=f"{run.runNumber}_calibrationMetrics_ts{timestamp}",
        )
        if prevCalibration is not None:
            GenerateTableWorkspaceFromListOfDictRecipe().executeRecipe(
                ListOfDict=list_to_raw(prevCalibration.focusGroupCalibrationMetrics.calibrationMetric),
                OutputWorkspace=f"{run.runNumber}_calibrationMetrics_v{prevCalibration.version}",
            )
        outputWorkspaces = [focussedData]
        focusGroupParameters = self.collectFocusGroupParameters([focusGroup], [pixelGroupingParam])
        record = CalibrationRecord(
            runNumber=run.runNumber,
            crystalInfo=crystalInfo,
            calibrationFittingIngredients=calibration,
            focusGroupParameters=focusGroupParameters,
            focusGroupCalibrationMetrics=metrics,
            workspaceNames=outputWorkspaces,
        )

        return record

    @FromString
    def retrievePixelGroupingParams(self, runID: str):
        calibration = self.dataFactoryService.getCalibrationState(runID)
        focusGroups = self.dataFactoryService.getFocusGroups(runID)

        pixelGroupingParams = self._getPixelGroupingParams(calibration.instrumentState, focusGroups)

        return pixelGroupingParams
