import json
import os
import time
from typing import List

from pydantic import parse_raw_as

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.ingredients.FitCalibrationWorkspaceIngredients import FitCalibrationWorkspaceIngredients
from snapred.backend.dao.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.request.CalibrationAssessmentRequest import CalibrationAssessmentRequest
from snapred.backend.dao.request.CalibrationExportRequest import CalibrationExportRequest
from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroupParameters import FocusGroupParameters
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.FitCalibrationWorkspaceRecipe import FitCalibrationWorkspaceRecipe
from snapred.backend.recipe.GenericRecipe import (
    CalibrationMetricExtractionRecipe,
    CalibrationReductionRecipe,
    FitMultiplePeaksRecipe,
    PurgeOverlappingPeaksRecipe,
    SmoothDataExcludingPeaksRecipe,
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
                data = self._calculatePixelGroupingParameters(calibrationState, groupingFile)
                calibrationState.instrumentState.pixelGroupingInstrumentParameters = data["parameters"]
                if export is True:
                    self.dataExportService.exportCalibrationState(runId=run.runNumber, calibration=calibrationState)
            except:
                raise
        return data

    @FromString
    def hasState(self, runId: str):
        calibrationFile = self.dataFactoryService.checkCalibrationStateExists(runId)
        if calibrationFile:
            record = self.dataFactoryService.getCalibrationRecord(runId)
            if record is None:
                return False
            else:
                return True
        else:
            return False

    @FromString
    def checkDataExists(self, run: RunConfig):
        if not run:
            raise ValueError("Runs list empty")
        else:
            return True

    def _calculatePixelGroupingParameters(self, calibrationState, groupingFile: str):
        groupingIngredients = PixelGroupingIngredients(
            calibrationState=calibrationState,
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
            raise Exception(f"No focussed data found for run {runId}, Please run Calibration Reduction on this Data.")
        else:
            focussedData = outputNameFormat.format(runId)
        return focussedData

    def _getPixelGroupingParams(self, calibration, focusGroups):
        pixelGroupingParams = []
        for focusGroup in focusGroups:
            pixelGroupingParams.append(
                self._calculatePixelGroupingParameters(calibration, focusGroup.definition)["parameters"]
            )
        return pixelGroupingParams

    def _fitAndCollectMetrics(self, instrumentState, focussedData, focusGroups, pixelGroupingParams, crystalInfo):
        groupedWorkspaceNames = [ws for ws in GroupWorkspaceIterator(focussedData)]
        metrics = []
        fittedWorkspaceNames = []
        for workspace, focusGroup, index in zip(groupedWorkspaceNames, focusGroups, range(len(focusGroups))):
            instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParams[index]
            ingredients = FitCalibrationWorkspaceIngredients(
                instrumentState=instrumentState,
                crystalInfo=crystalInfo,
                workspaceName=workspace,
                pixelGroupingParameters=pixelGroupingParams[index],
            )
            fitPeaksResult = FitCalibrationWorkspaceRecipe().executeRecipe(ingredients)

            pixelGroupingInput = list_to_raw(pixelGroupingParams[index])
            metric = parse_raw_as(
                List[CalibrationMetric],
                CalibrationMetricExtractionRecipe().executeRecipe(
                    InputWorkspace=fitPeaksResult, PixelGroupingParameter=pixelGroupingInput
                ),
            )
            fittedWorkspaceNames.append(fitPeaksResult)
            metrics.append(FocusGroupMetric(focusGroupName=focusGroup.name, calibrationMetric=metric))
        return fittedWorkspaceNames, metrics

    @FromString
    def assessQuality(self, request: CalibrationAssessmentRequest):
        run = request.run
        cifPath = request.cifPath
        reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
        calibration = self.dataFactoryService.getCalibrationState(run.runNumber)
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        instrumentState = calibration.instrumentState
        crystalInfoDict = CrystallographicInfoService().ingest(cifPath)
        crystalInfo = crystalInfoDict["crystalInfo"]
        focussedData = self._loadFocusedData(run.runNumber)
        pixelGroupingParams = self._getPixelGroupingParams(calibration, focusGroups)

        fittedWorkspaceNames, metrics = self._fitAndCollectMetrics(
            instrumentState, focussedData, focusGroups, pixelGroupingParams, crystalInfo
        )

        outputWorkspaces = []
        outputWorkspaces.extend(fittedWorkspaceNames)
        outputWorkspaces.append(focussedData)
        focusGroupParameters = self.collectFocusGroupParameters(focusGroups, pixelGroupingParams)
        record = CalibrationRecord(
            reductionIngredients=reductionIngredients,
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

        pixelGroupingParams = self._getPixelGroupingParams(calibration, focusGroups)

        return pixelGroupingParams
