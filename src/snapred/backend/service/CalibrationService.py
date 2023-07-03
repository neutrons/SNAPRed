import time
from typing import List

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.request.CalibrationAssessmentRequest import CalibrationAssessmentRequest
from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.GenericRecipe import (
    CalibrationReductionRecipe,
    CustomStripPeaksRecipe,
    FitMultiplePeaksRecipe,
    PurgeOverlappingPeaksRecipe,
)
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.Service import Service
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class CalibrationService(Service):
    _name = "calibration"
    dataFactoryService = DataFactoryService()
    dataExportService = DataExportService()

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.registerPath("reduction", self.reduction)
        self.registerPath("save", self.save)
        self.registerPath("initializeState", self.initializeState)
        self.registerPath("calculatePixelGroupingParameters", self.calculatePixelGroupingParameters)
        self.registerPath("assessment", self.assessQuality)
        return

    def name(self):
        return self._name

    @FromString
    def reduction(self, runs: List[RunConfig]):
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        for run in runs:
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
            try:
                CalibrationReductionRecipe().executeRecipe(reductionIngredients)
            except:
                raise
        return {}

    @FromString
    def save(self, entry: CalibrationIndexEntry):
        reductionIngredients = self.dataFactoryService.getReductionIngredients(entry.runNumber)
        # TODO: get peak fitting filepath
        # save calibration reduction result to disk
        calibrationWorkspaceName = Config["calibration.reduction.output.format"].format(entry.runNumber)
        self.dataExportService.exportCalibrationReductionResult(entry.runNumber, calibrationWorkspaceName)
        calibrationRecord = CalibrationRecord(parameters=reductionIngredients)
        calibrationRecord = self.dataExportService.exportCalibrationRecord(calibrationRecord)
        entry.version = calibrationRecord.version
        self.saveCalibrationToIndex(entry)

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
    def calculatePixelGroupingParameters(self, runs: List[RunConfig], groupingFile: str):
        for run in runs:
            calibrationState = self.dataFactoryService.getCalibrationState(run.runNumber)
            groupingIngredients = PixelGroupingIngredients(
                calibrationState=calibrationState,
                instrumentDefinitionFile="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
                groupingFile=groupingFile,
            )
            try:
                data = PixelGroupingParametersCalculationRecipe().executeRecipe(groupingIngredients)
                calibrationState.instrumentState.pixelGroupingInstrumentParameters = data["parameters"]
                self.dataExportService.exportCalibrationState(runId=run.runNumber, calibration=calibrationState)
            except:
                raise

    @FromString
    def assessQuality(self, request: CalibrationAssessmentRequest):
        run = request.run
        cifPath = request.cifPath
        outputNameFormat = Config["calibration.reduction.output.format"]
        reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
        calibration = self.dataFactoryService.getCalibrationState(run.runNumber)
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        instrumentState = self.dataFactoryService.getCalibrationState(run.runNumber).instrumentState
        crystalInfo = CrystallographicInfoService().ingest(cifPath)

        # check if there is focussed data for this run
        focussedData = self.dataFactoryService.getWorkspaceForName(outputNameFormat.format(run.runNumber))
        if focussedData is None:
            raise Exception(
                "No focussed data found for run {}, Please run Calibration Reduction on this Data.".format(
                    run.runNumber
                )
            )
        else:
            focussedData = outputNameFormat.format(run.runNumber)  # change back to workspace name, its easier this way

        purgePeakMap = PurgeOverlappingPeaksRecipe().executeRecipe(
            calibration.instrumentState, crystalInfo, len(focusGroups)
        )
        strippedFocussedData = CustomStripPeaksRecipe().executeRecipe(
            InputGroupWorkspace=focussedData,
            PeakPositions=purgePeakMap,
            FocusGroups=focusGroups,
            OutputWorkspace="strippedFocussedData",
        )
        FitMultiplePeaksRecipe().executeRecipe(reductionIngredients, instrumentState, crystalInfo, strippedFocussedData)
        # TODO: loaasd previous focussed data for comparison if it exists
        # TODO: generate graphs comparing results to default or previous calibration
        # the following should go in a gather metrics recipe/algorithm
        # TODO: save outputWorkspaceGroup to disk
        # TODO: save peakMetrics to disk
