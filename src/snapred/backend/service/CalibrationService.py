import json
import time
from typing import List

from pydantic import parse_raw_as

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
from snapred.backend.dao.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.request.CalibrationAssessmentRequest import CalibrationAssessmentRequest
from snapred.backend.dao.request.CalibrationExportRequest import CalibrationExportRequest
from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroupParameters import FocusGroupParameters
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.GenericRecipe import (
    CalibrationMetricExtractionRecipe,
    CalibrationReductionRecipe,
    CustomStripPeaksRecipe,
    FitMultiplePeaksRecipe,
    PurgeOverlappingPeaksRecipe,
)
from snapred.backend.recipe.GroupWorkspaceIterator import GroupWorkspaceIterator
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
        self.registerPath("load", self.load)
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

    def _calculatePixelGroupingParameters(self, calibrationState, groupingFile: str):
        groupingIngredients = PixelGroupingIngredients(
            calibrationState=calibrationState,
            instrumentDefinitionFile="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
            groupingFile=groupingFile,
        )
        try:
            data = PixelGroupingParametersCalculationRecipe().executeRecipe(groupingIngredients)
        except:
            raise
        return data

    @FromString
    def assessQuality(self, request: CalibrationAssessmentRequest):
        run = request.run
        cifPath = request.cifPath
        outputNameFormat = Config["calibration.reduction.output.format"]
        reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
        calibration = self.dataFactoryService.getCalibrationState(run.runNumber)
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        instrumentState = calibration.instrumentState
        crystalInfoDict = CrystallographicInfoService().ingest(cifPath)
        crystalInfo = crystalInfoDict["crystalInfo"]
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

        # for each focus group, get pixelgroupingparameters by passing focusgroup.definition as groupingfile
        pixelGroupingParams = []
        for focusGroup in focusGroups:
            pixelGroupingParams.append(
                self._calculatePixelGroupingParameters(calibration, focusGroup.definition)["parameters"]
            )

        focusGroupParameters = []
        for focusGroup, pixelGroupingParam in zip(focusGroups, pixelGroupingParams):
            focusGroupParameters.append(
                FocusGroupParameters(
                    focusGroupName=focusGroup.name,
                    pixelGroupingParameters=pixelGroupingParam,
                )
            )

        # TODO: should this be a list of lists? it only uses one focus group
        instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParams[0]

        purgePeakMap = PurgeOverlappingPeaksRecipe().executeRecipe(
            InstrumentState=instrumentState, CrystalInfo=crystalInfo, NumFocusGroups=str(len(focusGroups))
        )

        strippedFocussedData = CustomStripPeaksRecipe().executeRecipe(
            InputGroupWorkspace=focussedData,
            PeakPositions=purgePeakMap,
            FocusGroups=json.dumps([focusGroup.dict() for focusGroup in focusGroups]),
            OutputWorkspace="strippedFocussedData",
        )
        strippedFocussedDatas = [ws for ws in GroupWorkspaceIterator(strippedFocussedData)]
        metrics = []
        fittedWorkspaceNames = []
        for workspace, focusGroup in zip(strippedFocussedDatas, focusGroups):
            fitMultiplePeaksIngredients = FitMultiplePeaksIngredients(
                InstrumentState=instrumentState, CrystalInfo=crystalInfo, InputWorkspace=workspace
            )
            fitPeaksResult = FitMultiplePeaksRecipe().executeRecipe(
                FitMultiplePeaksIngredients=fitMultiplePeaksIngredients,
                OutputWorkspaceGroup="fitted_{}".format(workspace),
            )
            fittedWorkspaceNames.append(fitPeaksResult)
            metric = parse_raw_as(
                List[CalibrationMetric],
                CalibrationMetricExtractionRecipe().executeRecipe(InputWorkspace=fitPeaksResult),
            )
            metrics.append(FocusGroupMetric(focusGroupName=focusGroup.name, calibrationMetric=metric))
        self.dataExportService.deleteWorkspace(strippedFocussedData)

        # TODO: Seperate Request to load previous calibration record stuffs
        # previousCalibrationRecord = self.dataFactoryService.getCalibrationRecord(run.runNumber)
        # if previousCalibrationRecord:

        # Saving should perhaps be a follow up backend request
        outputWorkspaces = []
        outputWorkspaces.extend(fittedWorkspaceNames)
        outputWorkspaces.append(outputNameFormat.format(run.runNumber))
        record = CalibrationRecord(
            reductionIngredients=reductionIngredients,
            calibrationFittingIngredients=calibration,
            focusGroupParameters=focusGroupParameters,
            focusGroupCalibrationMetrics=metrics,
            workspaceNames=outputWorkspaces,
        )

        return record
