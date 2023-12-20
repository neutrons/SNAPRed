import json
import time
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
    FitCalibrationWorkspaceIngredients,
    FitMultiplePeaksIngredients,
    GroceryListItem,
    NormalizationCalibrationIngredients,
    PixelGroupingIngredients,
    SmoothDataExcludingPeaksIngredients,
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
from snapred.backend.dao.state import FocusGroup, FocusGroupParameters, InstrumentState
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.CalibrationNormalizationRecipe import CalibrationNormalizationRecipe
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe
from snapred.backend.recipe.GenerateCalibrationMetricsWorkspaceRecipe import GenerateCalibrationMetricsWorkspaceRecipe
from snapred.backend.recipe.GenericRecipe import (
    CalibrationMetricExtractionRecipe,
    CalibrationReductionRecipe,
    DetectorPeakPredictorRecipe,
    FitMultiplePeaksRecipe,
    GenerateTableWorkspaceFromListOfDictRecipe,
    SmoothDataExcludingPeaksRecipe,
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

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = FetchGroceriesRecipe()  # TODO replace with service
        self.registerPath("reduction", self.reduction)
        self.registerPath("save", self.save)
        self.registerPath("load", self.load)
        self.registerPath("initializeState", self.initializeState)
        self.registerPath("calculatePixelGroupingParameters", self.calculatePixelGroupingParameters)
        self.registerPath("hasState", self.hasState)
        self.registerPath("checkDataExists", self.calculatePixelGroupingParameters)
        self.registerPath("assessment", self.assessQuality)
        self.registerPath("normalization", self.normalization)
        self.registerPath("normalizationAssessment", self.normalizationAssessment)
        self.registerPath("saveNormalization", self.saveNormalization)
        self.registerPath("quality", self.readQuality)
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
        definition: str,
        useLiteMode: bool,
        nBinsAcrossPeakWidth: int = 10,
        calibration=None,
    ) -> Tuple[FocusGroup, InstrumentState]:
        if calibration is None:
            calibration = self.dataFactoryService.getCalibrationState(runNumber)
        instrumentState = calibration.instrumentState
        data = self._calculatePixelGroupingParameters(
            instrumentState,
            definition,
            useLiteMode,
        )
        pixelGroupingParams = data["parameters"]
        focusGroup = FocusGroup(
            name=definition.split("/")[-1],
            definition=definition,
        )
        instrumentState.pixelGroup = PixelGroup(
            pixelGroupingParameters=pixelGroupingParams,
            numberBinsAcrossPeakWidth=nBinsAcrossPeakWidth,
            focusGroup=focusGroup,
        )
        return (focusGroup, instrumentState)

    @FromString
    def diffractionCalibration(self, request: DiffractionCalibrationRequest):
        # preload the data and copy it to cacheworkspace

        diffCalInputWsName = wng.diffCalInput().runNumber(request.runNumber).build()
        self.dataFactoryService.getWorkspaceCached(request.runNumber, diffCalInputWsName)
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
            request.useLiteMode,
            request.nBinsAcrossPeakWidth,
        )
        # 4. grouped peak list
        # need to calculate these using DetectorPeakPredictor
        # 4a. InstrumentState
        # 4b. CrystalInfo
        cifFilePath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
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
            pixelGroup=instrumentState.pixelGroup,
        )
        focusFile = request.focusGroupPath.split("/")[-1]
        focusName = focusFile.split(".")[0]
        focusScheme = focusName.split("_")[-1]

        # get the needed input data
        groceryList = [
            GroceryListItem(
                workspaceType="nexus",
                runNumber=request.runNumber,
                loader="LoadEventNexus",
                useLiteMode=request.useLiteMode,
            ),
            GroceryListItem(
                workspaceType="grouping",
                groupingScheme=focusScheme,
                useLiteMode=request.useLiteMode,
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
    def calculatePixelGroupingParameters(
        self, runs: List[RunConfig], groupingFile: str, useLiteMode: bool, export: bool = True
    ):
        for run in runs:
            calibrationState = self.dataFactoryService.getCalibrationState(run.runNumber)
            try:
                data = self._calculatePixelGroupingParameters(
                    calibrationState.instrumentState,
                    groupingFile,
                    useLiteMode,
                )
                calibrationState.instrumentState.pixelGroup = PixelGroup(pixelGroupingParameters=data["parameters"])
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

    def _calculatePixelGroupingParameters(self, instrumentState, groupingFile: str, useLiteMode: bool):
        groupingIngredients = PixelGroupingIngredients(
            instrumentState=instrumentState,
        )

        # TODO replace this with grouping scheme passed instead as the parameter
        #  Doing so requires updating the UI to display focus group names instead of files
        groupingScheme = groupingFile.split("/")[-1].split(".")[0].replace("SNAPFocGroup_", "")

        getGrouping = GroceryListItem(
            workspaceType="grouping",
            groupingScheme=groupingScheme,
            useLiteMode=useLiteMode,
            instrumentPropertySource="InstrumentFilename",
            instrumentSource=self._getInstrumentDefinitionFilename(useLiteMode),
        )
        groupingWorkspace = self.groceryService.fetchGroupingDefinition(getGrouping)["workspace"]

        try:
            data = PixelGroupingParametersCalculationRecipe().executeRecipe(groupingIngredients, groupingWorkspace)
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

    def _getPixelGroupingParams(
        self,
        instrumentState: InstrumentState,
        focusGroups: List[FocusGroup],
        useLiteMode: bool,
    ):
        pixelGroupingParams = []
        for focusGroup in focusGroups:
            data = self._calculatePixelGroupingParameters(instrumentState, focusGroup.definition, useLiteMode)
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
        run = request.run
        focussedData = request.workspace
        calibration = self.dataFactoryService.getCalibrationState(run.runNumber)
        focusGroup, instrumentState = self._generateFocusGroupAndInstrumentState(
            run.runNumber, request.focusGroupPath, request.useLiteMode, request.nBinsAcrossPeakWidth, calibration
        )
        data = self._calculatePixelGroupingParameters(instrumentState, focusGroup.definition, request.useLiteMode)
        pixelGroupingParam = data["parameters"]
        cifFilePath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        crystalInfo = CrystallographicInfoService().ingest(cifFilePath)["crystalInfo"]
        # TODO: We Need to Fit the Data
        fitIngredients = FitMultiplePeaksIngredients(
            instrumentState=instrumentState, crystalInfo=crystalInfo, inputWorkspace=focussedData
        )
        fitResults = FitMultiplePeaksRecipe().executeRecipe(FitMultiplePeaksIngredients=fitIngredients)
        metrics = self._collectMetrics(fitResults, focusGroup, pixelGroupingParam)

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

        timestamp = int(round(time.time() * 1000))
        GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(calibrationRecord=record, timestamp=timestamp)
        )

        return record

    @FromString
    def normalization(self, request: NormalizationCalibrationRequest):
        groupingFile = request.groupingPath
        groupingScheme = groupingFile.split("/")[-1].split(".")[0].replace("SNAPFocGroup_", "")
        calibrantSample = self.dataFactoryService.getCalibrantSample(request.samplePath)
        sampleFilePath = self.dataFactoryService.getCifFilePath((request.samplePath).split("/")[-1].split(".")[0])
        crystalInfo = CrystallographicInfoService().ingest(sampleFilePath)["crystalInfo"]

        focusGroup, instrumentState = self._generateFocusGroupAndInstrumentState(
            request.runNumber,
            groupingFile,
            True,
        )

        reductionIngredients = self.dataFactoryService.getReductionIngredients(
            request.runNumber, instrumentState.pixelGroup
        )
        backgroundReductionIngredients = self.dataFactoryService.getReductionIngredients(
            request.backgroundRunNumber, instrumentState.pixelGroup
        )

        smoothDataIngredients = SmoothDataExcludingPeaksIngredients(
            smoothingParameter=request.smoothingParameter,
            instrumentState=instrumentState,
            crystalInfo=crystalInfo,
        )

        normalizationIngredients = NormalizationCalibrationIngredients(
            reductionIngredients=reductionIngredients,
            backgroundReductionIngredients=backgroundReductionIngredients,
            calibrantSample=calibrantSample,
            smoothDataIngredients=smoothDataIngredients,
        )

        groceryList = [
            GroceryListItem(
                workspaceType="nexus",
                runNumber=request.runNumber,
                useLiteMode=True,
                runConfig=reductionIngredients.runConfig,
            ),
            GroceryListItem(
                workspaceType="nexus",
                runNumber=request.backgroundRunNumber,
                useLiteMode=True,
                runConfig=backgroundReductionIngredients.runConfig,
            ),
            GroceryListItem(
                workspaceType="grouping",
                groupingScheme=groupingScheme,
                useLiteMode=True,
                instrumentPropertySource="InstrumentDonor",
                instrumentSource="prev",
            ),
        ]
        workspaceList = FetchGroceriesRecipe().executeRecipe(groceryList)["workspaces"]
        groceries = {
            "inputWorkspace": workspaceList[0],
            "backgroundWorkspace": workspaceList[1],
            "groupingWorkspace": workspaceList[2],
            "outputWorkspace": "focussedRawVanadium",
            "smoothedOutput": "smoothedOutput",
        }
        return CalibrationNormalizationRecipe().executeRecipe(normalizationIngredients, groceries)

    @FromString
    def normalizationAssessment(self, request: NormalizationCalibrationRequest):
        normalization = self.dataFactoryService.getNormalizationState(request.runNumber)
        record = NormalizationRecord(
            runNumber=request.runNumber,
            backgroundRunNumber=request.backgroundRunNumber,
            smoothingParameter=request.smoothingParameter,
            normalization=normalization,
        )
        return record

    @FromString
    def saveNormalization(self, request: NormalizationExportRequest):
        entry = request.normalizationIndexEntry
        normalizationRecord = request.normalizationRecord
        normalizationRecord = self.dataExportService.exportNormalizationRecord(normalizationRecord)
        entry.version = normalizationRecord.version
        self.saveNormalizationToIndex(entry)

    @FromString
    def saveNormalizationToIndex(self, entry: NormalizationIndexEntry):
        if entry.appliesTo is None:
            entry.appliesTo = ">" + entry.runNumber
        if entry.timestamp is None:
            entry.timestamp = int(round(time.time() * 1000))
        logger.info("Saving normalization index entry for Run Number {}".format(entry.runNumber))
        self.dataExportService.exportNormalizationIndexEntry(entry)

    @FromString
    def retrievePixelGroupingParams(self, runID: str, useLiteMode: bool = True):
        calibration = self.dataFactoryService.getCalibrationState(runID)
        focusGroups = self.dataFactoryService.getFocusGroups(runID)
        pixelGroupingParams = self._getPixelGroupingParams(calibration.instrumentState, focusGroups, useLiteMode)
        return pixelGroupingParams
