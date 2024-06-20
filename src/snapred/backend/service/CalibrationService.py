import time
from pathlib import Path
from typing import Dict, List, Optional

import pydantic

from snapred.backend.dao import Limit, RunConfig
from snapred.backend.dao.calibration import (
    CalibrationIndexEntry,
    CalibrationMetric,
    CalibrationRecord,
    FocusGroupMetric,
)
from snapred.backend.dao.indexing.Record import Nonrecord
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
    FitMultiplePeaksRequest,
    FocusSpectraRequest,
    HasStateRequest,
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
)
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.meta.validator.RunNumberValidator import RunNumberValidator

logger = snapredLogger.getLogger(__name__)


# import pdb
@Singleton
class CalibrationService(Service):
    """

    The CalibrationService orchestrates a suite of calibration processes, integrating various components
    such as RunConfig, CalibrationRecord, and FocusGroupMetric to facilitate comprehensive calibration
    tasks. This service leverages recipes like DiffractionCalibrationRecipe and
    GenerateCalibrationMetricsWorkspaceRecipe to perform diffraction calibration and generate workspace
    metrics, respectively. It manages the entire calibration workflow, from initializing state and preparing
    ingredients to assessing quality and exporting results. Key functionalities include preparing diffraction
    calibration ingredients, fetching groceries (workspace names), and executing recipes for focusing spectra,
    saving calibration data, and loading assessments.

    """

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
        self.registerPath("ingredients", self.prepDiffractionCalibrationIngredients)
        self.registerPath("groceries", self.fetchDiffractionCalibrationGroceries)
        self.registerPath("focus", self.focusSpectra)
        self.registerPath("fitpeaks", self.fitPeaks)
        self.registerPath("save", self.save)
        self.registerPath("load", self.load)
        self.registerPath("initializeState", self.initializeState)
        self.registerPath("hasState", self.hasState)
        self.registerPath("assessment", self.assessQuality)
        self.registerPath("loadQualityAssessment", self.loadQualityAssessment)
        self.registerPath("index", self.getCalibrationIndex)
        self.registerPath("diffraction", self.diffractionCalibration)
        return

    @staticmethod
    def name():
        return "calibration"

    @FromString
    def prepDiffractionCalibrationIngredients(
        self, request: DiffractionCalibrationRequest
    ) -> DiffractionCalibrationIngredients:
        # fetch the ingredients needed to focus and plot the peaks
        cifPath = self.dataFactoryService.getCifFilePath(Path(request.calibrantSamplePath).stem)
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
            fwhmMultipliers=request.fwhmMultipliers,
            maxChiSq=request.maxChiSq,
        )
        return self.sousChef.prepDiffractionCalibrationIngredients(farmFresh)

    @FromString
    def fetchDiffractionCalibrationGroceries(self, request: DiffractionCalibrationRequest) -> Dict[str, str]:
        # groceries
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("groupingWorkspace").fromRun(request.runNumber).grouping(
            request.focusGroup.name
        ).useLiteMode(request.useLiteMode).add()
        diffcalOutputName = (
            wng.diffCalOutput().unit(wng.Units.DSP).runNumber(request.runNumber).group(request.focusGroup.name).build()
        )
        diagnosticWorkspaceName = (
            wng.diffCalOutput().unit(wng.Units.DIAG).runNumber(request.runNumber).group(request.focusGroup.name).build()
        )
        calibrationTableName = wng.diffCalTable().runNumber(request.runNumber).build()
        calibrationMaskName = wng.diffCalMask().runNumber(request.runNumber).build()

        return self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
            outputWorkspace=diffcalOutputName,
            diagnosticWorkspace=diagnosticWorkspaceName,
            calibrationTable=calibrationTableName,
            maskWorkspace=calibrationMaskName,
        )

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
            wng.run()
            .runNumber(request.runNumber)
            .group(request.focusGroup.name)
            .unit(wng.Units.DSP)
            .auxiliary("F-dc")
            .build()
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
    def fitPeaks(self, request: FitMultiplePeaksRequest):
        return FitMultiplePeaksRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            DetectorPeaks=request.detectorPeaks,
            PeakFunction=request.peakFunction,
            OutputWorkspaceGroup=request.outputWorkspaceGroup,
        )

    @FromString
    def save(self, request: CalibrationExportRequest):
        """
        If no version is attached to the request, this will save at next version number
        """
        entry = request.calibrationIndexEntry
        record = request.calibrationRecord
        version = self.dataFactoryService.getThisOrNextCalibrationVersion(
            record.runNumber,
            record.useLiteMode,
            request.version,
        )
        # set the correct versions on the objects to be saved
        entry.version = version
        record.version = version
        record.calculationParameters.version = version

        # Rebuild the workspace names to strip any "iteration" number:
        savedWorkspaces = {}
        for key, wsName in record.workspaces.items():
            match key:
                case wngt.DIFFCAL_OUTPUT:
                    if wng.Units.DSP.lower() in wsName:
                        savedWorkspaces[key] = (
                            wng.diffCalOutput()
                            .unit(wng.Units.DSP)
                            .runNumber(record.runNumber)
                            .version(version)
                            .group(record.focusGroupCalibrationMetrics.focusGroupName)
                            .build()
                        )
                    else:
                        raise RuntimeError(
                            f"""
                            cannot save a workspace-type: {wngt.DIFFCAL_OUTPUT}
                            without a units token in its name {wsName}
                            """
                        )
                case wngt.DIFFCAL_DIAG:
                    savedWorkspaces[key] = (
                        wng.diffCalOutput()
                        .unit(wng.Units.DIAG)
                        .runNumber(record.runNumber)
                        .version(version)
                        .group(record.focusGroupCalibrationMetrics.focusGroupName)
                        .build()
                    )
                case wngt.DIFFCAL_MASK:
                    savedWorkspaces[key] = wng.diffCalMask().runNumber(record.runNumber).version(version).build()
                case wngt.DIFFCAL_TABLE:
                    savedWorkspaces[key] = wng.diffCalTable().runNumber(record.runNumber).version(version).build()
                case _:
                    raise RuntimeError(f"Unexpected output type {key} for {wsName}")

            self.groceryService.renameWorkspace(wsName, savedWorkspaces[key])

        record.workspaces = savedWorkspaces

        # save the objects at the indicated version
        self.dataExportService.exportCalibrationRecord(record, version)
        self.dataExportService.exportCalibrationWorkspaces(record, version)
        self.saveCalibrationToIndex(entry, version)

    @FromString
    def load(self, run: RunConfig, version: Optional[int] = None):
        """
        If no version is given, will load the latest version applicable to the run number
        """
        return self.dataFactoryService.getCalibrationRecord(run.runNumber, run.useLiteMode, version)

    @FromString
    def saveCalibrationToIndex(self, entry: CalibrationIndexEntry, version: Optional[int] = None):
        """
        If no version given, will save at next version following Indexor rules
        """
        if entry.appliesTo is None:
            entry.appliesTo = ">=" + entry.runNumber
        if entry.timestamp is None:
            entry.timestamp = int(round(time.time() * self.MILLISECONDS_PER_SECOND))
        logger.info("Saving calibration index entry for Run Number {}".format(entry.runNumber))
        self.dataExportService.exportCalibrationIndexEntry(entry, version)

    @FromString
    def initializeState(self, request: InitializeStateRequest):
        return self.dataExportService.initializeState(request.runId, request.useLiteMode, request.humanReadableName)

    @FromString
    def getState(self, runs: List[RunConfig]):
        states = []
        for run in runs:
            state = self.dataFactoryService.getStateConfig(run.runNumber, run.useLiteMode)
            states.append(state)
        return states

    @FromString
    def hasState(self, request: HasStateRequest):
        runId = request.runId
        if not RunNumberValidator.validateRunNumber(runId):
            logger.error(f"Invalid run number: {runId}")
            return False
        return self.dataFactoryService.checkCalibrationStateExists(runId)

    @staticmethod
    def parseCalibrationMetricList(src: str) -> List[CalibrationMetric]:
        # implemented as a separate method to facilitate testing
        return pydantic.TypeAdapter(List[CalibrationMetric]).validate_json(src)

    # TODO make the inputs here actually work
    def _collectMetrics(self, focussedData, focusGroup, pixelGroup):
        metric = self.parseCalibrationMetricList(
            CalibrationMetricExtractionRecipe().executeRecipe(
                InputWorkspace=focussedData,
                PixelGroup=pixelGroup.json(),
            ),
        )
        return FocusGroupMetric(focusGroupName=focusGroup.name, calibrationMetric=metric)

    @FromString
    def getCalibrationIndex(self, request: CalibrationIndexRequest):
        run = request.run
        calibrationIndex = self.dataFactoryService.getCalibrationIndex(run.runNumber, run.useLiteMode)
        return calibrationIndex

    @FromString
    def loadQualityAssessment(self, request: CalibrationLoadAssessmentRequest):
        runId = request.runId
        useLiteMode = request.useLiteMode
        version = request.version

        calibrationRecord = self.dataFactoryService.getCalibrationRecord(runId, useLiteMode, version)
        if calibrationRecord is Nonrecord:
            errorTxt = f"No calibration record found for run {runId}, version {version}."
            logger.error(errorTxt)
            raise ValueError(errorTxt)

        # check if any of the workspaces already exist
        if request.checkExistent:
            wkspaceExists = False
            wsName = None
            for metricName in ["sigma", "strain"]:
                wsName = wng.diffCalMetric().metricName(metricName).runNumber(runId).version(version).build()
                if self.dataFactoryService.workspaceDoesExist(wsName):
                    wkspaceExists = True
                    break
            if not wkspaceExists:
                for wsName in calibrationRecord.workspaces.values():
                    if self.dataFactoryService.workspaceDoesExist(wsName):
                        wkspaceExists = True
                        break
            if wkspaceExists:
                errorTxt = (
                    f"Calibration assessment for Run {runId} Version {version} "
                    f"is already loaded: see workspace {wsName}."
                )
                logger.error(errorTxt)
                raise RuntimeError(errorTxt)

        # generate metrics workspaces
        GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(calibrationRecord=calibrationRecord)
        )

        # load persistent data workspaces, assuming all workspaces are of WNG-type
        # NOTE the name properties are not important and are only to avoid collisions

        workspaces = calibrationRecord.workspaces.copy()
        # load the output
        wsName = workspaces.pop(wngt.DIFFCAL_OUTPUT, None)
        if wng.Units.DSP.lower() in wsName:
            self.groceryClerk.name(wngt.DIFFCAL_OUTPUT + "_" + str(0).zfill(4))
            self.groceryClerk.diffcal_output(runId, version).unit(wng.Units.DSP).group(
                calibrationRecord.focusGroupCalibrationMetrics.focusGroupName
            ).add()
        else:
            raise RuntimeError(
                f"cannot save a workspace-type: {wngt.DIFFCAL_OUTPUT} without a units token in its name {wsName}"
            )
        # load the diagnostic workspace
        workspaces.pop(wngt.DIFFCAL_DIAG, None)
        self.groceryClerk.name(wngt.DIFFCAL_DIAG + "_" + str(0).zfill(4))
        self.groceryClerk.diffcal_diagnostic(runId, version).unit(wng.Units.DIAG).group(
            calibrationRecord.focusGroupCalibrationMetrics.focusGroupName
        ).add()
        # Diffraction calibration requires a complete pair 'table' + 'mask':
        #   as above, the specific property name used here is not important.
        workspaces.pop(wngt.DIFFCAL_MASK, None)
        workspaces.pop(wngt.DIFFCAL_TABLE, None)
        self.groceryClerk.name(wngt.DIFFCAL_TABLE + "_" + str(0).zfill(4)).diffcal_table(runId, version).add()
        self.groceryClerk.name(wngt.DIFFCAL_MASK + "_" + str(0).zfill(4)).diffcal_mask(runId, version).add()

        if workspaces:
            raise RuntimeError(f"not implemented: unable to load unexpected workspace types: {workspaces}")

        self.groceryService.fetchGroceryDict(self.groceryClerk.buildDict())

    @FromString
    def assessQuality(self, request: CalibrationAssessmentRequest):
        cifPath = self.dataFactoryService.getCifFilePath(Path(request.calibrantSamplePath).stem)
        farmFresh = FarmFreshIngredients(
            runNumber=request.run.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            # fiddly bits
            peakFunction=request.peakFunction,
            crystalDBounds=Limit(minimum=request.crystalDMin, maximum=request.crystalDMax),
            peakIntensityThreshold=request.peakIntensityThreshold,
            nBinsAcrossPeakWidth=request.nBinsAcrossPeakWidth,
            fwhmMultipliers=request.fwhmMultipliers,
            maxChiSq=request.maxChiSq,
        )
        pixelGroup = self.sousChef.prepPixelGroup(farmFresh)
        detectorPeaks = self.sousChef.prepDetectorPeaks(farmFresh)

        # TODO: We Need to Fit the Data
        fitResults = FitMultiplePeaksRecipe().executeRecipe(
            InputWorkspace=request.workspaces[wngt.DIFFCAL_OUTPUT],
            DetectorPeaks=detectorPeaks,
        )
        metrics = self._collectMetrics(fitResults, request.focusGroup, pixelGroup)

        record = CalibrationRecord(
            runNumber=request.run.runNumber,
            useLiteMode=request.useLiteMode,
            crystalInfo=self.sousChef.prepCrystallographicInfo(farmFresh),
            calculationParameters=self.sousChef.prepCalibration(farmFresh),
            pixelGroups=[pixelGroup],
            focusGroupCalibrationMetrics=metrics,
            workspaces=request.workspaces,
        )

        timestamp = int(round(time.time() * self.MILLISECONDS_PER_SECOND))
        metricWorkspaces = GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(calibrationRecord=record, timestamp=timestamp)
        )

        return CalibrationAssessmentResponse(record=record, metricWorkspaces=metricWorkspaces)
