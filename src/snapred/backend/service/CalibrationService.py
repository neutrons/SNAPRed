from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pydantic

from snapred.backend.dao import Limit, RunConfig
from snapred.backend.dao.calibration import (
    CalibrationMetric,
    FocusGroupMetric,
)
from snapred.backend.dao.indexing import IndexEntry
from snapred.backend.dao.indexing.Versioning import VERSION_START, VersionState
from snapred.backend.dao.ingredients import (
    CalibrationMetricsWorkspaceIngredients,
    DiffractionCalibrationIngredients,
    GroceryListItem,
)
from snapred.backend.dao.request import (
    CalculateResidualRequest,
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    CalibrationIndexRequest,
    CalibrationLoadAssessmentRequest,
    CalibrationLockRequest,
    CalibrationWritePermissionsRequest,
    DiffractionCalibrationRequest,
    FarmFreshIngredients,
    FitMultiplePeaksRequest,
    FocusSpectraRequest,
    HasStateRequest,
    InitializeStateRequest,
    LoadCalibrationRecordRequest,
    MatchRunsRequest,
    OverrideRequest,
    RunMetadataRequest,
    SimpleDiffCalRequest,
)
from snapred.backend.dao.response.CalibrationAssessmentResponse import CalibrationAssessmentResponse
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.state.CalibrantSample import CalibrantSample
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.backend.profiling.ProgressRecorder import ComputationalOrder, WallClockTime
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.CalculateDiffCalResidualRecipe import CalculateDiffCalResidualRecipe
from snapred.backend.recipe.GenerateCalibrationMetricsWorkspaceRecipe import GenerateCalibrationMetricsWorkspaceRecipe
from snapred.backend.recipe.GenericRecipe import (
    CalibrationMetricExtractionRecipe,
    ConvertUnitsRecipe,
    FitMultiplePeaksRecipe,
    FocusSpectraRecipe,
)
from snapred.backend.recipe.GroupDiffCalRecipe import GroupDiffCalRecipe, GroupDiffCalServing
from snapred.backend.recipe.PixelDiffCalRecipe import PixelDiffCalRecipe, PixelDiffCalServing
from snapred.backend.service.Service import Register, Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta import Time
from snapred.meta.builder.GroceryListBuilder import GroceryListBuilder
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
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

    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.groceryClerk: GroceryListBuilder = GroceryListItem.builder()
        self.sousChef = SousChef()
        self.mantidSnapper = MantidSnapper(None, __name__)

    @classproperty
    def MINIMUM_PEAKS_PER_GROUP(cls):
        return Config["calibration.diffraction.minimumPeaksPerGroup"]

    @staticmethod
    def name():
        return "calibration"

    def _calibration_N_ref(self, request: DiffractionCalibrationRequest) -> float | None:
        # Calculate the reference value to use during the estimation of execution time for the
        #   "prepDiffractionCalibrationIngredients", "fetchDiffractioncCalibrationGroceries", and
        #   "diffractionCalibration" methods.

        # -- Returning `None` means that the `N_ref` value cannot be calculated, or alternatively,
        #    that it should not be used.
        # -- Please see:
        #    `snapred.readthedocs.io/en/latest/developer/implementation_notes/profiling_and_progress_recording.html`.

        N_ref = None
        inputFilePath = self.groceryService.createNeutronFilePath(request.runNumber, request.useLiteMode)
        if inputFilePath.exists():
            # As the workspaces aren't loaded yet, this estimate uses the file size.
            # Note: `st_size` is in bytes.
            dataSize = float(inputFilePath.stat().st_size)
            N_ref = dataSize
        return N_ref

    @WallClockTime(N_ref=_calibration_N_ref, order=ComputationalOrder.O_N)
    @FromString
    @Register("ingredients")
    def prepDiffractionCalibrationIngredients(
        self, request: DiffractionCalibrationRequest
    ) -> DiffractionCalibrationIngredients:
        # fetch the ingredients needed to focus and plot the peaks
        cifPath = self.dataFactoryService.getCifFilePath(Path(request.calibrantSamplePath).stem)
        state, _ = self.dataFactoryService.constructStateId(request.runNumber)
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroups=[request.focusGroup],
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            # fiddly-bits
            peakFunction=request.peakFunction,
            crystalDBounds={"minimum": request.crystalDMin, "maximum": request.crystalDMax},
            convergenceThreshold=request.convergenceThreshold,
            nBinsAcrossPeakWidth=request.nBinsAcrossPeakWidth,
            fwhmMultipliers=request.fwhmMultipliers,
            maxChiSq=request.maxChiSq,
            state=state,
        )
        ingredients = self.sousChef.prepDiffractionCalibrationIngredients(farmFresh)
        ingredients.removeBackground = request.removeBackground
        return ingredients

    @FromString
    @Register("groceries")
    def fetchDiffractionCalibrationGroceries(self, request: DiffractionCalibrationRequest) -> Dict[str, str]:
        # groceries

        # TODO:  It would be nice for groceryclerk to be smart enough to flatten versions
        # However I will save that scope for another time
        if request.startingTableVersion == VersionState.DEFAULT:
            request.startingTableVersion = VERSION_START()

        state, _ = self.dataFactoryService.constructStateId(request.runNumber)

        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(
            request.useLiteMode
        ).diffCalVersion(request.startingTableVersion).state(state).dirty().add()
        self.groceryClerk.name("groupingWorkspace").fromRun(request.runNumber).grouping(
            request.focusGroup.name
        ).useLiteMode(request.useLiteMode).dirty().add()
        self.groceryClerk.name("previousCalibration").diffcal_table(
            state, request.startingTableVersion, request.runNumber
        ).useLiteMode(request.useLiteMode).dirty().add()
        # names
        diffcalOutputName = (
            wng.diffCalOutput().unit(wng.Units.DSP).runNumber(request.runNumber).group(request.focusGroup.name).build()
        )
        diagnosticWorkspaceName = (
            wng.diffCalOutput().unit(wng.Units.DIAG).runNumber(request.runNumber).group(request.focusGroup.name).build()
        )
        calibrationTableName = wng.diffCalTable().runNumber(request.runNumber).build()
        calibrationMaskName = wng.diffCalMask().runNumber(request.runNumber).build()

        timestamp = Time.timestamp(True)
        combinedMask = wng.reductionPixelMask().runNumber(request.runNumber).timestamp(timestamp).build()

        groceryDict = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
            outputWorkspace=diffcalOutputName,
            diagnosticWorkspace=diagnosticWorkspaceName,
            calibrationTable=calibrationTableName,
            maskWorkspace=combinedMask,
        )
        if self.groceryService.workspaceDoesExist(calibrationMaskName):
            self.groceryService.renameWorkspace(calibrationMaskName, combinedMask)
        if request.pixelMasks:
            for mask in request.pixelMasks:
                if not self.groceryService.workspaceDoesExist(mask):
                    raise pydantic.ValidationError([f"Pixel mask workspace '{mask}' does not exist"])
                self.mantidSnapper.BinaryOperation(
                    InputWorkspace1=combinedMask,
                    InputWorkspace2=mask,
                    OutputWorkspace=combinedMask,
                    Operation="Or",
                )
                self.mantidSnapper.executeQueue()

        return groceryDict

    @WallClockTime(N_ref=_calibration_N_ref, order=ComputationalOrder.O_N)
    @FromString
    @Register("diffraction")
    def diffractionCalibration(self, request: DiffractionCalibrationRequest) -> Dict[str, Any]:
        self.validateRequest(request)

        # Profiling note: none of the following should be marked as sub-steps:
        #   their service methods are decorated separately.

        payload = SimpleDiffCalRequest(
            ingredients=self.prepDiffractionCalibrationIngredients(request),
            groceries=self.fetchDiffractionCalibrationGroceries(request),
        )

        pixelRes = self.pixelCalibration(payload)
        if not pixelRes.result:
            raise RuntimeError("Pixel Calibration failed")

        payload.groceries["previousCalibration"] = pixelRes.calibrationTable
        payload.groceries["inputWorkspace"] = pixelRes.outputWorkspace
        groupRes = self.groupCalibration(payload)
        if not groupRes.result:
            raise RuntimeError("Group Calibration failed")

        return {
            "calibrationTable": groupRes.calibrationTable,
            "diagnosticWorkspace": groupRes.diagnosticWorkspace,
            "outputWorkspace": groupRes.outputWorkspace,
            "maskWorkspace": groupRes.maskWorkspace,
            "steps": pixelRes.medianOffsets,
            "result": True,
        }

    def _calibration_substep_N_ref(self, request: SimpleDiffCalRequest) -> float | None:
        # Calculate the reference value to use during the estimation of execution time for the
        # "pixelCalibration" and "groupCalibration" methods.

        # -- This method must have the same calling signature as the decorated method.
        # -- Returning `None` means that the `N_ref` value cannot be calculated, or alternatively,
        #    that it should not be used.
        # -- Please see:
        #    `snapred.readthedocs.io/en/latest/developer/implementation_notes/profiling_and_progress_recording.html`.

        groceries = request.groceries
        inputWorkspace = groceries["inputWorkspace"]
        N_ref = None
        if self.mantidSnapper.mtd.doesExist(inputWorkspace):
            # Notes:
            #   -- Scaling actually has a sub-linear dependence on `N_subgroups`,
            #      but that should be accounted for during the estimator's spline fit.
            #   -- `getMemorySize()` output is in bytes, which contradicts its Mantid docs.
            #      This is important, as it allows compatibility with the default estimator,
            #      which is designed assuming its `N_ref` input will be in number of bytes.
            dataSize = float(self.mantidSnapper.mtd[inputWorkspace].getMemorySize())
            N_ref = dataSize
        return N_ref

    @WallClockTime(N_ref=_calibration_substep_N_ref, order=ComputationalOrder.O_N)
    @FromString
    @Register("pixel")
    def pixelCalibration(self, request: SimpleDiffCalRequest) -> PixelDiffCalServing:
        # cook recipe
        res = PixelDiffCalRecipe().cook(request.ingredients, request.groceries)
        maskWS = self.groceryService.getWorkspaceForName(res.maskWorkspace)
        percentMasked = maskWS.getNumberMasked() / maskWS.getNumberHistograms()
        threshold = Config["constants.maskedPixelThreshold"]
        if percentMasked > threshold:
            res.result = False
            raise Exception(
                (
                    f"WARNING: More than {threshold * 100}% of pixels failed calibration. Please check your input "
                    "data. If input data has poor statistics, you may get better results by disabling Cross "
                    "Correlation. You can also improve statistics by activating Lite mode if this is not "
                    "already activated."
                ),
            )
        return res

    @WallClockTime(N_ref=_calibration_substep_N_ref, order=ComputationalOrder.O_N)
    @FromString
    @Register("group")
    def groupCalibration(self, request: SimpleDiffCalRequest) -> GroupDiffCalServing:
        # cook recipe
        return GroupDiffCalRecipe().cook(request.ingredients, request.groceries)

    def validateRequest(self, request: DiffractionCalibrationRequest):
        """
        Validate the diffraction-calibration request.

        :param request: a diffraction-calibration request
        :type request: DiffractionCalibrationRequest
        """

        # This is a redundant call, but it is placed here to facilitate re-sequencing.
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=request.runNumber, continueFlags=request.continueFlags
        )
        self.validateWritePermissions(permissionsRequest)

    @Register("validateWritePermissions")
    def validateWritePermissions(self, request: CalibrationWritePermissionsRequest):
        """
        Validate that the diffraction-calibration workflow will be able to save its output.

        :param request: a write-permissions request containing the run number and existing continue flags
        :type request: CalibrationWritePermissionsRequest
        """
        # Note: this is split-out as a separate method and a registered service call.
        #   Permissions must be checked as early as possible in the workflow.

        # check that the user has write permissions to the save directory
        if (
            not self.checkWritePermissions(request.runNumber)
            and ContinueWarning.Type.CALIBRATION_HOME_WRITE_PERMISSION not in request.continueFlags
        ):
            raise ContinueWarning.calibrationHomeWritePermission()

        if ContinueWarning.Type.CALIBRATION_HOME_WRITE_PERMISSION in request.continueFlags:
            self.dataExportService.generateUserRootFolder()

    @FromString
    @Register("residual")
    def calculateResidual(self, request: CalculateResidualRequest):
        ingredients = None
        groceries = request.model_dump()
        return CalculateDiffCalResidualRecipe().cook(ingredients, groceries)

    @FromString
    @Register("focus")
    def focusSpectra(self, request: FocusSpectraRequest):
        # prep the ingredients -- a pixel group
        state, _ = self.dataFactoryService.constructStateId(request.runNumber)
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber, useLiteMode=request.useLiteMode, focusGroups=[request.focusGroup], state=state
        )
        pixelGroup = self.sousChef.prepPixelGroup(farmFresh)
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
            ConvertUnitsRecipe().executeRecipe(
                InputWorkspace=request.inputWorkspace,
                OutputWorkspace=focusedWorkspace,
                Target="dSpacing",
                Emode="Elastic",
            )
            FocusSpectraRecipe().executeRecipe(
                InputWorkspace=focusedWorkspace,
                GroupingWorkspace=groupingWorkspace,
                OutputWorkspace=focusedWorkspace,
                PixelGroup=pixelGroup,
                PreserveEvents=request.preserveEvents,
            )
        return focusedWorkspace, groupingWorkspace

    @FromString
    @Register("fitpeaks")
    def fitPeaks(self, request: FitMultiplePeaksRequest):
        return FitMultiplePeaksRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            DetectorPeaks=request.detectorPeaks,
            PeakFunction=request.peakFunction,
            OutputWorkspaceGroup=request.outputWorkspaceGroup,
        )

    @FromString
    @Register("lock")
    def obtainLock(self, request: CalibrationLockRequest):
        return self.dataExportService.obtainCalibrationLock(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
        )

    @FromString
    @Register("save")
    def save(self, request: CalibrationExportRequest):
        """
        If no version is attached to the request, this will save at next version number
        """
        record = self.dataFactoryService.createCalibrationRecord(request.createRecordRequest)
        state, _ = self.dataFactoryService.constructStateId(record.runNumber)
        version = record.version
        if self.dataFactoryService.calibrationExists(record.runNumber, record.useLiteMode, state):
            if version == VERSION_START():
                raise RuntimeError("Overwriting the default calibration is not allowed.")

        # Rebuild the workspace names to strip any "iteration" number:
        savedWorkspaces = {}
        for key_, wsNames in record.workspaces.items():
            key = wngt(key_)  # TODO: fix usage of `@FromString`. Probably get rid of it!

            savedWorkspaces[key] = []
            match key:
                case wngt.DIFFCAL_OUTPUT:
                    for wsName in wsNames:
                        if wng.Units.DSP.lower() in wsName:
                            savedWorkspaces[key].append(
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
                    for wsName in wsNames:
                        savedWorkspaces[key].append(
                            wng.diffCalOutput()
                            .unit(wng.Units.DIAG)
                            .runNumber(record.runNumber)
                            .version(version)
                            .group(record.focusGroupCalibrationMetrics.focusGroupName)
                            .build()
                        )
                case wngt.DIFFCAL_MASK:
                    for wsName in wsNames:
                        savedWorkspaces[key].append(
                            wng.diffCalMask().runNumber(record.runNumber).version(version).build()
                        )
                case wngt.DIFFCAL_TABLE:
                    for wsName in wsNames:
                        savedWorkspaces[key].append(
                            wng.diffCalTable().runNumber(record.runNumber).version(version).build()
                        )
                case _:
                    raise RuntimeError(f"Unexpected output type {key} for {wsNames}")
            for oldName, newName in zip(wsNames, savedWorkspaces[key]):
                self.groceryService.renameWorkspace(oldName, newName)

        record.workspaces = savedWorkspaces

        # save the objects at the indicated version
        self.dataExportService.exportCalibrationRecord(record)
        self.dataExportService.exportCalibrationWorkspaces(record)

    @FromString
    @Register("load")
    def load(self, request: LoadCalibrationRecordRequest):
        """
        If no version is given, will load the latest version applicable to the run number
        """
        run = request.runConfig
        version = request.version
        state, _ = self.dataFactoryService.constructStateId(run.runNumber)
        return self.dataFactoryService.getCalibrationRecord(run.runNumber, run.useLiteMode, version, state)

    def matchRunsToCalibrationVersions(self, request: MatchRunsRequest) -> Dict[str, Any]:
        """
        For each run in the list, find the calibration version that applies to it
        """
        response = {}
        for runNumber in request.runNumbers:
            state, _ = self.dataFactoryService.constructStateId(runNumber)
            response[runNumber] = self.dataFactoryService.getLatestApplicableCalibrationVersion(
                runNumber, request.useLiteMode, state
            )
        return response

    @FromString
    @Register("fetchMatches")
    def fetchMatchingCalibrations(self, request: MatchRunsRequest) -> Tuple[Set[WorkspaceName], Dict[str, Any]]:
        calibrations = self.matchRunsToCalibrationVersions(request)
        for runNumber in request.runNumbers:
            if runNumber in calibrations:
                state, _ = self.dataFactoryService.constructStateId(runNumber)
                # TODO: add a specified version to the grocery list
                self.groceryClerk.diffcal_table(state, calibrations[runNumber], sampleRunNumber=runNumber).useLiteMode(
                    request.useLiteMode
                ).add()
                # Calibration masks are also required, and are automatically loaded at the same time,
                #   however we need the actual mask-workspace name to be in the returned workspaces set.
                self.groceryClerk.diffcal_mask(state, calibrations[runNumber], sampleRunNumber=runNumber).useLiteMode(
                    request.useLiteMode
                ).add()
        workspaces = set(self.groceryService.fetchGroceryList(self.groceryClerk.buildList()))
        return workspaces, calibrations

    @FromString
    def saveCalibrationToIndex(self, entry: IndexEntry):
        """
        The entry must have the version set.
        """
        if entry.appliesTo is None:
            entry.appliesTo = ">=" + entry.runNumber
        logger.info("Saving calibration index entry for Run Number {}".format(entry.runNumber))
        self.dataExportService.exportCalibrationIndexEntry(entry)

    @FromString
    @Register("initializeState")
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
    @Register("hasState")
    def hasState(self, request: HasStateRequest):
        runId = request.runId
        if not RunNumberValidator.validateRunNumber(runId):
            raise ValueError(f"Invalid run number: {runId}")
        return self.dataFactoryService.checkCalibrationStateExists(runId)

    def checkWritePermissions(self, runNumber: str) -> bool:
        path = self.dataExportService.getCalibrationStateRoot(runNumber)
        return self.dataExportService.checkWritePermissions(path)

    def getSavePath(self, runNumber: str) -> Path:
        return self.dataExportService.getCalibrationStateRoot(runNumber)

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
    @Register("index")
    def getCalibrationIndex(self, request: CalibrationIndexRequest):
        run = request.run
        state, _ = self.dataFactoryService.constructStateId(run.runNumber)
        calibrationIndex = self.dataFactoryService.getCalibrationIndex(run.useLiteMode, state)
        return calibrationIndex

    @FromString
    @Register("loadQualityAssessment")
    def loadQualityAssessment(self, request: CalibrationLoadAssessmentRequest):
        runId = request.runId
        useLiteMode = request.useLiteMode
        version = request.version
        state, _ = self.dataFactoryService.constructStateId(runId)
        calibrationRecord = self.dataFactoryService.getCalibrationRecord(runId, useLiteMode, version, state)
        if calibrationRecord is None:
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
                for wss in calibrationRecord.workspaces.values():
                    for wsName in wss:
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
            CalibrationMetricsWorkspaceIngredients(
                runNumber=calibrationRecord.runNumber,
                version=calibrationRecord.version,
                focusGroupCalibrationMetrics=calibrationRecord.focusGroupCalibrationMetrics,
                timestamp=self.dataExportService.getUniqueTimestamp(),
            )
        )

        # load persistent data workspaces, assuming all workspaces are of WNG-type
        # NOTE the name properties are not important and are only to avoid collisions

        workspaces = calibrationRecord.workspaces.copy()
        for n, wsName in enumerate(workspaces.pop(wngt.DIFFCAL_OUTPUT, [])):
            # The specific property name used here will not be used later, but there must be no collisions.
            self.groceryClerk.name(wngt.DIFFCAL_OUTPUT + "_" + str(n).zfill(4))
            if wng.Units.DSP.lower() in wsName:
                (
                    self.groceryClerk.diffcal_output(state, version)
                    .useLiteMode(useLiteMode)
                    .unit(wng.Units.DSP)
                    .group(calibrationRecord.focusGroupCalibrationMetrics.focusGroupName)
                    .add()
                )
            else:
                raise RuntimeError(
                    f"cannot load a workspace-type: {wngt.DIFFCAL_OUTPUT} without a units token in its name {wsName}"
                )
        for n, wsName in enumerate(workspaces.pop(wngt.DIFFCAL_DIAG, [])):
            self.groceryClerk.name(wngt.DIFFCAL_DIAG + "_" + str(n).zfill(4))
            if wng.Units.DIAG.lower() in wsName:
                (
                    self.groceryClerk.diffcal_diagnostic(state, version)
                    .useLiteMode(useLiteMode)
                    .unit(wng.Units.DIAG)
                    .group(calibrationRecord.focusGroupCalibrationMetrics.focusGroupName)
                    .add()
                )
        for n, (tableWSName, maskWSName) in enumerate(
            zip(
                workspaces.pop(wngt.DIFFCAL_TABLE, []),
                workspaces.pop(wngt.DIFFCAL_MASK, []),
            )
        ):
            # Diffraction calibration requires a complete pair 'table' + 'mask':
            #   as above, the specific property name used here is not important.
            self.groceryClerk.name(wngt.DIFFCAL_TABLE + "_" + str(n).zfill(4)).diffcal_table(
                state, version, runId
            ).useLiteMode(useLiteMode).add()
            self.groceryClerk.name(wngt.DIFFCAL_MASK + "_" + str(n).zfill(4)).diffcal_mask(
                state, version, runId
            ).useLiteMode(useLiteMode).add()

        if workspaces:
            raise RuntimeError(f"not implemented: unable to load unexpected workspace types: {workspaces}")

        self.groceryService.fetchGroceryDict(self.groceryClerk.buildDict())

    @FromString
    @Register("assessment")
    def assessQuality(self, request: CalibrationAssessmentRequest):
        state, _ = self.dataFactoryService.constructStateId(request.run.runNumber)
        cifPath = self.dataFactoryService.getCifFilePath(Path(request.calibrantSamplePath).stem)
        farmFresh = FarmFreshIngredients(
            runNumber=request.run.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroups=[request.focusGroup],
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            # fiddly bits
            peakFunction=request.peakFunction,
            crystalDBounds=Limit(minimum=request.crystalDMin, maximum=request.crystalDMax),
            nBinsAcrossPeakWidth=request.nBinsAcrossPeakWidth,
            fwhmMultipliers=request.fwhmMultipliers,
            maxChiSq=request.maxChiSq,
            state=state,
        )
        pixelGroup = self.sousChef.prepPixelGroup(farmFresh)
        detectorPeaks = self.sousChef.prepDetectorPeaks(farmFresh)

        # TODO: We Need to Fit the Data
        fitResults = FitMultiplePeaksRecipe().executeRecipe(
            InputWorkspace=request.workspaces[wngt.DIFFCAL_OUTPUT][0],
            DetectorPeaks=detectorPeaks,
        )
        metrics = self._collectMetrics(fitResults, request.focusGroup, pixelGroup)

        version = self.dataFactoryService.getNextCalibrationVersion(
            useLiteMode=request.useLiteMode,
            state=state,
        )
        timestamp = self.dataExportService.getUniqueTimestamp()
        metricWorkspaces = GenerateCalibrationMetricsWorkspaceRecipe().executeRecipe(
            CalibrationMetricsWorkspaceIngredients(
                runNumber=request.run.runNumber,
                version=version,
                focusGroupCalibrationMetrics=metrics,
                timestamp=timestamp,
            )
        )

        return CalibrationAssessmentResponse(
            version=version,
            crystalInfo=self.sousChef.prepCrystallographicInfo(farmFresh),
            calculationParameters=self.sousChef.prepCalibration(farmFresh),
            pixelGroups=[pixelGroup],
            focusGroupCalibrationMetrics=metrics,
            workspaces=request.workspaces,
            metricWorkspaces=metricWorkspaces,
        )

    @FromString
    @Register("override")
    def handleOverrides(self, request: OverrideRequest):
        sample: CalibrantSample = self.dataFactoryService.getCalibrantSample(request.calibrantSamplePath)
        if sample.overrides:
            return sample.overrides
        else:
            return None

    @FromString
    @Register("runMetadata")
    def runMetadata(self, request: RunMetadataRequest) -> RunMetadata:
        return self.dataFactoryService.getRunMetadata(request.runId)
