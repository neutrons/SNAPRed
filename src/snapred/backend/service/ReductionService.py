import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from snapred.backend.dao import RunMetadata
from snapred.backend.dao.indexing.Versioning import VERSION_START, VersionState
from snapred.backend.dao.ingredients import (
    GroceryListItem,
    ReductionIngredients,
)
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.request import (
    CreateArtificialNormalizationRequest,
    FarmFreshIngredients,
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.response.ReductionResponse import ReductionResponse
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.WorkspaceMetadata import DiffcalStateMetadata, NormalizationStateMetadata, WorkspaceMetadata
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.GenericRecipe import ArtificialNormalizationRecipe, ConvertUnitsRecipe
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.builder.GroceryListBuilder import GroceryListBuilder
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceType as wngt,
)
from snapred.meta.validator.RunNumberValidator import RunNumberValidator

logger = snapredLogger.getLogger(__name__)


@Singleton
class ReductionService(Service):
    """

    The reduction service coordinates preparing ingredients and workspaces for use in the reduction workflow,
    and calling the reduction recipe.

    Note that reduction requires first loading all groupings for the state, prior to preparing the ingredients.
    This is a separate call to GroceryService, before the call to load the other workspace data.

    """

    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.groceryClerk: GroceryListBuilder = GroceryListItem.builder()
        self.sousChef = SousChef()
        self.mantidSnapper = MantidSnapper(None, __name__)
        self.registerPath("", self.reduction)
        self.registerPath("ingredients", self.prepReductionIngredients)
        self.registerPath("metadata", self.getRunMetadata)
        self.registerPath("groceries", self.fetchReductionGroceries)
        self.registerPath("groupings", self.fetchReductionGroupings)
        self.registerPath("loadGroupings", self.loadAllGroupings)
        self.registerPath("save", self.saveReduction)
        self.registerPath("load", self.loadReduction)
        self.registerPath("hasState", self.hasState)
        self.registerPath("getCompatibleMasks", self.getCompatibleMasks)
        self.registerPath("getUniqueTimestamp", self.getUniqueTimestamp)
        self.registerPath("checkWritePermissions", self.checkReductionWritePermissions)
        self.registerPath("getSavePath", self.getSavePath)
        self.registerPath("getStateIds", self.getStateIds)
        self.registerPath("validate", self.validateReduction)
        self.registerPath("artificialNormalization", self.artificialNormalization)
        self.registerPath("grabWorkspaceforArtificialNorm", self.grabWorkspaceforArtificialNorm)
        self.registerPath("hasLiveDataConnection", self.hasLiveDataConnection)
        self.registerPath("getLiveMetadata", self.getLiveMetadata)
        self.registerPath("getRunMetadata", self.getRunMetadata)
        return

    @staticmethod
    def name():
        return "reduction"

    def validateReduction(self, request: ReductionRequest):
        """
        Validate the reduction request, providing specific messages if normalization
        or calibration data is missing. Notify the user if artificial normalization
        will be created when normalization is absent.

        :param request: a reduction request
        :type request: ReductionRequest
        """

        if request.alternativeCalibrationFilePath is not None:
            # If an alternative calibration file is provided, it should be a valid path.
            if not request.alternativeCalibrationFilePath.exists():
                raise RuntimeError(
                    f"Alternative calibration file '{request.alternativeCalibrationFilePath}' does not exist."
                )

        if not self.dataFactoryService.stateExists(request.runNumber):
            if self.checkCalibrationWritePermissions(request.runNumber):
                raise RecoverableException.stateUninitialized(request.runNumber, request.useLiteMode)
            else:
                # TODO: Centralize these exception strings or handling.
                raise RuntimeError(
                    " This run has not been initialized for reduction"
                    + " and you lack the necessary permissions to do so."
                    + " Please contact your IS or CIS."  # noqa: E501
                )

        continueFlags = ContinueWarning.Type.UNSET
        message = ""

        state = request.alternativeState
        if state is None:
            # If no alternativeState state is provided, use the sample's state.
            state, _ = self.dataFactoryService.constructStateId(request.runNumber)

        # Check if a normalization is present
        normalizationExists = self.dataFactoryService.normalizationExists(request.runNumber, request.useLiteMode, state)
        if not normalizationExists:
            continueFlags |= ContinueWarning.Type.MISSING_NORMALIZATION

        # Notes:
        #  * At this point, the state will be initialized.
        #  * In an initialized state, `calVersion` will never be `None`.
        #  * `MISSING_DIFFRACTION_CALIBRATION` is now the same as the former `DEFAULT_DIFFRACTION_CALIBRATION`.
        #  * The default calibration uses `VERSION_START`.
        calibrationExists = True
        if request.alternativeCalibrationFilePath is None:
            calVersion = self.dataFactoryService.getLatestApplicableCalibrationVersion(
                request.runNumber, request.useLiteMode, state
            )
            if calVersion is None:
                raise RuntimeError(
                    "Usage error: for an initialized state, "
                    "diffraction-calibration version should always be at least the default version (VERSION_START)."
                )
            if calVersion == VERSION_START():
                calibrationExists = False
                continueFlags |= ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION

        # Determine the action based on missing components
        if not calibrationExists and normalizationExists:
            # Case: No calibration but normalization exists
            message = (
                "<p><b>Diffraction calibration is missing.</b></p>"
                "<p>Default calibration will be used in place of actual calibration.</p>"
                "<p>Would you like to continue anyway?</p>"
            )
        elif calibrationExists and not normalizationExists:
            # Case: Calibration exists but normalization is missing
            message = (
                "<p><b>Normalization calibration is missing.</b></p>"
                "<p>Artificial normalization will be created in place of actual normalization.</p>"
                "<p>Would you like to continue anyway?</p>"
            )
        elif not calibrationExists and not normalizationExists:
            # Case: No calibration and no normalization
            message = (
                "<p><b>Both normalization and diffraction calibrations are missing.</b></p>"
                "<p>Default calibration will be used in place of actual calibration.</p>"
                "<p>Artificial normalization will be created in place of actual normalization.</p>"
                "<p>Would you like to continue anyway?</p>"
            )

        # Remove any continue flags that are also present in the request by XOR-ing with the request flags
        if request.continueFlags:
            continueFlags ^= request.continueFlags & continueFlags

        # If there are any continue flags set, raise a ContinueWarning with the appropriate message
        if continueFlags and message:
            raise ContinueWarning(message, continueFlags)

        # Reinitialized continue flags for the upcoming permissions check
        continueFlags = ContinueWarning.Type.UNSET

        # Check that the user has write permissions to the save directory
        # (In live-data mode, this check should not apply.)
        if not request.liveDataMode and not self.checkReductionWritePermissions(request.runNumber):
            continueFlags |= ContinueWarning.Type.NO_WRITE_PERMISSIONS

        # Remove any continue flags that are present in the request by XOR-ing with the flags
        if request.continueFlags:
            continueFlags ^= request.continueFlags & continueFlags

        if continueFlags:
            msg = ""
            path = self.getSavePath(request.runNumber)
            if path is not None:
                msg = (
                    f"<p>It looks like you don't have permissions to write to "
                    f"<br><b>{path}</b>,<br>"
                    "but you can still save using the workbench tools.</p>"
                    "<p>Would you like to continue anyway?</p>"
                )
            else:
                msg = (
                    f"<p>No IPTS-directory exists yet for run '{request.runNumber}',<br>"
                    "but you can still save using the workbench tools.</p>"
                    "<p>Would you like to continue anyway?</p>"
                )

            raise ContinueWarning(msg, continueFlags)

    @FromString
    def reduction(self, request: ReductionRequest):
        """
        Perform reduction on a single run number, once for each grouping in this state.

        :param request: a ReductionRequest object holding needed information
        :type request: ReductionRequest
        """
        startTime = datetime.utcnow()

        groupingResults = self.fetchReductionGroupings(request)
        request.focusGroups = groupingResults["focusGroups"]

        # Fetch groceries first: `prepReductionIngredients` will need the combined mask.
        groceries = self.fetchReductionGroceries(request)

        ingredients = self.prepReductionIngredients(request, groceries.get("combinedPixelMask"))

        # attach the list of grouping workspaces to the grocery dictionary
        groceries["groupingWorkspaces"] = groupingResults["groupingWorkspaces"]

        workspaceMetadata: WorkspaceMetadata = self.groceryService.getSNAPRedWorkspaceMetadata(
            groceries["inputWorkspace"]
        )
        isDiagnostic = workspaceMetadata.diffcalState != DiffcalStateMetadata.EXISTS
        isDiagnostic = isDiagnostic or workspaceMetadata.normalizationState != NormalizationStateMetadata.EXISTS
        ingredients.isDiagnostic = isDiagnostic

        data = ReductionRecipe().cook(ingredients, groceries)
        record = self._createReductionRecord(request, ingredients, data["outputs"])

        # Execution wallclock time is required by the live-data workflow loop.
        executionTime = datetime.utcnow() - startTime

        return ReductionResponse(
            record=record, unfocusedData=data.get("unfocusedWS", None), executionTime=executionTime
        )

    def _createReductionRecord(
        self, request: ReductionRequest, ingredients: ReductionIngredients, workspaceNames: List[WorkspaceName]
    ) -> ReductionRecord:
        calibration = None
        normalization = None
        state = request.alternativeState
        if state is None:
            # If no alternativeState state is provided, use the sample's state.
            state, _ = self.dataFactoryService.constructStateId(request.runNumber)

        if request.continueFlags is not None:
            # Notes:
            #   * If a diffraction calibration exists,
            #     its version will have been filled in by `fetchReductionGroceries`.
            #   * `MISSING_DIFFRACTION_CALIBRATION` now means that the default diffraction calibration
            #     with `VERSION_START` is being applied.
            calibration = self.dataFactoryService.getCalibrationRecord(
                request.runNumber, request.useLiteMode, request.versions.calibration, state
            )
            if ContinueWarning.Type.MISSING_NORMALIZATION not in request.continueFlags:
                normalization = self.dataFactoryService.getNormalizationRecord(
                    request.runNumber, request.useLiteMode, state, request.versions.normalization
                )

        return ReductionRecord(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            timestamp=request.timestamp,
            calibration=calibration,
            normalization=normalization,
            pixelGroupingParameters={
                pg.focusGroup.name: [pg[gid] for gid in pg.groupIDs] for pg in ingredients.pixelGroups
            },
            workspaceNames=workspaceNames,
            alternativeCalibrationFilePath=request.alternativeCalibrationFilePath,
            hooks=request.hooks,
            snapredVersion=Config.snapredVersion(),
            snapwrapVersion=Config.snapwrapVersion(),  # type: ignore[call-arg]
        )

    @FromString
    def fetchReductionGroupings(self, request: ReductionRequest) -> Dict[str, Any]:
        """
        Load all groupings that are valid for a specific state using a ReductionRequest.

        :param request: a reduction request with at minimum a run number and lite mode flag
        :type request: ReductionRequest
        :return: a dictionary with keys

            - "focusGroups": a list of FocusGroup objects
            - "groupingWorkspaces": a list of the grouping workspace names

        :rtype: Dict[str, Any]
        """
        # fetch all valid groups for this run state
        result = self.loadAllGroupings(request.runNumber, request.useLiteMode)
        return result

    @FromString
    def loadAllGroupings(self, runNumber: str, useLiteMode: bool) -> Dict[str, Any]:
        """
        Load all groupings that are valid for a specific state, determined from the run number
        and corresponding to lite mode setting.

        :param runNumber: the run number defining the state
        :type runNumber: str
        :param useLiteMode: the lite mode flag
        :type useLiteMode: bool
        :return: a dictionary with keys

            - "focusGroups": a list of FocusGroup objects
            - "groupingWorkspaces": a list of the grouping workspace names

        :rtype: Dict[str, Any]
        """

        # if grouping exists in state, use it
        # else refer to root for default groups
        # This branch is mostly relevent when a user proceeds without calibration.
        try:
            groupingMap = self.dataFactoryService.getGroupingMap(runNumber)
        except StateValidationException:
            groupingMap = self.dataFactoryService.getDefaultGroupingMap()

        groupingMap = groupingMap.getMap(useLiteMode)
        for focusGroup in groupingMap.values():
            self.groceryClerk.fromRun(runNumber).grouping(focusGroup.name).useLiteMode(useLiteMode).add()
        groupingWorkspaces = self.groceryService.fetchGroceryList(self.groceryClerk.buildList())
        return {
            "focusGroups": list(groupingMap.values()),
            "groupingWorkspaces": groupingWorkspaces,
        }

    # WARNING: `WorkspaceName` does not work with `@FromString`!
    def prepCombinedMask(self, request: ReductionRequest) -> WorkspaceName:
        """
        Combine all of the individual pixel masks for application and final output
        """

        """
        Implementation notes:
            * RE incoming `pixelMasks`:
            sub-selection from
              `self.dataFactoryService.getCompatibleReductionMasks(ingredients.runNumber, ingredients.useLiteMode)`
            ==> TO / FROM mask-dropdown in Reduction panel
            This MUST be a list of valid `WorkspaceName` (i.e. containing their original `builder` attribute)
        """
        runNumber, useLiteMode, timestamp, state = (
            request.runNumber,
            request.useLiteMode,
            request.timestamp,
            request.alternativeState,
        )

        if state is None:
            # If no alternativeState state is provided, use the sample's state.
            state, _ = self.dataFactoryService.constructStateId(runNumber)

        combinedMask = wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()

        # if there is a mask associated with the diffcal file, load it here
        calVersion = request.versions.calibration
        if calVersion is VersionState.LATEST:
            calVersion = self.dataFactoryService.getLatestApplicableCalibrationVersion(
                runNumber, useLiteMode, state=state
            )

        if calVersion is None:
            raise RuntimeError(
                "Usage error: for an initialized state, "
                "diffraction-calibration version should always be at least the default version (VERSION_START)."
            )

        self.groceryClerk.name("diffcalMaskWorkspace").diffcal_mask(state, calVersion, runNumber).useLiteMode(
            useLiteMode
        )
        if request.alternativeCalibrationFilePath is not None:
            self.groceryClerk.diffCalFilePath(request.alternativeCalibrationFilePath)
        self.groceryClerk.add()

        # if the user specified masks to use, also pull those
        residentMasks = {}
        for mask in request.pixelMasks:
            match mask.tokens("workspaceType"):
                case wngt.REDUCTION_PIXEL_MASK:
                    runNumber, temp_ts = mask.tokens("runNumber", "timestamp")
                    self.groceryClerk.name(mask).reduction_pixel_mask(runNumber, temp_ts).useLiteMode(useLiteMode).add()
                case wngt.REDUCTION_USER_PIXEL_MASK:
                    numberTag = mask.tokens("numberTag")
                    residentMasks[mask] = wng.reductionUserPixelMask().numberTag(numberTag).build()
                case _:
                    raise RuntimeError(
                        f"reduction pixel mask '{mask}' has unexpected workspace-type '{mask.tokens('workspaceType')}'"  # noqa: E501
                    )
        # Load all pixel masks
        allMasks = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
            **residentMasks,
        )

        self.groceryService.fetchCompatiblePixelMask(combinedMask, runNumber, useLiteMode)
        for mask in allMasks.values():
            # If there is no mask corresponding to the diffraction calibration, it will be set to the empty string.
            if bool(mask):
                self.mantidSnapper.BinaryOperateMasks(
                    f"combine from pixel mask: '{mask}'...",
                    InputWorkspace1=combinedMask,
                    InputWorkspace2=mask,
                    OperationType="OR",
                    OutputWorkspace=combinedMask,
                )

        self.mantidSnapper.executeQueue()
        return combinedMask

    @FromString
    def prepReductionIngredients(
        self, request: ReductionRequest, combinedPixelMask: Optional[WorkspaceName] = None
    ) -> ReductionIngredients:
        """
        Prepare the needed ingredients for calculating reduction.
        Requires:

            - reduction request
            - an optional combined mask workspace

        :param request: a reduction request
        :type request: ReductionRequest
        :return: The needed reduction ignredients.
        :rtype: ReductionIngredients
        """
        if request.versions is None or request.versions.calibration is None or request.versions.normalization is None:
            raise ValueError("Reduction request must have versions set")

        state = request.alternativeState
        if state is None:
            # If no alternativeState state is provided, use the sample's state.
            state, _ = self.dataFactoryService.constructStateId(request.runNumber)

        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            timestamp=request.timestamp,
            focusGroups=request.focusGroups,
            keepUnfocused=request.keepUnfocused,
            convertUnitsTo=request.convertUnitsTo,
            versions=request.versions,
            state=state,
        )
        # TODO: Skip calibrant sample if there is no calibrant
        ingredients = self.sousChef.prepReductionIngredients(farmFresh, combinedPixelMask)
        ingredients.artificialNormalizationIngredients = request.artificialNormalizationIngredients
        return ingredients

    @FromString
    def fetchReductionGroceries(self, request: ReductionRequest) -> Dict[str, Any]:
        """
        Fetch the required groceries, including

            - neutron run data
            - diffcal tables
            - normalization
            - combined pixel-mask workspace

        :param request: a reduction request
        :type request: ReductionRequest
        :return: A grocery dictionary with keys

            - "inputworkspace"
            - "diffcalWorkspace"
            - "normalizationWorkspace"
            - "combinedPixelMask"

        :rtype: Dict[str, Any]
        """
        # As an interim solution: set the request "versions" field to the latest calibration and normalization versions.
        #   TODO: set these when the request is initially generated.
        calVersion = None
        normVersion = None

        state = request.alternativeState
        if state is None:
            # If no alternativeState state is provided, use the sample's state.
            state, _ = self.dataFactoryService.constructStateId(request.runNumber)

        calVersion = self.dataFactoryService.getLatestApplicableCalibrationVersion(
            request.runNumber, request.useLiteMode, state
        )
        if calVersion is None:
            raise RuntimeError(
                "Usage error: for an initialized state, "
                "diffraction-calibration version should always be at least the default version (VERSION_START)."
            )

        if ContinueWarning.Type.MISSING_NORMALIZATION not in request.continueFlags:
            normVersion = self.dataFactoryService.getLatestApplicableNormalizationVersion(
                request.runNumber, request.useLiteMode, state
            )

        # Fetch pixel masks -- if nothing is masked, nullify
        combinedPixelMask = self.prepCombinedMask(request)
        if not self.groceryService.checkPixelMask(combinedPixelMask):
            combinedPixelMask = None

        # Build the grocery clerk items to fetch the workspaces.
        # Build item for sample run workspace
        # NOTE: diffCalVersion specifies only the metadata version in the case diffCalFilePath is provided.
        #       Else the index is used to determine the diffCalFile.
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).state(
            state
        ).dirty().diffCalVersion(calVersion)

        if request.alternativeCalibrationFilePath is not None:
            self.groceryClerk.diffCalFilePath(request.alternativeCalibrationFilePath)

        if not request.liveDataMode:
            self.groceryClerk.add()
        else:
            self.groceryClerk.liveData(duration=request.liveDataDuration).add()

        # Build item for normalization workspace
        if normVersion is not None:  # WARNING: version may be _zero_!
            self.groceryClerk.name("normalizationWorkspace").normalization(
                request.runNumber,
                state,
                normVersion,
            ).useLiteMode(request.useLiteMode).dirty().diffCalVersion(calVersion)

            if request.alternativeCalibrationFilePath is not None:
                self.groceryClerk.diffCalFilePath(request.alternativeCalibrationFilePath)

            self.groceryClerk.add()

        groceries = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
            **({"combinedPixelMask": combinedPixelMask} if bool(combinedPixelMask) else {}),
        )

        self._markWorkspaceMetadata(request, groceries["inputWorkspace"])
        return groceries

    def _markWorkspaceMetadata(self, request: ReductionRequest, workspace: WorkspaceName):
        altDiffCalFilePath = DiffcalStateMetadata.UNSET

        state = request.alternativeState
        if state is None:
            # If no alternativeState state is provided, use the sample's state.
            state, _ = self.dataFactoryService.constructStateId(request.runNumber)

        if ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION in request.continueFlags:
            calibrationState = DiffcalStateMetadata.NONE
        elif request.alternativeState is not None:
            calibrationState = DiffcalStateMetadata.ALTERNATE
            altVersion = self.dataFactoryService.getLatestApplicableCalibrationVersion(
                request.runNumber, request.useLiteMode, state=state
            )
            altDiffCalFilePath = str(
                self.dataFactoryService.getCalibrationDataPath(request.useLiteMode, altVersion, state)
            )
        elif request.alternativeCalibrationFilePath is not None:
            calibrationState = DiffcalStateMetadata.ALTERNATE
            altVersion = request.versions.calibration
            altDiffCalFilePath = str(request.alternativeCalibrationFilePath)
        else:
            calibrationState = DiffcalStateMetadata.EXISTS

        # The reduction workflow will automatically create a "fake" vanadium, so it shouldn't ever be None?
        normalizationState = (
            NormalizationStateMetadata.FAKE
            if ContinueWarning.Type.MISSING_NORMALIZATION in request.continueFlags
            else NormalizationStateMetadata.EXISTS
        )

        metadata = WorkspaceMetadata(
            diffcalState=calibrationState, normalizationState=normalizationState, altDiffcalPath=altDiffCalFilePath
        )
        self.groceryService.writeWorkspaceMetadataAsTags(workspace, metadata)

    def saveReduction(self, request: ReductionExportRequest):
        self.dataExportService.exportReductionRecord(request.record)
        self.dataExportService.exportReductionData(request.record)

    def loadReduction(self, stateId: str, timestamp: float):
        # How to implement:
        # 1) Create the file path from the stateId and the timestamp;
        # 2) Load the reduction record and workspaces using `DataFactoryService.getReductionData`.
        raise NotImplementedError("not implemented: 'ReductionService.loadReduction")

    def hasState(self, runNumber: str):
        if not RunNumberValidator.validateRunNumber(runNumber):
            logger.error(f"Invalid run number: {runNumber}")
            return False
        return self.dataFactoryService.checkCalibrationStateExists(runNumber)

    def getUniqueTimestamp(self):
        return self.dataExportService.getUniqueTimestamp()

    def checkReductionWritePermissions(self, runNumber: str) -> bool:
        try:
            path = self.dataExportService.getReductionStateRoot(runNumber)
            return self.dataExportService.checkWritePermissions(path)
        except RuntimeError as e:
            # In live-data case, sometimes there is no IPTS directory at all.
            if "Cannot find IPTS directory" not in str(e):
                raise
        return False

    def checkCalibrationWritePermissions(self, runNumber: str) -> bool:
        path = self.dataExportService.getCalibrationStateRoot(runNumber)
        return self.dataExportService.checkWritePermissions(path)

    def getSavePath(self, runNumber: str) -> Path | None:
        path = None
        try:
            path = self.dataExportService.getReductionStateRoot(runNumber)
        except RuntimeError as e:
            # In the live-data case, the IPTS-directory may not exist at all.
            if "Cannot find IPTS directory" not in str(e):
                raise
        return path

    def getStateIds(self, runNumbers: List[str]) -> List[str]:
        stateIds = []
        for runNumber in runNumbers:
            stateId, _ = self.dataFactoryService.constructStateId(runNumber)
            stateIds.append(stateId)
        return stateIds

    def _groupByStateId(self, requests: List[SNAPRequest]):
        stateIDs = {}
        for request in requests:
            runNumber = json.loads(request.payload)["runNumber"]
            stateID, _ = self.dataFactoryService.constructStateId(runNumber)
            if stateIDs.get(stateID) is None:
                stateIDs[stateID] = []
            stateIDs[stateID].append(request)
        return stateIDs

    def _groupByVanadiumVersion(self, requests: List[SNAPRequest]):
        versions = {}
        for request in requests:
            runNumber = json.loads(request.payload)["runNumber"]
            useLiteMode = bool(json.loads(request.payload)["useLiteMode"])
            state, _ = self.dataFactoryService.constructStateId(runNumber)
            normalizationVersion = self.dataFactoryService.getLatestApplicableNormalizationVersion(
                runNumber, useLiteMode, state
            )
            version = "normalization_" + str(normalizationVersion)
            if versions.get(version) is None:
                versions[version] = []
            versions[version].append(request)
        return versions

    def getCompatibleMasks(self, request: ReductionRequest) -> List[WorkspaceName]:
        runNumber, useLiteMode = request.runNumber, request.useLiteMode
        return self.dataFactoryService.getCompatibleReductionMasks(runNumber, useLiteMode)

    def artificialNormalization(self, request: CreateArtificialNormalizationRequest):
        artificialNormWorkspace = ArtificialNormalizationRecipe().executeRecipe(
            InputWorkspace=request.diffractionWorkspace,
            peakWindowClippingSize=request.peakWindowClippingSize,
            smoothingParameter=request.smoothingParameter,
            decreaseParameter=request.decreaseParameter,
            lss=request.lss,
            OutputWorkspace=request.outputWorkspace,
        )
        return artificialNormWorkspace

    def grabWorkspaceforArtificialNorm(self, request: ReductionRequest):
        # TODO: REBASE NOTE:
        #   This method actually seems to be a reduction sub-recipe:
        #     something like `PreprocessArtificialNormalizationRecipe`.
        #   It should not get "special treatment" in comparison to any other `ReductionRecipe` sub-recipes!
        #   It also isn't obvious here that incoming pixel masks should be ignored?!
        """# PROBABLY THIS SHOULD BE something like:

        groupingResults = self.fetchReductionGroupings(request)
        request.focusGroups = groupingResults["focusGroups"]

        # Fetch groceries first: `prepReductionIngredients` will need the combined mask.
        groceries = self.fetchReductionGroceries(request)

        ingredients = self.prepReductionIngredients(request, groceries.get("combinedPixelMask"))

        # attach the list of grouping workspaces to the grocery dictionary
        groceries["groupingWorkspaces"] = groupingResults["groupingWorkspaces"]

        """

        # 1. Load raw run data
        if not request.liveDataMode:
            self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        else:
            self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(
                request.useLiteMode
            ).liveData(duration=request.liveDataDuration).add()
        runWorkspace = self.groceryService.fetchGroceryList(self.groceryClerk.buildList())[0]

        # 2. Load Column group TODO: Future work to apply a more general approach
        groups = self.loadAllGroupings(request.runNumber, request.useLiteMode)
        # find column group
        columnGroup = next((group for group in groups["focusGroups"] if "column" in group.name.lower()), None)
        columnGroupWorkspace = next(
            (group for group in groups["groupingWorkspaces"] if "column" in group.lower()), None
        )
        request.focusGroups = [columnGroup]

        # 2.5. get ingredients
        ingredients = self.prepReductionIngredients(request)

        artNormBasisWorkspace = (
            wng.artificialNormalizationPreview()
            .runNumber(request.runNumber)
            .group(wng.Groups.COLUMN)
            .type(wng.ArtificialNormWorkspaceType.SOURCE)
            .build()
        )

        ConvertUnitsRecipe().executeRecipe(
            InputWorkspace=runWorkspace,
            OutputWorkspace=artNormBasisWorkspace,
            Target="dSpacing",
            Emode="Elastic",
        )

        groceries = {
            "inputWorkspace": artNormBasisWorkspace,
            "groupingWorkspace": columnGroupWorkspace,
            "outputWorkspace": artNormBasisWorkspace,
        }

        # 3. Diffraction Focus Spectra, including rebinning
        ReductionGroupProcessingRecipe().cook(ingredients.groupProcessing(0), groceries)

        # 4. Return the result
        return artNormBasisWorkspace

    def hasLiveDataConnection(self) -> bool:
        """For 'live data' methods: verify that there is a listener connection to the instrument."""
        return self.dataFactoryService.hasLiveDataConnection()

    def getLiveMetadata(self) -> RunMetadata:
        return self.dataFactoryService.getLiveMetadata()

    def getRunMetadata(self, runNumber: str) -> RunMetadata:
        return self.dataFactoryService.getRunMetadata(runNumber)
