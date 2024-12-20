import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from snapred.backend.dao import LiveMetadata
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
from snapred.backend.recipe.GenericRecipe import ArtificialNormalizationRecipe
from snapred.backend.recipe.RebinFocussedGroupDataRecipe import RebinFocussedGroupDataRecipe
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
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
        self.groceryClerk = GroceryListItem.builder()
        self.sousChef = SousChef()
        self.mantidSnapper = MantidSnapper(None, __name__)
        self.registerPath("", self.reduction)
        self.registerPath("ingredients", self.prepReductionIngredients)
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

        # Check if a normalization is present
        normalizationExists = self.dataFactoryService.normalizationExists(request.runNumber, request.useLiteMode)
        # Check if a diffraction calibration is present
        calibrationExists = self.dataFactoryService.calibrationExists(request.runNumber, request.useLiteMode)

        # Determine the action based on missing components
        if not calibrationExists and normalizationExists:
            # Case: No calibration but normalization exists
            continueFlags |= ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION
            message = (
                "Warning: diffraction calibration is missing."
                "If you continue, default instrument geometry will be used."
            )
        elif calibrationExists and not normalizationExists:
            # Case: Calibration exists but normalization is missing
            continueFlags |= ContinueWarning.Type.MISSING_NORMALIZATION
            message = (
                "Warning: Reduction is missing normalization data. "
                "Artificial normalization will be created in place of actual normalization. "
                "Would you like to continue?"
            )
        elif not calibrationExists and not normalizationExists:
            # Case: No calibration and no normalization
            continueFlags |= (
                ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION | ContinueWarning.Type.MISSING_NORMALIZATION
            )
            message = (
                "Warning: Reduction is missing both normalization and calibration data. "
                "If you continue, default instrument geometry will be used and data will be artificially normalized. "
            )

        # Remove any continue flags that are present in the request by XOR-ing with the flags
        if request.continueFlags:
            continueFlags ^= request.continueFlags & continueFlags

        # If there are any continue flags set, raise a ContinueWarning with the appropriate message
        if continueFlags and message:
            raise ContinueWarning(message, continueFlags)

        # Ensure separate continue warnings for permission check
        continueFlags = ContinueWarning.Type.UNSET

        # Check that the user has write permissions to the save directory
        if not self.checkReductionWritePermissions(request.runNumber):
            continueFlags |= ContinueWarning.Type.NO_WRITE_PERMISSIONS

        # Remove any continue flags that are present in the request by XOR-ing with the flags
        if request.continueFlags:
            continueFlags ^= request.continueFlags & continueFlags

        if continueFlags:
            raise ContinueWarning(
                f"<p>It looks like you don't have permissions to write to "
                f"<br><b>{self.getSavePath(request.runNumber)}</b>,<br>"
                "but you can still save using the workbench tools.</p>"
                "<p>Would you like to continue anyway?</p>",
                continueFlags,
            )

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
        if request.continueFlags is not None:
            if ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION not in request.continueFlags:
                # If a diffraction calibration exists,
                #   its version will have been filled in by `fetchReductionGroceries`.
                calibration = self.dataFactoryService.getCalibrationRecord(
                    request.runNumber, request.useLiteMode, request.versions.calibration
                )
            if ContinueWarning.Type.MISSING_NORMALIZATION not in request.continueFlags:
                normalization = self.dataFactoryService.getNormalizationRecord(
                    request.runNumber, request.useLiteMode, request.versions.normalization
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
        runNumber, useLiteMode, timestamp = request.runNumber, request.useLiteMode, request.timestamp
        combinedMask = wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()

        # if there is a mask associated with the diffcal file, load it here
        calVersion = None
        if ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION not in request.continueFlags:
            calVersion = self.dataFactoryService.getLatestApplicableCalibrationVersion(runNumber, useLiteMode)
        if calVersion is not None:  # WARNING: version may be _zero_!
            self.groceryClerk.name("diffcalMaskWorkspace").diffcal_mask(runNumber, calVersion).useLiteMode(
                useLiteMode
            ).add()

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

        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            timestamp=request.timestamp,
            focusGroups=request.focusGroups,
            keepUnfocused=request.keepUnfocused,
            convertUnitsTo=request.convertUnitsTo,
            versions=request.versions,
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
        if ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION not in request.continueFlags:
            calVersion = self.dataFactoryService.getLatestApplicableCalibrationVersion(
                request.runNumber, request.useLiteMode
            )
        if ContinueWarning.Type.MISSING_NORMALIZATION not in request.continueFlags:
            normVersion = self.dataFactoryService.getLatestApplicableNormalizationVersion(
                request.runNumber, request.useLiteMode
            )

        # Fetch pixel masks -- if nothing is masked, nullify
        combinedPixelMask = self.prepCombinedMask(request)
        if not self.groceryService.checkPixelMask(combinedPixelMask):
            combinedPixelMask = None

        # gather the input workspace and the diffcal table
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode)
        if not request.liveDataMode:
            self.groceryClerk.add()
        else:
            self.groceryClerk.liveData(duration=request.liveDataDuration).add()

        if calVersion is not None:
            self.groceryClerk.name("diffcalWorkspace").diffcal_table(request.runNumber, calVersion).useLiteMode(
                request.useLiteMode
            ).add()

        if normVersion is not None:  # WARNING: version may be _zero_!
            self.groceryClerk.name("normalizationWorkspace").normalization(request.runNumber, normVersion).useLiteMode(
                request.useLiteMode
            ).add()

        groceries = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
            **({"combinedPixelMask": combinedPixelMask} if bool(combinedPixelMask) else {}),
        )

        self._markWorkspaceMetadata(request, groceries["inputWorkspace"])

        return groceries

    def _markWorkspaceMetadata(self, request: ReductionRequest, workspace: WorkspaceName):
        calibrationState = (
            DiffcalStateMetadata.NONE
            if ContinueWarning.Type.MISSING_DIFFRACTION_CALIBRATION in request.continueFlags
            else DiffcalStateMetadata.EXISTS
        )
        # The reduction workflow will automatically create a "fake" vanadium, so it shouldnt ever be None?
        normalizationState = (
            NormalizationStateMetadata.FAKE
            if ContinueWarning.Type.MISSING_NORMALIZATION in request.continueFlags
            else NormalizationStateMetadata.EXISTS
        )
        metadata = WorkspaceMetadata(diffcalState=calibrationState, normalizationState=normalizationState)
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
        path = self.dataExportService.getReductionStateRoot(runNumber)
        return self.dataExportService.checkWritePermissions(path)

    def checkCalibrationWritePermissions(self, runNumber: str) -> bool:
        path = self.dataExportService.getCalibrationStateRoot(runNumber)
        return self.dataExportService.checkWritePermissions(path)

    def getSavePath(self, runNumber: str) -> Path:
        return self.dataExportService.getReductionStateRoot(runNumber)

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
            normalizationVersion = self.dataFactoryService.getLatestApplicableNormalizationVersion(
                runNumber, useLiteMode
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
        groceries = {
            "inputWorkspace": runWorkspace,
            "groupingWorkspace": columnGroupWorkspace,
            "outputWorkspace": artNormBasisWorkspace,
        }

        # 3. Diffraction Focus Spectra
        ReductionGroupProcessingRecipe().cook(ingredients.groupProcessing(0), groceries)

        # 4. Rebin
        rebinIngredients = RebinFocussedGroupDataRecipe.Ingredients(
            pixelGroup=ingredients.pixelGroups[0], preserveEvents=True
        )

        # NOTE: This is PURPOSELY re-instanced to support testing.
        #       `assert_called_with` DOES NOT deep copy the dictionary.
        #       Thus re-using the above dict would fail the test.
        groceries = {"inputWorkspace": artNormBasisWorkspace}

        rebinResult = RebinFocussedGroupDataRecipe().cook(rebinIngredients, groceries)
        # 5. Return the rebin result
        return rebinResult

    def hasLiveDataConnection(self) -> bool:
        """For 'live data' methods: test if there is a listener connection to the instrument."""
        return self.dataFactoryService.hasLiveDataConnection()

    def getLiveMetadata(self) -> LiveMetadata:
        return self.dataFactoryService.getLiveMetadata()
