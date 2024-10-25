import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Dict, List

from snapred.backend.dao.ingredients import (
    ArtificialNormalizationIngredients,
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
from snapred.backend.dao.request.ReductionRequest import Versions
from snapred.backend.dao.response.ReductionResponse import ReductionResponse
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.GenericRecipe import ArtificialNormalizationRecipe
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceName,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceNameGenerator as wng,
)
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
        self.registerPath("checkWritePermissions", self.checkWritePermissions)
        self.registerPath("getSavePath", self.getSavePath)
        self.registerPath("getStateIds", self.getStateIds)
        self.registerPath("validateReduction", self.validateReduction)
        self.registerPath("artificialNormalization", self.artificialNormalization)
        self.registerPath("grabDiffractionWorkspaceforArtificialNorm", self.grabDiffractionWorkspaceforArtificialNorm)
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
        continueFlags = ContinueWarning.Type.UNSET
        useArtificialNorm = False
        message = ""

        # Check if a normalization is present
        normalizationExists = self.dataFactoryService.normalizationExists(request.runNumber, request.useLiteMode)
        # Check if a diffraction calibration is present
        calibrationExists = self.dataFactoryService.calibrationExists(request.runNumber, request.useLiteMode)

        # Determine the action based on missing components
        if not calibrationExists and not normalizationExists:
            # Case: No calibration and no normalization
            continueFlags |= ContinueWarning.Type.MISSING_CALIBRATION | ContinueWarning.Type.MISSING_NORMALIZATION
            message = (
                "Reduction is missing both normalization and calibration data. "
                "Would you like to continue in uncalibrated mode?"
            )
        elif calibrationExists and not normalizationExists:
            # Case: Calibration exists but normalization is missing
            continueFlags |= ContinueWarning.Type.MISSING_NORMALIZATION
            useArtificialNorm = True
            message = (
                "Reduction is missing normalization data. "
                "Artificial normalization will be created in place of actual normalization. "
                "Would you like to continue?"
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
        if not self.checkWritePermissions(request.runNumber):
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
        return useArtificialNorm

    @FromString
    def reduction(self, request: ReductionRequest):
        """
        Perform reduction on a single run number, once for each grouping in this state.

        :param request: a ReductionRequest object holding needed information
        :type request: ReductionRequest
        """

        groupingResults = self.fetchReductionGroupings(request)
        request.focusGroups = groupingResults["focusGroups"]
        ingredients = self.prepReductionIngredients(request)

        groceries = self.fetchReductionGroceries(request)
        # attach the list of grouping workspaces to the grocery dictionary
        groceries["groupingWorkspaces"] = groupingResults["groupingWorkspaces"]

        data = ReductionRecipe().cook(ingredients, groceries)
        record = self._createReductionRecord(request, ingredients, data["outputs"])
        return ReductionResponse(record=record, unfocusedData=data.get("unfocusedWS", None))

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
    def prepCombinedMask(
        self, runNumber: str, useLiteMode: bool, timestamp: float, pixelMasks: Iterable[WorkspaceName]
    ) -> WorkspaceName:
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
        combinedMask = wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()
        self.groceryService.fetchCompatiblePixelMask(combinedMask, runNumber, useLiteMode)
        for n, mask in enumerate(pixelMasks):
            self.mantidSnapper.BinaryOperateMasks(
                f"combine from pixel mask {n}...",
                InputWorkspace1=combinedMask,
                InputWorkspace2=mask,
                OperationType="OR",
                OutputWorkspace=combinedMask,
            )
        self.mantidSnapper.executeQueue()
        return combinedMask

    @FromString
    def prepReductionIngredients(self, request: ReductionRequest) -> ReductionIngredients:
        """
        Prepare the needed ingredients for calculating reduction.
        Requires:

            - runNumber
            - lite mode flag
            - timestamp
            - at least one focus group specified
            - a smoothing parameter
            - a calibrant sample path
            - a peak threshold

        :param request: a reduction request
        :type request: ReductionRequest
        :return: The needed reduction ignredients.
        :rtype: ReductionIngredients
        """
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
        return self.sousChef.prepReductionIngredients(farmFresh)

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
            - "maskWorkspace"

        :rtype: Dict[str, Any]
        """
        # Fetch pixel masks
        residentMasks = {}
        combinedMask = None
        if request.pixelMasks:
            for mask in request.pixelMasks:
                match mask.tokens("workspaceType"):
                    case wngt.REDUCTION_PIXEL_MASK:
                        runNumber, timestamp = mask.tokens("runNumber", "timestamp")
                        self.groceryClerk.name(mask).reduction_pixel_mask(runNumber, timestamp).useLiteMode(
                            request.useLiteMode
                        ).add()
                    case wngt.REDUCTION_USER_PIXEL_MASK:
                        numberTag = mask.tokens("numberTag")
                        residentMasks[mask] = wng.reductionUserPixelMask().numberTag(numberTag).build()
                    case _:
                        raise RuntimeError(
                            f"reduction pixel mask '{mask}' has unexpected workspace-type '{mask.tokens('workspaceType')}'"  # noqa: E501
                        )

            # Load any non-resident pixel masks
            maskGroceries = self.groceryService.fetchGroceryDict(
                self.groceryClerk.buildDict(),
                **residentMasks,
            )
            # combine all of the pixel masks, for application and final output
            combinedMask = self.prepCombinedMask(
                request.runNumber, request.useLiteMode, request.timestamp, maskGroceries.values()
            )

        # gather the input workspace and the diffcal table
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()

        # As an interim solution: set the request "versions" field to the latest calibration and normalization versions.
        #   TODO: set these when the request is initially generated.
        calVersion = None
        normVersion = None
        calVersion = self.dataFactoryService.getThisOrLatestCalibrationVersion(request.runNumber, request.useLiteMode)
        self.groceryClerk.name("diffcalWorkspace").diffcal_table(request.runNumber, calVersion).useLiteMode(
            request.useLiteMode
        ).add()

        if ContinueWarning.Type.MISSING_NORMALIZATION not in request.continueFlags:
            normVersion = self.dataFactoryService.getThisOrLatestNormalizationVersion(
                request.runNumber, request.useLiteMode
            )
            self.groceryClerk.name("normalizationWorkspace").normalization(request.runNumber, normVersion).useLiteMode(
                request.useLiteMode
            ).add()

        request.versions = Versions(
            calVersion,
            normVersion,
        )

        return self.groceryService.fetchGroceryDict(
            groceryDict=self.groceryClerk.buildDict(),
            **({"maskWorkspace": combinedMask} if combinedMask else {}),
        )

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

    def checkWritePermissions(self, runNumber: str) -> bool:
        path = self.dataExportService.getReductionStateRoot(runNumber)
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
            normalizationVersion = self.dataFactoryService.getThisOrCurrentNormalizationVersion(runNumber, useLiteMode)
            version = "normalization_" + str(normalizationVersion)
            if versions.get(version) is None:
                versions[version] = []
            versions[version].append(request)
        return versions

    def getCompatibleMasks(self, request: ReductionRequest) -> List[WorkspaceName]:
        runNumber, useLiteMode = request.runNumber, request.useLiteMode
        return self.dataFactoryService.getCompatibleReductionMasks(runNumber, useLiteMode)

    def artificialNormalization(self, request: CreateArtificialNormalizationRequest):
        ingredients = ArtificialNormalizationIngredients(
            peakWindowClippingSize=request.peakWindowClippingSize,
            smoothingParameter=request.smoothingParameter,
            decreaseParameter=request.decreaseParameter,
            lss=request.lss,
        )
        artificialNormWorkspace = ArtificialNormalizationRecipe().executeRecipe(
            InputWorkspace=request.diffractionWorkspace,
            Ingredients=ingredients,
        )
        return artificialNormWorkspace

    def grabDiffractionWorkspaceforArtificialNorm(self, request: ReductionRequest):
        calVersion = None
        calVersion = self.dataFactoryService.getThisOrLatestCalibrationVersion(request.runNumber, request.useLiteMode)
        groceryList = (
            self.groceryClerk.name("diffractionWorkspace")
            .diffcal_output(request.runNumber, calVersion)
            .useLiteMode(request.useLiteMode)
            .unit(wng.Units.DSP)
            .group("column")
            .buildDict()
        )

        groceries = self.groceryService.fetchGroceryDict(groceryList)
        diffractionWorkspace = groceries.get("diffractionWorkspace")
        return diffractionWorkspace
