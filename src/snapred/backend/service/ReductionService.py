import json
from collections.abc import Iterable
from typing import Any, Dict, List

from snapred.backend.dao.ingredients import GroceryListItem, ReductionIngredients
from snapred.backend.dao.request import (
    FarmFreshIngredients,
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.request.ReductionRequest import Versions
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
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
        return

    @staticmethod
    def name():
        return "reduction"

    @FromString
    def reduction(self, request: ReductionRequest):
        """
        Perform reduction on a list of run numbers, once for each grouping in this state.

        :param request: a ReductionRequest object holding needed information
        :type request: ReductionRequest
        """
        groupingResults = self.fetchReductionGroupings(request)
        request.focusGroups = groupingResults["focusGroups"]

        ingredients = self.prepReductionIngredients(request)
        groceries = self.fetchReductionGroceries(request)
        # attach the list of grouping workspaces to the grocery dictionary
        groceries["groupingWorkspaces"] = groupingResults["groupingWorkspaces"]

        return ReductionRecipe().cook(ingredients, groceries)

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
        # fetch all valid groups for this run state
        groupingMap = self.dataFactoryService.getGroupingMap(runNumber).getMap(useLiteMode)
        for focusGroup in groupingMap.values():
            self.groceryClerk.fromRun(runNumber).grouping(focusGroup.name).useLiteMode(useLiteMode).add()
        groupingWorkspaces = self.groceryService.fetchGroceryList(self.groceryClerk.buildList())
        return {
            "focusGroups": list(groupingMap.values()),
            "groupingWorkspaces": groupingWorkspaces,
        }

    # WARNING: `WorkspaceName` does not work with `@FromString`!
    def prepCombinedMask(self, runNumber: str, useLiteMode: bool, pixelMasks: Iterable[WorkspaceName]) -> WorkspaceName:
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

        # no reduction timestamp has been assigned yet
        combinedMask = wng.reductionPixelMask().runNumber(runNumber).build()
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
            focusGroups=request.focusGroups,
            keepUnfocused=request.keepUnfocused,
            convertUnitsTo=request.convertUnitsTo,
            versions=request.versions,
        )
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
            combinedMask = self.prepCombinedMask(request.runNumber, request.useLiteMode, maskGroceries.values())

        # gather the input workspace and the diffcal table
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()

        # As an interim solution: set the request "versions" field to the latest calibration and normalization versions.
        #   TODO: set these when the request is initially generated.
        request.versions = Versions(
            self.dataFactoryService.getThisOrLatestCalibrationVersion(request.runNumber, request.useLiteMode),
            self.dataFactoryService.getThisOrLatestNormalizationVersion(request.runNumber, request.useLiteMode),
        )

        self.groceryClerk.name("diffcalWorkspace").diffcal_table(
            request.runNumber, request.versions.calibration
        ).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("normalizationWorkspace").normalization(
            request.runNumber, request.versions.normalization
        ).useLiteMode(request.useLiteMode).add()

        return self.groceryService.fetchGroceryDict(
            groceryDict=self.groceryClerk.buildDict(),
            **({"maskWorkspace": combinedMask} if combinedMask else {}),
        )

    def saveReduction(self, request: ReductionExportRequest):
        record = request.reductionRecord
        self.dataExportService.exportReductionRecord(record)
        self.dataExportService.exportReductionData(record)

    def loadReduction(self, stateId: str, timestamp: float):
        # 1) Create the file path from the stateId and the timestamp;
        # 2) Load the reduction record and workspaces using `DataFactoryService.getReductionData`.
        raise NotImplementedError("not implemented: 'ReductionService.loadReduction")

    def hasState(self, runNumber: str):
        if not RunNumberValidator.validateRunNumber(runNumber):
            logger.error(f"Invalid run number: {runNumber}")
            return False
        return self.dataFactoryService.checkCalibrationStateExists(runNumber)

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
