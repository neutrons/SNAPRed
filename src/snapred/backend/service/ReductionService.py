import json
from typing import Any, Dict, List

from snapred.backend.dao.ingredients import GroceryListItem, ReductionIngredients
from snapred.backend.dao.request import (
    FarmFreshIngredients,
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.response.ReductionResponse import ReductionResponse
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
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

    dataFactoryService: "DataFactoryService"
    dataExportService: "DataExportService"

    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.groceryClerk = GroceryListItem.builder()
        self.sousChef = SousChef()
        self.registerPath("", self.reduction)
        self.registerPath("ingredients", self.prepReductionIngredients)
        self.registerPath("groceries", self.fetchReductionGroceries)
        self.registerPath("groupings", self.fetchReductionGroupings)
        self.registerPath("loadGroupings", self.loadAllGroupings)
        self.registerPath("save", self.saveReduction)
        self.registerPath("load", self.loadReduction)
        self.registerPath("hasState", self.hasState)
        return

    @staticmethod
    def name():
        return "reduction"

    @FromString
    def fakeMethod(self):  # pragma: no cover
        # NOTE this is not a real method
        # it's here to be used in the registered paths above, for the moment
        # when possible this and its registered paths should be deleted
        raise NotImplementedError("You tried to access an invalid path in the reduction service.")

    @FromString
    def reduction(self, request: ReductionRequest):
        """
        Perform reduction on a list of run numbers, once for each grouping in this state.

        :param request: a ReductionRequest object holding needed information
        :type request: ReductionRequest
        """
        groupResults = self.fetchReductionGroupings(request)
        focusGroups = groupResults["focusGroups"]
        groupingWorkspaces = groupResults["groupingWorkspaces"]
        request.focusGroups = focusGroups

        ingredients = self.prepReductionIngredients(request)

        groceries = self.fetchReductionGroceries(request)
        # attach the list of grouping workspaces to the grocery dictionary
        groceries["groupingWorkspaces"] = groupingWorkspaces

        data = ReductionRecipe().cook(ingredients, groceries)
        return ReductionResponse(
            workspaces=data["outputs"],
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
        res = self.loadAllGroupings(request.runNumber, request.useLiteMode)
        return res

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
            focusGroup=request.focusGroups,
            keepUnfocused=request.keepUnfocused,
            convertUnitsTo=request.convertUnitsTo,
        )
        return self.sousChef.prepReductionIngredients(farmFresh)

    @FromString
    def fetchReductionGroceries(self, request: ReductionRequest) -> Dict[str, Any]:
        """
        Fetch the required groceries, including

            - neutron run data
            - diffcal tables
            - pixel mask workspaces
            - normalizations

        :param request: a reduction request
        :type request: ReductionRequest
        :return: A grocery dictionary with keys

            - "inputworkspace"
            - "diffcalWorkspace"
            - "normalizationWorkspace"

        :rtype: Dict[str, Any]
        """
        # gather input workspace and the diffcal table
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("diffcalWorkspace").diffcal_table(request.runNumber).useLiteMode(
            request.useLiteMode
        ).add()
        self.groceryClerk.name("normalizationWorkspace").normalization(request.runNumber).useLiteMode(
            request.useLiteMode
        ).add()
        return self.groceryService.fetchGroceryDict(groceryDict=self.groceryClerk.buildDict())

    @FromString
    def saveReduction(self, request: ReductionExportRequest):
        record = request.reductionRecord
        record = self.dataExportService.exportReductionRecord(record)
        record = self.dataExportService.exportReductionData(record)

    def loadReduction(self):
        raise NotImplementedError("SNAPRed cannot load reductions")

    def hasState(self, runNumber: str):
        if not RunNumberValidator.validateRunNumber(runNumber):
            logger.error(f"Invalid run number: {runNumber}")
            return False
        return self.dataFactoryService.checkCalibrationStateExists(runNumber)

    def _groupByStateId(self, requests: List[SNAPRequest]):
        stateIDs = {}
        for request in requests:
            runNumber = str(json.loads(request.payload)["runNumber"])
            stateID, _ = self.dataFactoryService.constructStateId(runNumber)
            if stateIDs.get(stateID) is None:
                stateIDs[stateID] = []
            stateIDs[stateID].append(request)
        return stateIDs

    def _groupByVanadiumVersion(self, requests: List[SNAPRequest]):
        versions = {}
        for request in requests:
            runNumber = str(json.loads(request.payload)["runNumber"])
            stateID, _ = self.dataFactoryService.constructStateId(runNumber)
            useLiteMode = bool(json.loads(request.payload)["useLiteMode"])
            normalVersion = self.dataFactoryService.getNormalizationVersion(str(stateID), useLiteMode)
            version = "normalization_" + str(normalVersion)
            if versions.get(version) is None:
                versions[version] = []
            versions[version].append(request)
        return versions
