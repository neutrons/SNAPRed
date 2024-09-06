import json
from pathlib import Path
from typing import Any, Dict, List

from snapred.backend.dao.ingredients import GroceryListItem, ReductionIngredients
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
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
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
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
        self.registerPath("", self.reduction)
        self.registerPath("ingredients", self.prepReductionIngredients)
        self.registerPath("groceries", self.fetchReductionGroceries)
        self.registerPath("groupings", self.fetchReductionGroupings)
        self.registerPath("loadGroupings", self.loadAllGroupings)
        self.registerPath("save", self.saveReduction)
        self.registerPath("load", self.loadReduction)
        self.registerPath("hasState", self.hasState)
        self.registerPath("getUniqueTimestamp", self.getUniqueTimestamp)
        self.registerPath("checkWritePermissions", self.checkWritePermissions)
        self.registerPath("getSavePath", self.getSavePath)
        self.registerPath("getStateIds", self.getStateIds)
        return

    @staticmethod
    def name():
        return "reduction"

    def validateReduction(self, request: ReductionRequest):
        """
        Validate the reduction request.

        :param request: a reduction request
        :type request: ReductionRequest
        """

        ## CONTINUE WITHOUT CALIBRATION: NOT YET SUPPORTED IN 3.0.1 ##

        if (
            self.dataFactoryService.getNormalizationRecord(request.runNumber, request.useLiteMode) is None
            or self.dataFactoryService.getCalibrationRecord(request.runNumber, request.useLiteMode) is None
        ):
            raise RuntimeError(
                f"<p><b>Reduction is missing calibration data for run '{request.runNumber}'</b>:<br>"
                + "you will need to run diffraction-calibration and normalization-calibration before continuing.</p>"
            )

        # ... ensure separate continue warnings ...
        continueFlags = ContinueWarning.Type.UNSET

        # check that the user has write permissions to the save directory
        if not self.checkWritePermissions(request.runNumber):
            continueFlags |= ContinueWarning.Type.NO_WRITE_PERMISSIONS

        # remove any continue flags that are present in the request by xor-ing with the flags
        if request.continueFlags:
            continueFlags = continueFlags ^ (request.continueFlags & continueFlags)

        if continueFlags:
            raise ContinueWarning(
                f"<p>It looks like you don't have permissions to write to "
                f"<br><b>{self.getSavePath(request.runNumber)}</b>,<br>"
                + "but you can still save using the workbench tools.</p>"
                + "<p>Would you like to continue anyway?</p>",
                continueFlags,
            )

    @FromString
    def reduction(self, request: ReductionRequest):
        """
        Perform reduction on a single run number, once for each grouping in this state.

        :param request: a ReductionRequest object holding needed information
        :type request: ReductionRequest
        """
        self.validateReduction(request)

        groupResults = self.fetchReductionGroupings(request)
        focusGroups = groupResults["focusGroups"]
        groupingWorkspaces = groupResults["groupingWorkspaces"]
        request.focusGroups = focusGroups

        ingredients = self.prepReductionIngredients(request)

        groceries = self.fetchReductionGroceries(request)
        # attach the list of grouping workspaces to the grocery dictionary
        groceries["groupingWorkspaces"] = groupingWorkspaces

        data = ReductionRecipe().cook(ingredients, groceries)
        record = self._createReductionRecord(request, ingredients, data["outputs"])
        return ReductionResponse(record=record)

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
        else:
            breakpoint()

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
        self.dataExportService.exportReductionRecord(request.record)
        self.dataExportService.exportReductionData(request.record)

    def loadReduction(self, *, stateId: str, timestamp: float):
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
