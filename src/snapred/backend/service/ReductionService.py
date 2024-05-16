import time

from snapred.backend.dao.ingredients import (
    GroceryListItem,
)
from snapred.backend.dao.normalization import (
    NormalizationIndexEntry,
)
from snapred.backend.dao.request import (
    FarmFreshIngredients,
    ReductionRequest,
)
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe import ReductionRecipe
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class ReductionService(Service):
    """

    This service orchestrates reduction of scientific data, utilizing a range of data objects,
    services, and recipes.  It is a pivotal component designed to streamline the reductionn workflow,
    ensuring efficiency and accuracy across operations.
    Use it wisely.

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
        self.registerPath("save", self.saveReduction)
        self.registerPath("reduction", self.fakeReduction)
        self.registerPath("hasState", self.hasState)
        return

    @staticmethod
    def name():
        return "reduction"

    @FromString
    def reduction(self, request: ReductionRequest):
        if not self._sameStates(request.runNumber, request.backgroundRunNumber):
            raise ValueError("Run number and background run number must be of the same Instrument State.")

        # prepare ingredients
        cifPath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            smoothingParameter=request.smoothingParameter,
            peakIntensityThreshold=request.peakIntensityThreshold,
        )
        ingredients = self.sousChef.prepReductionIngredients(farmFresh)

        # fetch all groups into a grocery list
        groupingMap = self.dataFactoryService.getGroupingMap(request.runNumber).getMap(request.useLiteMode)
        for key, value in groupingMap.items():
            self.groceryClerk.fromRun(request.runNumber).grouping(value.name).useLiteMode(request.useLiteMode).add()
        groupingWorkspaces = self.groceryService.fetchGroceryList(self.groceryClerk.buildList())

        # gather input workspace and the diffcal table
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("diffcalWorkspace").diffcal_table(request.runNumber).add()
        self.groceryClerk.name("normalizationWorkspace").normalization(request.runNumber).add()
        groceries = GroceryService().fetchGroceryDict(groceryDict=self.groceryClerk.buildDict())
        # attach the list of grouping workspaces to the grocery dictionary
        groceries["groupingWorkspaces"] = groupingWorkspaces

        return ReductionRecipe.cook(ingredients, groceries)

    @FromString
    def saveReduction(self, request):
        entry = request.normalizationIndexEntry
        version = entry.version
        normalizationRecord = request.normalizationRecord
        normalizationRecord.version = version
        normalizationRecord = self.dataExportService.exportNormalizationRecord(normalizationRecord)
        normalizationRecord = self.dataExportService.exportNormalizationWorkspaces(normalizationRecord)
        entry.version = normalizationRecord.version
        self.saveReductionToIndex(entry)

    def saveReductionToIndex(self, entry: NormalizationIndexEntry):
        if entry.appliesTo is None:
            entry.appliesTo = ">" + entry.runNumber
        if entry.timestamp is None:
            entry.timestamp = int(round(time.time() * 1000))
        logger.info(f"Saving normalization index entry for Run Number {entry.runNumber}")
        self.dataExportService.exportReductionIndexEntry(entry)

    def fakeReduction(self):
        raise NotImplementedError("Reduction does not exist yet.")

    def hasState(self, runNumber: str):
        return self.dataFactoryService.checkCalibrationStateExists(runNumber)
