import json
import time
from typing import Dict, List

from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class ReductionService(Service):
    dataFactoryService: "DataFactoryService"
    dataExportService: "DataExportService"

    def __init__(self):
        super.__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.sousChef = SousChef()
        self.registerPath("reduction", self.fakeReduction)
        self.registerPath("hasState", self.hasState)

    def fakeReduction(self):
        raise NotImplementedError("Reduction does not exist yet.")

    def hasState(self, runNumber: str):
        return self.dataFactoryService.checkCalibrationStateExists(runNumber)
