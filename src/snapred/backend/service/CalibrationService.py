import time
from typing import List

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.CalibrationReductionRecipe import CalibrationReductionRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class CalibrationService(Service):
    _name = "calibration"
    dataFactoryService = DataFactoryService()
    dataExportService = DataExportService()

    # register the service in ServiceFactory please!
    def __init__(self):
        self.registerPath("reduction", self.reduction)
        self.registerPath("save", self.saveCalibrationToIndex)
        return

    def name(self):
        return self._name

    @FromString
    def reduction(self, runs: List[RunConfig]):
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        for run in runs:
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
            try:
                CalibrationReductionRecipe().executeRecipe(reductionIngredients)
            except:
                raise
        return {}

    @FromString
    def saveCalibrationToIndex(self, entry: CalibrationIndexEntry):
        if entry.appliesTo is None:
            entry.appliesTo = ">" + entry.runNumber
        if entry.timestamp is None:
            entry.timestamp = int(round(time.time() * 1000))
        logger.info("Saving calibration index entry for Run Number {}".format(entry.runNumber))
        self.dataExportService.exportCalibrationIndexEntry(entry)
