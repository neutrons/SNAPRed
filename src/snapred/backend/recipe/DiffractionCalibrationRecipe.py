from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.CalculateOffsetDIFC import (
    name as CalculateOffsetDIFC,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class DiffractionCalibrationRecipe:
    offsetDIFCAlgorithmName: str = CalculateOffsetDIFC

    def __init__(self):
        pass

    def chopIngredients(self, ingredients: DiffractionCalibrationIngredients):
        pass

    def executeRecipe(self, ingredients: DiffractionCalibrationIngredients) -> Dict[str, Any]:
        logger.info("Executing recipe for runId: %s" % ingredients.runConfig.runNumber)
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.offsetDIFCAlgorithmName)
        algo.setProperty("DiffractionCalibrationIngredients", ingredients.json())

        try:
            # TODO:
            # here we need to call SNAPRed equivalents of:
            # snp.instantiateGroupingWS
            # snp.inotGroupingParams
            # snp.peakPosFromCif
            # snp.removeOverlappingPeaks

            # TODO:
            # should work with two different algorithms, CalculateOffsetDIFC, and GroupByGroupCalibration
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished executing recipe for runId: %s" % ingredients.runConfig.runNumber)
        return data
