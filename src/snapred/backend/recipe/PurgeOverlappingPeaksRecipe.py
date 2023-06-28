from mantid.api import AlgorithmManager

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.InstrumentState import InstrumentState
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import (
    name as PurgeOverlappingPeaksAlgorithm,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class PurgeOverlappingPeaksRecipe:
    purgeAlgorithmName: str = PurgeOverlappingPeaksAlgorithm

    def __init__(self):
        pass

    def executeRecipe(self, instrumentState: InstrumentState, crystalInfo: CrystallographicInfo, numFocusGroups: int):
        logger.info("Executing recipe")
        data = None

        algo = AlgorithmManager.create(self.purgeAlgorithmName)
        algo.setProperty("InstrumentState", instrumentState.json())
        algo.setProperty("CrystalInfo", crystalInfo.json())
        algo.setProperty("NumFocusGroups", numFocusGroups)
        try:
            algo.execute()
            data = algo.getProperty("OutputPeakMap").value
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished executing recipe")
        return data
