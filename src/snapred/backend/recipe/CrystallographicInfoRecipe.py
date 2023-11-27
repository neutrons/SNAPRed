from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.CrystallographicInfoAlgorithm import CrystallographicInfoAlgorithm as Algo
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class CrystallographicInfoRecipe:
    def __init__(self):
        pass

    def executeRecipe(self, cifPath: str, dMin: float = 0.4, dMax: float = 100.0) -> Dict[str, Any]:
        logger.info("Ingesting crystal info: %s" % cifPath)
        data: Dict[str, Any] = {}
        algo = AlgorithmManager.create(Algo.__name__)
        algo.setProperty("cifPath", cifPath)
        algo.setProperty("dMin", dMin)
        algo.setProperty("dMax", dMax)

        try:
            data["result"] = algo.execute()
            data["crystalInfo"] = CrystallographicInfo.parse_raw(algo.getPropertyValue("crystalInfo"))
            data["crystalStructure"] = Crystallography.parse_raw(algo.getPropertyValue("crystalStructure"))
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished ingesting crystal info: %s" % cifPath)
        return data
