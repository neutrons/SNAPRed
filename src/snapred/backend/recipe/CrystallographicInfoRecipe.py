from typing import Any, Dict

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class CrystallographicInfoRecipe:
    D_MIN = Config["constants.CrystallographicInfo.crystalDMin"]
    D_MAX = Config["constants.CrystallographicInfo.crystalDMax"]

    def __init__(self):
        # NOTE: workaround, we just add an empty host algorithm.
        utensils = Utensils()
        utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    def executeRecipe(self, cifPath: str, crystalDMin: float = D_MIN, crystalDMax: float = D_MAX) -> Dict[str, Any]:
        logger.info("Ingesting crystal info: %s" % cifPath)
        data: Dict[str, Any] = {}

        xtalInfo, xtallography = self.mantidSnapper.CrystallographicInfoAlgorithm(
            f"Ingesting crystal info: {cifPath}",
            cifPath=cifPath,
            crystalDMin=crystalDMin,
            crystalDMax=crystalDMax,
            Crystallography="",  # NOTE must declare to get this as return
        )
        self.mantidSnapper.executeQueue()
        data["result"] = True
        data["crystalInfo"] = CrystallographicInfo.model_validate_json(xtalInfo.get())
        data["crystalStructure"] = Crystallography.model_validate_json(xtallography.get())

        logger.info("Finished ingesting crystal info: %s" % cifPath)
        return data
