from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.IngestCrystallographicInfo import (
    name as IngestCrystallographicInfo,
)
from snapred.meta.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class IngestCrystallographicInfoRecipe:
    ingestionAlgorithmName: str = IngestCrystallographicInfo

    def __init__(self):
        pass

    def executeRecipe(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.ingestionAlgorithmName)

        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        return data
