from typing import Any, Dict, List

from mantid.api import AlgorithmManager

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import (
    name as LiteDataCreationAlgo,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class LiteDataRecipe:
    liteDataCreationAlgoName: str = LiteDataCreationAlgo

    def __init__(self):
        pass

    def executeRecipe(self, InputWorkspace: str):
        logger.info("Executing LiteDataRecipe")
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.liteDataCreationAlgoName)
        algo.setProperty("InputWorkspace", InputWorkspace)
        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("LiteDataCreationAlgo complete")
        return data
