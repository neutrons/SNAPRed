from typing import Any, Dict, Generic, TypeVar

from mantid.api import AlgorithmManager

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)

T = TypeVar("T")


class GenericRecipe(Generic[T]):
    algo: str = T.__name__

    def __init__(self):
        pass

    def executeRecipe(self, **kwargs):
        logger.info("Executing generic recipe for algo: %s" % self.algo)
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.algo)
        for key, value in kwargs.items():
            algo.setProperty(key, value)
        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished executing recipe for algo: %s" % self.algo)
        return data
