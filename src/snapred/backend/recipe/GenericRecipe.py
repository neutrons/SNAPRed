import json
from typing import Generic, TypeVar, get_args

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.CalibrationReductionAlgorithm import CalibrationReductionAlgorithm
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm
from snapred.backend.recipe.algorithm.ReductionAlgorithm import ReductionAlgorithm
from snapred.meta.decorators.FromString import isBaseModel, isListOfBaseModel

logger = snapredLogger.getLogger(__name__)

T = TypeVar("T")


class GenericRecipe(Generic[T]):
    def __init__(self):
        self.algo: str = self.getAlgo().__name__
        self.mantidSnapper = MantidSnapper(None, self.algo)

    def _baseModelsToStrings(self, **kwargs):
        for key, value in kwargs.items():
            if isBaseModel(value.__class__):
                kwargs[key] = value.json()
            elif isListOfBaseModel(value.__class__):
                kwargs[key] = json.dumps([v.dict() for v in value])
        return kwargs

    def executeRecipe(self, **kwargs):
        logger.info("Executing generic recipe for algo: %s" % self.algo)

        kwargs = self._baseModelsToStrings(**kwargs)

        outputs = None
        try:
            outputs = self.mantidSnapper.__getattr__(self.algo)("", **kwargs)
            self.mantidSnapper.executeQueue()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished executing recipe for algo: %s" % self.algo)

        # unwrap callback wrappers
        if type(outputs) is tuple:
            for i in range(len(outputs)):
                outputs[i] = outputs[i].get()
        else:
            outputs = outputs.get()
        return outputs

    def getAlgo(self):
        return get_args(self.__orig_bases__[0])[0]


class CalibrationReductionRecipe(GenericRecipe[CalibrationReductionAlgorithm]):
    pass


class PurgeOverlappingPeaksRecipe(GenericRecipe[PurgeOverlappingPeaksAlgorithm]):
    pass


class ReductionRecipe(GenericRecipe[ReductionAlgorithm]):
    pass
