from typing import Generic, TypeVar

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.CalbrationMetricExtractionAlgorithm import CalibrationMetricExtractionAlgorithm
from snapred.backend.recipe.algorithm.CalibrationReductionAlgorithm import CalibrationReductionAlgorithm
from snapred.backend.recipe.algorithm.CustomStripPeaksAlgorithm import CustomStripPeaksAlgorithm
from snapred.backend.recipe.algorithm.ExtractionAlgorithm import ExtractionAlgorithm
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitMultiplePeaksAlgorithm
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksRecipe import PurgeOverlappingPeaksRecipe
from snapred.backend.recipe.algorithm.ReductionAlgorithm import ReductionAlgorithm

logger = snapredLogger.getLogger(__name__)

T = TypeVar("T")


class GenericRecipe(Generic[T]):
    algo: str = T.__name__

    def __init__(self):
        self.mantidSnapper = MantidSnapper(None, self.algo)

    def executeRecipe(self, **kwargs):
        logger.info("Executing generic recipe for algo: %s" % self.algo)

        outputs = None
        try:
            outputs = self.mantidSnapper.__getattr__(self.algo)(**kwargs)
            self.mantidSnapper.executeQueue()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished executing recipe for algo: %s" % self.algo)
        return outputs


class CalibrationMetricExtractionRecipe(GenericRecipe[CalibrationMetricExtractionAlgorithm]):
    pass


class CalibrationReductionRecipe(GenericRecipe[CalibrationReductionAlgorithm]):
    pass


class CustomStripPeaksRecipe(GenericRecipe[CustomStripPeaksAlgorithm]):
    pass


class ExtractionRecipe(GenericRecipe[ExtractionAlgorithm]):
    pass


class FitMultiplePeaksRecipe(GenericRecipe[FitMultiplePeaksAlgorithm]):
    pass


class PurgeOverlappingPeaksRecipe(GenericRecipe[PurgeOverlappingPeaksRecipe]):
    pass


class ReductionRecipe(GenericRecipe[ReductionAlgorithm]):
    pass
