import json
from typing import Generic, TypeVar, get_args

from mantid.simpleapi import ConvertTableToMatrixWorkspace

from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitMultiplePeaksAlgorithm
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo
from snapred.backend.recipe.GenericRecipe import DetectorPeakPredictor, GenericRecipe
from snapred.meta.decorators.FromString import isBaseModel, isListOfBaseModel

logger = snapredLogger.getLogger(__name__)

T = TypeVar("T")


class PeakManipulationRecipe(GenericRecipe[T]):
    def __init__(self):
        super().__init__()

    def executeRecipe(self, **kwargs):
        logger.info("Executing peak manipulation recipe for algo: %s" % self.algo)

        if kwargs.get("DetectorPeaks") is None:
            if "DetectorPeakIngredients" in kwargs:
                peakIngredients = kwargs["DetectorPeakIngredients"]
            else:
                peakIngredients = PeakIngredients(
                    instrumentState=x,
                    pixelGroup=y,
                    crystalInfo=z,
                    peakIntensityThreshold=w,
                )
            kwargs["DetectorPeaks"] = DetectorPeakPredictorRecipe().executeRecipe(Ingredients=peakIngredients)

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


class FitMultiplePeaksRecipe(PeakManipulationRecipe[FitMultiplePeaksAlgorithm]):
    pass


class PurgeOverlappingPeaksRecipe(PeakManipulationRecipe[PurgeOverlappingPeaksAlgorithm]):
    pass


class SmoothDataExcludingPeaksRecipe(PeakManipulationRecipe[SmoothDataExcludingPeaksAlgo]):
    pass


class DetectorPeakPredictorRecipe(PeakManipulationRecipe[DetectorPeakPredictor]):
    pass
