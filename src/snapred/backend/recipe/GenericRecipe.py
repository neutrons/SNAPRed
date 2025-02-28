import json
from typing import Generic, TypeVar, get_args

# TODO Replace the use of the import(s) below with MantidSnapper in EWM 9909
from mantid.simpleapi import ConvertTableToMatrixWorkspace, Minus  # noqa : TID251
from pydantic import BaseModel

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.BufferMissingColumnsAlgo import BufferMissingColumnsAlgo
from snapred.backend.recipe.algorithm.CalibrationMetricExtractionAlgorithm import CalibrationMetricExtractionAlgorithm
from snapred.backend.recipe.algorithm.CreateArtificialNormalizationAlgo import CreateArtificialNormalizationAlgo
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitMultiplePeaksAlgorithm
from snapred.backend.recipe.algorithm.FocusSpectraAlgorithm import FocusSpectraAlgorithm
from snapred.backend.recipe.algorithm.GenerateTableWorkspaceFromListOfDict import GenerateTableWorkspaceFromListOfDict
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo
from snapred.meta.decorators.FromString import isBaseModel

logger = snapredLogger.getLogger(__name__)

T = TypeVar("T")


class GenericRecipe(Generic[T]):
    def __init__(self):
        self.algo: str = self.getAlgo().__name__
        self.mantidSnapper = MantidSnapper(None, self.algo)

    def _baseModelsToStrings(self, **kwargs):
        for key, value in kwargs.items():
            if isBaseModel(value.__class__):
                kwargs[key] = value.model_dump_json()
            # NOTE the equivalent function isListOfBaseModel was not working
            # in the case of the smoothing algo.  The below does work.
            elif isinstance(value, list) and issubclass(value[0].__class__, BaseModel):
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
        if isinstance(outputs, tuple):
            outputs = [x.get() for x in list(outputs)]
        else:
            outputs = outputs.get()
        return outputs

    def getAlgo(self):
        return get_args(self.__orig_bases__[0])[0]


class CalibrationMetricExtractionRecipe(GenericRecipe[CalibrationMetricExtractionAlgorithm]):
    pass


class FitMultiplePeaksRecipe(GenericRecipe[FitMultiplePeaksAlgorithm]):
    pass


class PurgeOverlappingPeaksRecipe(GenericRecipe[PurgeOverlappingPeaksAlgorithm]):
    pass


class SmoothDataExcludingPeaksRecipe(GenericRecipe[SmoothDataExcludingPeaksAlgo]):
    pass


class LiteDataRecipe(GenericRecipe[LiteDataCreationAlgo]):
    pass


class RawVanadiumCorrectionRecipe(GenericRecipe[RawVanadiumCorrectionAlgorithm]):
    pass


class DetectorPeakPredictorRecipe(GenericRecipe[DetectorPeakPredictor]):
    pass


class GenerateTableWorkspaceFromListOfDictRecipe(GenericRecipe[GenerateTableWorkspaceFromListOfDict]):
    pass


class FocusSpectraRecipe(GenericRecipe[FocusSpectraAlgorithm]):
    pass


class ConvertTableToMatrixWorkspaceRecipe(GenericRecipe[ConvertTableToMatrixWorkspace]):
    pass


class BufferMissingColumnsRecipe(GenericRecipe[BufferMissingColumnsAlgo]):
    pass


class ArtificialNormalizationRecipe(GenericRecipe[CreateArtificialNormalizationAlgo]):
    pass


class MinusRecipe(GenericRecipe[Minus]):
    pass
