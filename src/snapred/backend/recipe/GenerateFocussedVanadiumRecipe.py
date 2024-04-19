import json
from typing import Any, Dict

from snapred.backend.dao.ingredients import GenerateFocussedVanadiumIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class GenerateFocussedVanadiumRecipe:
    """

    The purpose of this recipe is to generate a focussed vandium. This recipe will take a pre-processed
    raw vanadium workspace that has been preprocessed and then grouped via group processing recipe and
    then apply the SNAPRed Algo: SmoothDataExcludingPeaksAlgo using the smoothing parameter and
    crystallographic info applied during normalization calibration.

    """

    def __init__(self):
        pass

    def chopIngredients(self, ingredients: Ingredients):
        self.smoothingParameter = ingredients.smoothingParameter
        self.detectorPeaks = ingredients.detectorPeaks

    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.rawInput = groceries["inputWorkspace"]
        self.outputWS = groceries["outputWorkspace"]

    def executeRecipe(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceries)
        data: Dict[str, Any] = {"result": False}

        if self.rawInput is not None:
            logger.info("Generating focussed vanadium...")
            smoothAlgo = SmoothDataExcludingPeaksAlgo()
            smoothAlgo.initialize()
            smoothAlgo.setProperty("InputWorkspace", self.rawInput)
            smoothAlgo.setProperty("OutputWorkspace", self.outputWS)
            smoothAlgo.setProperty("DetectorPeaks", ingredients.detectorPeaks)
            smoothAlgo.setProperty("SmoothingParameter", ingredients.smoothingParameter)

            smoothAlgo.execute()
            data["outputWorkspace"] = smoothAlgo.getPropertyValue("OutputWorkspace")
        else:
            raise NotImplementedError

        logger.info(f"Finished generating focussed vanadium for {self.rawInput}...")
        data["result"] = True
        return data
