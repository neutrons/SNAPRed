from typing import Any, Dict, List, Tuple

from snapred.backend.dao.ingredients import GenerateFocussedVanadiumIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class GenerateFocussedVanadiumRecipe(Recipe[Ingredients]):
    """

    The purpose of this recipe is to generate a focussed vandium. This recipe will take a
    raw vanadium workspace that has been preprocessed and then grouped via group processing recipe and
    then apply the SNAPRed Algo: SmoothDataExcludingPeaksAlgo using the smoothing parameter and
    crystallographic info applied during normalization calibration.

    """

    def chopIngredients(self, ingredients: Ingredients):
        self.smoothingParameter = ingredients.smoothingParameter
        self.detectorPeaks = list_to_raw(ingredients.detectorPeaks)
        self.dMin = ingredients.pixelGroup.dMin()
        self.dMax = ingredients.pixelGroup.dMax()
        self.dBin = ingredients.pixelGroup.dBin()

    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.inputWS = groceries["inputWorkspace"]
        self.outputWS = groceries.get("outputWorkspace", groceries["inputWorkspace"])

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries.
        """
        self.mantidSnapper.SmoothDataExcludingPeaksAlgo(
            "Smoothing Data Excluding Peaks...",
            InputWorkspace=self.outputWS,
            OutputWorkspace=self.outputWS,
            DetectorPeaks=self.detectorPeaks,
            SmoothingParameter=self.smoothingParameter,
        )

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        self.prep(ingredients, groceries)
        output = None
        if self.inputWS is not None:
            self.execute()
            output = self.outputWS
        else:
            raise NotImplementedError("Fake Vanadium not implemented yet.")

        logger.info(f"Finished generating focussed vanadium for {self.inputWS}...")
        return output

    def cater(self, shipment: List[Pallet]):
        outputs = []
        for ingredients, groceries in shipment:
            outputs.append(self.cook(ingredients, groceries))

        return outputs
