from typing import Any, Dict, List, Set, Tuple

from snapred.backend.dao.ingredients import GenerateFocussedVanadiumIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.RebinFocussedGroupDataRecipe import RebinFocussedGroupDataRecipe
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.decorators.Singleton import Singleton

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

    def allGroceryKeys(self) -> Set[str]:
        return {"inputWorkspace", "outputWorkspace"}

    def mandatoryInputWorkspaces(self) -> Set[str]:
        return {"inputWorkspace"}

    def chopIngredients(self, ingredients: Ingredients):
        self.smoothingParameter = ingredients.smoothingParameter
        self.detectorPeaks = ingredients.detectorPeaks
        self.pixelGroup = ingredients.pixelGroup

        self.artificialNormalizationIngredients = ingredients.artificialNormalizationIngredients

    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.inputWS = groceries["inputWorkspace"]
        self.outputWS = groceries.get("outputWorkspace", groceries["inputWorkspace"])

    def queueArtificialNormalization(self):
        """
        Queues up the artificial normalization recipe if the ingredients are available.
        """
        self.mantidSnapper.CreateArtificialNormalizationAlgo(
            "Create Artificial Normalization...",
            InputWorkspace=self.inputWS,
            OutputWorkspace=self.outputWS,
            peakWindowClippingSize=self.artificialNormalizationIngredients.peakWindowClippingSize,
            smoothingParameter=self.artificialNormalizationIngredients.smoothingParameter,
            decreaseParameter=self.artificialNormalizationIngredients.decreaseParameter,
            LSS=self.artificialNormalizationIngredients.lss,
        )

    def queueNaturalNormalization(self):
        self.mantidSnapper.SmoothDataExcludingPeaksAlgo(
            "Smoothing Data Excluding Peaks...",
            InputWorkspace=self.inputWS,
            OutputWorkspace=self.outputWS,
            DetectorPeaks=self.detectorPeaks,
            SmoothingParameter=self.smoothingParameter,
        )

    def _rebinInputWorkspace(self):
        """
        Rebins the input workspace to the pixel group.
        """
        rebinRecipe = RebinFocussedGroupDataRecipe(self.utensils)
        rebinIngredients = RebinFocussedGroupDataRecipe.Ingredients(pixelGroup=self.pixelGroup)
        rebinRecipe.cook(rebinIngredients, {"inputWorkspace": self.inputWS})

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries.
        """
        self._rebinInputWorkspace()

        if self.artificialNormalizationIngredients is not None:
            self.queueArtificialNormalization()
        else:
            self.queueNaturalNormalization()

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        self.prep(ingredients, groceries)

        self.execute()
        output = self.outputWS

        logger.info(f"Finished generating focussed vanadium for {self.inputWS}...")
        return output

    def cater(self, shipment: List[Pallet]):
        outputs = []
        for ingredients, groceries in shipment:
            outputs.append(self.cook(ingredients, groceries))

        return outputs
