from typing import Any, Dict, List, Tuple

from snapred.backend.dao.ingredients import PreprocessReductionIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class PreprocessReductionRecipe(Recipe[Ingredients]):
    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """
        self.maskList = ingredients.maskList
        if not self.maskList:
            logger.info("No mask list provided. Will not apply any masking.")
            self.maskList = []

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing

        """
        self.sampleWs = groceries["inputWorkspace"]
        self.diffcalWs = groceries.get("diffcalWorkspace", "")

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        if self.diffcalWs:
            self.mantidSnapper.ApplyDiffCal(
                "Applying diffcal..", InstrumentWorkspace=self.sampleWs, CalibrationWorkspace=self.diffcalWs
            )
        for maskWs in self.maskList:
            self.mantidSnapper.MaskDetectorFlags(
                "Masking workspace...",
                MaskWorkspace=maskWs,
                OutputWorkspace=self.sampleWs,
            )

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep(ingredients, groceries)
        self.execute()
        return self.sampleWs

    def cater(self, shipment: List[Pallet]) -> List[Dict[str, Any]]:
        """
        A secondary interface method for the recipe.
        It is a batched version of cook.
        Given a shipment of ingredients and groceries, it prepares, executes and returns the final workspaces.
        """
        output = []
        for ingredient, grocery in shipment:
            self.prep(ingredient, grocery)
            output.append(self.sampleWs)
        self.execute()
        return output
