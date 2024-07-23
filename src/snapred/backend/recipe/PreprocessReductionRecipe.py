from typing import Any, Dict, List, Tuple

from snapred.backend.dao.ingredients import PreprocessReductionIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class PreprocessReductionRecipe(Recipe[Ingredients]):
    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """
        pass

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing

        """
        self.sampleWs = groceries["inputWorkspace"]
        self.diffcalWs = groceries.get("diffcalWorkspace", "")
        self.maskWs = groceries.get("maskWorkspace", "")

    def queueAlgos(self):
        """
        Queues up the processing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """

        if self.maskWs:
            self.mantidSnapper.MaskDetectorFlags(
                "Applying pixel mask...",
                MaskWorkspace=self.maskWs,
                OutputWorkspace=self.sampleWs,
            )

        if self.diffcalWs:
            self.mantidSnapper.ApplyDiffCal(
                "Applying diffcal..", InstrumentWorkspace=self.sampleWs, CalibrationWorkspace=self.diffcalWs
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
