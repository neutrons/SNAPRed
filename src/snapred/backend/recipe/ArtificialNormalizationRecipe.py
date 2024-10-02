import json
from typing import Any, Dict, List, Tuple

from snapred.backend.dao.ingredients import ArtificialNormalizationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class ArtificialNormalizationRecipe(Recipe[Ingredients]):
    """
    The purpose of this recipe is to apply artificial normalization
    using the CreateArtificialNormalizationAlgo algorithm.
    """

    def chopIngredients(self, ingredients: Ingredients):
        self.peakWindowClippingSize = ingredients.peakWindowClippingSize
        self.smoothingParameter = ingredients.smoothingParameter
        self.decreaseParameter = ingredients.decreaseParameter
        self.lss = ingredients.lss

    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.inputWS = groceries["inputWorkspace"]
        self.outputWS = groceries.get("outputWorkspace", groceries["inputWorkspace"])

    def queueAlgos(self):
        """
        Queues up the processing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        ingredients = {
            "peakWindowClippingSize": self.peakWindowClippingSize,
            "smoothingParameter": self.smoothingParameter,
            "decreaseParameter": self.decreaseParameter,
            "lss": self.lss,
        }
        ingredientsStr = json.dumps(ingredients)

        self.mantidSnapper.CreateArtificialNormalizationAlgo(
            "Creating artificial normalization...",
            InputWorkspace=self.inputWS,
            OutputWorkspace=self.outputWS,
            Ingredients=ingredientsStr,
        )

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes, and returns the final workspace.
        """
        self.prep(ingredients, groceries)
        self.execute()
        return self.outputWS

    def cater(self, shipment: List[Pallet]) -> List[Dict[str, Any]]:
        """
        A secondary interface method for the recipe.
        It is a batched version of cook.
        Given a shipment of ingredients and groceries, it prepares, executes and returns the final workspaces.
        """
        output = []
        for ingredient, grocery in shipment:
            output.append(self.cook(ingredient, grocery))
        self.execute()
        return output
