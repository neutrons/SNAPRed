from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager

from snapred.backend.dao.ingredients import PreprocessReductionIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class PreprocessReductionRecipe:
    def __init__(self, utensils: Utensils = None):
        """
        Sets up the recipe with the necessary utensils.
        """
        # NOTE: workaround, we just add an empty host algorithm.
        if utensils is None:
            utensils = Utensils()
            utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """
        self.maskList = ingredients.maskList

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing

        """
        self.sampleWs = groceries["inputWorkspace"]
        self.diffcalWs = groceries.get("diffcalWorkspace", "")

    def validateInputs(self):
        """
        Confirms that the ingredients and groceries make sense together.
        Requires: unbagged groceries and chopped ingredients.
        """
        pass

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        if self.calibrationWs:
            self.mantidSnapper.ApplyDiffCal(
                "Applying diffcal..", InstrumentWorkspace=self.sampleWs, CalibrationWorkspace=self.calibrationWs
            )
        for maskWs in self.maskList:
            self.mantidSnapper.MaskDetectorFlags(
                "Masking workspace...",
                MaskWorkspace=maskWs,
                OutputWorkspace=self.sampleWs,
            )

    def prep(self, ingredients: Ingredients, groceries: Dict[str, str]):
        """
        Convinience method to prepare the recipe for execution.
        """
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceries)
        self.validateInputs()
        self.queueAlgos()

    def execute(self):
        """
        Final step in a recipe, executes the queued algorithms.
        Requires: queued algorithms.
        """
        self.mantidSnapper.executeQueue()

    # NOTE: Metaphorically, would ingredients better have been called Spices?
    # Considering they are mostly never the meat of a recipe.
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
