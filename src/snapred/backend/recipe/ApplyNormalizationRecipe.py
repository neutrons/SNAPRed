from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager

from snapred.backend.dao.ingredients import ApplyNormalizationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class ApplyNormalizationRecipe:
    NUM_BINS = Config["constants.ResampleX.NumberBins"]
    LOG_BINNING = True

    def __init__(self, utensils: Utensils = None):
        """
        Sets up the recipe with the necessary utensils.
        """
        # NOTE: workaround, we just add an empty host algorithm.
        if utensils is None:
            utensils = AlgorithmManager.create(Utensils.__name__)
        self.mantidSnapper = MantidSnapper(utensils, Utensils.__name__)

    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        We are mostly concerned about the drange for a ResampleX operation.
        """
        self.pixelGroup = ingredients.pixelGroup
        self.dMin = self.pixelGroup.dMin
        self.dMax = self.pixelGroup.dMax

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing
        The normalization workspace, normalizationWorkspace, is optional, in dspacing.
        The background workspace, backgroundWorkspace, is optional, not implemented, in dspacing.
        """
        self.sampleWs = groceries["inputWorkspace"]
        self.normalizationWs = groceries.get("normalizationWorkspace", "")
        self.backgroundWs = groceries.get("backgroundWorkspace", "")

    def validateInputs(self):
        """
        Confirms that the ingredients and groceries make sense together.
        Mostly just checks that the background subtraction is not implemented.
        Requires: unbagged groceries and chopped ingredients.
        """
        # NOTE: Should background subtraction even take place here?  ME thinks not!! *finger wag*
        if self.backgroundWs != "":
            raise NotImplementedError("Background Subtraction is not implemented for this release.")

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        if self.normalizationWs:
            self.mantidSnapper.Divide(
                "Dividing out the normalization..", LHSWorkspace=self.sampleWs, RHSWorkspace=self.normalizationWs
            )
        # NOTE: ResampleX is considered a workaround until Mantid can handle ragged workspaces.
        self.mantidSnapper.ResampleX(
            "Resampling X-axis...",
            InputWorkspace=self.sampleWs,
            XMin=self.dMin,
            XMax=self.dMax,
            NumberBins=self.NUM_BINS,
            LogBinning=self.LOG_BINNING,
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
