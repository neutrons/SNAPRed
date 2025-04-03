from typing import Any, Dict, Set, Tuple

from snapred.backend.dao.ingredients import RebinFocussedGroupDataIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


class RebinFocussedGroupDataRecipe(Recipe[Ingredients]):
    NUM_BINS = Config["constants.ResampleX.NumberBins"]
    LOG_BINNING = True

    def allGroceryKeys(self) -> Set[str]:
        return {"inputWorkspace"}

    def mandatoryInputWorkspaces(self) -> Set[str]:
        return {"inputWorkspace"}

    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        We are mostly concerned about the drange for a ResampleX operation.
        """

        self.pixelGroup = ingredients.pixelGroup
        dMin = self.pixelGroup.dMin()
        dMax = self.pixelGroup.dMax()
        dBin = self.pixelGroup.dBin()

        if not all(_dMax > _dMin for _dMin, _dMax in zip(dMin, dMax)):
            raise ValueError("Invalid d-spacing range detected: some dMax <= dMin.")

        self.dMin = dMin
        self.dMax = dMax
        self.dBin = dBin

        self.preserveEvents = ingredients.preserveEvents

    def unbagGroceries(self, groceries: Dict[str, WorkspaceName]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing
        The normalization workspace, normalizationWorkspace, is optional, in dspacing.
        The background workspace, backgroundWorkspace, is optional, not implemented, in dspacing.
        """
        self.sampleWs = groceries["inputWorkspace"]

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        self.mantidSnapper.RebinRagged(
            "Rebinning workspace for group...",
            InputWorkspace=self.sampleWs,
            XMin=self.dMin,
            XMax=self.dMax,
            Delta=self.dBin,
            OutputWorkspace=self.sampleWs,
            PreserveEvents=self.preserveEvents,
            FullBinsOnly=True,
        )

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
