from typing import Any, Dict, List, Set, Tuple

from snapred.backend.dao.ingredients import ReductionGroupProcessingIngredients as Ingredients
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class ReductionGroupProcessingRecipe(Recipe[Ingredients]):
    def allGroceryKeys(self):
        return {
            "inputWorkspace",
            "groupingWorkspace",
            "outputWorkspace",
            # the following need to be here for consistency
            "diffcalWorkspace",
            "maskWorkspace",
            "backgroundWorkspace",
        }

    def mandatoryInputWorkspaces(self) -> Set[WorkspaceName]:
        return {"inputWorkspace", "groupingWorkspace"}

    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.rawInput = groceries["inputWorkspace"]
        self.outputWS = groceries.get("outputWorkspace", groceries["inputWorkspace"])
        self.groupingWS = groceries["groupingWorkspace"]

    def chopIngredients(self, ingredients):
        self.pixelGroup = ingredients.pixelGroup

    def queueAlgos(self):
        """
        Queues up the processing algorithms for the recipe.
        Requires: unbagged groceries.
        """
        self.mantidSnapper.ConvertUnits(
            "Converting to TOF...",
            InputWorkspace=self.rawInput,
            Target="TOF",
            OutputWorkspace=self.outputWS,
        )

        self.mantidSnapper.FocusSpectraAlgorithm(
            "Focusing Spectra...",
            InputWorkspace=self.outputWS,
            OutputWorkspace=self.outputWS,
            GroupingWorkspace=self.groupingWS,
            Ingredients=self.pixelGroup.json(),
            RebinOutput=False,
        )

        self.mantidSnapper.NormalizeByCurrentButTheCorrectWay(
            "Normalizing Current ... but the correct way!",
            InputWorkspace=self.outputWS,
            OutputWorkspace=self.outputWS,
        )

    def execute(self):
        """
        Final step in a recipe, executes the queued algorithms.
        Requires: queued algorithms.
        """
        try:
            self.mantidSnapper.executeQueue()
        except AlgorithmException as e:
            errorString = str(e)
            if "NORMALIZATIONFACTOR" in errorString:
                errorString = (
                    "Input raw data has already been normalized by current.\n "
                    "Please use one that has not had current normalization applied."
                    "i.e. sample logs dont contain entries for gd_prtn_chrg or proton_charge"
                )
            raise RuntimeError(errorString) from e

    def cook(self, ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
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
        for ingredients, grocery in shipment:
            self.prep(ingredients, grocery)
            output.append(self.outputWS)
        self.execute()
        return output
