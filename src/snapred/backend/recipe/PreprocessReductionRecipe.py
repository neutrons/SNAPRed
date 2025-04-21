from typing import Any, Dict, List, Set, Tuple

from snapred.backend.dao.ingredients import PreprocessReductionIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


class PreprocessReductionRecipe(Recipe[Ingredients]):
    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """
        pass

    def allGroceryKeys(self) -> Set[str]:
        return {
            "inputWorkspace",
            "backgroundWorkspace",
            "groupingWorkspace",
            "diffcalWorkspace",
            "maskWorkspace",
            "outputWorkspace",
        }

    def mandatoryInputWorkspaces(self) -> Set[str]:
        return {"inputWorkspace"}

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing

        """
        self.sampleWs = groceries["inputWorkspace"]
        self.diffcalWs = groceries.get("diffcalWorkspace", "")
        self.maskWs = groceries.get("maskWorkspace", "")
        self.outputWs = groceries.get("outputWorkspace", groceries["inputWorkspace"])

    def queueAlgos(self):
        """
        Queues up the processing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """

        if self.outputWs != self.sampleWs:
            self.mantidSnapper.CloneWorkspace(
                "Cloning workspace...",
                InputWorkspace=self.sampleWs,
                OutputWorkspace=self.outputWs,
            )

        if self.maskWs != "":
            self.mantidSnapper.MaskDetectorFlags(
                "Applying pixel mask...",
                MaskWorkspace=self.maskWs,
                OutputWorkspace=self.outputWs,
            )

        if self.diffcalWs != "":
            self.mantidSnapper.ApplyDiffCal(
                "Applying diffcal..",
                InstrumentWorkspace=self.outputWs,
                CalibrationWorkspace=self.diffcalWs,
            )

        # convert to tof if needed
        self.mantidSnapper.ConvertUnits(
            "Converting to TOF...",
            InputWorkspace=self.outputWs,
            Target="TOF",
            OutputWorkspace=self.outputWs,
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
