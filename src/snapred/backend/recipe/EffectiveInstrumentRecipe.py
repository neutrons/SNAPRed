from typing import Any, Dict, List, Tuple

from snapred.backend.dao.ingredients import EffectiveInstrumentIngredients as Ingredients
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class EffectiveInstrumentRecipe(Recipe[Ingredients]):
    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.inputWS = groceries["inputWorkspace"]
        self.outputWS = groceries.get("outputWorkspace", groceries["inputWorkspace"])
 
    def chopIngredients(self, ingredients):
        self.unmaskedPixelGroup = ingredients.unmaskedPixelGroup

    def queueAlgos(self):
        """
        Queues up the processing algorithms for the recipe.
        Requires: unbagged groceries.
        """
        self.mantidSnapper.EditInstrumentGeometry(
            f"Editing instrument geometry for grouping '{self.unmaskedPixelGroup.focusGroup.name}'",
            Workspace=self.inputWS,
            L2=self.unmaskedPixelGroup.L2,
            Polar=self.unmaskedPixelGroup.twoTheta,
            Azimuthal=self.unmaskedPixelGroup.azimuth,
            InstrumentName=f"SNAP_{self.unmaskedPixelGroup.focusGroup.name}"
        )
        self.outputWS = self.inputWS

    def validateInputs(self, ingredients: Ingredients, groceries: Dict[str, WorkspaceName]):
        pass

    def execute(self):
        """
        Final step in a recipe, executes the queued algorithms.
        Requires: queued algorithms.
        """
        try:
            self.mantidSnapper.executeQueue()
        except AlgorithmException as e:
            errorString = str(e)
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
