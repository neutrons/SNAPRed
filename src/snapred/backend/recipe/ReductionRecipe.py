from typing import Any, Dict, List, Tuple, Type

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.ApplyNormalizationRecipe import ApplyNormalizationRecipe
from snapred.backend.recipe.GenerateFocussedVanadiumRecipe import GenerateFocussedVanadiumRecipe
from snapred.backend.recipe.PreprocessReductionRecipe import PreprocessReductionRecipe
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class ReductionRecipe(Recipe[Ingredients]):
    """
    Currently requires:
        Ingredients:
            self.maskList = ingredients.maskList
            self.pixelGroup = ingredients.pixelGroup
            self.smoothingParameter = ingredients.smoothingParameter
            self.detectorPeaks = ingredients.detectorPeaks

        Groceries:
            self.sampleWs = groceries["inputWorkspace"]
            self.diffcalWs = groceries.get("diffcalWorkspace", "")
            self.normalizationWs = groceries.get("normalizationWorkspace", "")
            ~~self.backgroundWs = groceries.get("backgroundWorkspace", "")~~
            self.groupingWS = groceries["groupingWorkspace"]
    """

    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """
        self.ingredients = ingredients

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing

        """
        self.groceries = groceries
        self.sampleWs = groceries["inputWorkspace"]
        self.normalizationWs = groceries.get("normalizationWorkspace", "")
        self.groupWorkspaces = groceries["groupingWorkspaces"]

    def _cloneWorkspace(self, inputWorkspace: str, outputWorkspace: str) -> str:
        self.mantidSnapper.CloneWorkspace(
            "Cloning workspace...",
            InputWorkspace=inputWorkspace,
            OutputWorkspace=outputWorkspace,
        )
        self.mantidSnapper.executeQueue()
        return outputWorkspace

    def _cloneIntermediateWorkspace(self, inputWorkspace: str, outputWorkspace: str) -> str:
        self.mantidSnapper.MakeDirtyDish(
            "Cloning workspace...", InputWorkspace=inputWorkspace, OutputWorkspace=outputWorkspace
        )
        self.mantidSnapper.executeQueue()
        return inputWorkspace

    def _deleteWorkspace(self, workspace: str):
        self.mantidSnapper.DeleteWorkspace(
            "Deleting workspace...",
            Workspace=workspace,
        )
        self.mantidSnapper.executeQueue()

    def _applyRecipe(self, recipe: Type[Recipe], inputWorkspace: str):
        if inputWorkspace:
            self.groceries["inputWorkspace"] = inputWorkspace
            recipe().cook(self.ingredients, self.groceries)

    def _prepGroupWorkspaces(self, index: int):
        # TODO:  We need the wng to be able to deconstruct the workspace name
        # so that we can appropriately name the cloned workspaces
        # For now we are just appending it to the end, probably preferable
        # as it keeps the output colocated.
        self.ingredients.pixelGroup = self.ingredients.pixelGroups[index]
        self.ingredients.detectorPeaks = self.ingredients.detectorPeaksMany[index]
        groupName = self.ingredients.pixelGroup.focusGroup.name
        sampleClone = self._cloneWorkspace(self.sampleWs, f"output_{self.sampleWs}_{groupName}")
        self.groceries["inputWorkspace"] = sampleClone
        normalizationClone = None
        if self.normalizationWs:
            normalizationClone = self._cloneWorkspace(
                self.normalizationWs, f"output_{self.normalizationWs}_{groupName}"
            )
            self.groceries["normalizationWorkspace"] = normalizationClone
        return sampleClone, normalizationClone

    def validateInputs(self, ingredients: Ingredients, groceries: Dict[str, WorkspaceName]):
        pass

    def queueAlgos(self):
        pass

    def _applyNormalization(self, sampleWorkspace: str, normalizationWorkspace: str):
        self.groceries["inputWorkspace"] = sampleWorkspace
        self.groceries["normalizationWorkspace"] = normalizationWorkspace
        ApplyNormalizationRecipe().cook(self.ingredients, self.groceries)

    def execute(self):
        # 1. PreprocessReductionRecipe
        outputs = []
        self._applyRecipe(PreprocessReductionRecipe, self.sampleWs)
        self._cloneIntermediateWorkspace(self.sampleWs, "sample_preprocessed")
        self._applyRecipe(PreprocessReductionRecipe, self.normalizationWs)
        self._cloneIntermediateWorkspace(self.normalizationWs, "normalization_preprocessed")
        for i, groupWs in enumerate(self.groupWorkspaces):
            self.groceries["groupingWorkspace"] = groupWs

            # Clone
            sampleClone, normalizationClone = self._prepGroupWorkspaces(i)
            # TODO: Set pixel group specific stuff

            # Apply Calculations
            self._applyRecipe(ReductionGroupProcessingRecipe, sampleClone)
            self._cloneIntermediateWorkspace(sampleClone, f"sample_GroupProcessing_{i}")
            self._applyRecipe(ReductionGroupProcessingRecipe, normalizationClone)
            self._cloneIntermediateWorkspace(normalizationClone, f"normalization_GroupProcessing_{i}")

            self._applyRecipe(GenerateFocussedVanadiumRecipe, normalizationClone)
            self._cloneIntermediateWorkspace(normalizationClone, f"normalization_FoocussedVanadium_{i}")

            self._applyNormalization(sampleClone, normalizationClone)
            self._cloneIntermediateWorkspace(sampleClone, f"sample_ApplyNormalization_{i}")

            # Cleanup
            outputs.append(sampleClone)
            self._deleteWorkspace(normalizationClone)
        return outputs

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep(ingredients, groceries)
        return self.execute()

    def cater(self, shipment: List[Pallet]) -> List[Dict[str, Any]]:
        """
        A secondary interface method for the recipe.
        It is a batched version of cook.
        Given a shipment of ingredients and groceries, it prepares, executes and returns the final workspaces.
        """
        output = []
        for ingredient, grocery in shipment:
            output.append(self.cook(ingredient, grocery))
        return output
