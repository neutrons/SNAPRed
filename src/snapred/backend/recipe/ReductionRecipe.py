from typing import Any, Dict, List, Tuple, Type

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.ApplyNormalizationRecipe import ApplyNormalizationRecipe
from snapred.backend.recipe.GenerateFocussedVanadiumRecipe import GenerateFocussedVanadiumRecipe
from snapred.backend.recipe.PreprocessReductionRecipe import PreprocessReductionRecipe
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

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
        self.ingredients = ingredients.copy()
        self.keepUnfocused = self.ingredients.keepUnfocused
        self.convertUnitsTo = self.ingredients.convertUnitsTo

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing

        """
        self.groceries = groceries.copy()
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

    def _convertWorkspace(self, workspace: str, units: str):
        unitsAbrev = ""
        if units == "TOF":
            unitsAbrev = wng.Units.TOF
        elif units == "Wavelength":
            unitsAbrev = wng.Units.LAM
        elif units == "MomentumTransfer":
            unitsAbrev = wng.Units.QSP
        elif units == "dSpacing":
            unitsAbrev = wng.Units.DSP
        outWS = workspace.replace("tof", unitsAbrev.lower())
        print(outWS)
        self.mantidSnapper.ConvertUnits(
            "Convert the clone of the final output back to TOFl",
            InputWorkspace=workspace,
            OutputWorkspace=outWS,
            Target=units,
        )
        if outWS != workspace:
            self._deleteWorkspace(workspace)
        self.mantidSnapper.executeQueue()

    def _applyRecipe(self, recipe: Type[Recipe], ingredients_, **kwargs):
        if "inputWorkspace" in kwargs:
            self.groceries.update(kwargs)
            recipe().cook(ingredients_, self.groceries)

    def _prepGroupWorkspaces(self, groupingIndex: int):
        # TODO:  We need the wng to be able to deconstruct the workspace name
        # so that we can appropriately name the cloned workspaces
        # For now we are just appending it to the end, probably preferable
        # as it keeps the output colocated.

        groupName = self.ingredients.pixelGroups[groupingIndex].focusGroup.name
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

    def execute(self):
        # 1. PreprocessReductionRecipe
        outputs = []
        self._applyRecipe(
            PreprocessReductionRecipe,
            self.ingredients.preprocess(),
            inputWorkspace=self.sampleWs,
        )
        self._cloneIntermediateWorkspace(self.sampleWs, "sample_preprocessed")
        self._applyRecipe(
            PreprocessReductionRecipe,
            self.ingredients.preprocess(),
            inputWorkspace=self.normalizationWs,
        )
        self._cloneIntermediateWorkspace(self.normalizationWs, "normalization_preprocessed")

        for groupingIndex, groupWs in enumerate(self.groupWorkspaces):
            self.groceries["groupingWorkspace"] = groupWs

            # Clone
            sampleClone, normalizationClone = self._prepGroupWorkspaces(groupingIndex)
            # TODO: Set pixel group specific stuff

            # 2. ReductionGroupProcessingRecipe
            self._applyRecipe(
                ReductionGroupProcessingRecipe,
                self.ingredients.groupProcessing(groupingIndex),
                inputWorkspace=sampleClone,
            )
            self._cloneIntermediateWorkspace(sampleClone, f"sample_GroupProcessing_{groupingIndex}")
            self._applyRecipe(
                ReductionGroupProcessingRecipe,
                self.ingredients.groupProcessing(groupingIndex),
                inputWorkspace=normalizationClone,
            )
            self._cloneIntermediateWorkspace(normalizationClone, f"normalization_GroupProcessing_{groupingIndex}")

            # 3. GenerateFocussedVanadiumRecipe
            self._applyRecipe(
                GenerateFocussedVanadiumRecipe,
                self.ingredients.generateFocussedVanadium(groupingIndex),
                inputWorkspace=normalizationClone,
            )
            self._cloneIntermediateWorkspace(normalizationClone, f"normalization_FoocussedVanadium_{groupingIndex}")

            # 4. ApplyNormalizationRecipe
            self._applyRecipe(
                ApplyNormalizationRecipe,
                self.ingredients.applyNormalization(groupingIndex),
                inputWorkspace=sampleClone,
                normalizationWorkspace=normalizationClone,
            )
            self._cloneIntermediateWorkspace(sampleClone, f"sample_ApplyNormalization_{groupingIndex}")

            # Cleanup
            outputs.append(sampleClone)

            if self.keepUnfocused:
                self._convertWorkspace(normalizationClone, self.convertUnitsTo)
            else:
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
