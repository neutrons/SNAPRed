from typing import Any, Dict, List, Set, Tuple
from mantid.simpleapi import mtd

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
        return {"inputWorkspace", "diffcalWorkspace", "outputWorkspace"}

    def mandatoryInputWorkspaces(self) -> Set[str]:
        return {"inputWorkspace"}

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing

        """
        self.sampleWs = groceries["inputWorkspace"]
        self.diffcalWs = groceries.get("diffcalWorkspace", "")
        self.outputWs = groceries.get("outputWorkspace", groceries["inputWorkspace"])

    def findMaskBinsTableWorkspaces(self):
        """
        Locates bin Mask workspaces on the basis of their names and creates a list of these
        """

        self.binMasks = []
        for ws in mtd.getObjectNames():
            if "maskBins_" in ws:
                self.binMasks.append(ws)
        print(f"{len(self.binMasks)} binMasks were found in ADS")
        if len(self.binMasks) >= 1:
            self.hasBinMasks = True
        else:
            self.hasBinMasks = False
    
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

        if self.diffcalWs != "":
            self.mantidSnapper.ApplyDiffCal(
                "Applying diffcal..",
                InstrumentWorkspace=self.outputWs,
                CalibrationWorkspace=self.diffcalWs,
            )

        #check if any bin masks exist
        self.findMaskBinsTableWorkspaces()
        #apply bin Masks if they were found
        if self.hasBinMasks:
            for mask in self.binMasks:
                #extract units from ws name (table workspaces don't have logs)
                maskUnits = mask.split('_')[-1]
                #ensure units of workspace match
                self.mantidSnapper.ConvertUnits(
                    f"Converting units to match Bin Mask with units of {maskUnits}",
                    InputWorkspace=self.outputWs,
                    Target = maskUnits,
                    OutputWorkspace=self.outputWs
                )
                #mask bins
                self.mantidSnapper.MaskBinsFromTable(
                    "Masking bins...",
                    InputWorkspace=self.outputWs,
                    MaskingInformation=mask,
                    OutputWorkspace=self.outputWs
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
