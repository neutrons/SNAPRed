from typing import Any, Dict, List, Set, Tuple

from snapred.backend.dao.ingredients import ReductionGroupProcessingIngredients as Ingredients
from snapred.backend.dao.WorkspaceMetadata import ParticleNormalizationMethod, WorkspaceMetadata
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.backend.recipe.WriteWorkspaceMetadata import WriteWorkspaceMetadata
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


class ReductionGroupProcessingRecipe(Recipe[Ingredients]):
    def allGroceryKeys(self):
        return {"inputWorkspace", "groupingWorkspace", "outputWorkspace"}

    def mandatoryInputWorkspaces(self) -> Set[WorkspaceName]:
        return {"inputWorkspace", "groupingWorkspace"}

    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.rawInput = groceries["inputWorkspace"]
        self.outputWS = groceries.get("outputWorkspace", groceries["inputWorkspace"])
        self.groupingWS = groceries["groupingWorkspace"]

    def chopIngredients(self, ingredients):
        self.pixelGroup = ingredients.pixelGroup
        self.preserveEvents = ingredients.preserveEvents
        logger.debug(f"dMin: {self.pixelGroup.dMin()}")
        logger.debug(f"dMax: {self.pixelGroup.dMax()}")
        logger.debug(f"dBin: {self.pixelGroup.dBin()}")

    def _validateWSUnits(self, key, ws):
        if key == "inputWorkspace":
            # assert that the input workspace is in dSpacing
            wsInstance = self.mantidSnapper.mtd[ws]
            wsUnit = wsInstance.getAxis(0).getUnit().unitID()
            if wsUnit != "dSpacing":
                raise RuntimeError(
                    (
                        f"Input workspace {ws} is of units {wsUnit}."
                        " Please convert it to dSpacing before using this recipe."
                    )
                )

    def _validateGrocery(self, key, ws):
        super()._validateGrocery(key, ws)
        self._validateWSUnits(key, ws)

    def queueAlgos(self):
        """
        Queues up the processing algorithms for the recipe.
        Requires: unbagged groceries.
        """

        self.mantidSnapper.FocusSpectraAlgorithm(
            "Focusing Spectra...",
            InputWorkspace=self.rawInput,
            OutputWorkspace=self.outputWS,
            GroupingWorkspace=self.groupingWS,
            PixelGroup=self.pixelGroup.json(),
            PreserveEvents=self.preserveEvents
        )

        normalizeArgs = {
            "InputWorkspace": self.outputWS,
            "OutputWorkspace": self.outputWS,
        }

        if Config["mantid.workspace.normalizeByBeamMonitor"]:
            normalizeArgs["NormalizeByMonitorCounts"] = self.mantidSnapper.mtd.getSNAPRedLog(
                self.rawInput, "normalizeByMonitorFactor"
            )

        self.mantidSnapper.NormalizeByCurrentButTheCorrectWay(
            "Normalizing Current ... but the correct way!",
            **normalizeArgs,
        )

    def execute(self):
        """
        Final step in a recipe, executes the queued algorithms.
        Requires: queued algorithms.
        """
        try:
            self.mantidSnapper.executeQueue()

            workspaceMetadata = WorkspaceMetadata(particleNormalizationMethod=ParticleNormalizationMethod.PROTON_CHARGE)

            if Config["mantid.workspace.normalizeByBeamMonitor"]:
                workspaceMetadata = WorkspaceMetadata(
                    particleNormalizationMethod=ParticleNormalizationMethod.MONITOR_COUNTS
                )
            WriteWorkspaceMetadata().cook(workspaceMetadata, {"workspace": self.outputWS})

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
