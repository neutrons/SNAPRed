from typing import Any, Dict, List, Tuple

from snapred.backend.dao.state.PixelGroup import PixelGroup as Ingredients
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class ReductionGroupProcessingRecipe(Recipe[Ingredients]):
    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.rawInput = groceries["inputWorkspace"]
        self.outputWS = groceries.get("outputWorkspace", groceries["inputWorkspace"])
        # self.geometryOutputWS = groceries["geometryOutputWorkspace"]
        # self.diffFocOutputWS = groceries["diffFocOutputWorkspace"]
        self.groupingWS = groceries["groupingWorkspace"]

    def chopIngredients(self, ingredients):
        self.pixelGroup = ingredients.pixelGroup

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries.
        """
        # TODO: This is all subject to change based on EWM 4798
        # if self.rawInput is not None:
        #     logger.info("Processing Reduction Group...")
        #     estimateGeometryAlgo = EstimateFocusedInstrumentGeometry()
        #     estimateGeometryAlgo.initialize()
        #     estimateGeometryAlgo.setProperty("GroupingWorkspace", self.groupingWS)
        #     estimateGeometryAlgo.setProperty("OutputWorkspace", self.geometryOutputWS)
        #     try:
        #         estimateGeometryAlgo.execute()
        #         data["focusParams"] = estimateGeometryAlgo.getPropertyValue("FocusParams")
        #     except RuntimeError as e:
        #         errorString = str(e)
        #         raise RuntimeError(errorString) from e
        # else:
        #     raise NotImplementedError

        # self.mantidSnapper.EditInstrumentGeometry(
        #     "Editing Instrument Geometry...",
        #         Workspace=self.geometryOutputWS,
        #         L2=data["focusParams"].L2,
        #         Polar=data["focusParams"].Polar,
        #         Azimuthal=data["focusParams"].Azimuthal,
        # )
        # self.rawInput = self.geometryOutputWS

        self.mantidSnapper.FocusSpectraAlgorithm(
            "Focusing Spectra...",
            InputWorkspace=self.rawInput,
            OutputWorkspace=self.rawInput,
            GroupingWorkspace=self.groupingWS,
            Ingredients=self.pixelGroup.json(),
            RebinOutput=False,
        )

        self.mantidSnapper.NormalizeByCurrentButTheCorrectWay(
            "Normalizing Current ... but the correct way!",
            InputWorkspace=self.rawInput,
            OutputWorkspace=self.rawInput,
        )
        self.outputWS = self.rawInput

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
