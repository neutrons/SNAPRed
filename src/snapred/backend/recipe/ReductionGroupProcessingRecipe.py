from typing import Any, Dict, List, Tuple

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Dict[str, str]]


@Singleton
class ReductionGroupProcessingRecipe:
    def __init__(self, utensils: Utensils = None):
        """
        Sets up the recipe with the necessary utensils.
        """
        # NOTE: workaround, we just add an empty host algorithm.
        if utensils is None:
            utensils = Utensils()
            utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.rawInput = groceries["inputWorkspace"]
        self.outputWS = groceries["outputWorkspace"]
        self.geometryOutputWS = groceries["geometryOutputWorkspace"]
        self.diffFocOutputWS = groceries["diffFocOutputWorkspace"]
        self.groupingWS = groceries["groupingWorkspace"]

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

        self.mantidSnapper.DiffractionFocussing(
            "Applying Diffraction Focussing...",
            InputWorkspace=self.geometryOutputWS,
            GroupingWorkspace=self.groupingWS,
            OutputWorkspace=self.diffFocOutputWS,
        )

        self.mantidSnapper.NormaliseByCurrent(
            "Normalizing Current ...",
            InputWorkspace=self.diffFocOutputWS,
            OutputWorkspace=self.outputWS,
        )

    def prep(self, groceries: Dict[str, str]):
        """
        Convinience method to prepare the recipe for execution.
        """
        self.unbagGroceries(groceries)
        self.queueAlgos()

    def execute(self):
        """
        Final step in a recipe, executes the queued algorithms.
        Requires: queued algorithms.
        """
        self.mantidSnapper.executeQueue()

    def cook(self, groceries: Dict[str, str]) -> Dict[str, Any]:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep(groceries)
        self.execute()
        return self.outputWS

    def cater(self, shipment: List[Pallet]) -> List[Dict[str, Any]]:
        """
        A secondary interface method for the recipe.
        It is a batched version of cook.
        Given a shipment of ingredients and groceries, it prepares, executes and returns the final workspaces.
        """
        output = []
        for grocery in shipment:
            self.prep(grocery)
            output.append(self.outputWS)
        self.execute()
        return output
