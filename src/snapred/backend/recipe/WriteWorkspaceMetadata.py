import json
from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager

from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.ReadWorkspaceMetadata import ReadWorkspaceMetadata
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[WorkspaceMetadata, Dict[str, str]]


@Singleton
class WriteWorkspaceMetadata:
    TAG_PREFIX = Config["metadata.tagPrefix"]

    def __init__(self, utensils: Utensils = None):
        """
        Sets up the recipe with the necessary utensils.
        """
        # NOTE: workaround, we just add an empty host algorithm.
        if utensils is None:
            utensils = Utensils()
            utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    def chopIngredients(self, ingredients: WorkspaceMetadata):
        """
        Chops off the needed elements of the ingredients.
        """
        ingredients = WorkspaceMetadata.parse_obj(ingredients)
        prevMetadata = ReadWorkspaceMetadata().cook({"workspace": self.workspace})

        # if the input WorkspaceMetadata has unset values, set them from the logs
        self.properties = list(WorkspaceMetadata.schema()["properties"].keys())
        for prop in self.properties:
            if getattr(ingredients, prop) == UNSET:
                setattr(ingredients, prop, getattr(prevMetadata, prop))

        # create the needed lists of logs to add
        self.metadataNames = [f"{self.TAG_PREFIX}{prop}" for prop in self.properties]
        self.metadataValues = [getattr(ingredients, prop) for prop in self.properties]

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        """
        self.workspace = groceries["workspace"]

    def validateInputs(self, ingredients: WorkspaceMetadata, groceries: Dict[str, str]):
        """
        Confirms that the ingredients and groceries make sense together.
        """
        # ensure the workspace exists in the ADS
        for name, ws in groceries.items():
            if not self.mantidSnapper.mtd.doesExist(ws):
                raise RuntimeError(f"The indicated workspace {ws} not found in Mantid ADS.")
        # ensure all log values are valid
        WorkspaceMetadata.parse_obj(ingredients)

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        self.mantidSnapper.AddSampleLogMultiple(
            "Add the metadata tags as logs to the workspace",
            Workspace=self.workspace,
            LogNames=self.metadataNames,
            LogValues=self.metadataValues,
        )

    def prep(self, ingredients: WorkspaceMetadata, groceries: Dict[str, str]):
        """
        Convinience method to prepare the recipe for execution.
        """
        self.validateInputs(ingredients, groceries)
        self.unbagGroceries(groceries)
        self.chopIngredients(ingredients)
        self.queueAlgos()

    def execute(self):
        """
        Final step in a recipe, executes the queued algorithms.
        Requires: queued algorithms.
        """
        self.mantidSnapper.executeQueue()

    def cook(self, ingredients: WorkspaceMetadata, groceries: Dict[str, str]) -> bool:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep(ingredients, groceries)
        self.execute()
        return True

    def cater(self, shipment: List[Pallet]) -> List[Dict[str, Any]]:
        """
        A secondary interface method for the recipe.
        It is a batched version of cook.
        Given a shipment of ingredients and groceries, it prepares, executes and returns the final workspaces.
        """
        output = []
        for ingredient, grocery in shipment:
            self.prep(ingredient, grocery)
            output.append(True)
        self.execute()
        return output
