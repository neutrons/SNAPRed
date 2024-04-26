import json
from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager

from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[WorkspaceMetadata, Dict[str, str]]


@Singleton
class ReadWorkspaceMetadata:
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

    def chopIngredients(self, ingredients):  # noqa ARG002
        """
        Chops off the needed elements of the ingredients.
        """
        properties = list(WorkspaceMetadata.schema()["properties"].keys())
        self.metadata = {}
        run = self.mantidSnapper.mtd[self.workspace].getRun()  # NOTE this is a mantid object that holds logs
        for prop in properties:
            propLogName = f"{self.TAG_PREFIX}{prop}"
            self.metadata[prop] = run.getLogData(propLogName).value if run.hasProperty(propLogName) else UNSET
        self.metadata = WorkspaceMetadata.parse_obj(self.metadata)

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        """
        self.workspace = groceries["workspace"]

    def validateInputs(self, ingredients, groceries):  # noqa ARG002
        """
        Confirms that the ingredients and groceries make sense together.
        """
        # ensure the workspace exists in the ADS
        for x, y in groceries.items():
            if not self.mantidSnapper.mtd.doesExist(y):
                raise RuntimeError(f"The indicated workspace {y} not found in Mantid ADS.")

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        pass

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
        pass

    def cook(self, groceries: Dict[str, str]) -> WorkspaceMetadata:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep("", groceries)
        self.execute()
        return self.metadata
