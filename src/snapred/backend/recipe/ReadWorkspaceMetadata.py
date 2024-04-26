import json
from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager

from snapred.backend.dao.WorkspaceMetadata import UNSET
from snapred.backend.dao.WorkspaceMetadata import WorkspaceMetadata as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, WorkspaceName]]


@Singleton
class ReadWorkspaceMetadata(Recipe):
    TAG_PREFIX = Config["metadata.tagPrefix"]

    def chopIngredients(self, ingredients):  # noqa ARG002
        """
        Chops off the needed elements of the ingredients.
        """
        properties = list(Ingredients.schema()["properties"].keys())
        self.metadata = {}
        run = self.mantidSnapper.mtd[self.workspace].getRun()  # NOTE this is a mantid object that holds logs
        for prop in properties:
            propLogName = f"{self.TAG_PREFIX}{prop}"
            self.metadata[prop] = run.getLogData(propLogName).value if run.hasProperty(propLogName) else UNSET
        self.metadata = Ingredients.parse_obj(self.metadata)

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        """
        self.workspace = groceries["workspace"]

    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        pass

    def execute(self):
        """
        Final step in a recipe, executes the queued algorithms.
        Requires: queued algorithms.
        """
        pass

    def cook(self, groceries: Dict[str, str]) -> Ingredients:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep({}, groceries)
        self.execute()
        return self.metadata
