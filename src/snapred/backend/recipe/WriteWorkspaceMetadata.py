import json
from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager

from snapred.backend.dao.WorkspaceMetadata import UNSET
from snapred.backend.dao.WorkspaceMetadata import WorkspaceMetadata as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.ReadWorkspaceMetadata import ReadWorkspaceMetadata
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


@Singleton
class WriteWorkspaceMetadata(Recipe):
    TAG_PREFIX = Config["metadata.tagPrefix"]

    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """
        ingredients = Ingredients.parse_obj(ingredients)
        prevMetadata = ReadWorkspaceMetadata().cook({"workspace": self.workspace})

        # if the input WorkspaceMetadata has unset values, set them from the logs
        self.properties = list(Ingredients.schema()["properties"].keys())
        for prop in self.properties:
            if getattr(ingredients, prop) == UNSET:
                setattr(ingredients, prop, getattr(prevMetadata, prop))

        # create the needed lists of logs to add
        self.metadataNames = [f"{self.TAG_PREFIX}{prop}" for prop in self.properties]
        self.metadataValues = [getattr(ingredients, prop) for prop in self.properties]

    def unbagGroceries(self, groceries: Dict[str, WorkspaceName]):
        """
        Unpacks the workspace data from the groceries.
        """
        self.workspace = groceries["workspace"]

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
