from typing import Any, Dict, Tuple

from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[WorkspaceMetadata, Dict[str, WorkspaceName]]


@Singleton
class ReadWorkspaceMetadata(Recipe[WorkspaceMetadata]):
    TAG_PREFIX = Config["metadata.tagPrefix"]

    def chopIngredients(self, ingredients):  # noqa ARG002
        """
        Create the metadata object
        """
        properties = list(WorkspaceMetadata.model_json_schema()["properties"].keys())
        self.metadata = {}
        run = self.mantidSnapper.mtd[self.workspace].getRun()  # NOTE this is a mantid object that holds logs
        for prop in properties:
            propLogName = f"{self.TAG_PREFIX}{prop}"
            self.metadata[prop] = run.getLogData(propLogName).value if run.hasProperty(propLogName) else UNSET
        self.metadata = WorkspaceMetadata.model_validate(self.metadata)

    def unbagGroceries(self, groceries: Dict[str, Any]):
        self.workspace = groceries["workspace"]

    def queueAlgos(self):
        pass

    def execute(self):
        pass

    def cook(self, groceries: Dict[str, str]) -> WorkspaceMetadata:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep({}, groceries)
        self.execute()
        return self.metadata
