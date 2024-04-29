import json
from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager

from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.ReadWorkspaceMetadata import ReadWorkspaceMetadata
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[WorkspaceMetadata, Dict[str, str]]


@Singleton
class WriteWorkspaceMetadata(Recipe[WorkspaceMetadata]):
    TAG_PREFIX = Config["metadata.tagPrefix"]

    def chopIngredients(self, ingredients: WorkspaceMetadata):
        prevMetadata = ReadWorkspaceMetadata().cook({"workspace": self.workspace})

        # if the input WorkspaceMetadata has unset values, set them from the logs
        self.properties = list(WorkspaceMetadata.schema()["properties"].keys())
        metadata = WorkspaceMetadata.parse_obj(ingredients).dict()
        for prop in self.properties:
            if metadata[prop] == UNSET:
                metadata[prop] = getattr(prevMetadata, prop)

        # create the needed lists of logs to add
        self.metadataNames = [f"{self.TAG_PREFIX}{prop}" for prop in self.properties]
        self.metadataValues = [metadata[prop] for prop in self.properties]

    def validateInputs(self, ingredients: WorkspaceMetadata, groceries: Dict[str, WorkspaceName]):
        WorkspaceMetadata.parse_obj(ingredients)
        if not self.mantidSnapper.mtd.doesExist(groceries["workspace"]):
            raise RuntimeError(f"The indicated workspace {groceries['workspace']} not found in Mantid ADS.")

    def unbagGroceries(self, groceries: Dict[str, WorkspaceName]):
        self.workspace = groceries["workspace"]

    def queueAlgos(self):
        self.mantidSnapper.AddSampleLogMultiple(
            "Add the metadata tags as logs to the workspace",
            Workspace=self.workspace,
            LogNames=self.metadataNames,
            LogValues=self.metadataValues,
        )
