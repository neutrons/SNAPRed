import json
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.ReadWorkspaceMetadata import ReadWorkspaceMetadata


class WriteWorkspaceMetadata(PythonAlgorithm):
    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("Workspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace to contain the logs",
        )
        self.declareProperty("WorkspaceMetadata", defaultValue="", direction=Direction.Input)
        # self.declareProperty("MetadataLogName", defaultValue=[], direction=Direction.Input) # string array property
        # self.declareProperty("MetadataLogValue", defaultValue=[], direction=Direction.Input) # string array property
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: WorkspaceMetadata):
        # create an object to hold any previously-added metadata
        prevMetadata = self.mantidSnapper.ReadWorkspaceMetadata(
            "Read any previous metadata logs",
            Workspace=self.getPropertyValue("Workspace"),
        )
        self.mantidSnapper.executeQueue()
        # NOTE the read algo validates the properties for us
        prevMetadata = json.loads(prevMetadata.get())

        # if the input WorkspaceMetadata has unset values, set them from the logs
        properties = list(WorkspaceMetadata.schema()["properties"].keys())
        for prop in properties:
            if getattr(ingredients, prop) == UNSET:
                setattr(ingredients, prop, prevMetadata[prop])

        self.metadataNames = [f"SNAPRed_{prop}" for prop in properties]
        self.metadataValues = [getattr(ingredients, prop) for prop in properties]

    def unbagGroceries(self):
        self.workspace = self.getPropertyValue("Workspace")

    def validateInputs(self) -> Dict[str, str]:
        return {}

    def PyExec(self):
        self.log().notice("Set the workspace metadata logs")
        metadata = WorkspaceMetadata.parse_raw(self.getPropertyValue("WorkspaceMetadata"))
        self.chopIngredients(metadata)
        self.unbagGroceries()

        # NOTE if the log already exists, it will be quietly overwritten
        self.mantidSnapper.AddSampleLogMultiple(
            "Add the metadata tags as logs to the workspace",
            Workspace=self.workspace,
            LogNames=self.metadataNames,
            LogValues=self.metadataValues,
        )
        self.mantidSnapper.executeQueue()
        self.setProperty("Workspace", self.workspace)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(WriteWorkspaceMetadata)
