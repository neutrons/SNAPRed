import json
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction, StringArrayProperty
from pydantic import ValidationError

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
        self.declareProperty("WorkspaceMetadata", defaultValue="{}", direction=Direction.Input)
        # writing individual logs without an object
        self.declareProperty(StringArrayProperty("MetadataLogNames", values=[], direction=Direction.Input))
        self.declareProperty(StringArrayProperty("MetadataLogValues", values=[], direction=Direction.Input))
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

        # create an object from the input string logs
        stringLogNames = self.getProperty("MetadataLogNames").value
        stringLogValues = self.getProperty("MetadataLogValues").value
        stringMetadata = WorkspaceMetadata.parse_obj(dict(zip(stringLogNames, stringLogValues)))

        # if the input WorkspaceMetadata has unset values, set them from the logs
        properties = list(WorkspaceMetadata.schema()["properties"].keys())
        for prop in properties:
            if getattr(ingredients, prop) == UNSET:
                setattr(ingredients, prop, prevMetadata[prop])

        # in the input WorkspaceMetadata has unset values, set them from the inputs
        for prop in properties:
            if getattr(ingredients, prop) == UNSET:
                setattr(ingredients, prop, getattr(stringMetadata, prop))

        self.metadataNames = [f"SNAPRed_{prop}" for prop in properties]
        self.metadataValues = [getattr(ingredients, prop) for prop in properties]

    def unbagGroceries(self):
        self.workspace = self.getPropertyValue("Workspace")

    def validateInputs(self) -> Dict[str, str]:
        userSetDAO = not self.getProperty("WorkspaceMetadata").isDefault
        userSetList = not self.getProperty("MetadataLogNames").isDefault
        if not (userSetDAO ^ userSetList):
            msg = "You may set logs EITHER through object or as lists"
            return {"WorkspaceMetadata": msg, "MetadataLogNames": msg}
        elif userSetDAO:
            try:
                WorkspaceMetadata.parse_raw(self.getPropertyValue("WorkspaceMetadata"))
            except ValidationError as e:
                msg = f"Invalid WorkspaceMetadata object detected: {str(e)}"
                return {"WorkspaceMetadata": msg}
        elif userSetList:
            props = self.getProperty("MetadataLogNames").value
            vals = self.getProperty("MetadataLogValues").value
            if len(props) != len(vals):
                msg = f"Properties and values do not match in size: {props} vs {vals}"
                return {"MetadataLogNames": msg, "MetadataLogValues": msg}
            obj_dict = dict(zip(props, vals))
            try:
                WorkspaceMetadata.parse_obj(obj_dict)
            except ValidationError:
                msg = f"Invalid metadata names or values: {obj_dict}"
                return {"MetadataLogNames": msg, "MetadataLogValues": msg}
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
