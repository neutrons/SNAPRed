from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.WorkspaceMetadata import WorkspaceMetadata
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class ReadWorkspaceMetadata(PythonAlgorithm):
    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty("Workspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("WorkspaceMetadata", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients):
        pass

    def unbagGroceries(self):
        self.workspace = self.getPropertyValue("Workspace")

    def PyExec(self):
        self.log().notice("Fetch the workspace metadata logs")
        self.unbagGroceries()

        properties = list(WorkspaceMetadata.schema()["properties"].keys())
        metadata = {}
        run = self.mantidSnapper.mtd[self.workspace].getRun()  # NOTE this is a mantid object that holds logs
        for prop in properties:
            if run.hasProperty(f"SNAPRed_{prop}"):
                metadata[prop] = run.getLogData(f"SNAPRed_{prop}").value
        metadata = WorkspaceMetadata.parse_obj(metadata)
        self.setPropertyValue("WorkspaceMetadata", metadata.json())


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReadWorkspaceMetadata)
