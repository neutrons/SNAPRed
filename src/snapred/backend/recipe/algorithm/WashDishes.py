from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction, StringArrayProperty
from mantid.simpleapi import DeleteWorkspace, DeleteWorkspaces, mtd

from snapred.meta.Config import Config


class WashDishes(PythonAlgorithm):
    """
    You have made a big mess.  You need to wash up.
    But not unless the CIS wants to investigate the mess first.
    """

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty("Workspace", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty(StringArrayProperty(name="WorkspaceList", values=[], direction=Direction.Input))
        self.setRethrows(True)
        self._CISmode: bool = Config._config.get("cis_mode", False)

    def PyExec(self) -> None:
        self.log().notice("Washing the dishes...")
        workspace = self.getProperty("Workspace").value
        if workspace != "" and mtd.doesExist(workspace) and not self._CISmode:
            DeleteWorkspace(Workspace=workspace)
        workspaces = self.getProperty("WorkspaceList").value
        workspaces = [w for w in workspaces if mtd.doesExist(w)]
        if workspaces != [] and not self._CISmode:
            DeleteWorkspaces(workspaces)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(WashDishes)
