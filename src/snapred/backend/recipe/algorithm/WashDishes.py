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

        cisModeConfig = Config["cis_mode"]
        self.enabled: bool = cisModeConfig.get("enabled")
        self.preserve: bool = cisModeConfig.get("preserveDiagnosticWorkspaces")

        if self.enabled != self.preserve:
            self.log().notice(
                f"Mismatch in config: cis_mode.enabled={self.enabled}, "
                f"cis_mode.preserveDiagnosticWorkspaces={self.preserve}."
            )

        self.setRethrows(True)

    def PyExec(self) -> None:
        self.log().notice("Washing the dishes...")

        workspace = self.getProperty("Workspace").value
        if workspace and mtd.doesExist(workspace):
            if not self.enabled:
                if not self.preserve:
                    DeleteWorkspace(Workspace=workspace)

        workspaces = self.getProperty("WorkspaceList").value
        workspaces = [w for w in workspaces if mtd.doesExist(w)]
        if workspaces:
            if not self.enabled:
                if not self.preserve:
                    DeleteWorkspaces(WorkspaceList=workspaces)


# Register algorithm
AlgorithmFactory.subscribe(WashDishes)
