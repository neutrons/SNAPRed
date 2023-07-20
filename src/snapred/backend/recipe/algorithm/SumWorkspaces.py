from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "SumWorkspaces"


class SumWorkspaces(PythonAlgorithm):
    """
    Given a list of workspaces, will sum them all together.
    At each x-value, will add the corresponding y-values inside each workspace.
    input:
        InputWorkspaces: List[str] -- a list of names of MatrixWorkspaces to be summed
    output:
        OutputWorkspace: str -- the name of the workspace containing the sum
        also returns pointer to the sum workspace
    """

    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspaces", defaultValue=[""], direction=Direction.Input)  # noqa: F821
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        # run the algo
        wsList = list(self.getProperty("InputWorkspaces").value)
        outputWS = self.getProperty("OutputWorkspace").value
        if len(wsList) < 1:
            self.mantidSnapper.CreateWorkspace(
                "No worksheets given, creating empty workspace",
                OutputWorkspace=outputWS,
                DataX=[0],
                DataY=[0],
            )
        else:
            self.mantidSnapper.RenameWorkspace(
                "Initialize to first element of list",
                InputWorkspace=wsList[0],
                OutputWorkspace=outputWS,
            )
            for ws in wsList[1:]:
                self.mantidSnapper.Plus(
                    f"Adding {ws}",
                    LHSWorkspace=outputWS,
                    RHSWorkspace=ws,
                    OutputWorkspace=outputWS,
                )
                self.mantidSnapper.DeleteWorkspace(
                    f"Deleting {ws}",
                    Workspace=ws,
                )
        self.mantidSnapper.executeQueue()
        return outputWS


# Register algorithm with Mantid
AlgorithmFactory.subscribe(SumWorkspaces)
