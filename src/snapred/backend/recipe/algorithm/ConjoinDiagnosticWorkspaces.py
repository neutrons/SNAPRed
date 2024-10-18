from mantid.api import MatrixWorkspace, PythonAlgorithm, WorkspaceGroupProperty
from mantid.dataobjects import TableWorkspace
from mantid.kernel import Direction, IntPropertyWithValue
from mantid.simpleapi import (
    BufferMissingColumnsAlgo,
    CloneWorkspace,
    ConjoinTableWorkspaces,
    ConjoinWorkspaces,
    DeleteWorkspace,
    ExtractSingleSpectrum,
    GroupWorkspaces,
    mtd,
)

from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitOutputEnum


class ConjoinDiagnosticWorkspaces(PythonAlgorithm):
    """
    Given the grouped diagnostic output from PDCalibration run on one spectum at a time,
    combine the sub-workspaces in an intelligent way.
    """

    INPUTGRPPROP1 = "DiagnosticWorkspace"
    OUTPUTGRPPROP = "TotalDiagnosticWorkspace"

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            WorkspaceGroupProperty(self.INPUTGRPPROP1, "", direction=Direction.Input),
            doc="Table workspace from peak-fitting diagnosis.",
        )
        self.declareProperty(IntPropertyWithValue("AddAtIndex", 0))
        self.declareProperty(
            WorkspaceGroupProperty(self.OUTPUTGRPPROP, "", direction=Direction.Output),
            doc="Result of conjoining the diagnostic workspaces",
        )
        self.declareProperty("AutoDelete", False)
        self.setRethrows(True)
        self.diagnosticSuffix = {
            FitOutputEnum.PeakPosition.value: "_dspacing",
            FitOutputEnum.Parameters.value: "_fitparam",
            FitOutputEnum.Workspace.value: "_fitted",
        }

    def PyExec(self) -> None:
        self.autoDelete = self.getProperty("AutoDelete").value
        index = self.getProperty("AddAtIndex").value
        diag1 = self.getPropertyValue(self.INPUTGRPPROP1)
        outws = self.getPropertyValue(self.OUTPUTGRPPROP)

        oldNames = [f"{diag1}{suffix}" for suffix in self.diagnosticSuffix.values()]
        newNames = [f"{outws}{suffix}" for suffix in self.diagnosticSuffix.values()]

        if index == 0:
            for old, new in zip(oldNames, newNames):
                CloneWorkspace(
                    InputWorkspace=old,
                    OutputWorkspace=new,
                )
                if isinstance(mtd[new], MatrixWorkspace):
                    ExtractSingleSpectrum(
                        InputWorkspace=new,
                        OutputWorkspace=new,
                        WorkspaceIndex=index,
                    )
            GroupWorkspaces(
                InputWorkspaces=newNames,
                OutputWorkspace=outws,
            )
        else:
            for old, new in zip(oldNames, newNames):
                ws = mtd[old]
                if isinstance(ws, MatrixWorkspace):
                    self.conjoinMatrixWorkspaces(old, new, index)
                elif isinstance(ws, TableWorkspace):
                    self.conjoinTableWorkspaces(old, new, index)
                else:
                    raise RuntimeError(f"Unrecognized workspace type {type(ws)}")

        self.setProperty(self.OUTPUTGRPPROP, mtd[outws])

    def conjoinMatrixWorkspaces(self, inws, outws, index):
        ExtractSingleSpectrum(
            InputWorkspace=inws,
            Outputworkspace=inws,
            WorkspaceIndex=index,
        )
        ConjoinWorkspaces(
            InputWorkspace1=outws,
            InputWorkspace2=inws,
            CheckOverlapping=False,
        )
        if self.autoDelete:
            DeleteWorkspace(inws)
        assert outws in mtd

    def conjoinTableWorkspaces(self, inws, outws, index):  # noqa: ARG002
        BufferMissingColumnsAlgo(
            Workspace1=inws,
            Workspace2=outws,
        )
        BufferMissingColumnsAlgo(
            Workspace1=outws,
            Workspace2=inws,
        )
        ConjoinTableWorkspaces(
            InputWorkspace1=outws,
            InputWorkspace2=inws,
            AutoDelete=self.autoDelete,
        )
