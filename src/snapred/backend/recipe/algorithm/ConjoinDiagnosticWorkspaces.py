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
    RenameWorkspace,
    UnGroupWorkspace,
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
        # NOTE must be in alphabetical order
        self.diagnosticSuffix = {
            FitOutputEnum.PeakPosition.value: "_dspacing",
            FitOutputEnum.ParameterError.value: "_fiterror",
            FitOutputEnum.Parameters.value: "_fitparam",
            FitOutputEnum.Workspace.value: "_fitted",
        }

    def PyExec(self) -> None:
        self.autoDelete = self.getProperty("AutoDelete").value
        index = self.getProperty("AddAtIndex").value
        diag1 = self.getPropertyValue(self.INPUTGRPPROP1)
        outws = self.getPropertyValue(self.OUTPUTGRPPROP)

        # sort by name to pevent bad things from happening
        mtd[diag1].sortByName()

        oldNames = mtd[diag1].getNames()
        toInclude = [suffix for suffix in self.diagnosticSuffix.values() if suffix in (" ").join(oldNames)]
        newNames = [f"{outws}{suffix}" for suffix in toInclude]

        # if the input is expected to autodelete, it must be ungrouped first
        if self.autoDelete:
            UnGroupWorkspace(diag1)

        if index == 0:
            for old, new in zip(oldNames, newNames):
                if self.autoDelete:
                    RenameWorkspace(
                        InputWorkspace=old,
                        OutputWorkspace=new,
                    )
                else:
                    CloneWorkspace(
                        InputWorkspace=old,
                        OutputWorkspace=new,
                    )
                if isinstance(mtd[new], MatrixWorkspace) and index < mtd[new].getNumberHistograms():
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
        tmpws = f"{inws}_{index}"
        if index < mtd[inws].getNumberHistograms():
            ExtractSingleSpectrum(
                InputWorkspace=inws,
                Outputworkspace=tmpws,
                WorkspaceIndex=index,
            )
        else:
            CloneWorkspace(
                InputWorkspace=inws,
                OutputWorkspace=tmpws,
            )
        ConjoinWorkspaces(
            InputWorkspace1=outws,
            InputWorkspace2=tmpws,
            CheckOverlapping=False,
        )
        if self.autoDelete and inws in mtd:
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
