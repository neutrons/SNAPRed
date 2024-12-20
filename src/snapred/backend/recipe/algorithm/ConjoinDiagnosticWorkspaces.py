from typing import List

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

from snapred.meta.mantid.FitPeaksOutput import FIT_PEAK_DIAG_SUFFIX


class ConjoinDiagnosticWorkspaces(PythonAlgorithm):
    """
    Given the grouped diagnostic output from PDCalibration run on one spectum at a time,
    combine the sub-workspaces in an intelligent way.
    """

    INPUTGRPPROP1 = "DiagnosticWorkspace"
    OUTPUTGRPPROP = "TotalDiagnosticWorkspace"

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def newNamesFromOld(self, oldNames: List[str], newName: str) -> List[str]:
        selectedNames = set(self.diagnosticSuffix.values())
        suffixes = []
        for oldName in oldNames:
            elements = oldName.split("_")
            suffix = next((f"_{x}" for x in elements if f"_{x}" in selectedNames), None)
            if suffix is not None:
                suffixes.append(suffix)
        return [f"{newName}{suffix}" for suffix in suffixes]

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
        self.diagnosticSuffix = FIT_PEAK_DIAG_SUFFIX.copy()

    def PyExec(self) -> None:
        self.autoDelete = self.getProperty("AutoDelete").value
        index = self.getProperty("AddAtIndex").value
        diag1 = self.getPropertyValue(self.INPUTGRPPROP1)
        outws = self.getPropertyValue(self.OUTPUTGRPPROP)

        # sort by name to pevent bad things from happening
        mtd[diag1].sortByName()
        oldNames = mtd[diag1].getNames()
        newNames = self.newNamesFromOld(oldNames, outws)

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

        if self.autoDelete:
            for oldName in oldNames:
                if oldName in mtd:
                    DeleteWorkspace(oldName)

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
            CheckMatchingBins=False # Not available in 6.11.0.3rc2
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
