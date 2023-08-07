import json

from mantid.api import AlgorithmFactory, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CustomGroupWorkspace"


class CustomGroupWorkspace(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("StateConfig", defaultValue="", direction=Direction.Input)
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Optional),
            doc="Workspace to take the instrument object from",
        )

        self.declareProperty("InstrumentName", defaultValue="SNAP", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="CommonRed", direction=Direction.Output)
        self.mantidSnapper = MantidSnapper(self, name)

    # TODO: This was largely copied from Malcolm's prototype and is due for a refactor
    def PyExec(self):
        stateConfig = StateConfig(**json.loads(self.getProperty("StateConfig").value))
        focusGroups = stateConfig.focusGroups
        self.getProperty("InstrumentName").value
        outputWorkspace = self.getProperty("OutputWorkspace").value

        donorWorkspace = self.getProperty("InputWorkspace").value
        loadEmptyInstrument = bool(not donorWorkspace)
        if loadEmptyInstrument:
            donorWorkspace = "idf"  # name doesn't matter
            self.mantidSnapper.LoadEmptyInstrument(
                "Loading empty instrument...",
                Workspace=donorWorkspace,
                Filename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
                RewriteSpectraMap=False,
            )
            self.mantidSnapper.executeQueue()
        else:
            # convert workspace to string to avoid dangling pointers
            donorWorkspace = str(donorWorkspace)

        for grpIndx, focusGroup in enumerate(focusGroups):
            self.mantidSnapper.LoadDetectorsGroupingFile(
                f"Loading grouping file for focus group {focusGroup.name}...",
                InputFile=focusGroup.definition,
                InputWorkspace=donorWorkspace,
                OutputWorkspace=focusGroup.name,
            )
            self.mantidSnapper.executeQueue()

        # cleanup temporary workspace
        if loadEmptyInstrument:
            self.mantidSnapper.DeleteWorkspace("Deleting empty instrument...", Workspace=donorWorkspace)
            self.mantidSnapper.executeQueue()

        # create a workspace group of GroupWorkspaces
        self.mantidSnapper.GroupWorkspaces(
            "Grouping workspaces...",
            InputWorkspaces=[focusGroup.name for focusGroup in focusGroups],
            OutputWorkspace=outputWorkspace,
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomGroupWorkspace)
