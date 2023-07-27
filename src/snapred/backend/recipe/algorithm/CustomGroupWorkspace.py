import json

from mantid.api import AlgorithmFactory, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import (
    CreateWorkspace,
    DeleteWorkspace,
    GroupWorkspaces,
    LoadDetectorsGroupingFile,
    LoadInstrument,
)

from snapred.backend.dao.StateConfig import StateConfig

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
            CreateWorkspace(OutputWorkspace=donorWorkspace, DataX=1, DataY=1)
            LoadInstrument(
                Workspace=donorWorkspace,
                Filename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
                MonitorList="-2--1",
                RewriteSpectraMap=False,
            )
        else:
            # convert workspace to string to avoid dangling pointers
            donorWorkspace = str(donorWorkspace)

        for grpIndx, focusGroup in enumerate(focusGroups):
            LoadDetectorsGroupingFile(
                InputFile=focusGroup.definition, InputWorkspace=donorWorkspace, OutputWorkspace=focusGroup.name
            )

        # cleanup temporary workspace
        if loadEmptyInstrument:
            DeleteWorkspace(Workspace=donorWorkspace)

        # create a workspace group of GroupWorkspaces
        GroupWorkspaces(
            InputWorkspaces=[focusGroup.name for focusGroup in focusGroups], OutputWorkspace=outputWorkspace
        )


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomGroupWorkspace)
