import json

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import CreateWorkspace, GroupWorkspaces, LoadDetectorsGroupingFile, LoadInstrument

from snapred.backend.dao.StateConfig import StateConfig

name = "CustomGroupWorkspace"


class CustomGroupWorkspace(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("StateConfig", defaultValue="", direction=Direction.Input)
        self.declareProperty("InstrumentName", defaultValue="SNAP", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="CommonRed", direction=Direction.Output)
        pass

    # TODO: This was largely copied from Malcolm's prototype and is due for a refactor
    def PyExec(self):
        stateConfig = StateConfig(**json.loads(self.getProperty("StateConfig").value))
        focusGroups = stateConfig.focusGroups
        self.getProperty("InstrumentName").value
        outputWorkspace = self.getProperty("OutputWorkspace").value

        CreateWorkspace(OutputWorkspace="idf", DataX=1, DataY=1)
        LoadInstrument(
            Workspace="idf",
            Filename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
            MonitorList="-2--1",
            RewriteSpectraMap=False,
        )

        for grpIndx, focusGroup in enumerate(focusGroups):
            LoadDetectorsGroupingFile(
                InputFile=focusGroup.definition, InputWorkspace="idf", OutputWorkspace=focusGroup.name
            )

        GroupWorkspaces(
            InputWorkspaces=[focusGroup.name for focusGroup in focusGroups], OutputWorkspace=outputWorkspace
        )


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomGroupWorkspace)
