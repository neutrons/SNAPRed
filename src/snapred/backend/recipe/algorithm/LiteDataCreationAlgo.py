import os
import json

from mantid.api import *
from mantid.api import AlgorithmFactory, PythonAlgorithm, mtd
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config

name = "LiteDataCreationAlgo"


class LiteDataCreationAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("RunNumber", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        self.log().notice("Lite Data Creation START!")

        # get output workspace name
        outputWorkspaceName = self.getProperty("OutputWorkspace").value

        # load input workspace
        inputWorkspace = self.getProperty("InputWorkspace").value

        # load run number
        runNumber = self.getProperty("RunNumber").value

        # clone the workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            InputWorkspace=inputWorkspace,
            OutputWorkspace=outputWorkspaceName,
        )
        self.mantidSnapper.executeQueue()
        outputWorkspace = self.mantidSnapper.mtd[outputWorkspaceName]
        liteWorkspacename = f'{runNumber}_lite'
        # use group detector with specific grouping file to create lite data
        self.mantidSnapper.GroupDetectors(
            "Creating lite version...",
            InputWorkspace=outputWorkspace,
            outputWorkspace=liteWorkspacename,
            MapFile=str(Config["instrument.lite.definition.grouping"]),
        )
        self.mantidSnapper.LoadInstrument(
            "Updating instrument definition...",
            Workspace=liteWorkspacename,
            Filename=str(Config["instrument.lite.definition.file"]),
            RewriteSpectraMap=False,
        )
        self.mantidSnapper.executeQueue()
        liteWorkspace = self.mantidSnapper.mtd[liteWorkspacename]
        self.setProperty("OutputWorkspace", liteWorkspacename)
        return liteWorkspacename

# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
