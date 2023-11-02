import json

from mantid.api import *
from mantid.api import AlgorithmFactory, PythonAlgorithm, mtd
from mantid.kernel import Direction

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
from snapred.meta.Config import Config

name = "LiteDataCreationAlgo"


class LiteDataCreationAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("Run", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, run: RunConfig):
        self.runNumber = run.runNumber
        self.IPTS = run.IPTS
        print(self.IPTS)
        self.rawDataPath: str = self.IPTS + "nexus/SNAP_{}.nxs.h5".format(self.runNumber)
        pass

    def PyExec(self):
        self.log().notice("Lite Data Creation START!")

        # load input workspace
        inputWorkspaceName = self.getProperty("InputWorkspace").value

        # load run number
        run = RunConfig(**json.loads(self.getProperty("Run").value))
        self.chopIngredients(run)

        # create outputworkspace
        outputWorkspaceName = self.getProperty("OutputWorkspace").value

        # load file
        self.mantidSnapper.LoadEventNexus(
            "Loading Event Nexus for {}...".format(self.rawDataPath),
            Filename=self.rawDataPath,
            OutputWorkspace=inputWorkspaceName,
            NumberOfBins=1,
            LoadMonitors=False,
        )

        groupingWorkspaceName = f"{self.runNumber}_lite_grouping_ws"

        # load grouping map
        self.mantidSnapper.LoadGroupingDefinition(
            "Loading lite grouping information...",
            GroupingFilename=str(Config["instrument.lite.map.file"]),
            InstrumentFilename=str(Config["instrument.native.definition.file"]),
            OutputWorkspace=groupingWorkspaceName,
        )

        self.mantidSnapper.executeQueue()
        groupingWorkspace = self.mantidSnapper.mtd[groupingWorkspaceName]

        # use group detector with specific grouping file to create lite data
        self.mantidSnapper.GroupDetectors(
            "Creating lite version...",
            InputWorkspace=inputWorkspaceName,
            OutputWorkspace=outputWorkspaceName,
            CopyGroupingFromWorkspace=groupingWorkspaceName,
        )

        self.mantidSnapper.CompressEvents(
            f"Compressing events in {outputWorkspaceName}...",
            InputWorkspace=outputWorkspaceName,
            OutputWorkspace=outputWorkspaceName,
            Tolerance=1,
        )

        self.mantidSnapper.executeQueue()

        self.mantidSnapper.DeleteWorkspace(
            f"Deleting {groupingWorkspaceName}...",
            Workspace=groupingWorkspaceName,
        )

        self.mantidSnapper.executeQueue()
        self.mantidSnapper.mtd[outputWorkspaceName]
        self.setProperty("OutputWorkspace", outputWorkspaceName)

# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
