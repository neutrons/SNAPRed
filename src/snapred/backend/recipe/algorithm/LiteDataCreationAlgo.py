import json

from mantid.api import *
from mantid.api import AlgorithmFactory, PythonAlgorithm, mtd
from mantid.kernel import Direction

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config

name = "LiteDataCreationAlgo"


class LiteDataCreationAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing full resolution the data set to be converted to lite",
        )
        self.declareProperty("RunConfig", defaultValue="", direction=Direction.Input)
        self.declareProperty("AutoDeleteNonLiteWS", defaultValue=False, direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, run: RunConfig):
        self.runNumber = run.runNumber
        self.IPTS = run.IPTS
        self.rawDataPath: str = self.IPTS + "nexus/SNAP_{}.nxs.h5".format(self.runNumber)
        self.isLite = run.isLite
        pass

    def PyExec(self):
        self.log().notice("Lite Data Creation START!")

        # load input workspace
        inputWorkspaceName = self.getPropertyValue("InputWorkspace")

        # flag for auto deletion of non-lite data
        autoDelete = self.getProperty("AutoDeleteNonLiteWS").value

        # load run number
        run = RunConfig.parse_raw(self.getPropertyValue("RunConfig"))
        self.chopIngredients(run)

        # create outputworkspace
        outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

        # # load file - NOTE: Algo doesn't need to load its own data.
        # self.mantidSnapper.LoadEventNexus(
        #     "Loading Event Nexus for {}...".format(self.rawDataPath),
        #     Filename=self.rawDataPath,
        #     OutputWorkspace=inputWorkspaceName,
        #     NumberOfBins=1,
        #     LoadMonitors=False,
        # )

        groupingWorkspaceName = f"{self.runNumber}_lite_grouping_ws"

        # load grouping map
        self.mantidSnapper.LoadGroupingDefinition(
            "Loading lite grouping information...",
            GroupingFilename=str(Config["instrument.lite.map.file"]),
            InstrumentFilename=str(Config["instrument.native.definition.file"]),
            OutputWorkspace=groupingWorkspaceName,
        )

        self.mantidSnapper.executeQueue()

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

        if autoDelete is True:
            self.mantidSnapper.DeleteWorkspace(
                f"Deleting {inputWorkspaceName}...",
                Workspace=f"{self.runNumber}_raw",
            )

        self.mantidSnapper.DeleteWorkspace(
            f"Deleting {groupingWorkspaceName}...",
            Workspace=groupingWorkspaceName,
        )

        self.mantidSnapper.executeQueue()
        self.mantidSnapper.mtd[outputWorkspaceName]
        self.setProperty("OutputWorkspace", outputWorkspaceName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
