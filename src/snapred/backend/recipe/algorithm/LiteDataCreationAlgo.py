import json

from mantid.api import *
from mantid.api import AlgorithmFactory, PythonAlgorithm, mtd
from mantid.kernel import Direction

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class LiteDataCreationAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing full resolution the data set to be converted to lite",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("LiteDataMapWorkspace", "", Direction.Input, PropertyMode.Optional),
            doc="Grouping workspace which converts maps full pixel resolution to lite data",
        )
        self.declareProperty(
            "LiteInstrumentDefinitionFile",
            str(Config["instrument.lite.definition.file"]),
            Direction.Input,
        )
        self.declareProperty("AutoDeleteNonLiteWS", defaultValue=False, direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def PyExec(self):
        self.log().notice("Lite Data Creation START!")

        # load input workspace
        inputWorkspaceName = self.getPropertyValue("InputWorkspace")

        # flag for auto deletion of non-lite data
        autoDelete = self.getProperty("AutoDeleteNonLiteWS").value

        # create outputworkspace
        if self.getProperty("OutputWorkspace").isDefault:
            # TODO use workspace name generatoe
            outputWorkspaceName = inputWorkspaceName + "_lite"
            self.setPropertyValue("OutputWorkspace", outputWorkspaceName)
        else:
            outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

        if self.getProperty("LiteDataMapWorkspace").isDefault:
            groupingWorkspaceName = "lite_grouping_map"
            self.mantidSnapper.LoadGroupingDefinition(
                "Load lite grouping pixel map",
                GroupingFilename=str(Config["instrument.lite.map.file"]),
                InstrumentFilename=str(Config["instrument.native.definition.file"]),
                OutputWorkspace=groupingWorkspaceName,
            )
        else:
            groupingWorkspaceName = self.getPropertyValue("LiteDataMapWorkspace")

        # use group detector with specific grouping file to create lite data
        self.mantidSnapper.GroupDetectors(
            "Creating lite version...",
            InputWorkspace=inputWorkspaceName,
            OutputWorkspace=outputWorkspaceName,
            CopyGroupingFromWorkspace=groupingWorkspaceName,
        )

        # iterate over spectra in grouped workspace, delete old ids and replace
        self.mantidSnapper.executeQueue()
        liteWksp = self.mantidSnapper.mtd[outputWorkspaceName]
        nHst = liteWksp.getNumberHistograms()
        for i in range(nHst):
            el = liteWksp.getSpectrum(i)
            el.clearDetectorIDs()
            el.addDetectorID(i)

        # replace instrument definition with lite
        self.mantidSnapper.LoadInstrument(
            "Replacing instrument definition with Lite instrument",
            Workspace=outputWorkspaceName,
            Filename=self.getPropertyValue("LiteInstrumentDefinitionFile"),
            RewriteSpectraMap=False,
        )

        ###END Pete's suggestion to fix pixelID mapping
        self.mantidSnapper.CompressEvents(
            f"Compressing events in {outputWorkspaceName}...",
            InputWorkspace=outputWorkspaceName,
            OutputWorkspace=outputWorkspaceName,
            Tolerance=1,
        )

        if autoDelete is True:
            self.mantidSnapper.WashDishes(
                f"Deleting {inputWorkspaceName}...",
                Workspace=inputWorkspaceName,
            )

        if self.getProperty("LiteDataMapWorkspace").isDefault:
            self.mantidSnapper.WashDishes(
                "Removing lite map workspace",
                Workspace=groupingWorkspaceName,
            )

        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
