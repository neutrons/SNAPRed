import json

from mantid.api import *
from mantid.api import AlgorithmFactory, PythonAlgorithm, mtd
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "LiteDataCreationAlgo"


class LiteDataCreationAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        self.log().notice("Lite Data Creation START!")
        # define super pixel dimensions
        superPixel = [8, 8]

        # get output workspace name
        outputWorkspaceName = self.getProperty("OutputWorkspace").value

        # load input workspace
        inputWorkspace = self.getProperty("InputWorkspace").value
        ws = self.mantidSnapper.mtd[inputWorkspace]

        # clone the workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            InputWorkspace=inputWorkspace,
            OutputWorkspace=outputWorkspaceName,
        )
        self.mantidSnapper.executeQueue()
        outputWorkspace = self.mantidSnapper.mtd(outputWorkspaceName)

        # IEventWorkspace ID is equivalent to spec ID
        numSpec = outputWorkspace.getNumberHistograms()

        # access the instrument definition
        outputWorkspace.getInstrument()

        # iterate over IEventWorkspace IDs
        for spec in range(numSpec):
            # wsID = outputWorkspace.getSpectrum(spec)

            # get event list per workspace
            eventList = outputWorkspace.getEventList(workspace_index=spec)

            # allocate array of event IDs
            for event in eventList:
                eventID = eventList.getDetectorIDs()
                superID = self.getSuperID(nativeID=eventID, xdim=superPixel[0], ydim=superPixel[0])

                # clear detector IDs and then add the new modified super ID
                event.clearDetectorIDs()
                event.addDetectorID(superID)

        self.mantidSnapper.DeleteWorkspace("Cleaning up input workspace...", Workspace=ws)
        self.mantidSnapper.executeQueue()

        self.setProperty("OutputWorkspace", outputWorkspaceName)
        return outputWorkspaceName

    def getSuperID(self, nativeID, xdim, ydim):
        # Constants
        Nx, Ny = 256, 256  # Native number of horizontal and vertical pixels
        NNat = Nx * Ny  # Native number of pixels per panel

        # Calculate super panel basics
        superNx = Nx // xdim
        superNy = Ny // ydim
        superN = superNx * superNy

        # Calculate reduced ID and native (reduced) coordinates on pixel face
        firstPix = nativeID // NNat * NNat
        redID = nativeID % NNat
        i, j = divmod(redID, Ny)

        # Calculate super pixel IDs
        superi = i // xdim
        superj = j // ydim
        superFirstPix = firstPix // NNat * superN
        superIDs = (superi * superNy + superj + superFirstPix).astype(int)

        return superIDs


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
