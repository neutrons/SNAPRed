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

        # clone the workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            InputWorkspace=inputWorkspace,
            OutputWorkspace=outputWorkspaceName,
        )
        self.mantidSnapper.executeQueue()
        outputWorkspace = self.mantidSnapper.mtd[outputWorkspaceName]

        # IEventWorkspace ID is equivalent to spec ID
        numSpec = outputWorkspace.getNumberHistograms()

        # array to hold spectrum data
        spectrumData = []

        # iterate over IEventWorkspace IDs
        for spec in range(numSpec):
            # get event list per workspace
            spectrum = outputWorkspace.getSpectrum(workspaceIndex=spec)
            spectrumData.append(spectrum)

        # allocate array of event IDs
        for spectrum in spectrumData:
            eventIDs = spectrum.getDetectorIDs()
            for event in eventIDs:
                superID = self.getSuperID(nativeID=event, xdim=superPixel[0], ydim=superPixel[1])

                # clear detector IDs and then add the new modified super ID
                spectrum.clearDetectorIDs()
                spectrum.addDetectorID(superID)

        self.mantidSnapper.LoadInstrument(
            "Loading instrument...",
            Workspace=outputWorkspace,
            Filename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
            RewriteSpectraMap=False,
        )
        # self.mantidSnapper.DeleteWorkspace("Cleaning up input workspace...", Workspace=ws)
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
        superIDs = superi * superNy + superj + superFirstPix

        return superIDs


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
