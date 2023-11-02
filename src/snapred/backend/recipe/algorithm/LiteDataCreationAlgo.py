import os
import json
import numpy as np

from mantid.api import *
from mantid.api import AlgorithmFactory, PythonAlgorithm, mtd
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.dao.RunConfig import RunConfig
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
            LoadMonitors=True,
        )

        groupingWorkspaceName = f'{self.runNumber}_lite_grouping_ws'

        # create the lite grouping workspace using input as template
        self.mantidSnapper.CreateGroupingWorkspace(
            f"Creating {groupingWorkspaceName}...",
            InputWorkspace=inputWorkspaceName,
            GroupDetectorsBy='All',
            OutputWorkspace=groupingWorkspaceName,
        )

        self.mantidSnapper.executeQueue()
        groupingWorkspace = self.mantidSnapper.mtd[groupingWorkspaceName]

        # create mapping for grouping workspace
        nHst = groupingWorkspace.getNumberHistograms()
        for spec in range(nHst):
            groupingWorkspace.setY(spec,[self.superID(spec, 8, 8)])

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
        self.mantidSnapper.DeleteWorkspace(
            f"Deleting {self.runNumber}_raw_monitors...",
            Workspace=str(self.runNumber) + "_raw_monitors",
        )

        self.mantidSnapper.executeQueue()
        outputWorkspace = self.mantidSnapper.mtd[outputWorkspaceName]
        self.setProperty("OutputWorkspace", outputWorkspaceName)
    
    def superID(self, nativeID, xdim, ydim):
        
        # native number of pixels per panel
        Nx = int(Config["instrument.lite.resolution.Nx"])
        Ny = int(Config["instrument.lite.resolution.Ny"])
        NNat = Nx*Ny 
        
        # reduced ID beginning at zero in each panel
        firstPix = (nativeID // NNat)*NNat
        redID = nativeID % NNat 
        
        # native (reduced) coordinates on pixel face
        (i,j) = divmod(redID,Ny) 
        superi = divmod(i,xdim)[0]
        superj = divmod(j,ydim)[0]

        # some basics of the super panel
        # 32 running from 0 to 31   
        superNx = Nx/xdim 
        superNy = Ny/ydim
        superN = superNx*superNy

        superFirstPix = (firstPix/NNat)*superN
        
        super = superi*superNy+superj+superFirstPix
        
        return super

# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
