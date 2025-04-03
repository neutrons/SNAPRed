# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

# a class for analysing bin edges in mantid workspaces

# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

class xarray:

    def __init__(self,wsName,specIndex):

        self.wsName = wsName
        self.specIndex = int(specIndex)


        ws = mtd[wsName]
        x = ws.readX(self.specIndex)

        self.nEdges = len(x)

        self.x = x
        self.min = min(x)
        self.max = max(x)

        self.binWidths = x[1:]-x[0:-1]
        #check that x-array is monotonically increasing
        if np.all(self.binWidths > 0):
            print("x-array is monotonically increasing")
            self.monotonic = True
        else:
            print("x-array is NOT monotonically increasing")
            self.monotonic = False

        nEdges = len(x)
        if self.binWidths[0] == self.binWidths[-2]:
            self.binType = "lin"
            self.delta = x[1]-x[0]
        else:
            self.binType = "log"
            self.delta= (x[1]/x[0]-1)

    def binInfo(self):

        print(f"I think this workspace is {self.binType} binned with Delta = {self.delta:.12f}")
        print(f"It has {self.nEdges} bin edges, with min: {self.min} and max: {self.max}")
        print(f"It's largest bin is {max(self.binWidths):.5f}")
        

    def makeBinWS(self):
        
        indices = np.arange(len(self.binWidths))
        CreateWorkspace(DataX=indices,
                        DataY=self.binWidths,
                        OutputWorkspace=f"{self.wsName}_bins")
        
        print(f"bin sizes have been saved to workspace: {self.wsName}_bins")
        
normX = xarray("tof_column_s+f-vanadium_059039", 5)
redX = xarray("reduced_dsp_column_059039_2025-04-03T105140", 5)

normX.binInfo()
redX.binInfo()

normX.makeBinWS()
redX.makeBinWS()