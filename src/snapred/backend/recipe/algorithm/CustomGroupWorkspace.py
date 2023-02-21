from snapred.backend.dao.StateConfig import StateConfig

from mantid.kernel import *
from mantid.api import *
from mantid.simpleapi import *

import time
import json

import numpy as np

name = "CustomGroupWorkspace"

class CustomGroupWorkspace(PythonAlgorithm):

    def PyInit(self):
        # declare properties
        self.declareProperty('StateConfig', '')
        self.declareProperty('CalibrantWorkspace','TOF_rawVmB')
        self.declareProperty('InstrumentName', "SNAP")
        self.declareProperty('OutputWorkspace', "CommonRed")
        pass

    def PyExec(self):
        stateConfig = StateConfig(**json.loads(self.getProperty("StateConfig").value))
        focusGroups = stateConfig.focusGroups
        instrumentName = self.getProperty("InstrumentName").value
        calibrantWorkspace = self.getProperty("CalibrantWorkspace").value
        gpString = '' # Expects a string
        outputWorkspace = self.getProperty("OutputWorkspace").value

        # createGroupWorkspace = self.createChildAlgorithm("CreateGroupWorkspace")
        CreateGroupingWorkspace(InputWorkspace=calibrantWorkspace,GroupDetectorsBy='Column',OutputWorkspace='gpTemplate')

        for grpIndx,focusGroup in enumerate(focusGroups):
            CloneWorkspace(InputWorkspace='gpTemplate',
            OutputWorkspace=f'{instrumentName}{focusGroup.name}Gp')

            currentWorkspaceName = f'{instrumentName}{focusGroup.name}Gp'

            ws = mtd[currentWorkspaceName]
            nh = ws.getNumberHistograms()
            NSubGrp = len(focusGroup.definition if focusGroup.definition != None else [])
            # print(f'creating grouping for {focusGroup} with {NSubGrp} subgroups')
            for pixel in range(nh):
                ws.setY(pixel,np.array([0.0])) #set to zero to ignore unless pixel is defined as part of group beklow.
                for subGrp in range(NSubGrp):
                    if pixel in focusGroup.definition[subGrp]:
                        ws.setY(pixel,np.array([subGrp+1]))

            gpString = gpString + ',' + currentWorkspaceName

        GroupWorkspaces(InputWorkspaces=gpString,
                OutputWorkspace=outputWorkspace
                )

        print('State pixel groups initialised')
        DeleteWorkspace(Workspace='gpTemplate')

# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomGroupWorkspace)