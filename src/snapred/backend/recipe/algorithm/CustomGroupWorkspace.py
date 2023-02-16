from mantid.kernel import *
from mantid.api import *
import time

name = "CustomGroupWorkspace"

class CustomGroupWorkspace(PythonAlgorithm):

    def PyInit(self):
        # declare properties
        self.declareProperty('FocusGroups', None)
        self.declareProperty('InstrumentName', "SNAP")
        pass

    def PyExec(self):
        focusGroups = self.getProperty("FocusGroups").value
        instrumentName = self.getProperty("InstrumentName").value

        # createGroupWorkspace = self.createChildAlgorithm("CreateGroupWorkspace")
        CreateGroupingWorkspace(InputWorkspace='TOF_rawVmB',GroupDetectorsBy='Column',OutputWorkspace='gpTemplate')

        for grpIndx,focusGroup in enumerate(focusGroups):
            CloneWorkspace(InputWorkspace='gpTemplate',
            OutputWorkspace=f'{instrumentName}{focusGroup.name}Gp')

            currentWorkspaceName = f'{instrumentName}{focusGroup.name}Gp'

            ws = mtd[currentWorkspaceName]
            nh = ws.getNumberHistograms()
            NSubGrp = len(focusGroup.definition)
            # print(f'creating grouping for {focusGroup} with {NSubGrp} subgroups')
            for pixel in range(nh):
                ws.setY(pixel,np.array([0.0])) #set to zero to ignore unless pixel is defined as part of group beklow.
                for subGrp in range(NSubGrp):
                    if pixel in focusGroup.definition[subGrp]:
                        ws.setY(pixel,np.array([subGrp+1]))

            gpString = gpString + ',' + currentWorkspaceName

            GroupWorkspaces(InputWorkspaces=gpString,
                OutputWorkspace='CommonRed'
                )

        print('State pixel groups initialised')
        DeleteWorkspace(Workspace='gpTemplate')

        # set params
        createGroupWorkspace.execute()

# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomGroupWorkspace)