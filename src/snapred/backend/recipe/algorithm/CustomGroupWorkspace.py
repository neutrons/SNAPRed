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


    # TODO: This was largely copied from Malcolm's prototype and is due for a refactor
    def PyExec(self):
        stateConfig = StateConfig(**json.loads(self.getProperty("StateConfig").value))
        focusGroups = stateConfig.focusGroups
        instrumentName = self.getProperty("InstrumentName").value
        calibrantWorkspace = self.getProperty("CalibrantWorkspace").value
        outputWorkspace = self.getProperty("OutputWorkspace").value

        CreateWorkspace(OutputWorkspace='idf',DataX=1,DataY=1)
        LoadInstrument(Workspace='idf',Filename='/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml', MonitorList='-2--1', RewriteSpectraMap=False)




        for grpIndx,focusGroup in enumerate(focusGroups):
            LoadDetectorsGroupingFile(InputFile=focusGroup.definition, InputWorkspace='idf', OutputWorkspace=focusGroup.name)

        GroupWorkspaces(InputWorkspaces=[focusGroup.name for focusGroup in focusGroups],
                OutputWorkspace=outputWorkspace
                )
        

# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomGroupWorkspace)