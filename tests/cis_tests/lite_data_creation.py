# Use this script to test LiteDataCreationAlgo.py
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService

#User input ###########################
runNumber = "47278"
#######################################

dataFactoryService = DataFactoryService()

runConfig = dataFactoryService.getRunConfig(runNumber)
ipts = runConfig.IPTS
run = RunConfig(runNumber=runNumber,IPTS=ipts)

LDCA = LiteDataCreationAlgo()
LDCA.initialize()
LDCA.setProperty("InputWorkspace", "47278_raw")
LDCA.setProperty("Run", run.json())
LDCA.setProperty("OutputWorkspace", "47278_lite")
LDCA.execute()