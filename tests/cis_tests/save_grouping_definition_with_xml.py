"""
This script tests the algorithms to Save/Load a grouping definition
"""


# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import pathlib
import os.path
import time

from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition as LoadingAlgo
from snapred.backend.recipe.algorithm.SaveGroupingDefinition import SaveGroupingDefinition as SavingAlgo
from snapred.meta.Config import Config, Resource

from snapred.backend.log.logger import snapredLogger
snapredLogger._level = 40

localDir = os.path.expanduser('~/tmp/')
PGDDir = '/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/'
groupingFileXML = PGDDir + "SNAPFocGroup_Column.xml"
groupingFileHDF = localDir + pathlib.Path(groupingFileXML).stem + ".hdf"

instrumentName = "SNAP"
instrumentFile = "/SNS/SNAP/shared/Malcolm/dataFiles/SNAP_Definition.xml"
instrumentDonor = "instrument_donor"
LoadEmptyInstrument(
    OutputWorkspace = instrumentDonor,
    Filename = instrumentFile,
)
assert mtd.doesExist(instrumentDonor)

Config._config["cis_mode"] = True

grwsXML = "gr_ws_from_XML"
grwsHDF = "gr_ws_from_HDF"

loads = {}
saves = {}

###### TESTS OF LOAD #######################################

# load the data directly from the XML with instrument name
loadingAlgo = LoadingAlgo()
loadingAlgo.initialize()
loadingAlgo.setProperty("GroupingFilename", groupingFileXML)
loadingAlgo.setProperty("InstrumentName", instrumentName)
loadingAlgo.setProperty("OutputWorkspace", grwsXML)
start = time.time()
assert loadingAlgo.execute()
end = time.time()
loads["XML and NAME"] = (end - start)

# load the data directly from the XML with instrument file
reload = "from xml and file"
loadingAlgo = LoadingAlgo()
loadingAlgo.initialize()
loadingAlgo.setProperty("GroupingFilename", groupingFileXML)
loadingAlgo.setProperty("InstrumentFilename", instrumentFile)
loadingAlgo.setProperty("OutputWorkspace", reload)
start = time.time()
assert loadingAlgo.execute()
end = time.time()
loads["XML and FILE"] = (end - start)

assert CompareWorkspaces(
    Workspace1 = grwsXML,
    Workspace2 = reload,
)

# load the data directly from the XML with instrument donor
reload = "from xml and donor"
loadingAlgo = LoadingAlgo()
loadingAlgo.initialize()
loadingAlgo.setProperty("GroupingFilename", groupingFileXML)
loadingAlgo.setProperty("InstrumentDonor", instrumentDonor)
loadingAlgo.setProperty("OutputWorkspace", reload)
start = time.time()
assert loadingAlgo.execute()
end = time.time()
loads["XML and DONOR"] = (end - start)

assert CompareWorkspaces(
    Workspace1 = grwsXML,
    Workspace2 = reload,
)

###### TESTS OF SAVE #######################################

###### save/load with XML file and instrument name
savingAlgo = SavingAlgo()
savingAlgo.initialize()
savingAlgo.setProperty("GroupingFilename", groupingFileXML)
savingAlgo.setProperty("OutputFilename", groupingFileHDF)
savingAlgo.setProperty("InstrumentName", instrumentName)
start = time.time()
assert savingAlgo.execute()
end = time.time()
saves["XML and NAME"] = (end - start)

loadingAlgo = LoadingAlgo()
loadingAlgo.initialize()
loadingAlgo.setProperty("GroupingFilename", groupingFileHDF)
loadingAlgo.setProperty("InstrumentName", instrumentName)
loadingAlgo.setProperty("OutputWorkspace", grwsHDF)
start = time.time()
assert loadingAlgo.execute()
end = time.time()
loads["HDF and NAME"] = (end - start)

assert CompareWorkspaces(
    Workspace1 = grwsHDF,
    Workspace2 = grwsXML,
)

###### save/load with XML file and instrument file
savingAlgo = SavingAlgo()
savingAlgo.initialize()
savingAlgo.setProperty("GroupingFilename", groupingFileXML)
savingAlgo.setProperty("OutputFilename", groupingFileHDF)
savingAlgo.setProperty("InstrumentFilename", instrumentFile)
start = time.time()
assert savingAlgo.execute()
end = time.time()
saves["XML and FILE"] = (end - start)

loadingAlgo = LoadingAlgo()
loadingAlgo.initialize()
loadingAlgo.setProperty("GroupingFilename", groupingFileHDF)
loadingAlgo.setProperty("InstrumentFilename", instrumentFile)
loadingAlgo.setProperty("OutputWorkspace", grwsHDF)
start = time.time()
assert loadingAlgo.execute()
end = time.time()
loads["HDF and FILE"] = (end - start)

assert CompareWorkspaces(
    Workspace1 = grwsXML,
    Workspace2 = grwsHDF,
)

###### save/load with XML file and instrument donor
result = "xml_and_instrument_donor"
savingAlgo = SavingAlgo()
savingAlgo.initialize()
savingAlgo.setProperty("GroupingFilename", groupingFileXML)
savingAlgo.setProperty("OutputFilename", groupingFileHDF)
savingAlgo.setProperty("InstrumentDonor", instrumentDonor)
start = time.time()
assert savingAlgo.execute()
end = time.time()
saves["XML and DONOR"] = (end - start)

start = time.time()
loadingAlgo = LoadingAlgo()
loadingAlgo.initialize()
loadingAlgo.setProperty("GroupingFilename", groupingFileHDF)
loadingAlgo.setProperty("InstrumentDonor", instrumentDonor)
loadingAlgo.setProperty("OutputWorkspace", grwsHDF)
start = time.time()
assert loadingAlgo.execute()
end = time.time()
loads["HDF and DONOR"] = (end - start)

assert CompareWorkspaces(
    Workspace1 = grwsXML,
    Workspace2 = grwsHDF,
)

###### save with HDF file and instrument name
groupingFileHDF2 = localDir + pathlib.Path(groupingFileXML).stem + "2.hdf"
savingAlgo = SavingAlgo()
savingAlgo.initialize()
savingAlgo.setProperty("GroupingFilename", groupingFileHDF)
savingAlgo.setProperty("OutputFilename", groupingFileHDF2)
savingAlgo.setProperty("InstrumentName", instrumentName)
start = time.time()
assert savingAlgo.execute()
end = time.time()
saves["HDF and NAME"] = (end - start)
print(groupingFileHDF)
print(savingAlgo.supported_calib_file_extensions)

###### save with HDF file and instrument file
savingAlgo = SavingAlgo()
savingAlgo.initialize()
savingAlgo.setProperty("GroupingFilename", groupingFileHDF)
savingAlgo.setProperty("OutputFilename", groupingFileHDF2)
savingAlgo.setProperty("InstrumentFilename", instrumentFile)
start = time.time()
assert savingAlgo.execute()
end = time.time()
saves["HDF and FILE"] = (end - start)

###### save with HDF file and instrument donor
savingAlgo = SavingAlgo()
savingAlgo.initialize()
savingAlgo.setProperty("GroupingFilename", groupingFileHDF)
savingAlgo.setProperty("OutputFilename", groupingFileHDF2)
savingAlgo.setProperty("InstrumentDonor", instrumentDonor)
start = time.time()
assert savingAlgo.execute()
end = time.time()
saves["HDF and DONOR"] = (end - start)



##### PRINT TIME RESULTS
for x,y in loads.items():
    print(f"TIME FOR LOAD ALGO {x}: {y}")
    
for x,y in saves.items():
    print(f"TIME FOR SAVE ALGO {x}: {y}")
