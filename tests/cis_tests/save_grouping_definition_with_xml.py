"""
This script tests the algorithms to Save/Load a grouping definition
"""

# the algorithms to be tests
import snapred.backend.recipe.algorithm

# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import LoadEmptyInstrument, LoadGroupingDefinition, mtd, SaveGroupingDefinition
from mantid.testing import assert_almost_equal as assert_wksp_almost_equal
import matplotlib.pyplot as plt
import numpy as np
import pathlib
import os.path
import time

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

Config._config["cis_mode.enabled"] = True
Config._config["cis_mode.preserveDiagnosticWorkspaces"] = True

grwsXML = "gr_ws_from_XML"
grwsHDF = "gr_ws_from_HDF"

loads = {}
saves = {}

###### TESTS OF LOAD #######################################

# load the data directly from the XML with instrument name
start = time.time()
assert LoadGroupingDefinition(
    GroupingFilename = groupingFileXML,
    InstrumentName = instrumentName,
    OutputWorkspace = grwsXML,
)

end = time.time()
loads["XML and NAME"] = (end - start)

# load the data directly from the XML with instrument file
reload = "from xml and file"

start = time.time()
assert LoadGroupingDefinition(
    GroupingFilename = groupingFileXML,
    InstrumentFilename = instrumentFile,
    OutputWorkspace = reload,
)
end = time.time()
loads["XML and FILE"] = (end - start)

assert_wksp_almost_equal(
    Workspace1 = grwsXML,
    Workspace2 = reload,
)

# load the data directly from the XML with instrument donor
reload = "from xml and donor"

start = time.time()
assert LoadGroupingDefinition(
    GroupingFilename = groupingFileXML,
    InstrumentDonor = instrumentDonor,
    OutputWorkspace = reload,
)
end = time.time()
loads["XML and DONOR"] = (end - start)

assert_wksp_almost_equal(
    Workspace1 = grwsXML,
    Workspace2 = reload,
)

###### TESTS OF SAVE #######################################

###### save/load with XML file and instrument name
start = time.time()
SaveGroupingDefinition(
    GroupingFilename = groupingFileXML,
    OutputFilename = groupingFileHDF,
    InstrumentName = instrumentName,
)
end = time.time()
saves["XML and NAME"] = (end - start)

start = time.time()
LoadGroupingDefinition(
    GroupingFilename = groupingFileHDF,
    InstrumentName = instrumentName,
    OutputWorkspace = grwsHDF,
)
end = time.time()
loads["HDF and NAME"] = (end - start)

assert_wksp_almost_equal(
    Workspace1 = grwsHDF,
    Workspace2 = grwsXML,
)

###### save/load with XML file and instrument file
start = time.time()
SaveGroupingDefinition(
    GroupingFilename = groupingFileXML,
    OutputFilename = groupingFileHDF,
    InstrumentFilename = instrumentFile,  
)
end = time.time()
saves["XML and FILE"] = (end - start)

start = time.time()
LoadGroupingDefinition(
    GroupingFilename = groupingFileHDF,
    InstrumentFilename = instrumentFile,
    OutputWorkspace = grwsHDF,
)
end = time.time()
loads["HDF and FILE"] = (end - start)

assert_wksp_almost_equal(
    Workspace1 = grwsXML,
    Workspace2 = grwsHDF,
)

###### save/load with XML file and instrument donor
result = "xml_and_instrument_donor"
start = time.time()
SaveGroupingDefinition(
    GroupingFilename = groupingFileXML,
    OutputFilename = groupingFileHDF,
    InstrumentDonor = instrumentDonor,
)
end = time.time()
saves["XML and DONOR"] = (end - start)

start = time.time()
LoadGroupingDefinition(
    GroupingFilename = groupingFileHDF,
    InstrumentDonor = instrumentDonor,
    OutputWorkspace = grwsHDF,
)
end = time.time()
loads["HDF and DONOR"] = (end - start)

assert_wksp_almost_equal(
    Workspace1 = grwsXML,
    Workspace2 = grwsHDF,
)

###### save with HDF file and instrument name
groupingFileHDF2 = localDir + pathlib.Path(groupingFileXML).stem + "2.hdf"
start = time.time()
SaveGroupingDefinition(
    GroupingFilename = groupingFileHDF,
    OutputFilename = groupingFileHDF2,
    InstrumentName = instrumentName,
)
end = time.time()
saves["HDF and NAME"] = (end - start)

###### save with HDF file and instrument file
start = time.time()
SaveGroupingDefinition(
    GroupingFilename = groupingFileHDF,
    OutputFilename = groupingFileHDF2,
    InstrumentFilename = instrumentFile,
)
end = time.time()
saves["HDF and FILE"] = (end - start)

###### save with HDF file and instrument donor
start = time.time()
SaveGroupingDefinition(
    GroupingFilename = groupingFileHDF,
    OutputFilename = groupingFileHDF2,
    InstrumentDonor = instrumentDonor,
)
end = time.time()
saves["HDF and DONOR"] = (end - start)

##### PRINT TIME RESULTS
for x,y in loads.items():
    print(f"TIME FOR LOAD ALGO {x}: {y}")
    
for x,y in saves.items():
    print(f"TIME FOR SAVE ALGO {x}: {y}")
