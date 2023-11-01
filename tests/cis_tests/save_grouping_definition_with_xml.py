# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import pathlib
import os.path

from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition as LoadingAlgo
from snapred.backend.recipe.algorithm.SaveGroupingDefinition import SaveGroupingDefinition as SavingAlgo
from snapred.meta.Config import Config, Resource

localDir = '~/tmp/'
PGDDir = '/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/'
groupingFileXML = PGDDir + "SNAPFocGroup_Column.xml"
groupingFileHDF = localDir + pathlib.Path(groupingFileXML).stem + ".hdf"

Config._config["cis_mode"] = False

# save the input grouping file in the calibration format
savingAlgo = SavingAlgo()
savingAlgo.initialize()
savingAlgo.setProperty("GroupingFilename", groupingFileXML)
savingAlgo.setProperty("OutputFilename", groupingFileHDF)
savingAlgo.setProperty("InstrumentName", "SNAP")
assert savingAlgo.execute()

loadingAlgo = LoadingAlgo()
loadingAlgo.initialize()
loadingAlgo.setProperty("GroupingFilename", groupingFileHDF)
loadingAlgo.setProperty("InstrumentName", "SNAP")
loadingAlgo.setProperty("OutputWorkspace", "gr_ws_from_HDF")
assert loadingAlgo.execute()

loadingAlgo2 = LoadingAlgo()
loadingAlgo2.initialize()
loadingAlgo2.setProperty("GroupingFilename", groupingFileXML)
loadingAlgo2.setProperty("InstrumentName", "SNAP")
loadingAlgo2.setProperty("OutputWorkspace", "gr_ws_from_XML")
assert loadingAlgo2.execute()

assert CompareWorkspaces(
    Workspace1 = "gr_ws_from_HDF",
    Workspace2 = "gr_ws_from_XML",
)