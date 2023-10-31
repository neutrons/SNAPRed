# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import pathlib
import os.path

from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition as LoadingAlgo
from snapred.backend.recipe.algorithm.SaveGroupingDefinition import SaveGroupingDefinition as SavingAlgo
from snapred.meta.Config import Config, Resource

localDir = '/SNS/users/8l2/tmp/'
PGDDir = '/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/'
groupingFile = PGDDir + "SNAPFocGroup_Column.xml"

Config._config["cis_mode"] = True # <<-- change to True and rerun

output_file_name = pathlib.Path(groupingFile).stem + ".hdf"
outputFilePath = os.path.join(localDir, output_file_name)
# save the input grouping file in the calibration format
savingAlgo = SavingAlgo()
savingAlgo.initialize()
savingAlgo.setProperty("GroupingFilename", groupingFile)
savingAlgo.setProperty("OutputFilename", outputFilePath)
savingAlgo.setProperty("InstrumentName", "SNAP")
assert savingAlgo.execute()