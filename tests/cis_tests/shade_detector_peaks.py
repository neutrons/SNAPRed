"""
This script is simply used to prototype plot displays for the UI
"""

from mantid.simpleapi import *
from mantid.plots.datafunctions import get_spectrum
import matplotlib.pyplot as plt
import numpy as np
import json

from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

# for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

# for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef

snapredLogger._level = 20

#User inputs ###########################

runNumber = '58882'#58409'
cifPath = '/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif'
groupingScheme = "Column"
isLite = True
# SET TO TRUE TO STOP WASHING DISHES
Config._config['cis_mode'] = False
#######################################

### FETCH GROCERIES
clerk = GroceryListItem.builder()
clerk.neutron(runNumber).useLiteMode(isLite).add()
clerk.grouping(groupingScheme).useLiteMode(isLite).fromRun(runNumber).add()
groceries = GroceryService().fetchGroceryList(clerk.buildList())

ConvertUnits(
    InputWorkspace = groceries[0],
    OutputWorkspace = groceries[0],
    Target="dSpacing",
)
DiffractionFocussing(
    InputWorkspace = groceries[0],
    OutputWorkspace = groceries[0],
    GroupingWorkspace = groceries[1],
)
Rebin(
    InputWorkspace = groceries[0],
    OutputWorkspace = groceries[0],
    Params = (-1./73.),
)

### PREP INGREDIENTS
farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroup={"name":groupingScheme, "definition":""},
    cifPath=cifPath,
    peakIntensityThreshold=0.1,
    crystalDBounds={"minimum": 1.5, "maximum": 3},
)
peakList = SousChef().prepDetectorPeaks(farmFresh)

numSpec = len(peakList)
ncols = min(numSpec, 3)
nrows = int(np.ceil( (numSpec-ncols)/ncols )) + 1

figure = plt.figure(constrained_layout=True)
for i,group in enumerate(peakList):
    tableName = f'peakProperties{i+1}'
    ax = figure.add_subplot(nrows, ncols, i + 1, projection="mantid")
    ax.plot(mtd[groceries[0]], wkspIndex=i, normalize_by_bin_width=True)
    ax.axvline(x=farmFresh.crystalDBounds.minimum, color="red")
    ax.axvline(x=farmFresh.crystalDBounds.maximum, color="red")
    x, y, _, _ = get_spectrum(mtd[groceries[0]], i, normalize_by_bin_width=True)
    for peak in group.peaks:
        ax.fill_between(x, y, where=[(peak.minimum < xx and xx < peak.maximum) for xx in x], color="blue", alpha=0.5)
    ax.legend() # show the legend
figure.show()
