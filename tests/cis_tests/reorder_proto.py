# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
from mantid.plots.datafunctions import get_spectrum

# Use this script to test Diffraction Calibration
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import json
from typing import List


## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef

## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

# for running through service layer
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest
from snapred.backend.dao.request.SimpleDiffCalRequest import SimpleDiffCalRequest
from snapred.backend.dao.request.FocusSpectraRequest import FocusSpectraRequest
from snapred.backend.dao.request.FitMultiplePeaksRequest import FitMultiplePeaksRequest

from snapred.meta.Config import Config

#User input ###########################
runNumber = "46680"
groupingScheme = "Bank (Lite)"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/diamond.cif"
calibrantSamplePath = "Diamond_001.json"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1
isLite = True
Config._Config["cis_mode.enabled"] = False
#######################################

focusGroup = {"name": groupingScheme, "definition": ""}

### PREP INGREDIENTS ################
farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroups=[focusGroup],
    cifPath=cifPath,
    calibrantSamplePath=calibrantSamplePath,
    peakIntensityThreshold=peakThreshold,
    convergenceThreshold=offsetConvergenceLimit,
    maxOffset=100.0,
)
ingredients = SousChef().prepDiffractionCalibrationIngredients(farmFresh)

### FETCH GROCERIES ##################

clerk = GroceryListItem.builder()
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
groceries = GroceryService().fetchGroceryDict(
    clerk.buildDict(),
    outputWorkspace="_out_",
    diagnosticWorkspace="_diag",
    maskWorkspace="_mask_",
    calibrationTable="_DIFC_",    
)


### CALL CALIBRATION SERVICE
diffcalRequest = DiffractionCalibrationRequest(
    runNumber = runNumber,
    calibrantSamplePath = calibrantSamplePath,
    useLiteMode = isLite,
    focusGroup = focusGroup,
    convergenceThreshold = offsetConvergenceLimit,
    removeBackground=False,
)

calibrationService = CalibrationService()

##################################################
# 1. run pixel calibration
# 2. focus data
# 3. fit peaks
# 4. display
##################################################

payload = SimpleDiffCalRequest(
    ingredients=ingredients,
    groceries=groceries,
)
response = calibrationService.pixelCalibration(payload)

payload = FocusSpectraRequest(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroup=focusGroup,
    inputWorkspace=groceries["inputWorkspace"],
    groupingWorkspace=groceries["groupingWorkspace"],
)
response = calibrationService.focusSpectra(payload)

focusedWorkspace = response[0]

payload = FitMultiplePeaksRequest(
    inputWorkspace=focusedWorkspace,
    outputWorkspaceGroup="diag_",
    detectorPeaks=ingredients.groupedPeakLists,
)
response = calibrationService.fitPeaks(payload)

def optimizeRowsAndCols(numGraphs):
    # Get best size for layout
    sqrtSize = int(numGraphs**0.5)
    if sqrtSize == numGraphs**0.5:
        rowSize = sqrtSize
        colSize = sqrtSize
    elif numGraphs <= ((sqrtSize + 1) * sqrtSize):
        rowSize = sqrtSize
        colSize = sqrtSize + 1
    else:
        rowSize = sqrtSize + 1
        colSize = sqrtSize + 1
    return rowSize, colSize
    

allPeaks = ingredients.groupedPeakLists
numGraphs = len(peaks)
nrows, ncols = optimizeRowsAndCols(numGraphs)

goodPeaksCount = [0] * numGraphs
badPeaks = [[]] * numGraphs

fitted_peaks = mtd["diag_"].getItem(3)
param_table = mtd["diag_"].getItem(2).toDict()
index = param_table["wsindex"]
allChisq = param_table["chi2"]
maxChiSq = farmFresh.maxChiSq

figure = plt.figure(constrained_layout=True)
for wkspIndex in range(numGraphs):
    peaks = allPeaks[wkspIndex].peaks
    # collect the fit chi-sq parameters for this spectrum, and the fits
    chisq = [x2 for i, x2 in zip(index, allChisq) if i == wkspIndex]
    goodPeaksCount[wkspIndex] = len([peak for chi2, peak in zip(chisq, peaks) if chi2 < maxChiSq])
    badPeaks[wkspIndex] = [peak for chi2, peak in zip(chisq, peaks) if chi2 >= maxChiSq]
    # prepare the plot area
    ax = figure.add_subplot(nrows, ncols, wkspIndex + 1, projection="mantid")
    ax.tick_params(direction="in")
    ax.set_title(f"Group ID: {wkspIndex + 1}")
    # plot the data and fitted
    ax.plot(mtd[focusedWorkspace], wkspIndex=wkspIndex, label="data", normalize_by_bin_width=True)
    ax.plot(fitted_peaks, wkspIndex=wkspIndex, label="fit", color="black", normalize_by_bin_width=True)
    ax.legend(loc=1)
    # fill in the discovered peaks for easier viewing
    x, y, _, _ = get_spectrum(mtd[focusedWorkspace], wkspIndex, normalize_by_bin_width=True)
    # for each detected peak in this group, shade in the peak region
    for chi2, peak in zip(chisq, peaks):
        # areas inside peak bounds (to be shaded)
        under_peaks = [(peak.minimum < xx and xx < peak.maximum) for xx in x]
        # the color: blue = GOOD, red = BAD
        color = "blue" if chi2 < float(maxChiSq) else "red"
        alpha = 0.3 if chi2 < float(maxChiSq) else 0.8
        # now shade
        ax.fill_between(x, y, where=under_peaks, color=color, alpha=alpha)
    # plot the min and max value for peaks
    ax.axvline(x=max(min(x), farmFresh.crystalDBounds.minimum), label="xtal $d_{min}$", color="red")
    ax.axvline(x=min(max(x), farmFresh.crystalDBounds.maximum), label="xtal $d_{max}$", color="red")
# resize window and redraw
# self.setMinimumHeight(
#     self.initialLayoutHeight + int((self.figure.get_size_inches()[1] + self.FIGURE_MARGIN) * self.figure.dpi)
# )
plt.show()
