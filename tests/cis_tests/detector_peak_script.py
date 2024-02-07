## This script is for EWM 2128
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2128
# This is testing that:
#  1. the widths of peaks are treated larger on right side than left side
#  2. changing the peak tail coefficient property will change this width


from mantid.simpleapi import *
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
peakFractionalThreshold = 0.01
isLite = True

# SET TO TRUE TO STOP WASHING DISHES
Config._config['cis_mode'] = False
#######################################


### PREP INGREDIENTS
farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroup={"name":groupingScheme, "definition":""},
    cifPath=cifPath,
    peakIntensityThreshold=peakFractionalThreshold,
)
peakIngredients = SousChef().prepPeakIngredients(farmFresh)
instrumentState = peakIngredients.instrumentState
crystalInfo = peakIngredients.crystalInfo


### RUN ALGORITHM
detectorAlgo = DetectorPeakPredictor()
detectorAlgo.initialize()
detectorAlgo.setProperty("InstrumentState", instrumentState.json())
detectorAlgo.setProperty("CrystalInfo", crystalInfo.json())
detectorAlgo.setProperty("PeakIntensityFractionThreshold", peakFractionalThreshold)
detectorAlgo.execute()

peakList = json.loads(detectorAlgo.getProperty("DetectorPeaks").value)
print(peakList)

peakCenter = []
peakMin = []
peakMax = []
for group in peakList:
    peakCenter.append([peak['position']['value'] for peak in group['peaks']])
    peakMin.append([peak['position']['minimum'] for peak in group['peaks']])
    peakMax.append([peak['position']['maximum'] for peak in group['peaks']])
    
################################################################
# Plot the found peaks on focused data
################################################################

clerk = GroceryListItem.builder()
clerk.neutron(runNumber).useLiteMode(isLite).add()
clerk.grouping(runNumber, groupingScheme).useLiteMode(isLite).add()
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
    Params = (0.01),
)

#do the plotting
for i,group in enumerate(peakList):
    tableName = f'peakProperties{i+1}'
    fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
    ax.plot(mtd[groceries[0]], wkspIndex=i)# plot the initial guess with black line
    ax.vlines(peakCenter[i], ymin=1e6, ymax=1e8, color='red')
    ax.vlines(peakMin[i], ymin=1e6, ymax=1e8, color='orange')
    ax.vlines(peakMax[i], ymin=1e6, ymax=1e8, color='orange')
    ax.legend() # show the legend
    fig.show()

#########################################################
# TRY CHANGING PEAK TAIL MAX AND RE_RUN FOR COMPARISON
#########################################################
assert False
# using previously found ingredients, change the peakTailCoefficient within instrumentstate
instrumentState.peakTailCoefficient = 10

detectorAlgo = DetectorPeakPredictor()
detectorAlgo.initialize()
detectorAlgo.setProperty("InstrumentState", instrumentState.json())
detectorAlgo.setProperty("CrystalInfo", crystalInfoDict['crystalInfo'].json())
detectorAlgo.setProperty("PeakIntensityFractionThreshold", 0.01)
detectorAlgo.execute()

peakList = json.loads(detectorAlgo.getProperty("DetectorPeaks").value)
print(peakList)

peakCenter = []
peakMin = []
peakMax = []
for group in peakList:
    peakCenter.append([peak['position']['value'] for peak in group['peaks']])
    peakMin.append([peak['position']['minimum'] for peak in group['peaks']])
    peakMax.append([peak['position']['maximum'] for peak in group['peaks']])

#do the plotting
for i,group in enumerate(peakList):
    tableName = f'peakProperties{i+1}_after'
    fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
    ax.plot(mtd[groceries[0]], wkspIndex=i) # plot the initial guess with black line
    ax.vlines(peakCenter[i], ymin=1e6, ymax=1e8, color='red')
    ax.vlines(peakMin[i], ymin=1e6, ymax=1e8, color='orange')
    ax.vlines(peakMax[i], ymin=1e6, ymax=1e8, color='orange')
    ax.legend() # show the legend
    fig.show()
