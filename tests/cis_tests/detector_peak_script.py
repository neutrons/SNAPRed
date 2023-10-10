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
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.log.logger import snapredLogger
from snapred.meta.redantic import list_to_raw_pretty

snapredLogger._level = 20

#User inputs ###########################

runNumber = '58882'#58409'
cifPath = '/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif'
groupingFile = '/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml'

#######################################

dataFactoryService=DataFactoryService()
calibrationService = CalibrationService()

pixelGroupingParameters = calibrationService.retrievePixelGroupingParams(runNumber)

calibration = dataFactoryService.getCalibrationState(runNumber)
# focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
instrumentState = calibration.instrumentState
crystalInfoDict = CrystallographicInfoService().ingest(cifPath)
instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParameters[0]

detectorAlgo = DetectorPeakPredictor()
detectorAlgo.initialize()
detectorAlgo.setProperty("InstrumentState", instrumentState.json())
detectorAlgo.setProperty("CrystalInfo", crystalInfoDict['crystalInfo'].json())
detectorAlgo.setProperty("PeakIntensityFractionThreshold", 0.01)
detectorAlgo.execute()

peakList = json.loads(detectorAlgo.getProperty("DetectorPeaks").value)
print(peakList)

for i,group in enumerate(peakList):
    tableName = f'peakProperties{i+1}'
    CreateEmptyTableWorkspace(OutputWorkspace=tableName)
    tableWS = mtd[tableName]
    tableWS.addColumn(type='int', name='peak number')
    tableWS.addColumn(type='float', name='value')
    tableWS.addColumn(type='float', name='min')
    tableWS.addColumn(type='float', name='max')
    for j,peak in enumerate(group['peaks']):
        tableWS.addRow({
            'peak number': j,
            'value': peak['position']['value'],
            'min': peak['position']['minimum'],
            'max': peak['position']['maximum'],
        })


################################################################
# Pause here to run diffraction calibration on input workspace
################################################################


#do the plotting
tableName = 'peakProperties1'
fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
ax.plot(mtd['_DSP_58882_raw'], wkspIndex=0)# plot the initial guess with black line
ax.vlines(mtd[tableName].column(1), ymin=1e6, ymax=1e8, color='red')
ax.vlines(mtd[tableName].column(2), ymin=1e6, ymax=1e8, color='orange')
ax.vlines(mtd[tableName].column(3), ymin=1e6, ymax=1e8, color='orange')
ax.legend() # show the legend
fig.show()


#########################################################
# TRY CHANGING PEAK TAIL MAX AND RE_RUN FOR COMPARISON
#########################################################

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

for i,group in enumerate(peakList):
    tableName = f'peakProperties{i+1}_after'
    CreateEmptyTableWorkspace(OutputWorkspace=tableName)
    tableWS = mtd[tableName]
    tableWS.addColumn(type='int', name='peak number')
    tableWS.addColumn(type='float', name='value')
    tableWS.addColumn(type='float', name='min')
    tableWS.addColumn(type='float', name='max')
    for j,peak in enumerate(group['peaks']):
        tableWS.addRow({
            'peak number': j,
            'value': peak['position']['value'],
            'min': peak['position']['minimum'],
            'max': peak['position']['maximum'],
        })

#do the plotting
tableName = 'peakProperties1_after'
fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
ax.plot(mtd['_DSP_58882_raw'], wkspIndex=0)# plot the initial guess with black line
ax.vlines(mtd[tableName].column(1), ymin=1e6, ymax=1e8, color='red')
ax.vlines(mtd[tableName].column(2), ymin=1e6, ymax=1e8, color='orange')
ax.vlines(mtd[tableName].column(3), ymin=1e6, ymax=1e8, color='orange')
ax.legend() # show the legend
fig.show()
