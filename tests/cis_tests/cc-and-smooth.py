# import mantid algorithms, numpy and matplotlib
import snapred.backend.recipe.algorithm
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import json
import time
## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef

## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

from snapred.meta.Config import Config
from snapred.meta.pointer import create_pointer, access_pointer

#User input ###########################
runNumber = "58882"
groupingScheme = "Column"
calibrantSamplePath = "Silicon_NIST_640D_001.json"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1
isLite = True
Config._config["cis_mode.enabled"] = True
#######################################

### PREP INGREDIENTS ################
farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroups=[{"name": groupingScheme, "definition": ""}],
    # cifPath=cifPath,
    calibrantSamplePath=calibrantSamplePath,
    peakIntensityThreshold=peakThreshold,
    convergenceThreshold=offsetConvergenceLimit,
    maxOffset=100.0,
)
pixelGroup = SousChef().prepPixelGroup(farmFresh)
detectorPeaks = SousChef().prepDetectorPeaks(farmFresh)

total = "total"
background = "background"
peaks = "peaks"
ref = "blanked"

### FETCH GROCERIES ##################

clerk = GroceryListItem.builder()
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
groceries = GroceryService().fetchGroceryDict(clerk.buildDict())

## UNBAG GROCERIES
inputWorkspace = groceries["inputWorkspace"]
focusWorkspace = groceries["groupingWorkspace"]
Rebin(
    InputWorkspace=inputWorkspace,
    OutputWorkspace=inputWorkspace,
    Params=pixelGroup.timeOfFlight.params,
)

## CHOP INGREDIENTS
groupIDs = []
for peakList in detectorPeaks:
    groupIDs.append(peakList.groupID)
groupDetectorIDs = access_pointer(GroupedDetectorIDs(focusWorkspace))

def getRefID(detectorIDs):
    return sorted(detectorIDs)[int(np.round((len(detectorIDs)-1) / 2.0))]
    
def performCrossCorrelation(inws):
    inws_tof_update = f"{inws}_tof_temp"
    inws_dsp_final = f"{inws}_dsp_after"
    inws_dsp = f"{inws}_dsp_tmp"
    outws = f"{inws}_cc"
    difcTable = f"{inws}_difc"
    CloneWorkspace(
        InputWorkspace=inws,
        OutputWorkspace=inws_tof_update,
    )
    CalculateDiffCalTable(
        InputWorkspace=inws_tof_update,
        CalibrationTable=difcTable,
    )
    medianOffsets = [100]
    while medianOffsets[-1] > 0.5:
        ConvertUnits(
            InputWorkspace=inws_tof_update,
            OutputWorkspace=inws_dsp,
            Target="dSpacing",
        )
        for i, groupID in enumerate(groupIDs):
            workspaceIndices = list(groupDetectorIDs[groupID])
            refID = getRefID(workspaceIndices)
            wstemp = f"{outws}_{i}"
            CrossCorrelate(
                InputWorkspace=inws_dsp,
                OutputWorkspace=wstemp,
                ReferenceSpectra=refID,
                WorkspaceIndexList=workspaceIndices,
                XMin = 0.4,
                XMax = 4.0,
                MaxDSpaceShift=0.1,
            )
            if i==0:
                CloneWorkspace(
                    InputWorkspace=wstemp,
                    OutputWorkspace=outws,
                )
                DeleteWorkspace(wstemp)
            else:
                ConjoinWorkspaces(
                    InputWorkspace1=outws,
                    InputWorkspace2=wstemp,
                )
        GetDetectorOffsets(
            InputWorkspace=outws,
            OutputWorkspace=f"{inws}_offset",
            MaskWorkspace=f"{inws}_mask",
            OffsetMode="Signed",
            Xmin=-50,
            Xmax=50,
            MaxOffset=10,
        )
        ConvertDiffCal(
            PreviousCalibration=difcTable,
            OffsetsWorkspace=f"{inws}_offset",
            OutputWorkspace=difcTable,
            OffsetMode="Signed",
            BinWidth=min(pixelGroup.dBin()),
        )
        ApplyDiffCal(
            InstrumentWorkspace=inws_tof_update,
            CalibrationWorkspace=difcTable,
        )
        offsetStats = access_pointer(OffsetStatistics(f"{inws}_offset"))
        medianOffsets.append(offsetStats["medianOffset"])
    # process over -- apply DIFC to raw data   
    CloneWorkspace(
        InputWorkspace=inputWorkspace,
        OutputWorkspace=inws_dsp_final,
    )
    ApplyDiffCal(
        InstrumentWorkspace=inws_dsp_final,
        CalibrationWorkspace=difcTable,
    )
    ConvertUnits(
        InputWorkspace=inws_dsp_final,
        OutputWorkspace=inws_dsp_final,
        Target="dSpacing",
    )
    return medianOffsets
        

ConvertUnits(
    InputWorkspace=inputWorkspace,
    OutputWorkspace="total_dsp_before",
    Target="dSpacing",
)

### NO BACKGROUND REMOVAL ##

CloneWorkspace(
    InputWorkspace=inputWorkspace,
    OutputWorkspace=total,
)

offsets = performCrossCorrelation(total)
print(offsets)

### REMOVE EVENT BACKGROUND BY BLANKS ##

""" Logic notes:
    Given event data, and a list of known peak windows, remove all events not in a peak window.
    The events can be removed with masking.
    The peak windows are usually given in d-spacing, so requries first converting units to d-space.
    The peak windows are specific to a grouping, so need to act by-group.
    On each group, remove non-peak events from all detectors in that group.
"""

# perform the steps of the prototype algo

blanks = {}
for peakList in detectorPeaks:
    blanks[peakList.groupID] = [(0, peakList.peaks[0].minimum)]
    for i in range(len(peakList.peaks) - 1):
        blanks[peakList.groupID].append((peakList.peaks[i].maximum, peakList.peaks[i+1].minimum))
    blanks[peakList.groupID].append((peakList.peaks[-1].maximum, 10.0))

ws = ConvertUnits(
    InputWorkspace=inputWorkspace,
    OutputWorkspace=ref,
    Target="dSpacing",
)
for groupID in groupIDs:
    for detid in groupDetectorIDs[groupID]:
        event_list = ws.getEventList(detid)
        for blank in blanks[groupID]:
            event_list.maskTof(blank[0], blank[1])
ConvertUnits(
    InputWorkspace=ref,
    OutputWorkspace=ref,
    Target="TOF",
)

blankOffsets = performCrossCorrelation(ref)
print(blankOffsets)

### REMOVE EVENT BACKGROUND BY SMOOTHING ##

start = time.time()
RemoveSmoothedBackground(
    InputWorkspace=inputWorkspace,
    GroupingWorkspace=focusWorkspace,
    OutputWorkspace=peaks,
    DetectorPeaks = create_pointer(detectorPeaks),
    SmoothingParameter=0.5,
)
end = time.time()
print(f"TIME FOR ALGO = {end-start}")

ConvertUnits(
    InputWorkspace=peaks,
    OutputWorkspace=peaks,
    Target="dSpacing",
)

removeOffsets = performCrossCorrelation(peaks)
print(removeOffsets)


## CONVERT SPECTRUM AXIS    
for x in ["total_cc", "peaks_cc", "blanked_cc"]:
    ConvertSpectrumAxis(
        InputWorkspace=x,
        OutputWorkspace=x,
        Target="SignedTheta",
    )

## FOCUS FOR SAKE OF GRAPHING

for x in ["total_dsp_after", "total_dsp_before", "peaks_dsp_after", "blanked_dsp_after"]:
    ConvertSpectrumAxis(
        InputWorkspace=x,
        Outputworkspace=x,
        Target="SignedTheta",
    )
    DiffractionFocussing(
        InputWorkspace=x,
        OutputWorkspace=f"{x}_foc",
        GroupingWorkspace=focusWorkspace,
    )

### PLOT PEAK RESULTS #################################
fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
ax.plot(mtd[f"{total}_dsp_before_foc"], wkspIndex=0, label="Raw", normalize_by_bin_width=True)
ax.plot(mtd[f"{total}_dsp_after_foc"], wkspIndex=0, label="Total Data", normalize_by_bin_width=True)
ax.plot(mtd[f"{ref}_dsp_after_foc"], wkspIndex=0, label="Event Blanking", normalize_by_bin_width=True)
ax.plot(mtd[f"{peaks}_dsp_after_foc"], wkspIndex=0, label="Smoothing Subtraction", normalize_by_bin_width=True)
ax.legend()
fig.show()


### PLOT CC RESULTS #################################
fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
ax.plot(mtd["no_removal_foc"], wkspIndex=0, label="Total Data", normalize_by_bin_width=True)
ax.plot(mtd["event_blank_foc"], wkspIndex=0, label="Event Blanking", normalize_by_bin_width=True)
ax.plot(mtd["smoothing_foc"], wkspIndex=0, label="Smoothing Subtraction", normalize_by_bin_width=True)
ax.legend()
fig.show()