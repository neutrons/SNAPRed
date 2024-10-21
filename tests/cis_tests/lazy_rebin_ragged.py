import json
import random
import unittest
import unittest.mock as mock

import pytest
import snapred.backend.recipe.algorithm
from mantid.simpleapi import *
import matplotlib.pyplot as plt
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffCalRecipe as GroupRx

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from util.dao import DAOFactory

from snapred.meta.Config import Config, Resource
Config._config['cis_mode'] = True
Resource._resourcesPath = os.path.expanduser("~/SNAPRed/tests/resources/")

#### CREATE INGREDIENTS
fakeRunNumber = "555"
fakeRunConfig = RunConfig(runNumber=str(fakeRunNumber))

# fake instrument state
TOFMin = 10
TOFMax = 1000
TOFBin = 0.001
fakePixelGroup = DAOFactory.synthetic_pixel_group.copy()
fakePixelGroup.timeOfFlight.minimum = TOFMin
fakePixelGroup.timeOfFlight.maximum = TOFMax
fakePixelGroup.timeOfFlight.binWidth = TOFBin
fakePixelGroup.timeOfFlight.binningMode = -1 # LOG binning

def makePeakList(values, mins, maxs):
    crystalPeak = {"hkl": (0,0,0), "dSpacing": 1.0, "fSquared": 1.0, "multiplicity": 1.0}
    return [
        DetectorPeak.model_validate({
            "position": {"value": value, "minimum": min, "maximum": max},
            "peak": crystalPeak,
        }) for value, min, max in zip(values, mins, maxs)
    ]

def makeGroup(groupID, values, mins, maxs):
    peakList = makePeakList(values, mins, maxs)
    return GroupPeakList(groupID=groupID, peaks=peakList)

group3  = makeGroup( 3, [0.370, 0.248], [0.345, 0.225], [0.397, 0.268])
group7  = makeGroup( 7, [0.579, 0.386], [0.538, 0.352], [0.619, 0.417])
group2  = makeGroup( 2, [0.328, 0.219], [0.306, 0.199], [0.351, 0.237])
group11 = makeGroup(11, [0.438, 0.294], [0.406, 0.269], [0.470, 0.316])

print(fakePixelGroup.json(indent=2))

fakeIngredients = DiffractionCalibrationIngredients(
    runConfig=fakeRunConfig,
    groupedPeakLists=[group2, group3, group7, group11],
    calPath=Resource.getPath("outputs/calibration/"),
    convergenceThreshold=1.0,
    pixelGroup=fakePixelGroup,
)

#Set data to be used for RebinRagged
dMin = fakeIngredients.pixelGroup.dMin()
dMax = fakeIngredients.pixelGroup.dMax()
dBin = fakeIngredients.pixelGroup.dBin()
groupIDs = fakeIngredients.pixelGroup.groupIDs


#### CREATE DATA
inputWStof = f"_TOF_{fakeRunNumber}"
inputWSdsp = f"_DSP_{fakeRunNumber}"
diffocWSdsp = f"_DSP_{fakeRunNumber}_diffoc"
diffocWStof = f"_TOF_{fakeRunNumber}_diffoc"
groupingWS = f"grouping_ws"
DIFCpixel = "DIFCpixel"

midpoint = (TOFMax + TOFMin) / 2.0
CreateSampleWorkspace(
    OutputWorkspace = inputWStof,
    Function = "Powder Diffraction",
    XMin = TOFMin,
    XMax = TOFMax,
    BinWidth = 0.001,
    XUnit = "TOF",
    NumBanks = 4,
    BankPixelWidth=2,
    Random=True,
)
LoadInstrument(
    Workspace=inputWStof,
    Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
    RewriteSpectraMap=True,
)
Rebin(
    InputWorkspace=inputWStof,
    OutputWorkspace=inputWStof,
    Params=(TOFMin, 0.001, TOFMax),
    BinningMode = "Logarithmic",
)

# load the detector groupings
LoadDetectorsGroupingFile(
    "Loading grouping definition from detectors grouping file...",
    InputFile=Resource.getPath("/inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
    InputWorkspace=inputWStof,
    OutputWorkspace=groupingWS,
)


# create a DIFC table
CalculateDiffCalTable(
    InputWorkspace = inputWStof,
    CalibrationTable = DIFCpixel,
    OffsetMode = "Signed",
    BinWidth = TOFBin,
)


# create the diffoc TOF data
ConvertUnits(
    InputWorkspace = inputWStof,
    OutputWorkspace = inputWSdsp,
    Target = "dSpacing",
)
DiffractionFocussing(
    InputWorkspace = inputWSdsp,
    OutputWorkspace = diffocWSdsp,
    GroupingWorkspace = groupingWS,
)
RebinRagged(
    InputWorkspace = diffocWSdsp,
    OutputWorkspace = diffocWSdsp,
    XMin = dMin,
    XMax = dMax,
    Delta = dBin,
)
ConvertUnits(
    InputWorkspace = diffocWSdsp,
    OutputWorkspace = diffocWStof,
    Target = "TOF",
)

print(fakeIngredients.groupedPeakLists)

# now run the algorithm
groceries = {
    "inputWorkspace": inputWStof,
    "groupingWorkspace": groupingWS,
    "calibrationTable": "_final_DIFc_table",
    "previousCalTable": DIFCpixel,
    "outputWorkspace": f"_test_out_{fakeRunNumber}_tof",
}
res = GroupRx.cook(fakeIngredients, groceries)

## print graph, check all groups fit the peak
fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
ax.plot(mtd[diffocWStof], wkspIndex=0, label="original peak")
ax.plot(mtd["_PDCal_diag"], wkspIndex=0, label="group2")
ax.plot(mtd["_PDCal_diag"], wkspIndex=1, label="group3")
ax.plot(mtd["_PDCal_diag"], wkspIndex=2, label="group7")
ax.plot(mtd["_PDCal_diag"], wkspIndex=3, label="group11")
ax.legend() # show the legend
fig.show()
