import json
import random
import unittest
import unittest.mock as mock

import pytest
from mantid.simpleapi import *
import matplotlib.pyplot as plt
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.CalculateDiffCalTable import CalculateDiffCalTable

# the algorithm to test
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import (
    GroupDiffractionCalibration as Algo,  # noqa: E402
)
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
fakePixelGroup = PixelGroup.parse_raw(Resource.read("inputs/diffcal/fakePixelGroup.json"))
fakePixelGroup.timeOfFlight.minimum = TOFMin
fakePixelGroup.timeOfFlight.maximum = TOFMax
fakePixelGroup.timeOfFlight.binWidth = TOFBin
fakePixelGroup.timeOfFlight.binningMode = -1 # LOG binning

peakList3 = [
    DetectorPeak.parse_obj({"position": {"value": 0.370, "minimum":0.345, "maximum": 0.397}}),
    DetectorPeak.parse_obj({"position": {"value": 0.248, "minimum":0.225, "maximum": 0.268}}),
]
group3 = GroupPeakList(groupID=3, peaks=peakList3)
peakList7 = [
    DetectorPeak.parse_obj({"position": {"value": 0.579, "minimum": 0.538, "maximum": 0.619}}),
    DetectorPeak.parse_obj({"position": {"value": 0.386, "minimum": 0.352, "maximum": 0.417}}),
]
group7 = GroupPeakList(groupID=7, peaks=peakList7)
peakList2 = [
    DetectorPeak.parse_obj({"position": {"value": 0.328, "minimum": 0.306, "maximum":0.351}}),
    DetectorPeak.parse_obj({"position": {"value": 0.219, "minimum": 0.199, "maximum":0.237}}),
]
group2 = GroupPeakList(groupID=2, peaks=peakList2)
peakList11 = [
    DetectorPeak.parse_obj({"position": {"value": 0.438, "minimum": 0.406, "maximum": 0.470}}),
    DetectorPeak.parse_obj({"position": {"value": 0.294, "minimum": 0.269, "maximum": 0.316}}),
]
group11 = GroupPeakList(groupID=11, peaks=peakList11)

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
    Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
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
cc = CalculateDiffCalTable()
cc.initialize()
cc.setProperty("InputWorkspace", inputWStof)
cc.setProperty("CalibrationTable", DIFCpixel)
cc.setProperty("OffsetMode", "Signed")
cc.setProperty("BinWidth", TOFBin)
cc.execute()


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
algo = Algo()
algo.initialize()
algo.setProperty("Ingredients", fakeIngredients.json())
algo.setProperty("InputWorkspace", inputWStof)
algo.setProperty("GroupingWorkspace", groupingWS)
algo.setProperty("FinalCalibrationTable", "_final_DIFc_table")
algo.setProperty("OutputWorkspace", f"_test_out_{fakeRunNumber}")
algo.setProperty("PreviousCalibrationTable", DIFCpixel)
algo.chopIngredients(fakeIngredients)
algo.focusWSname = groupingWS
assert algo.execute()

## print graph, check all groups fit the peak
fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
ax.plot(mtd[diffocWStof], wkspIndex=0, label="original peak")
ax.plot(mtd["_PDCal_diag"], wkspIndex=0, label="group2")
ax.plot(mtd["_PDCal_diag"], wkspIndex=1, label="group3")
ax.plot(mtd["_PDCal_diag"], wkspIndex=2, label="group7")
ax.plot(mtd["_PDCal_diag"], wkspIndex=3, label="group11")
ax.legend() # show the legend
fig.show()
