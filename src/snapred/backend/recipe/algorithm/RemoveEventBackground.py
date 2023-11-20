# Logic notes:
"""
    Logic notes:
    Given event data, and a list of know peak windows, remove all events not in a peak window.
    The events can be removed with masking.
    The peak windows are usually given in d-spacing, so requries first converting units to d-space.
    The peak windoews are specific to a grouping, so need to act by-group.
    On each group, remove non-peak events from all detectors in that group.
"""
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    IEventWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.recipe.algorithm.MakeDirtyDish import MakeDirtyDish
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class RemoveEventBackground(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty(
            IEventWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Event workspace containing the background to be removed, in TOF units",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace holding the detector grouping information, for assigning peak windows",
        )
        self.declareProperty(
            IEventWorkspaceProperty("OutputWorkspace", "", Direction.Output),
            doc="Event workspace with the background removed",
        )
        self.declareProperty(
            "DetectorPeaks",
            defaultValue="",
            direction=Direction.Input,
            doc="A list of GroupPeakList objects, as a string",
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, predictedPeaksList: List[GroupPeakList]):
        # for each group, create a list of regions to blank out
        # these are between the peaks, from one peak min to the next peak max
        # also blank regions on each end of the spectrum
        self.blanks: Dict[int, List[Tuple[float, float]]] = {}
        for peakList in predictedPeaksList:
            self.blanks[peakList.groupID] = [(0, peakList.peaks[0].minimum)]
            for i in range(len(peakList.peaks) - 1):
                self.blanks[peakList.groupID].append((peakList.peaks[i].maximum, peakList.peaks[i + 1].minimum))
            self.blanks[peakList.groupID].append((peakList.peaks[-1].maximum, 10.0))  # TODO get better dMax

        # get handle to group focusing workspace and retrieve all detector IDs in each group
        focusWSname: str = str(self.getPropertyValue("GroupingWorkspace"))
        focusWS = self.mantidSnapper.mtd[focusWSname]
        self.groupIDs: List[int] = [int(x) for x in focusWS.getGroupIDs()]
        peakgroupIDs = [peakList.groupID for peakList in predictedPeaksList]
        if self.groupIDs != peakgroupIDs:
            raise RuntimeError(f"Groups IDs in workspace and peak list do not match: {self.groupIDs} vs {peakgroupIDs}")
        # get a list of the detector IDs in each group
        self.groupDetectorIDs: Dict[int, List[int]] = {}
        for groupID in self.groupIDs:
            self.groupDetectorIDs[groupID] = [int(x) for x in focusWS.getDetectorIDsOfGroup(groupID)]

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        if self.getProperty("OutputWorkspace").isDefault:
            self.outputWorkspaceName = self.inputWorkspaceName + "_peaks"  # TODO make a better name
            self.setPropertyValue("OutputWorkspace", self.outputWorkspaceName)
        else:
            self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

    def PyExec(self):
        """
        Removes background from event data, given known peak windows
        This helps with cross-correlation for high-background data
        Extracts peaks by deleting events outsidethe peak windows
        """
        self.log().notice("Removing background")

        # get the peak predictions from user input
        predictedPeaksList = parse_raw_as(List[GroupPeakList], self.getPropertyValue("DetectorPeaks"))
        self.chopIngredients(predictedPeaksList)
        self.unbagGroceries()

        self.mantidSnapper.MakeDirtyDish(
            "Creating copy of initial TOF data",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.inputWorkspaceName + "_extractBegin",
        )
        self.mantidSnapper.ConvertUnits(
            "Convert to d-spacing to match peak windows",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
            Target="dSpacing",
        )
        self.mantidSnapper.MakeDirtyDish(
            "Creating copy of initial d-spacing data",
            InputWorkspace=self.outputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName + "_extractDSP",
        )
        # get a handle on the workspace
        # inside each group, at each detector in the group,
        # blank out all times outside the peak windows
        self.mantidSnapper.executeQueue()
        ws = self.mantidSnapper.mtd[self.outputWorkspaceName]
        for groupID in self.groupIDs:
            for detid in self.groupDetectorIDs[groupID]:
                event_list = ws.getEventList(detid)
                for blank in self.blanks[groupID]:
                    event_list.maskTof(blank[0], blank[1])
        # for inspection, make a copy of peak-stripped d-space data
        self.mantidSnapper.MakeDirtyDish(
            "Creating copy of initial d-spacing data",
            InputWorkspace=self.outputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName + "_extractDSP_stripped",
        )
        # now convert back to TOF
        self.mantidSnapper.ConvertUnits(
            "Convert units back to TOF",
            InputWorkspace=self.outputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
            Target="TOF",
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(RemoveEventBackground)
