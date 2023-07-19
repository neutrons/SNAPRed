import json
from typing import List

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CustomStripPeaks"


class CustomStripPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InputGroupWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("PeakPositions", defaultValue="", direction=Direction.Input)
        self.declareProperty("FocusGroups", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        inputGroupWorkspace = self.getProperty("InputGroupWorkspace").value
        peakPositions = json.loads(self.getProperty("PeakPositions").value)
        outputWorkspace = self.getProperty("OutputWorkspace").value
        focusGroups = parse_raw_as(List[FocusGroup], self.getProperty("FocusGroups").value)
        # for each group in the workspace, strip peaks with associated peak positions
        inputGroupWorkspace = self.mantidSnapper.mtd[inputGroupWorkspace]
        strippedWorkspaces = []
        for workspaceIndex in range(len(focusGroups)):
            strippedWorkspaces.append(
                self.mantidSnapper.StripPeaks(
                    "Stripping peaks...",
                    InputWorkspace=inputGroupWorkspace.getItem(workspaceIndex),
                    PeakPositions=peakPositions[workspaceIndex],
                    OutputWorkspace=outputWorkspace + "_stripped_{}".format(workspaceIndex),
                )
            )
        output = self.mantidSnapper.GroupWorkspaces(
            "Grouping stripped results...",
            InputWorkspaces=[strippedWorkspace for strippedWorkspace in strippedWorkspaces],
            OutputWorkspace=outputWorkspace,
        )
        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspace", output.get())


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomStripPeaks)
