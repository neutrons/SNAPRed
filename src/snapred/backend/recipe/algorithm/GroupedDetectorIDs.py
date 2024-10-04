import json
from typing import Dict, List

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction


class GroupedDetectorIDs(PythonAlgorithm):
    """
    From a grouping workspace, extract  detector IDs corresponding to each group,
    as a dictionary {groupID: list_of_detector_IDs}
    """

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def validateInputs(self) -> Dict[str, str]:
        err = {}
        focusWS = mtd[self.getPropertyValue("GroupingWorkspace")]
        if focusWS.id() != "GroupingWorkspace":
            err["GroupingWorkspace"] = "The workspace must be a GroupingWorkspace"
        return err

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the grouping information",
        )
        self.declareProperty("GroupWorkspaceIndices", "", direction=Direction.Output)
        self.setRethrows(True)

    def PyExec(self):
        # get handle to group focusing workspace and retrieve workspace indices for all detectors in each group
        focusWSname: str = str(self.getPropertyValue("GroupingWorkspace"))
        focusWS = mtd[focusWSname]
        groupWorkspaceIndices: Dict[int, List[int]] = {}
        for groupID in [int(x) for x in focusWS.getGroupIDs()]:
            groupWorkspaceIndices[groupID] = [int(x) for x in focusWS.getDetectorIDsOfGroup(groupID)]
        self.setProperty("GroupWorkspaceIndices", json.dumps(groupWorkspaceIndices))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(GroupedDetectorIDs)
