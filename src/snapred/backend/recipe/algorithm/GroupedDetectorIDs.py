from typing import Dict, List

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    mtd,
)
from mantid.dataobjects import GroupingWorkspace
from mantid.kernel import Direction, ULongLongPropertyWithValue

from snapred.meta.pointer import create_pointer


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
        if not isinstance(focusWS, GroupingWorkspace):
            err["GroupingWorkspace"] = "The workspace must be a GroupingWorkspace"
        return err

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the grouping information",
        )
        self.declareProperty(
            ULongLongPropertyWithValue("GroupWorkspaceIndices", id(None), direction=Direction.Output),
            doc="A pointer to the output dictionary (must be cast to object from memory address).",
        )
        self.setRethrows(True)

    def PyExec(self):
        # Retrieve pixel-IDs for all pixels in each subgroup.
        groupingWs = mtd[self.getPropertyValue("GroupingWorkspace")]
        
        groupWorkspaceIndices: Dict[int, List[int]] = {}
        
        #`<numpy ndarray>.tolist()` both converts to the nearest native-Python type, which is in this case `int`,
        #    and also converts to a Python list.
        for subgroupID in groupingWs.getGroupIDs().tolist():
            groupWorkspaceIndices[subgroupID] = groupingWs.getDetectorIDsOfGroup(subgroupID).tolist()
        
        self.setProperty("GroupWorkspaceIndices", create_pointer(groupWorkspaceIndices))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(GroupedDetectorIDs)
