from typing import Dict, List

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    mtd,
)
from mantid.dataobjects import GroupingWorkspace
from mantid.kernel import Direction, ULongLongPropertyWithValue

from snapred.backend.log.logger import snapredLogger
from snapred.meta.pointer import create_pointer

logger = snapredLogger.getLogger(__name__)

# Top-level component names for the two SNAP detector panels.
PANEL_NAMES = ("East", "West")


class DetectorPanelStraddleCheck(PythonAlgorithm):
    """
    Check if any group in a GroupingWorkspace straddles both the East and West
    detector panels of the SNAP instrument.

    A group "straddles" if it contains pixels from more than one panel.

    Performance: builds a panel lookup from the instrument component tree by
    iterating only over banks (not individual pixels).  Group membership
    checks are vectorised with numpy.
    """

    def category(self):
        return "SNAPRed Internal"

    def validateInputs(self) -> Dict[str, str]:
        err = {}
        ws = mtd[self.getPropertyValue("GroupingWorkspace")]
        if not isinstance(ws, GroupingWorkspace):
            err["GroupingWorkspace"] = "The workspace must be a GroupingWorkspace"
        return err

    def PyInit(self):
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="GroupingWorkspace with instrument geometry attached",
        )
        self.declareProperty(
            ULongLongPropertyWithValue("StraddlingGroups", id(None), direction=Direction.Output),
            doc="A pointer to the list of group IDs that straddle East/West panels.",
        )
        self.setRethrows(True)

    @staticmethod
    def _collect_bank_id_ranges(component):
        """
        Recursively collect (min_id, max_id) for every RectangularDetector
        under *component*.  Stops descending once a bank is reached.
        """
        ranges = []
        if hasattr(component, "minDetectorID"):
            # RectangularDetector – leaf bank level.
            ranges.append((component.minDetectorID(), component.maxDetectorID()))
        elif hasattr(component, "nelements"):
            # CompAssembly – descend into children.
            for i in range(component.nelements()):
                ranges.extend(DetectorPanelStraddleCheck._collect_bank_id_ranges(component[i]))
        return ranges

    @staticmethod
    def _build_panel_lookup(groupingWs):
        """
        Return a numpy int8 array indexed by detector ID.

        Values: 0 = unassigned, 1..N = panel index (matching ``PANEL_NAMES`` order).

        Only iterates over banks in the component tree, not individual pixels,
        so setup cost is O(n_banks) rather than O(n_pixels).
        """
        instrument = groupingWs.getInstrument()
        det_info = groupingWs.detectorInfo()
        all_det_ids = det_info.detectorIDs()
        max_det_id = int(np.max(all_det_ids))

        panel_lookup = np.zeros(max_det_id + 1, dtype=np.int8)

        for panel_code, panel_name in enumerate(PANEL_NAMES, start=1):
            panel = instrument.getComponentByName(panel_name)
            if panel is None:
                logger.warning(f"Panel '{panel_name}' not found in instrument definition")
                continue
            for min_id, max_id in DetectorPanelStraddleCheck._collect_bank_id_ranges(panel):
                panel_lookup[min_id : max_id + 1] = panel_code

        return panel_lookup

    def PyExec(self):
        groupingWs = mtd[self.getPropertyValue("GroupingWorkspace")]
        panel_lookup = self._build_panel_lookup(groupingWs)

        straddling_groups: List[int] = []
        for group_id in groupingWs.getGroupIDs():
            det_ids = groupingWs.getDetectorIDsOfGroup(int(group_id))
            panels_in_group = panel_lookup[det_ids]
            unique_panels = np.unique(panels_in_group)
            # Ignore panel code 0 (unassigned).
            unique_panels = unique_panels[unique_panels > 0]
            if len(unique_panels) > 1:
                straddling_groups.append(int(group_id))

        self.setProperty("StraddlingGroups", create_pointer(straddling_groups))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(DetectorPanelStraddleCheck)
