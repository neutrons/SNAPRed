from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    IEventWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceUnitValidator,
)
from mantid.kernel import Direction
from mantid.kernel import ULongLongPropertyWithValue as PointerProperty
from mantid.simpleapi import (
    ConvertToMatrixWorkspace,
    ConvertUnits,
    GroupedDetectorIDs,
    MakeDirtyDish,
    mtd,
)
from scipy.interpolate import make_smoothing_spline

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.meta.Config import Config
from snapred.meta.pointer import access_pointer


class RemoveEventBackground(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            IEventWorkspaceProperty(
                "InputWorkspace", "", Direction.Input, PropertyMode.Mandatory, validator=WorkspaceUnitValidator("TOF")
            ),
            doc="Event workspace containing the data with peaks and background, in TOF units",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace holding the detector grouping information, for assigning peak windows",
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace",
                "",
                Direction.Output,
                validator=WorkspaceUnitValidator("TOF"),
            ),
            doc="Histogram workspace representing the extracted background",
        )
        self.declareProperty(
            PointerProperty("DetectorPeaks", id(None)),
            "The memory adress pointing to the list of grouped peaks.",
        )
        self.declareProperty(
            "SmoothingParameter",
            defaultValue=Config["calibration.diffraction.smoothingParameter"],
            direction=Direction.Input,
        )
        self.setRethrows(True)

    def validateInputs(self) -> Dict[str, str]:
        err = {}
        if self.getProperty("DetectorPeaks").isDefault:
            err["DetectorPeaks"] = "You must pass a pointer to a DetectorPeaks object"
        return err

    def chopIngredients(self, predictedPeaksList: List[GroupPeakList]):
        # for each group, create a list of regions to mask (i.e., the peak regions)
        # these are the ranges between the peak min and max values
        self.maskRegions: Dict[int, List[Tuple[float, float]]] = {}
        for peakList in predictedPeaksList:
            self.maskRegions[peakList.groupID] = []
            for peak in peakList.peaks:
                self.maskRegions[peakList.groupID].append((peak.minimum, peak.maximum))
        # get handle to group focusing workspace and retrieve all detector IDs in each group
        focusWSname: str = str(self.getPropertyValue("GroupingWorkspace"))
        # get a list of the detector IDs in each group
        result_ptr: PointerProperty = GroupedDetectorIDs(focusWSname)
        self.groupDetectorIDs = access_pointer(result_ptr)
        self.groupIDs: List[int] = list(self.groupDetectorIDs.keys())
        peakgroupIDs = [peakList.groupID for peakList in predictedPeaksList]
        if self.groupIDs != peakgroupIDs:
            raise RuntimeError(f"Groups IDs in workspace and peak list do not match: {self.groupIDs} vs {peakgroupIDs}")

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.outputBackgroundWorkspaceName = self.getPropertyValue("OutputWorkspace")
        self.smoothingParameter = float(self.getPropertyValue("SmoothingParameter"))

    def PyExec(self):
        """
        Extracts background from event data by masking the peak regions.
        This background can later be subtracted from the main data.
        """
        self.log().notice("Extracting background")

        # get the peak predictions from user input
        peak_ptr: PointerProperty = self.getProperty("DetectorPeaks").value
        predictedPeaksList = access_pointer(peak_ptr)
        self.chopIngredients(predictedPeaksList)
        self.unbagGroceries()

        # Creating copy of initial TOF data
        MakeDirtyDish(
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.inputWorkspaceName + "_extractBegin",
        )
        # Convert to d-spacing to match peak windows
        ConvertUnits(
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputBackgroundWorkspaceName,
            Target="dSpacing",
        )
        # Creating copy of initial d-spacing data
        MakeDirtyDish(
            InputWorkspace=self.outputBackgroundWorkspaceName,
            OutputWorkspace=self.outputBackgroundWorkspaceName + "_extractDSP",
        )

        # Converting EventWorkspace to MatrixWorkspace...
        ConvertToMatrixWorkspace(
            InputWorkspace=self.outputBackgroundWorkspaceName,
            OutputWorkspace=self.outputBackgroundWorkspaceName,
        )

        # Replace peak regions with interpolated values from surrounding data
        ws = mtd[self.outputBackgroundWorkspaceName]
        for groupID in self.groupIDs:
            for detid in self.groupDetectorIDs[groupID]:
                y_data = ws.readY(detid).copy()
                x_data = ws.readX(detid)

                for mask in self.maskRegions[groupID]:
                    mask_indices = (x_data >= mask[0]) & (x_data <= mask[1])
                    before_mask = np.where(x_data < mask[0])[0][-1]
                    after_mask = np.where(x_data > mask[1])[0][0]

                    # Linear interpolation across the masked region
                    interp_values = np.linspace(y_data[before_mask], y_data[after_mask], mask_indices.sum())
                    y_data[mask_indices[:-1]] = interp_values

                ws.setY(detid, y_data)

        # Apply smoothing to the entire dataset
        self.applySmoothing(ws)

        # Convert back to TOF
        ConvertUnits(
            InputWorkspace=self.outputBackgroundWorkspaceName,
            OutputWorkspace=self.outputBackgroundWorkspaceName,
            Target="TOF",
        )

        self.setPropertyValue("OutputWorkspace", self.outputBackgroundWorkspaceName)

    def applySmoothing(self, workspace):
        """
        Applies smoothing to the entire workspace data.
        """
        numSpec = workspace.getNumberHistograms()

        for index in range(numSpec):
            x = workspace.readX(index)
            y = workspace.readY(index)

            # Apply spline smoothing to the entire dataset
            tck = make_smoothing_spline(x[:-1], y, lam=self.smoothingParameter)
            y_smooth = tck(x[:-1], extrapolate=False)

            # Ensure no negative values after smoothing
            y_smooth[y_smooth < 0] = 0

            workspace.setY(index, y_smooth)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(RemoveEventBackground)
