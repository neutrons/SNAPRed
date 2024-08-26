from typing import Dict, List, Tuple

from mantid.api import (
    AlgorithmFactory,
    IEventWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction
from pydantic import TypeAdapter
from scipy.interpolate import make_smoothing_spline

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class RemoveEventBackground(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            IEventWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Event workspace containing the data with peaks and background, in TOF units",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace holding the detector grouping information, for assigning peak windows",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output),
            doc="Histogram workspace representing the extracted background",
        )
        self.declareProperty(
            "DetectorPeaks",
            defaultValue="",
            direction=Direction.Input,
            doc="A list of GroupPeakList objects, as a string",
        )
        self.declareProperty(
            "SmoothingParameter",
            defaultValue=0.001,
            direction=Direction.Input,
            doc="Smoothing parameter for the spline smoothing after background extraction",
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

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
        self.outputBackgroundWorkspaceName = self.getPropertyValue("OutputWorkspace")
        self.smoothingParameter = float(self.getPropertyValue("SmoothingParameter"))

    def PyExec(self):
        """
        Extracts background from event data by masking the peak regions.
        This background can later be subtracted from the main data.
        """
        self.log().notice("Extracting background")

        # get the peak predictions from user input
        detectorPeaksJson = self.getPropertyValue("DetectorPeaks")
        typeAdapter = TypeAdapter(List[GroupPeakList])
        predictedPeaksList = typeAdapter.validate_json(detectorPeaksJson)
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
            OutputWorkspace=self.outputBackgroundWorkspaceName,
            Target="dSpacing",
        )
        self.mantidSnapper.MakeDirtyDish(
            "Creating copy of initial d-spacing data",
            InputWorkspace=self.outputBackgroundWorkspaceName,
            OutputWorkspace=self.outputBackgroundWorkspaceName + "_extractDSP",
        )

        # get a handle on the workspace
        ws = self.mantidSnapper.mtd[self.outputBackgroundWorkspaceName]
        for groupID in self.groupIDs:
            for detid in self.groupDetectorIDs[groupID]:
                event_list = ws.getEventList(detid)
                for mask in self.maskRegions[groupID]:
                    event_list.maskTof(mask[0], mask[1])

        self.mantidSnapper.ConvertToMatrixWorkspace(
            "Converting EventWorkspace to MatrixWorkspace...",
            InputWorkspace=self.outputBackgroundWorkspaceName,
            OutputWorkspace=self.outputBackgroundWorkspaceName,
        )
        self.mantidSnapper.executeQueue()

        ws = self.mantidSnapper.mtd[self.outputBackgroundWorkspaceName]

        # Apply smoothing after masking the peaks
        self.applySmoothing(ws)

        # Convert back to TOF
        self.mantidSnapper.ConvertUnits(
            "Convert units back to TOF",
            InputWorkspace=self.outputBackgroundWorkspaceName,
            OutputWorkspace=self.outputBackgroundWorkspaceName,
            Target="TOF",
        )
        self.mantidSnapper.executeQueue()

        self.setPropertyValue("OutputWorkspace", self.outputBackgroundWorkspaceName)

    def applySmoothing(self, workspace):
        """
        Applies smoothing to the workspace using a spline function after peaks have been masked.
        """
        numSpec = workspace.getNumberHistograms()

        for index in range(numSpec):
            x = workspace.readX(index)
            y = workspace.readY(index)

            xMidpoints = (x[:-1] + x[1:]) / 2
            yMidpoints = y.copy()

            # Throw an exception if y or xMidpoints are empty
            if len(yMidpoints) == 0 or len(xMidpoints) == 0:
                raise ValueError("No data in the workspace, all data removed by peak masking.")

            # Generate spline with purged dataset
            tck = make_smoothing_spline(xMidpoints, yMidpoints, lam=self.smoothingParameter)
            # Fill in the removed data using the spline function and original datapoints
            smoothing_results = tck(xMidpoints, extrapolate=False)
            workspace.setY(index, smoothing_results)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(RemoveEventBackground)
