from typing import Dict, List

from mantid.api import (
    IEventWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceUnitValidator,
)
from mantid.dataobjects import GroupingWorkspaceProperty
from mantid.kernel import Direction, FloatBoundedValidator
from mantid.kernel import ULongLongPropertyWithValue as PointerProperty
from mantid.simpleapi import (
    ConvertToEventWorkspace,
    ConvertToMatrixWorkspace,
    ConvertUnits,
    DeleteWorkspaces,
    GroupDetectors,
    GroupedDetectorIDs,
    MakeDirtyDish,
    SmoothDataExcludingPeaksAlgo,
    mtd,
)

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.meta.Config import Config
from snapred.meta.pointer import access_pointer, create_pointer


class RemoveSmoothedBackground(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            IEventWorkspaceProperty(
                "InputWorkspace",
                defaultValue="",
                direction=Direction.Input,
                optional=PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("TOF"),
            ),
            doc="Event workspace containing the data with peaks and baseline, in TOF units",
        )
        self.declareProperty(
            GroupingWorkspaceProperty(
                "GroupingWorkspace",
                defaultValue="",
                direction=Direction.Input,
                optional=PropertyMode.Mandatory,
            ),
            doc="Workspace holding the detector grouping information, for assigning peak windows",
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace",
                defaultValue="",
                direction=Direction.Output,
                validator=WorkspaceUnitValidator("TOF"),
            ),
            doc="Histogram workspace representing the extracted background",
        )
        self.declareProperty(
            PointerProperty("DetectorPeaks", id(None)),
            doc="The memory adress pointing to the list of grouped peaks.",
        )
        self.declareProperty(
            "SmoothingParameter",
            defaultValue=Config["calibration.diffraction.smoothingParameter"],
            validator=FloatBoundedValidator(lower=0.0),
        )
        self.setRethrows(True)

    def validateInputs(self) -> Dict[str, str]:
        err = {}
        if self.getProperty("DetectorPeaks").isDefault:
            err["DetectorPeaks"] = "You must pass a pointer to a DetectorPeaks object"
        return err

    def chopIngredients(self, predictedPeaksList: List[GroupPeakList]):
        self.smoothingParameter = float(self.getProperty("SmoothingParameter").value)
        # get handle to group focusing workspace and retrieve all detector IDs in each group
        focusWSname: str = str(self.getPropertyValue("GroupingWorkspace"))
        # get a list of the detector IDs in each group
        result_ptr: PointerProperty = GroupedDetectorIDs(focusWSname)
        self.groupDetectorIDs: Dict[int, List[int]] = access_pointer(result_ptr)
        self.groupIDs: List[int] = list(self.groupDetectorIDs.keys())
        peakgroupIDs: List[int] = [peakList.groupID for peakList in predictedPeaksList]
        if self.groupIDs != peakgroupIDs:
            raise RuntimeError(f"Groups IDs in workspace and peak list do not match: {self.groupIDs} vs {peakgroupIDs}")

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")
        self.focusWorkspace = self.getPropertyValue("GroupingWorkspace")
        self.isEventWs = mtd[self.inputWorkspaceName].id() == "EventWorkspace"

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

        tmpDSPws = mtd.unique_name(prefix="dsp_")
        diffocWSname = mtd.unique_name(prefix="diffoc_")
        backgroundWSname = mtd.unique_name(prefix="bkgr_")

        # Creating copy of initial TOF data
        MakeDirtyDish(
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.inputWorkspaceName + "_extractTOF_before",
        )
        # Convert to d-spacing to match peak windows
        ConvertUnits(
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=tmpDSPws,
            Target="dSpacing",
        )
        # Creating copy of initial d-spacing data
        MakeDirtyDish(
            InputWorkspace=tmpDSPws,
            OutputWorkspace=self.outputWorkspaceName + "_extractDSP_before",
        )

        # find average spectra over focus groups
        if self.isEventWs:
            ConvertToMatrixWorkspace(
                InputWorkspace=tmpDSPws,
                OutputWorkspace=tmpDSPws,
            )
        GroupDetectors(
            InputWorkspace=self.inputWorkspaceName,
            CopyGroupingFromWorkspace=self.focusWorkspace,
            OutputWorkspace=diffocWSname,
            Behaviour="Average",
            PreserveEvents=False,
        )
        ConvertUnits(
            InputWorkspace=diffocWSname,
            OutputWorkspace=diffocWSname,
            Target="dSpacing",
        )
        # create the smoothed background from averaged spectra
        SmoothDataExcludingPeaksAlgo(
            InputWorkspace=diffocWSname,
            OutputWorkspace=backgroundWSname,
            DetectorPeaks=create_pointer(predictedPeaksList),
            SmoothingParameter=self.smoothingParameter,
        )

        # Subtract off the scaled background estimation
        focusWS = mtd[diffocWSname]
        smoothWS = mtd[backgroundWSname]
        outputWS = mtd[tmpDSPws]
        for wkspindx, groupID in enumerate(self.groupIDs):
            y_smooth = smoothWS.readY(wkspindx).copy()
            y_data = focusWS.readY(wkspindx).copy()
            scale_denom = sum(y_data) / len(y_data)
            for detid in self.groupDetectorIDs[groupID]:
                # subtract off the background and update the date in the workspace
                y_new = outputWS.readY(detid).copy()
                scale_num = sum(y_new) / len(y_new)

                y_new = y_new - (scale_num / scale_denom) * y_smooth
                y_new[y_new < 0] = 0
                outputWS.setY(detid, y_new)

        MakeDirtyDish(
            InputWorkspace=tmpDSPws,
            OutputWorkspace=self.outputWorkspaceName + "_extractDSP_after",
        )
        ConvertUnits(
            InputWorkspace=tmpDSPws,
            OutputWorkspace=self.outputWorkspaceName,
            Target="TOF",
        )
        MakeDirtyDish(
            InputWorkspace=self.outputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName + "_extractTOF_after",
        )
        if self.isEventWs:
            ConvertToEventWorkspace(
                InputWorkspace=self.outputWorkspaceName,
                OutputWorkspace=self.outputWorkspaceName,
            )

        # Cleanup
        DeleteWorkspaces(WorkspaceList=[diffocWSname, backgroundWSname, tmpDSPws])

        self.setPropertyValue("OutputWorkspace", self.outputWorkspaceName)
