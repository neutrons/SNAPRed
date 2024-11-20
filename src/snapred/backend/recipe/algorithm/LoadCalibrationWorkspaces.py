import pathlib
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    ITableWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    mtd,
)
from mantid.dataobjects import MaskWorkspaceProperty
from mantid.kernel import Direction

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

logger = snapredLogger.getLogger(__name__)


class LoadCalibrationWorkspaces(PythonAlgorithm):
    """
    This algorithm loads any or all of the table workspace, mask workspace, and grouping workspace
    from disk in the 'SaveDiffCal' hdf-5 format.
    inputs:

        Filename: str -- path to the input HDF5 format file
        InstrumentDonor: str -- name of the instrument donor workspace
        CalibrationTable: str -- name of the output table workspace
        MaskWorkspace: str -- name of the output mask workspace
        GroupingWorkspace: str -- name of the output mask workspace
    """

    def category(self):
        return "SNAPRed Data Handling"

    def PyInit(self) -> None:
        # define supported file name extensions
        self.supported_file_extensions = ["H5", "HD5", "HDF"]
        self.all_extensions = self.supported_file_extensions

        self.declareProperty(
            FileProperty(
                "Filename",
                defaultValue="",
                action=FileAction.Load,
                extensions=self.all_extensions,
                direction=Direction.Input,
            ),
            doc="Path to HDF5 format file to be loaded",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("InstrumentDonor", "", Direction.Input),
            doc="Workspace to use as an instrument donor",
        )
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationTable", "", Direction.Output, PropertyMode.Optional),
            doc="Name of the output table workspace",
        )
        self.declareProperty(
            MaskWorkspaceProperty("MaskWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Name of the output mask workspace",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Name of the output mask workspace",
        )

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        if self.getProperty("Filename").isDefault:
            errors["Filename"] = "You must specify the HDF5 format filename to load"
            return errors

        filename = self.getPropertyValue("Filename")
        extension = pathlib.Path(filename).suffix[1:].upper()
        # TODO this validation SHOULD occur as part of property validation
        if extension not in self.supported_file_extensions:
            errors["Filename"] = f"File extension {extension} is not supported"

        tableName = self.getPropertyValue("CalibrationTable")
        maskName = self.getPropertyValue("MaskWorkspace")
        groupingName = self.getPropertyValue("GroupingWorkspace")
        if not tableName and not maskName and not groupingName:
            errors["CalibrationTable"] = (
                "at least one of 'CalibrationTable', 'MaskWorkspace', or 'GroupingWorkspace' must be specified"
            )

        return errors

    def chopIngredients(self, filePath: str):
        pass

    def unbagGroceries(self):
        pass

    def PyExec(self) -> None:
        self.calibrationTable = self.getPropertyValue("CalibrationTable")
        self.maskWorkspace = self.getPropertyValue("MaskWorkspace")
        self.groupingWorkspace = self.getPropertyValue("GroupingWorkspace")

        baseName = mtd.unique_hidden_name()
        self.mantidSnapper.LoadDiffCal(
            "Loading workspaces from calibration file...",
            Filename=self.getPropertyValue("Filename"),
            InputWorkspace=self.getPropertyValue("InstrumentDonor"),
            MakeCalWorkspace=True if self.calibrationTable else False,
            MakeMaskWorkspace=True if self.maskWorkspace else False,
            MakeGroupingWorkspace=True if self.groupingWorkspace else False,
            WorkspaceName=baseName,
        )
        if self.calibrationTable:
            self.mantidSnapper.RenameWorkspace(
                "Renaming table workspace...",
                InputWorkspace=baseName + "_cal",
                OutputWorkspace=self.calibrationTable,
            )
        if self.maskWorkspace:
            self.mantidSnapper.RenameWorkspace(
                "Renaming mask workspace...",
                InputWorkspace=baseName + "_mask",
                OutputWorkspace=self.maskWorkspace,
            )
        if self.groupingWorkspace:
            self.mantidSnapper.RenameWorkspace(
                "Renaming grouping workspace...",
                InputWorkspace=baseName + "_group",
                OutputWorkspace=self.groupingWorkspace,
            )
        self.mantidSnapper.executeQueue()

        if self.calibrationTable:
            # NOTE ConvertDiffCal has issues ifthe "tofmin" column is present,
            # and LoadFiffCal is written to always add this column on load.
            # Easiest temporary solution is to delete this column, until mantid is updated.
            calTab = self.mantidSnapper.mtd[self.calibrationTable]
            calTab.removeColumn("tofmin")
            self.setPropertyValue("CalibrationTable", self.calibrationTable)
        if self.maskWorkspace:
            self.setPropertyValue("MaskWorkspace", self.maskWorkspace)
        if self.groupingWorkspace:
            self.setPropertyValue("GroupingWorkspace", self.groupingWorkspace)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LoadCalibrationWorkspaces)
