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
    This algorithm creates a table workspace and a mask workspace from a calibration-data hdf5 file.
    inputs:

        Filename: str -- path to the input HDF5 format file
        InstrumentDonor: str -- name of the instrument donor workspace
        CalibrationTable: str -- name of the output table workspace
        MaskWorkspace: str -- name of the output mask workspace
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
            ITableWorkspaceProperty("CalibrationTable", "", Direction.Output, PropertyMode.Mandatory),
            doc="Name of the output table workspace",
        )
        self.declareProperty(
            MaskWorkspaceProperty("MaskWorkspace", "", Direction.Output, PropertyMode.Mandatory),
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

        return errors

    def chopIngredients(self, filePath: str):
        pass

    def unbagGroceries(self):
        pass

    def PyExec(self) -> None:
        self.calibrationTable = self.getPropertyValue("CalibrationTable")
        self.maskWorkspace = self.getPropertyValue("MaskWorkspace")

        baseName = mtd.unique_hidden_name()
        self.mantidSnapper.LoadDiffCal(
            "Loading constants table and mask from calibration file...",
            Filename=self.getPropertyValue("Filename"),
            InputWorkspace=self.getPropertyValue("InstrumentDonor"),
            MakeGroupingWorkspace=False,
            MakeCalWorkspace=True,
            MakeMaskWorkspace=True,
            WorkspaceName=baseName,
        )
        self.mantidSnapper.RenameWorkspace(
            "Renaming table workspace...",
            InputWorkspace=baseName + "_cal",
            OutputWorkspace=self.calibrationTable,
        )
        self.mantidSnapper.RenameWorkspace(
            "Renaming mask workspace...",
            InputWorkspace=baseName + "_mask",
            OutputWorkspace=self.maskWorkspace,
        )
        self.mantidSnapper.executeQueue()

        self.setPropertyValue("CalibrationTable", self.calibrationTable)
        self.setPropertyValue("MaskWorkspace", self.maskWorkspace)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LoadCalibrationWorkspaces)
