import pathlib
from datetime import datetime

from mantid.api import AlgorithmFactory, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class LoadGroupingDefinition(PythonAlgorithm):
    """
    This algorithm creates a grouping workspace from a grouping definition file.
    inputs:

        GroupingFilename: str -- path of an input grouping definition file (in NEXUS, XML, or HDF format)
        InstrumentName: str -- name of an associated instrument
        InstrumentFilename: str -- path of an associated instrument definition file
        InstrumentDonor: str -- Workspace to optionally take the associate instrument from
        OutputWorkspace: str -- name of an output grouping workspace
    """

    def PyInit(self) -> None:
        # declare properties
        self.declareProperty(
            "GroupingFilename",
            defaultValue="",
            direction=Direction.Input,
            doc="Path of an input grouping definition file",
        )
        self.declareProperty(
            "InstrumentName",
            defaultValue="",
            direction=Direction.Input,
            doc="Name of an associated instrument",
        )
        self.declareProperty(
            "InstrumentFilename",
            defaultValue="",
            direction=Direction.Input,
            doc="Path of an associated instrument definition file",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("InstrumentDonor", "", Direction.Input, PropertyMode.Optional),
            doc="Workspace to optionally take the instrument from, when GroupingFilename is in XML format",
        )
        self.declareProperty(
            "OutputWorkspace",
            defaultValue="",
            direction=Direction.Output,
            doc="Name of an output grouping workspace",
        )

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

        # define supported file name extensions
        self.supported_calib_file_extensions = ["H5", "HD5", "HDF"]
        self.supported_nexus_file_extensions = ["NXS", "NXS5"]
        self.supported_xml_file_extensions = ["XML"]

    def validateInput(self) -> None:
        grouping_file_name = self.getProperty("GroupingFilename").value
        file_extension = pathlib.Path(grouping_file_name).suffix.upper()[1:]
        supported_extensions = (
            self.supported_nexus_file_extensions
            + self.supported_xml_file_extensions
            + self.supported_calib_file_extensions
        )
        if file_extension not in supported_extensions:
            raise Exception(f"GroupingFilename has an unsupported file name extension {file_extension}")

        if file_extension in self.supported_calib_file_extensions:
            instrument_name = self.getProperty("InstrumentName").value
            instrument_file_name = self.getProperty("InstrumentFilename").value
            if instrument_name + instrument_file_name != "":
                if not (instrument_name == "" or instrument_file_name == ""):
                    raise Exception("Either InstrumentName or InstrumentFilename must be specified, but not both.")

        if file_extension not in self.supported_xml_file_extensions:
            instrument_donor = self.getProperty("InstrumentDonor").value
            if instrument_donor:
                logger.warn("InstrumentDonor will only be used if GroupingFilename is in XML format.")

    def PyExec(self) -> None:
        self.validateInput()
        grouping_file_name = self.getProperty("GroupingFilename").value
        output_ws_name = self.getProperty("OutputWorkspace").value
        file_extension = pathlib.Path(grouping_file_name).suffix.upper()[1:]
        isLite: bool = ".lite" in grouping_file_name
        if isLite and self.getProperty("InstrumentFilename").value == "":
            self.setProperty("InstrumentFilename", Config["instrument.lite.definition.file"])
        if file_extension in self.supported_calib_file_extensions:
            self.mantidSnapper.LoadDiffCal(
                "Loading grouping definition from calibration file...",
                Filename=grouping_file_name,
                InstrumentName=self.getProperty("InstrumentName").value,
                InstrumentFilename=self.getProperty("InstrumentFilename").value,
                MakeGroupingWorkspace=True,
                MakeCalWorkspace=False,
                MakeMaskWorkspace=False,
                WorkspaceName=output_ws_name,
            )
            # Remove the "_group" suffix, which is added by LoadDiffCal to the output workspace name
            self.mantidSnapper.RenameWorkspace(
                "Renaming grouping workspace...",
                InputWorkspace=output_ws_name + "_group",
                OutputWorkspace=output_ws_name,
            )
        elif file_extension in self.supported_xml_file_extensions:
            instrument_donor = self.getProperty("InstrumentDonor").value
            preserve_donor = True if instrument_donor else False
            if not instrument_donor:
                # create one from the instrument definition file
                instrument_donor = datetime.now().ctime() + "_idf"
                self.mantidSnapper.LoadEmptyInstrument(
                    "Loading instrument definition file...",
                    Filename=self.getProperty("InstrumentFilename").value,
                    InstrumentName=self.getProperty("InstrumentName").value,
                    OutputWorkspace=instrument_donor,
                )
            self.mantidSnapper.LoadDetectorsGroupingFile(
                "Loading grouping definition from detectors grouping file...",
                InputFile=grouping_file_name,
                InputWorkspace=instrument_donor,
                OutputWorkspace=output_ws_name,
            )
            if not preserve_donor:
                self.mantidSnapper.WashDishes(
                    "Deleting instrument definition workspace...",
                    Workspace=instrument_donor,
                )
        else:  # must be a NEXUS file
            self.mantidSnapper.LoadNexusProcessed(
                "Loading grouping definition from grouping workspace...",
                Filename=grouping_file_name,
                OutputWorkspace=output_ws_name,
            )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LoadGroupingDefinition)
