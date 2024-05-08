import pathlib
from datetime import datetime
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction, logger
from mantid.simpleapi import _create_algorithm_function

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


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

    def category(self):
        return "SNAPRed Data Handling"

    def PyInit(self) -> None:
        # define supported file name extensions
        self.supported_calib_file_extensions = ["H5", "HD5", "HDF"]
        self.supported_nexus_file_extensions = ["NXS", "NXS5"]
        self.supported_xml_file_extensions = ["XML"]
        self.all_extensions = (
            self.supported_nexus_file_extensions
            + self.supported_xml_file_extensions
            + self.supported_calib_file_extensions
        )

        self.declareProperty(
            FileProperty(
                "GroupingFilename",
                defaultValue="",
                action=FileAction.Load,
                extensions=self.all_extensions,
                direction=Direction.Input,
            ),
            doc="Path to the grouping file to be loaded",
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
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory),
            doc="Name of an output grouping workspace",
        )

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        if self.getProperty("GroupingFilename").isDefault:
            errors["GroupingFilename"] = "You must specify the grouping filename to load"
            return errors

        groupingFilename = self.getPropertyValue("GroupingFilename")
        groupingFileExt = pathlib.Path(groupingFilename).suffix[1:].upper()
        # TODO this validation SHOULD occur as part of property validation
        if groupingFileExt not in self.all_extensions:
            errors["GroupingFilename"] = f"Grouping file extension {groupingFileExt} is not supported"

        if groupingFileExt not in self.supported_xml_file_extensions:
            if not self.getProperty("InstrumentDonor").isDefault:
                logger.warning("InstrumentDonor will only be used if GroupingFilename is in XML format.")

        instrumentSources = ["InstrumentDonor", "InstrumentName", "InstrumentFilename"]
        specifiedSources = [s for s in instrumentSources if not self.getProperty(s).isDefault]
        if len(specifiedSources) > 1:
            msg = "You can only declare ONE way to get an instrument.  " + f"You specified {specifiedSources}"
            errors.update({i: msg for i in specifiedSources})
        elif len(specifiedSources) == 0:
            msg = f"You MUST specify an instrument source from {instrumentSources}"
            errors.update({i: msg for i in instrumentSources})
        return errors

    def chopIngredients(self, filePath: str):
        ext = pathlib.Path(filePath).suffix[1:].upper()
        if ext in self.supported_calib_file_extensions and not self.getProperty("InstrumentDonor").isDefault:
            self.instrumentSource = {"InputWorkspace": self.getPropertyValue("InstrumentDonor")}
        else:
            instrumentSources = ["InstrumentDonor", "InstrumentName", "InstrumentFilename"]
            self.instrumentSource = {
                s: self.getPropertyValue(s) for s in instrumentSources if not self.getProperty(s).isDefault
            }
        return ext

    def unbagGroceries(self):
        pass

    def PyExec(self) -> None:
        self.groupingFilePath = self.getPropertyValue("GroupingFilename")
        self.groupingFileExt = self.chopIngredients(self.groupingFilePath)
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

        isLite: bool = ".lite" in self.groupingFilePath
        if isLite and self.getProperty("InstrumentFilename").isDefault:
            self.setProperty("InstrumentFilename", Config["instrument.lite.definition.file"])

        if self.groupingFileExt in self.supported_calib_file_extensions:
            self.mantidSnapper.LoadDiffCal(
                "Loading grouping definition from calibration file...",
                Filename=self.groupingFilePath,
                MakeGroupingWorkspace=True,
                MakeCalWorkspace=False,
                MakeMaskWorkspace=False,
                WorkspaceName=self.outputWorkspaceName,
                **self.instrumentSource,
            )
            # Remove the "_group" suffix, which is added by LoadDiffCal to the output workspace name
            self.mantidSnapper.RenameWorkspace(
                "Renaming grouping workspace...",
                InputWorkspace=self.outputWorkspaceName + "_group",
                OutputWorkspace=self.outputWorkspaceName,
            )
        elif self.groupingFileExt in self.supported_xml_file_extensions:
            instrument_donor = self.getPropertyValue("InstrumentDonor")
            preserve_donor = True if instrument_donor else False
            if not instrument_donor:
                # create one from the instrument definition file
                instrument_donor = datetime.now().ctime() + "_idf"
                self.mantidSnapper.LoadEmptyInstrument(
                    "Loading instrument definition file...",
                    Filename=self.getPropertyValue("InstrumentFilename"),
                    InstrumentName=self.getPropertyValue("InstrumentName"),
                    OutputWorkspace=instrument_donor,
                )
            self.mantidSnapper.LoadDetectorsGroupingFile(
                "Loading grouping definition from detectors grouping file...",
                InputFile=self.groupingFilePath,
                InputWorkspace=instrument_donor,
                OutputWorkspace=self.outputWorkspaceName,
            )
            if not preserve_donor:
                self.mantidSnapper.WashDishes(
                    "Deleting instrument definition workspace...",
                    Workspace=instrument_donor,
                )
        elif self.groupingFileExt in self.supported_nexus_file_extensions:  # must be a NEXUS file
            self.mantidSnapper.LoadNexusProcessed(
                "Loading grouping definition from grouping workspace...",
                Filename=self.groupingFilePath,
                OutputWorkspace=self.outputWorkspaceName,
            )
        else:
            raise RuntimeError(
                f"""
                Somehow you dodged the validation checks on file extensions
                and gave a {self.groupingFileExt} file when the only allowed
                extensions are {self.all_extensions}
                """
            )
        self.mantidSnapper.executeQueue()
        self.setPropertyValue("OutputWorkspace", self.outputWorkspaceName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LoadGroupingDefinition)
# Puts function in simpleapi globals
algo = LoadGroupingDefinition()
algo.initialize()
_create_algorithm_function(LoadGroupingDefinition.__name__, 1, algo)
