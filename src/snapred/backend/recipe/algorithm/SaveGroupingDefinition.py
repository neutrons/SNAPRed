import os
import pathlib
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class SaveGroupingDefinition(PythonAlgorithm):
    """
    This algorithm takes in a grouping definition (file or object) and saves it in a diffraction calibration file format
    inputs:

        GroupingFilename: str -- path of an input grouping file (in NEXUS or XML format)
        GroupingWorkspace: str -- name of an input grouping workspace
        Note, either GroupingFilename or GroupingWorkspace must be specified, but not both.

    output:

        OutputFilename: str -- path of an output file. Supported file name extensions: "h5", "hd5", "hdf".
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

        # declare properties
        self.declareProperty(
            FileProperty(
                "GroupingFilename",
                defaultValue="",
                action=FileAction.OptionalLoad,
                direction=Direction.Input,
                extensions=self.all_extensions,
            ),
            doc="Path of an input grouping file",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Optional),
            doc="Name of an input grouping workspace.",
        )
        self.declareProperty(
            FileProperty(
                "OutputFilename",
                defaultValue="",
                action=FileAction.Save,
                direction=Direction.Input,
                extensions=self.supported_calib_file_extensions,
            ),
            doc="Path of an output file",
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

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        # must have an output file defined
        if self.getProperty("OutputFilename").isDefault:
            errors["OutputFilename"] = "Output file MUST be specified to save grouping definition"
            return errors

        # either file name or workspace name must be specified
        grouping_sources = ["GroupingFilename", "GroupingWorkspace"]
        specifiedGrouping = [x for x in grouping_sources if not self.getProperty(x).isDefault]
        if len(specifiedGrouping) == 0:
            msg = "Either GroupingFilename or GroupingWorkspace MUST be specified"
            errors["GroupingFilename"] = msg
            errors["GroupingWorkspace"] = msg
            return errors
        elif len(specifiedGrouping) == 2:
            msg = "May specify ONE of GroupingFilename or GroupingWorkspace, but not both."
            errors["GroupingFilename"] = msg
            errors["GroupingWorkspace"] = msg
            return errors

        # check that the input filename has a supported extension
        if not self.getProperty("GroupingFilename").isDefault:
            groupingFilename = self.getPropertyValue("GroupingFilename")
            if not os.path.exists(groupingFilename):
                errors["GroupingFilename"] = f"GroupingFilename {groupingFilename} does not exist in"
                return errors
            groupingFileExt = pathlib.Path(groupingFilename).suffix[1:].upper()
            if groupingFileExt not in self.all_extensions:
                errors["GroupingFilename"] = (
                    f"GroupingFilename has an unsupported file name extension {groupingFileExt}"
                )

        # check that the output file name has a supported extension
        outputFilename = self.getPropertyValue("OutputFilename")
        outputFileExt = pathlib.Path(outputFilename).suffix[1:].upper()
        if outputFileExt not in self.supported_calib_file_extensions:
            errors["OutputFilename"] = f"OutputFilename has an unsupported file name extension {outputFileExt}"

        # check that exactly one instrument source was specified
        instrumentSources = ["InstrumentDonor", "InstrumentName", "InstrumentFilename"]
        specifiedSources = [s for s in instrumentSources if not self.getProperty(s).isDefault]
        if len(specifiedSources) > 1:
            msg = "You can only declare ONE way to get an instrument.  " + f"You specified {specifiedSources}"
            errors.update({i: msg for i in specifiedSources})
        elif len(specifiedSources) == 0 and not self.getProperty("GroupingFilename").isDefault:
            msg = f"To load grouping file, you MUST specify an instrument source from {instrumentSources}"
            errors.update({i: msg for i in instrumentSources})

        # return results of checks
        return errors

    def chopIngredients(self):
        pass

    def unbagGroceries(self):
        pass

    def PyExec(self) -> None:
        if self.getProperty("GroupingWorkspace").isDefault:  # create grouping workspace from file
            grouping_ws_name = "gr_ws_name"
            self.mantidSnapper.LoadGroupingDefinition(
                "Loading grouping definition...",
                GroupingFilename=self.getPropertyValue("GroupingFilename"),
                OutputWorkspace=grouping_ws_name,
                InstrumentFileName=self.getPropertyValue("InstrumentFileName"),
                InstrumentName=self.getPropertyValue("InstrumentName"),
                InstrumentDonor=self.getPropertyValue("InstrumentDonor"),
            )
        else:  # retrieve grouping workspace from analysis data service
            grouping_ws_name = self.getPropertyValue("GroupingWorkspace")

        outputFilename = self.getPropertyValue("OutputFilename")

        self.mantidSnapper.SaveDiffCal(
            f"Saving grouping workspace to {outputFilename}",
            GroupingWorkspace=grouping_ws_name,
            Filename=outputFilename,
        )
        if not self.getProperty("GroupingFilename").isDefault:
            self.mantidSnapper.WashDishes(
                f"Cleanup the grouping workspace {grouping_ws_name}",
                Workspace=grouping_ws_name,
            )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(SaveGroupingDefinition)
