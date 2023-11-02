import pathlib
import h5py

from mantid.api import AlgorithmFactory, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import mtd

from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "SaveGroupingDefinition"


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

    def PyInit(self) -> None:
        # declare properties
        self.declareProperty(
            "GroupingFilename", defaultValue="", direction=Direction.Input, doc="Path of an input grouping file"
        )
        self.declareProperty(
            "GroupingWorkspace", defaultValue="", direction=Direction.Input, doc="Name of an input grouping workspace"
        )
        self.declareProperty("OutputFilename", defaultValue="", direction=Direction.Input, doc="Path of an output file")

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
        self.mantidSnapper = MantidSnapper(self, name)

        # define supported file name extensions
        self.supported_calib_file_extensions = ["H5", "HD5", "HDF"]
        self.supported_nexus_file_extensions = ["NXS", "NXS5"]
        self.supported_xml_file_extensions = ["XML"]

    def validateInput(self) -> None:
        # either file name or workspace name must be specified
        grouping_file_name = self.getProperty("GroupingFilename").value
        grouping_ws_name = self.getProperty("GroupingWorkspace").value
        absent_input_properties_count = sum(not prop for prop in [grouping_file_name, grouping_ws_name])
        if absent_input_properties_count == 0 or absent_input_properties_count == 2:
            raise Exception("Either GroupingFilename or GroupingWorkspace must be specified, but not both.")

        # check that the input file name has a supported extension
        if grouping_file_name != "":
            file_extension = pathlib.Path(grouping_file_name).suffix.upper()[1:]
            supported_extensions = (
                self.supported_nexus_file_extensions
                + self.supported_xml_file_extensions
                + self.supported_calib_file_extensions
            )
            if file_extension not in supported_extensions:
                raise Exception(f"GroupingFilename has an unsupported file name extension {file_extension}")

        # check that the output file name has a supported extension
        outputFilename = self.getProperty("OutputFilename").value
        file_extension = pathlib.Path(outputFilename).suffix.upper()[1:]
        if file_extension not in self.supported_calib_file_extensions:
            raise Exception(f"OutputFilename has an unsupported file name extension {file_extension}")

        instrumentName = bool(self.getProperty("InstrumentName").value)
        instrumentFileName = bool(self.getProperty("InstrumentFileName").value)
        instrumentDonor = bool(self.getProperty("InstrumentDonor").value)
        instrumentPropertiesCount = sum(prop for prop in [instrumentName, instrumentFileName, instrumentDonor])
        if instrumentPropertiesCount == 0 or instrumentPropertiesCount > 1:
            raise Exception(
                "Either InstrumentName, InstrumentFileName, or InstrumentDonor must be specified, but not all three."
            )

    def PyExec(self) -> None:
        self.validateInput()

        grouping_file_name = self.getProperty("GroupingFilename").value
        if grouping_file_name != "":  # create grouping workspace from file
            grouping_ws_name = "gr_ws_name"
            self.mantidSnapper.LoadGroupingDefinition(
                "Loading grouping definition...",
                GroupingFilename=grouping_file_name,
                OutputWorkspace=grouping_ws_name,
                InstrumentFileName=self.getProperty("InstrumentFileName").value,
                InstrumentName=self.getProperty("InstrumentName").value,
                InstrumentDonor=self.getProperty("InstrumentDonor").value,
            )
            self.mantidSnapper.executeQueue()
        else:  # retrieve grouping workspace from analysis data service
            grouping_ws_name = self.getProperty("GroupingWorkspace").value
        self.grouping_ws = mtd[grouping_ws_name]

        outputFilename = self.getProperty("OutputFilename").value
        groupIDs = self.grouping_ws.extractY()[:,0]
        nothing = [0] * len(groupIDs) #np.zeros_like(detIDs)
        instrument = self.grouping_ws.getInstrument()
        detIDs = []
        for groupID in self.grouping_ws.getGroupIDs():
            detIDs.extend(self.grouping_ws.getDetectorIDsOfGroup(int(groupID)))

        with h5py.File(outputFilename, 'w') as f:
            f.create_dataset("calibration/group", data=groupIDs)
            f.create_dataset("calibration/detid", data=detIDs)
            f.create_dataset("calibration/difa", data=nothing)
            f.create_dataset("calibration/difc", data=nothing)
            f.create_dataset("calibration/instrument/name", data=instrument.getName())
            # f.create_dataset("calibration/instrument/instrument_source", data=grouping_file_name) # TODO
            f.create_dataset("calibration/zero", data=nothing)
            f.create_dataset("calibration/use", data=nothing)

        if grouping_file_name != "":
            self.mantidSnapper.WashDishes(
                f"Cleanup the grouping workspace {grouping_ws_name}",
                Workspace=grouping_ws_name,
            )
        self.mantidSnapper.executeQueue()



# Register algorithm with Mantid
AlgorithmFactory.subscribe(SaveGroupingDefinition)
