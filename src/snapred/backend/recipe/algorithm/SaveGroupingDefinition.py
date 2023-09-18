import pathlib

from mantid.api import AlgorithmFactory, PythonAlgorithm
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
            if file_extension not in self.supported_nexus_file_extensions + self.supported_xml_file_extensions:
                raise Exception(f"GroupingFilename has an unsupported file name extension {file_extension}")

        # check that the output file name has a supported extension
        outputFilename = self.getProperty("OutputFilename").value
        file_extension = pathlib.Path(outputFilename).suffix.upper()[1:]
        if file_extension not in self.supported_calib_file_extensions:
            raise Exception(f"OutputFilename has an unsupported file name extension {file_extension}")

    def PyExec(self) -> None:
        self.validateInput()

        grouping_file_name = self.getProperty("GroupingFilename").value
        if grouping_file_name != "":  # create grouping workspace from file
            grouping_ws_name = "gr_ws_name"
            self.mantidSnapper.LoadGroupingDefinition(
                "Loading grouping definition...", GroupingFilename=grouping_file_name, OutputWorkspace=grouping_ws_name
            )
            self.mantidSnapper.executeQueue()
        else:  # retrieve grouping workspace from analysis data service
            grouping_ws_name = self.getProperty("GroupingWorkspace").value
        self.grouping_ws = mtd[grouping_ws_name]

        # To save the grouping workspace using Mantid SaveDiffCal algorithm
        # we need to supply it with a calibration workspace
        cal_ws_name = "cal_ws"
        self.CreateZeroCalibrationWorkspace(cal_ws_name)

        outputFilename = self.getProperty("OutputFilename").value
        self.mantidSnapper.SaveDiffCal(
            f"Saving grouping workspace to {outputFilename}",
            CalibrationWorkspace=cal_ws_name,
            GroupingWorkspace=grouping_ws_name,
            Filename=outputFilename,
        )
        self.mantidSnapper.executeQueue()

    def CreateZeroCalibrationWorkspace(self, cal_ws_name) -> None:
        self.mantidSnapper.CreateEmptyTableWorkspace(
            "Creating empty table workspace...",
            OutputWorkspace=cal_ws_name,
        )
        self.mantidSnapper.executeQueue()
        cal_ws = mtd[cal_ws_name]

        cal_ws.addColumn(type="int", name="detid", plottype=6)
        cal_ws.addColumn(type="float", name="difc", plottype=6)
        cal_ws.addColumn(type="float", name="difa", plottype=6)
        cal_ws.addColumn(type="float", name="tzero", plottype=6)
        cal_ws.addColumn(type="float", name="tofmin", plottype=6)

        groupIDs = self.grouping_ws.getGroupIDs()
        for groupID in groupIDs:
            detIDs = self.grouping_ws.getDetectorIDsOfGroup(int(groupID))
            for detID in detIDs:
                nextRow = {"detid": int(detID), "difc": 0, "difa": 0, "tzero": 0, "tofmin": 0}
                cal_ws.addRow(nextRow)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(SaveGroupingDefinition)
