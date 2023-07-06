import json
import math
import pathlib

from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "PixelGroupingParametersCalculationAlgorithm"


class PixelGroupingParametersCalculationAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InputState", defaultValue="", direction=Direction.Input)

        self.declareProperty("InstrumentDefinitionFile", defaultValue="", direction=Direction.Input)

        self.declareProperty("GroupingFile", defaultValue="", direction=Direction.Input)

        self.declareProperty("OutputParameters", defaultValue="", direction=Direction.Output)

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        self.log().notice("Calculate pixel grouping state-derived parameters")

        # create a grouping workspace
        grouping_ws_name = "pgpca_grouping_ws"
        self.CreateGroupingWorkspace(grouping_ws_name)

        # load an instrument into the workspace
        self.LoadSNAPInstrument(grouping_ws_name)

        # create a dummy grouped-by-detector workspace from the grouping workspace
        grouped_ws_name = "pgpca_grouped_ws"
        self.mantidSnapper.GroupDetectors(
            "Grouping detectors...",
            InputWorkspace=grouping_ws_name,
            CopyGroupingFromWorkspace=grouping_ws_name,
            OutputWorkspace=grouped_ws_name,
        )
        self.mantidSnapper.executeQueue()

        # define/calculate some auxiliary state-derived parameters
        calibrationState = Calibration.parse_raw(self.getProperty("InputState").value)
        instrumentState = calibrationState.instrumentState
        tofMin = instrumentState.particleBounds.tof.minimum
        tofMax = instrumentState.particleBounds.tof.maximum
        deltaTOverT = instrumentState.instrumentConfig.delTOverT
        delLOverL = instrumentState.instrumentConfig.delLOverL
        L = instrumentState.instrumentConfig.L1 + instrumentState.instrumentConfig.L2
        delL = delLOverL * L
        delTheta = instrumentState.instrumentConfig.delThWithGuide

        # estimate the relative resolution for all pixel groupings
        self.mantidSnapper.EstimateResolutionDiffraction(
            "Estimating diffraction resolution...",
            InputWorkspace=grouped_ws_name,
            OutputWorkspace="pgpca_resolution_ws",
            PartialResolutionWorkspaces="pgpca_resolution_partials",
            DeltaTOFOverTOF=deltaTOverT,
            SourceDeltaL=delL,
            SourceDeltaTheta=delTheta,
        )
        self.mantidSnapper.executeQueue()

        # calculate and store all pixel grouping parameters as strings
        allGroupingParams_str = []
        grouped_ws = mtd[grouped_ws_name]
        resws = mtd["pgpca_resolution_ws"]
        specInfo = grouped_ws.spectrumInfo()
        for groupIndex in range(grouped_ws.getNumberHistograms()):
            twoTheta = specInfo.twoTheta(groupIndex)
            dMin = 3.9561e-3 * (1 / (2 * math.sin(twoTheta / 2))) * tofMin / L
            dMax = 3.9561e-3 * (1 / (2 * math.sin(twoTheta / 2))) * tofMax / L
            delta_d_over_d = resws.readY(groupIndex)[0]

            groupingParams_str = PixelGroupingParameters(
                twoTheta=twoTheta, dResolution=Limit(minimum=dMin, maximum=dMax), dRelativeResolution=delta_d_over_d
            ).json()
            allGroupingParams_str.append(groupingParams_str)

        outputParams = json.dumps(allGroupingParams_str)
        self.setProperty("OutputParameters", outputParams)

    # create a grouping workspace from a grouping file
    def CreateGroupingWorkspace(self, grouping_ws_name):
        groupingFilePath = self.getProperty("GroupingFile").value
        if "lite" in groupingFilePath:
            self.mantidSnapper.CreateWorkspace(
                "Creating Instrument Definition Workspace ...", OutputWorkspace="idf", DataX=1, DataY=1
            )
            self.mantidSnapper.LoadInstrument(
                "Loading instrument definition file ...",
                Workspace="idf",
                Filename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
                MonitorList="-2--1",
                RewriteSpectraMap=False,
            )
            self.mantidSnapper.LoadDetectorsGroupingFile(
                "Loading detectors grouping file...",
                InputFile=groupingFilePath,
                InputWorkspace="idf",
                OutputWorkspace=grouping_ws_name,
            )
            self.mantidSnapper.DeleteWorkspace("Deleting idf...", Workspace="idf")
        else:
            file_extension = pathlib.Path(groupingFilePath).suffix
            if file_extension.upper()[1:] == "XML":
                self.mantidSnapper.LoadDetectorsGroupingFile(
                    "Loading detectors grouping file...",
                    InputFile=groupingFilePath,
                    InputWorkspace="idf",
                    OutputWorkspace=grouping_ws_name,
                )
            else:  # from a workspace
                self.mantidSnapper.LoadNexusProcessed(
                    "Loading grouping workspace...", Filename=groupingFilePath, OutputWorkspace=grouping_ws_name
                )
        self.mantidSnapper.executeQueue()

    # load SNAP instrument into a workspace
    def LoadSNAPInstrument(self, ws_name):
        self.log().notice("Load SNAP instrument based on the Instrument Definition File")

        # load the idf into workspace
        idf = self.getProperty("InstrumentDefinitionFile").value

        # get detector state from the input state
        calibrationState = Calibration.parse_raw(self.getProperty("InputState").value)
        detectorState = calibrationState.instrumentState.detectorState

        # add sample logs with detector "arc" and "lin" parameters to the workspace
        for param_name in ["arc", "lin"]:
            for index in range(2):
                self.mantidSnapper.AddSampleLog(
                    "Adding sample log...",
                    Workspace=ws_name,
                    LogName="det_" + param_name + str(index + 1),
                    LogText=str(getattr(detectorState, param_name)[index]),
                    LogType="Number Series",
                )
        self.mantidSnapper.executeQueue()

        self.mantidSnapper.LoadInstrument(
            "Loading instrument...", Workspace=ws_name, FileName=idf, RewriteSpectraMap="False"
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(PixelGroupingParametersCalculationAlgorithm)
