import json
import math
import pathlib
from statistics import mean

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction

from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "PixelGroupingParametersCalculationAlgorithm"


class PixelGroupingParametersCalculationAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)

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
        instrumentState = InstrumentState.parse_raw(self.getProperty("InstrumentState").value)
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

        # calculate parameters for all pixel groupings and store them in json format
        allGroupingParams_json = []
        grouping_ws = self.mantidSnapper.mtd[grouping_ws_name]
        grouped_ws = self.mantidSnapper.mtd[grouped_ws_name]
        resws = mtd["pgpca_resolution_ws"]

        grouping_detInfo = grouping_ws.detectorInfo()
        for groupIndex in range(grouped_ws.getNumberHistograms()):
            detIDsInGroup = grouping_ws.getDetectorIDsOfGroup(groupIndex + 1)

            groupMin2Theta = 2 * np.pi
            groupMax2Theta = 0.0
            groupAverage2Theta = 0.0
            count = 0
            for detID in detIDsInGroup:
                if grouping_detInfo.isMonitor(int(detID)) or grouping_detInfo.isMasked(int(detID)):
                    continue
                count += 1
                twoThetaTemp = grouping_detInfo.twoTheta(int(detID))
                groupMin2Theta = min(groupMin2Theta, twoThetaTemp)
                groupMax2Theta = max(groupMax2Theta, twoThetaTemp)
                groupAverage2Theta += twoThetaTemp
            if count > 0:
                groupAverage2Theta /= count
            del twoThetaTemp

            dMin = 3.9561e-3 * (1 / (2 * math.sin(groupMax2Theta / 2))) * tofMin / L
            dMax = 3.9561e-3 * (1 / (2 * math.sin(groupMin2Theta / 2))) * tofMax / L
            delta_d_over_d = resws.readY(groupIndex)[0]

            groupingParams_json = PixelGroupingParameters(
                twoTheta=groupAverage2Theta,
                dResolution=Limit(minimum=dMin, maximum=dMax),
                dRelativeResolution=delta_d_over_d,
            ).json()
            allGroupingParams_json.append(groupingParams_json)

        outputParams = json.dumps(allGroupingParams_json)
        self.setProperty("OutputParameters", outputParams)

    # create a grouping workspace from a grouping file
    def CreateGroupingWorkspace(self, grouping_ws_name):
        groupingFilePath = self.getProperty("GroupingFile").value
        file_extension = pathlib.Path(groupingFilePath).suffix
        if file_extension.upper()[1:] == "XML":
            self.mantidSnapper.LoadDetectorsGroupingFile(
                "Loading detectors grouping file...",
                InputFile=groupingFilePath,
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

        # get detector state from the input state
        instrumentState = InstrumentState.parse_raw(self.getProperty("InstrumentState").value)
        detectorState = instrumentState.detectorState

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

        # load the idf into workspace
        idf = self.getProperty("InstrumentDefinitionFile").value
        self.mantidSnapper.LoadInstrument(
            "Loading instrument...", Workspace=ws_name, FileName=idf, RewriteSpectraMap=False
        )

        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(PixelGroupingParametersCalculationAlgorithm)
