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
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
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
        return

    def chopIngredients(self, instrumentState: InstrumentState) -> None:
        # define/calculate some auxiliary state-derived parameters
        self.tofMin = instrumentState.particleBounds.tof.minimum
        self.tofMax = instrumentState.particleBounds.tof.maximum
        self.deltaTOverT = instrumentState.instrumentConfig.delTOverT
        self.delLOverL = instrumentState.instrumentConfig.delLOverL
        self.L = instrumentState.instrumentConfig.L1 + instrumentState.instrumentConfig.L2
        self.delL = self.delLOverL * self.L
        self.delTheta = instrumentState.instrumentConfig.delThWithGuide
        self.grouping_ws_name = "pgpca_grouping_ws"
        self.grouped_ws_name = "pgpca_grouped_ws"
        self.resolution_ws_name = "pgpca_resolution_ws"
        self.partial_resolution_group_ws_name = "pgpca_partial_resolution_group_ws"
        return

    def retrieveFromPantry(self):
        # load grouping definition into a workspace
        self.mantidSnapper.LoadGroupingDefinition(
            "Loading grouping definition...",
            GroupingFilename=self.getProperty("GroupingFile").value,
            InstrumentFilename=self.getProperty("InstrumentDefinitionFile").value,
            OutputWorkspace=self.grouping_ws_name,
        )
        self.mantidSnapper.executeQueue()
        self.LoadSNAPInstrument(self.grouping_ws_name)

    def PyExec(self):
        self.log().notice("Calculate pixel grouping state-derived parameters")

        # define/calculate some auxiliary state-derived parameters
        self.chopIngredients(InstrumentState.parse_raw(self.getProperty("InstrumentState").value))

        # create a grouping workspace and load an instrument into the workspace
        self.retrieveFromPantry()

        # create a dummy grouped-by-detector workspace from the grouping workspace
        self.mantidSnapper.GroupDetectors(
            "Grouping detectors...",
            InputWorkspace=self.grouping_ws_name,
            CopyGroupingFromWorkspace=self.grouping_ws_name,
            OutputWorkspace=self.grouped_ws_name,
        )
        self.mantidSnapper.executeQueue()

        # estimate the relative resolution for all pixel groupings
        self.mantidSnapper.EstimateResolutionDiffraction(
            "Estimating diffraction resolution...",
            InputWorkspace=self.grouped_ws_name,
            OutputWorkspace=self.resolution_ws_name,
            PartialResolutionWorkspaces=self.partial_resolution_group_ws_name,
            DeltaTOFOverTOF=self.deltaTOverT,
            SourceDeltaL=self.delL,
            SourceDeltaTheta=self.delTheta,
        )
        self.mantidSnapper.executeQueue()

        # calculate parameters for all pixel groupings and store them in json format
        allGroupingParams_json = []
        grouping_ws = self.mantidSnapper.mtd[self.grouping_ws_name]

        resws = mtd[self.resolution_ws_name]

        grouping_detInfo = grouping_ws.detectorInfo()
        groupIDs = grouping_ws.getGroupIDs()
        grouping_detInfo = grouping_ws.detectorInfo()
        for groupIndex, groupID in enumerate(groupIDs):
            detIDsInGroup = grouping_ws.getDetectorIDsOfGroup(int(groupID))

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

            dMin = 3.9561e-3 * (1 / (2 * math.sin(groupMax2Theta / 2))) * self.tofMin / self.L
            dMax = 3.9561e-3 * (1 / (2 * math.sin(groupMin2Theta / 2))) * self.tofMax / self.L
            delta_d_over_d = resws.readY(groupIndex)[0]

            groupingParams_json = PixelGroupingParameters(
                groupID=groupID,
                twoTheta=groupAverage2Theta,
                dResolution=Limit(minimum=dMin, maximum=dMax),
                dRelativeResolution=delta_d_over_d,
            ).json()
            allGroupingParams_json.append(groupingParams_json)

        outputParams = json.dumps(allGroupingParams_json)
        self.setProperty("OutputParameters", outputParams)
        self.mantidSnapper.DeleteWorkspace("Cleaning up grouping workspace.", Workspace=self.grouping_ws_name)
        self.mantidSnapper.DeleteWorkspace("Cleaning up grouped workspace.", Workspace=self.grouped_ws_name)
        self.mantidSnapper.DeleteWorkspace("Cleaning up resolution workspace.", Workspace=self.resolution_ws_name)
        self.mantidSnapper.DeleteWorkspace(
            "Cleaning up partial resolution group workspace.", Workspace=self.partial_resolution_group_ws_name
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
