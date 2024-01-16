import json
import math
import pathlib
from statistics import mean

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction, PhysicalConstants

from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.redantic import list_to_raw


class PixelGroupingParametersCalculationAlgorithm(PythonAlgorithm):
    # conversion factor from microsecond/Angstrom to meters
    CONVERSION_FACTOR = Config["constants.m2cm"] * PhysicalConstants.h / PhysicalConstants.NeutronMass

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            "Ingredients",
            defaultValue="",
            direction=Direction.Input,
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="The grouping workspace defining this grouping scheme",
        )
        self.declareProperty(
            "OutputParameters",
            defaultValue="",
            direction=Direction.Output,
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)
        return

    def chopIngredients(self, ingredients: PixelGroupingIngredients) -> None:
        # define/calculate some auxiliary state-derived parameters
        self.tofMin = ingredients.instrumentState.particleBounds.tof.minimum
        self.tofMax = ingredients.instrumentState.particleBounds.tof.maximum
        self.deltaTOverT = ingredients.instrumentState.instrumentConfig.delTOverT
        self.delLOverL = ingredients.instrumentState.instrumentConfig.delLOverL
        self.L = ingredients.instrumentState.instrumentConfig.L1 + ingredients.instrumentState.instrumentConfig.L2
        self.delL = self.delLOverL * self.L
        self.delTheta = ingredients.instrumentState.delTh
        return

    def unbagGroceries(self, ingredients: PixelGroupingIngredients):
        self.groupingWorkspaceName: str = self.getPropertyValue("GroupingWorkspace")
        self.resolutionWorkspaceName: str = "pgp_resolution"  # TODO use WNG
        self.partialResolutionWorkspaceName: str = self.resolutionWorkspaceName + "_partial"
        self.loadNeededLogs(self.groupingWorkspaceName, ingredients)

    def PyExec(self):
        self.log().notice("Calculate pixel grouping state-derived parameters")

        # define/calculate some auxiliary state-derived parameters
        ingredients = PixelGroupingIngredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.unbagGroceries(ingredients)

        # create a dummy grouped-by-detector workspace from the grouping workspace
        self.mantidSnapper.GroupDetectors(
            "Grouping detectors...",
            InputWorkspace=self.groupingWorkspaceName,
            CopyGroupingFromWorkspace=self.groupingWorkspaceName,
            OutputWorkspace=self.resolutionWorkspaceName,
        )

        # estimate the relative resolution for all pixel groupings
        self.mantidSnapper.EstimateResolutionDiffraction(
            "Estimating diffraction resolution...",
            InputWorkspace=self.resolutionWorkspaceName,
            OutputWorkspace=self.resolutionWorkspaceName,
            PartialResolutionWorkspaces=self.partialResolutionWorkspaceName,
            DeltaTOFOverTOF=self.deltaTOverT,
            SourceDeltaL=self.delL,
            SourceDeltaTheta=self.delTheta,
        )
        self.mantidSnapper.WashDishes(
            "Remove the partial resolution workspace",
            Workspace=self.partialResolutionWorkspaceName,
        )
        self.mantidSnapper.executeQueue()

        # calculate parameters for all pixel groupings and store them in json format
        allGroupingParams = []
        groupingWS = self.mantidSnapper.mtd[self.groupingWorkspaceName]
        resolutionWS = self.mantidSnapper.mtd[self.resolutionWorkspaceName]

        groupIDs = groupingWS.getGroupIDs()
        grouping_detInfo = groupingWS.detectorInfo()
        for groupIndex, groupID in enumerate(groupIDs):
            detIDsInGroup = groupingWS.getDetectorIDsOfGroup(int(groupID))

            groupMin2Theta = 2 * np.pi
            groupMax2Theta = 0.0
            groupAverage2Theta = 0.0
            count = 0
            for detID in detIDsInGroup:
                detIndex = grouping_detInfo.indexOf(int(detID))
                if grouping_detInfo.isMonitor(int(detIndex)) or grouping_detInfo.isMasked(int(detIndex)):
                    continue
                count += 1
                twoThetaTemp = grouping_detInfo.twoTheta(int(detIndex))
                groupMin2Theta = min(groupMin2Theta, twoThetaTemp)
                groupMax2Theta = max(groupMax2Theta, twoThetaTemp)
                groupAverage2Theta += twoThetaTemp
            if count > 0:
                groupAverage2Theta /= count
            del twoThetaTemp

            dMin = self.CONVERSION_FACTOR * (1 / (2 * math.sin(groupMax2Theta / 2))) * self.tofMin / self.L
            dMax = self.CONVERSION_FACTOR * (1 / (2 * math.sin(groupMin2Theta / 2))) * self.tofMax / self.L
            delta_d_over_d = resolutionWS.readY(groupIndex)[0]

            allGroupingParams.append(
                PixelGroupingParameters(
                    groupID=groupID,
                    twoTheta=groupAverage2Theta,
                    dResolution=Limit(minimum=dMin, maximum=dMax),
                    dRelativeResolution=delta_d_over_d,
                )
            )

        self.setProperty("OutputParameters", list_to_raw(allGroupingParams))
        self.mantidSnapper.WashDishes(
            "Cleaning up resolution workspaces...",
            Workspace=self.resolutionWorkspaceName,
        )
        self.mantidSnapper.executeQueue()

    # load SNAP instrument into a workspace
    def loadNeededLogs(self, ws_name: str, ingredients: PixelGroupingIngredients):
        self.log().notice("Add necessary logs (det_arc, det_lin) to calculate resolution")

        # get detector state from the input state
        detectorState = ingredients.instrumentState.detectorState

        # add sample logs with detector "arc" and "lin" parameters to the workspace
        # NOTE after adding the logs, it is necessary to update the instrument to
        #  factor in these new parameters, or else calculations will be inconsistent.
        #  This is done with a call to `ws->populateInstrumentParameters()` from within mantid.
        #  This call only needs to happen with the last log
        logsAdded = 0
        for param_name in ["arc", "lin"]:
            for index in range(2):
                self.mantidSnapper.AddSampleLog(
                    "Adding sample log...",
                    Workspace=ws_name,
                    LogName="det_" + param_name + str(index + 1),
                    LogText=str(getattr(detectorState, param_name)[index]),
                    LogType="Number Series",
                    UpdateInstrumentParameters=(logsAdded >= 3),
                )
                logsAdded += 1
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(PixelGroupingParametersCalculationAlgorithm)
