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
from mantid.simpleapi import mtd
from mantid.dataobjects import MaskWorkspaceProperty
from mantid.kernel import Direction, PhysicalConstants

from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.algorithm.MaskDetectorFlags import MaskDetectorFlags
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
            doc="The grouping workspace defining this grouping scheme,\n" + 
                "with instrument-location parameters initialized according to the run number",
        )
        self.declareProperty(
            MaskWorkspaceProperty("MaskWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="The mask workspace for a specified calibration run number and version",
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

    def unbagGroceries(self, ingredients: PixelGroupingIngredients): # noqa: ARG002
        self.groupingWorkspaceName: str = self.getPropertyValue("GroupingWorkspace")
        self.maskWorkspaceName: str = self.getPropertyValue("MaskWorkspace")
        self.resolutionWorkspaceName: str = "pgp_resolution"  # TODO use WNG
        self.partialResolutionWorkspaceName: str = self.resolutionWorkspaceName + "_partial"

    def PyExec(self):
        self.log().notice("Calculate pixel grouping state-derived parameters")

        # define/calculate some auxiliary state-derived parameters
        ingredients = PixelGroupingIngredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.unbagGroceries(ingredients)

        # create a copy of the grouping workspace with the correct detector mask flags
        tmpGroupingWSName = mtd.unique_hidden_name()
        self.mantidSnapper.CloneWorkspace(
            "Cloning grouping workspace",
            InputWorkspace=self.groupingWorkspaceName,
            OutputWorkspace=tmpGroupingWSName,
        )
        self.mantidSnapper.MaskDetectorFlags(
            "Setting grouping workspace mask flags",
            MaskWorkspace=self.maskWorkspaceName,
            OutputWorkspace=tmpGroupingWSName,
        )
        self.mantidSnapper.executeQueue()
        
        # Create a grouped-by-detector workspace from the grouping workspace
        # and estimate the relative resolution for all pixel groupings.
        # These algorithms use detector mask-flag information from the 'InputWorkspace'.

        self.mantidSnapper.GroupDetectors(
            "Grouping detectors...",
            InputWorkspace=tmpGroupingWSName,
            CopyGroupingFromWorkspace=tmpGroupingWSName,
            OutputWorkspace=self.resolutionWorkspaceName,
        )
        
        #  => resolution will be _zero_ for any fully-masked pixel group
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
        groupingWS = self.mantidSnapper.mtd[tmpGroupingWSName]
        resolutionWS = self.mantidSnapper.mtd[self.resolutionWorkspaceName]

        groupIDs = groupingWS.getGroupIDs()
        detectorInfo = groupingWS.detectorInfo()
        for groupIndex, groupID in enumerate(groupIDs):
            detIDs = groupingWS.getDetectorIDsOfGroup(int(groupID))

            groupMin2Theta = 2.0 * np.pi
            groupMax2Theta = 0.0
            groupAverage2Theta = 0.0
            pixelCount = 0
            for detID in detIDs:
                detIndex = detectorInfo.indexOf(int(detID))
                if detectorInfo.isMonitor(int(detIndex)) or detectorInfo.isMasked(int(detIndex)):
                    continue
                pixelCount += 1
                twoThetaTemp = detectorInfo.twoTheta(int(detIndex))
                groupMin2Theta = min(groupMin2Theta, twoThetaTemp)
                groupMax2Theta = max(groupMax2Theta, twoThetaTemp)
                groupAverage2Theta += twoThetaTemp
            if pixelCount > 0:
                groupAverage2Theta /= pixelCount

                dMin = self.CONVERSION_FACTOR * (1.0 / (2.0 * math.sin(groupMax2Theta / 2.0))) * self.tofMin / self.L
                dMax = self.CONVERSION_FACTOR * (1.0 / (2.0 * math.sin(groupMin2Theta / 2.0))) * self.tofMax / self.L
                delta_d_over_d = resolutionWS.readY(groupIndex)[0]

                allGroupingParams.append(
                    PixelGroupingParameters(
                        groupID=groupID,
                        isMasked=False,
                        twoTheta=groupAverage2Theta,
                        dResolution=Limit(minimum=dMin, maximum=dMax),
                        dRelativeResolution=delta_d_over_d,
                    )
                )
            else:
                # Construct a `PixelGroupingParameters` instance corresponding to a fully-masked group:                
                #
                #   * The cleanest approach here would have been to use `None` as the `PixelGroupingParameters` for a
                #     fully-masked group; however, this breaks too many things later in the code.
                #
                #   * To avoid out-of-range positions, values from the first detector in the group are used:
                #     consuming methods need to check either the `PixelGroupingParameters.isMasked` flag,
                #     or equivalently, test for an _empty_ `dResolution` `Limit` domain.
                #     
                detID = detIDs[0]
                detIndex = detectorInfo.indexOf(int(detID))
                twoTheta = detectorInfo.twoTheta(int(detIndex))
                dMin = self.CONVERSION_FACTOR * (1.0 / (2.0 * math.sin(twoTheta / 2.0))) * self.tofMin / self.L
                delta_d_over_d = resolutionWS.readY(groupIndex)[0]
                allGroupingParams.append(
                    PixelGroupingParameters(
                        groupID=groupID,
                        
                        # Fully-masked group
                        isMasked=True,
                        
                        twoTheta=twoTheta,
                        
                        # Empty limit domain
                        dResolution=Limit(minimum=dMin, maximum=dMin),
                        
                        # Resolution value for fully-masked group (as set by `EstimateResolutionDiffraction`):
                        #   -- depending on end use, it may be necessary to modify this value. 
                        dRelativeResolution=delta_d_over_d,
                    )
                )
                
                
        self.setProperty("OutputParameters", list_to_raw(allGroupingParams))
        self.mantidSnapper.WashDishes(
            "Cleaning up workspaces",
            WorkspaceList=[self.resolutionWorkspaceName, tmpGroupingWSName],
        )
        self.mantidSnapper.executeQueue()

# Register algorithm with Mantid
AlgorithmFactory.subscribe(PixelGroupingParametersCalculationAlgorithm)
