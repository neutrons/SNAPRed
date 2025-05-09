import math

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.dataobjects import MaskWorkspaceProperty
from mantid.kernel import Direction, PhysicalConstants
from mantid.simpleapi import mtd

from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


class PixelGroupingParametersCalculation(PythonAlgorithm):
    # conversion factor from microsecond/Angstrom to meters

    @classproperty
    def CONVERSION_FACTOR(cls):
        return Config["constants.m2cm"] * PhysicalConstants.h / PhysicalConstants.NeutronMass

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
            doc="The grouping workspace defining this grouping scheme,\n"
            + "with instrument-location parameters initialized according to the run number",
        )
        self.declareProperty(
            MaskWorkspaceProperty("MaskWorkspace", "", Direction.Input, PropertyMode.Optional),
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

    def unbagGroceries(self, ingredients: PixelGroupingIngredients):  # noqa: ARG002
        self.groupingWorkspaceName: str = self.getPropertyValue("GroupingWorkspace")
        self.maskWorkspaceName: str = self.getPropertyValue("MaskWorkspace")
        self.resolutionWorkspaceName: str = "pgp_resolution"  # TODO use WNG
        self.partialResolutionWorkspaceName: str = self.resolutionWorkspaceName + "_partial"

    def PyExec(self):
        self.log().notice("Calculate pixel grouping state-derived parameters")

        lowdSpacingCrop = Config["constants.CropFactors.lowdSpacingCrop"]
        highdSpacingCrop = Config["constants.CropFactors.highdSpacingCrop"]

        if lowdSpacingCrop < 0:
            raise ValueError("Low d-spacing crop factor must be positive")
        if highdSpacingCrop < 0:
            raise ValueError("High d-spacing crop factor must be positive")

        if (lowdSpacingCrop > 100) or (highdSpacingCrop > 100):
            raise ValueError("d-spacing crop factors are too large")

        # define/calculate some auxiliary state-derived parameters
        ingredients = PixelGroupingIngredients.model_validate_json(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.unbagGroceries(ingredients)

        # create a copy of the grouping workspace with the correct detector mask flags
        tmpGroupingWSName = mtd.unique_hidden_name()
        self.mantidSnapper.CloneWorkspace(
            "Cloning grouping workspace",
            InputWorkspace=self.groupingWorkspaceName,
            OutputWorkspace=tmpGroupingWSName,
        )

        # If the optional mask workspace is present, apply it to the grouping workspace:
        if self.maskWorkspaceName:
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

            groupMeanL2 = 0.0

            groupMin2Theta = 2.0 * np.pi
            groupMax2Theta = 0.0
            groupMean2Theta = 0.0

            groupMeanPhi = 0.0

            pixelCount = 0
            normalizationFactor = 0.0
            for detID in detIDs:
                detIndex = detectorInfo.indexOf(int(detID))
                if detectorInfo.isMonitor(int(detIndex)) or detectorInfo.isMasked(int(detIndex)):
                    continue

                twoTheta = detectorInfo.twoTheta(int(detIndex))
                solidAngleFactor = np.sin(twoTheta / 2.0)
                groupMin2Theta = min(groupMin2Theta, twoTheta)
                groupMax2Theta = max(groupMax2Theta, twoTheta)

                twoTheta *= solidAngleFactor
                groupMean2Theta += twoTheta

                try:
                    phi = detectorInfo.azimuthal(int(detIndex))
                    phi *= solidAngleFactor
                    groupMeanPhi += phi
                except RuntimeError as e:
                    # Also entered as defect EWM#5073:
                    # `DetectorInfo.azimuthal()` has issues in calculating ambiguous azimuth values:
                    #   * by convention, these values can be set to zero, without overly affecting the mean value.
                    if "Failed to create up axis orthogonal to the beam direction" not in str(e):
                        raise
                    logger.debug(e)

                L2 = detectorInfo.l2(int(detIndex))
                L2 *= solidAngleFactor
                groupMeanL2 += L2

                normalizationFactor += solidAngleFactor
                pixelCount += 1

            if pixelCount > 0:
                if normalizationFactor > np.finfo(float).eps:
                    groupMeanL2 /= normalizationFactor
                    groupMean2Theta /= normalizationFactor
                    groupMeanPhi /= normalizationFactor
                else:
                    # special case: all on-axis pixels
                    groupMeanL2 = 0.0
                    groupMean2Theta = 0.0
                    groupMeanPhi = 0.0

                dMin = (
                    (self.CONVERSION_FACTOR * (1.0 / (2.0 * math.sin(groupMax2Theta / 2.0))) * self.tofMin / self.L)
                    + Config["constants.CropFactors.lowdSpacingCrop"]
                    if groupMax2Theta > np.finfo(float).eps
                    else 0.0
                )
                dMax = (
                    (self.CONVERSION_FACTOR * (1.0 / (2.0 * math.sin(groupMin2Theta / 2.0))) * self.tofMax / self.L)
                    - Config["constants.CropFactors.highdSpacingCrop"]
                    if groupMin2Theta > np.finfo(float).eps
                    else 0.0
                )

                delta_d_over_d = resolutionWS.readY(groupIndex)[0]

                allGroupingParams.append(
                    PixelGroupingParameters(
                        groupID=groupID,
                        L2=groupMeanL2,
                        twoTheta=groupMean2Theta,
                        azimuth=groupMeanPhi,
                        dResolution=Limit(minimum=dMin, maximum=dMax),
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
AlgorithmFactory.subscribe(PixelGroupingParametersCalculation)
