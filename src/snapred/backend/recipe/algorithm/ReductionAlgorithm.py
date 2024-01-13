import json

from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import *
from mantid.kernel import Direction
from mantid.simpleapi import mtd

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


#######################################################
# ATTENTION: Could be replaced by alignAndFocusPowder #
# please confirm that attenutation correction before  #
# and after is equivalent                             #
#######################################################
class ReductionAlgorithm(PythonAlgorithm):
    def category(self):
        return "SNAPRed Reduction"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Input neutron data to be reduced.",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("VanadiumWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="The raw vanadium correction workspace.",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("MaskWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="A masking workspace loading from the same calibration table.",
        )
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationTable", "", Direction.Output, PropertyMode.Mandatory),
            doc="The Calibration table holding DIFC values",
        )
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="The reduced data workspace.",
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: ReductionIngredients):
        self.focusGroups = ingredients.reductionState.stateConfig.focusGroups
        self.groupIDs = ingredients.pixelGroup.groupIDs
        self.dMin = ingredients.pixelGroup.dMin()
        self.dMax = ingredients.pixelGroup.dMax()
        self.dBin = ingredients.pixelGroup.dBin()

    def unbagGroceries(self):
        self.inputWorkspace = self.getPropertyValue("InputWorkspace")
        self.outputWorkspace = self.getPropertyValue("OutputWorkspace")
        self.vanadiumWorkspace = self.getPropertyValue("VanadiumWorkspace")
        self.tmpVanadium = self.vanadiumWorkspace + "_tmp"
        self.mantidSnapper.CloneWorkspace(
            "Setup output workspace",
            InputWorkspace=self.inputWorkspace,
            OutputWorkspace=self.outputWorkspace,
        )
        self.mantidSnapper.CloneWorkspace(
            "Setup temporary vanadium workspace",
            InputWorkspace=self.vanadiumWorkspace,
            OutputWorkspace=self.tmpVanadium,
        )
        self.maskWorkspace = self.getPropertyValue("MaskWorkspace")
        self.DIFC = self.getPropertyValue("CalibrationTable")
        self.mantidSnapper.executeQueue()

    def PyExec(self):
        ingredients = ReductionIngredients.parse_raw(self.getProperty("ReductionIngredients").value)
        self.chopIngredients(ingredients)
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")

        # 7 Does it have a container?
        # Apply Container Mask to Raw Vanadium and Data output from SumNeighbours
        #     -- done to both data and vanadium
        # self.applyContainerMask()

        # 8 CreateGroupWorkspace
        # TODO: Assess performance, use alternative Andrei came up with that is faster

        groupingworkspace = self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=ingredients.reductionState.stateConfig.json(),
            InputWorkspace="vanadium",
            OutputWorkSpace="CommonRed",
        )

        # 3 ApplyDiffCal  -- just apply to data
        diffCalPrefix = "diffcal"
        self.mantidSnapper.LoadDiffCal(
            "Loading Diffcal for {} ...".format(diffCalPath),
            InstrumentFilename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
            MakeGroupingWorkspace=False,
            MakeMaskWorkspace=True,
            Filename=diffCalPrefix,
            WorkspaceName=diffCalPrefix,
        )

        # 6 Apply Calibration Mask to Raw Vanadium and Data output from SumNeighbours
        #              -- done to both data, can be applied to vanadium per state
        for ws in [self.outputWorkspace, self.tmpVanadium]:
            self.mantidSnapper.MaskDetectors(
                "Applying Pixel Mask to Raw Data...",
                Workspace=ws,
                MaskedWorkspace=self.maskWorkspace,
            )
            self.mantidSnapper.ApplyDiffCal(
                "Applying Diffcal...",
                InstrumentWorkspace=ws,
                CalibrationWorkspace=self.DIFC,
            )
            # 9 Does it have a container? Apply Container Attenuation Correction
            data = self.mantidSnapper.ConvertUnits(
                "Converting to Units of dSpacing...",
                InputWorkspace=ws,
                EMode="Elastic",
                Target="dSpacing",
                OutputWorkspace=ws,
                ConvertFromPointData=True,
            )

        # TODO: May impact performance of lite mode data
        # TODO: Params is supposed to be smallest dmin, smalled dbin, largest dmax
        # self.enqueueAlgorithm('Rebin', "Rebinning", isChild=False,  InputWorkspace=data,
        #                       Params='0.338, -0.00086, 5.0', PreserveEvents=False,
        #                       OutputWorkspace="rebinned_data_before_focus")
        # data = "rebinned_data_before_focus"
        # vanadium = self.enqueueAlgorithm('Rebin', "Rebinning", isChild=False, InputWorkspace=vanadium,
        #                                  Params='0.338, -0.00086, 5.0', PreserveEvents=False,
        #                                  OutputWorkspace="rebinned_vanadium_before_focus")
        # vanadium = "rebinned_vanadium_before_focus"
        # 11 For each Group (no for each loop, the algos apply things based on groups of group workspace)
        data = self.mantidSnapper.DiffractionFocussing(
            "Applying Diffraction Focussing...",
            InputWorkspace=data,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="focused_data",
        )
        vanadium = self.mantidSnapper.DiffractionFocussing(
            "Applying Diffraction Focussing...",
            InputWorkspace=vanadium,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="diffraction_focused_vanadium",
        )

        # 2 NormalizeByCurrent -- just apply to data
        self.mantidSnapper.NormaliseByCurrent(
            "Normalizing Current ...",
            InputWorkspace=data,
            OutputWorkspace=data,
        )

        # self.deleteWorkspace(Workspace=rebinned_data_before_focus)
        self.mantidSnapper.WashDishes(
            "Deleting Rebinned Data Before Focus...",
            Workspace="CommonRed",
        )

        # compress data
        # data = self.compressEvents(InputWorkspace=data, OutputWorkspace='event_compressed_data')

        # sum chunks if files are large
        # TODO: Implement New Strip Peaks that allows for multiple FWHM, one per group,
        # for now just grab the first one to get it to run
        peakPositions = ",".join(str(s) for s in ingredients.reductionState.stateConfig.normalizationCalibrant.peaks)

        vanadium = self.mantidSnapper.StripPeaks(
            "Stripping Peaks...",
            InputWorkspace=vanadium,
            FWHM=ingredients.reductionState.stateConfig.focusGroups[0].FWHM[0],
            PeakPositions=peakPositions,
            OutputWorkspace="peaks_stripped_vanadium",
        )
        vanadium = self.mantidSnapper.SmoothData(
            "Smoothing Data...",
            InputWorkspace=vanadium,
            NPoints=ingredients.reductionState.stateConfig.normalizationCalibrant.smoothPoints,
            OutputWorkspace="smoothed_data_vanadium",
        )

        data = self.mantidSnapper.RebinToWorkspace(
            "Rebinning to Workspace...",
            WorkspaceToRebin=data,
            WorkspaceToMatch=vanadium,
            OutputWorkspace="rebinned_data",
            PreserveEvents=False,
        )
        data = self.mantidSnapper.Divide(
            "Rebinning ragged bins...",
            LHSWorkspace=data,
            RHSWorkspace=vanadium,
            OutputWorkspace="data_minus_vanadium",
        )

        # TODO: Refactor so excute only needs to be called once
        self.mantidSnapper.executeQueue()

        dMin = {pgp.groupID: pgp.dResolution.minimum for pgp in ingredients.pixelGroupingParameters}
        dMax = {pgp.groupID: pgp.dResolution.maximum for pgp in ingredients.pixelGroupingParameters}
        dBin = {
            pgp.groupID: pgp.dRelativeResolution / ingredients.pixelGroup.nBinsAcrossPeakWidth
            for pgp in ingredients.pixelGroupingParameters
        }
        groupIDs = [pgp.groupID for pgp in ingredients.pixelGroupingParameters]
        groupIDs.sort()
        groupedData = data
        for index, groupID in enumerate(groupIDs):
            data = self.mantidSnapper.RebinRagged(
                "Rebinning ragged bins...",
                InputWorkspace=mtd[groupedData].getItem(index),  # or groupID
                XMin=dMin[groupID],
                XMax=dMax[groupID],
                Delta=dBin[groupID],
                OutputWorkspace="data_rebinned_ragged_" + str(focusGroups[index].name),  # or groupID
            )
        self.mantidSnapper.WashDishes(
            "Freeing workspace...",
            Workspace="data_minus_vanadium",
        )
        # self.renameWorkspace(InputWorkspace=data, OutputWorkspace="SomethingSensible")

        self.mantidSnapper.executeQueue()

        if self._export:
            with open("/SNS/users/wqp/git/snapred/snap_reduction.py", "w") as file:
                file.write(self._exportScript)

        self.mantidSnapper.cleanup()
        self.log().notice("Execution of ReductionAlgorithm COMPLETE!")
        return data

        # set outputworkspace to data


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReductionAlgorithm)
