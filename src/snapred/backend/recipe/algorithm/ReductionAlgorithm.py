import json

from mantid.api import AlgorithmFactory, PythonAlgorithm, mtd
from mantid.kernel import Direction

from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "ReductionAlgorithm"


#######################################################
# ATTENTION: Could be replaced by alignAndFocusPowder #
# please confirm that attenutation correction before  #
# and after is equivalent                             #
#######################################################
class ReductionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        reductionIngredients = ReductionIngredients(**json.loads(self.getProperty("ReductionIngredients").value))
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")

        # TODO: Reorg how filepaths are stored
        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
        vanadiumFilePath = reductionIngredients.reductionState.stateConfig.vanadiumFilePath
        diffCalPath = reductionIngredients.reductionState.stateConfig.diffractionCalibrant.diffCalPath

        raw_data = self.mantidSnapper.LoadEventNexus(
            "Load Event Nexus... ", Filename=rawDataPath, OutputWorkspace="raw_data"
        )
        vanadium = self.mantidSnapper.LoadNexus("Load Nexus... ", Filename=vanadiumFilePath, OutputWorkspace="vanadium")

        # 4 Not Lite? SumNeighbours  -- just apply to data
        # self.sumNeighbours(InputWorkspace=raw_data, SumX=SuperPixEdge, SumY=SuperPixEdge, OutputWorkspace=raw_data)

        # 7 Does it have a container? Apply Container Mask to Raw Vanadium and Data output from SumNeighbours
        #                           -- done to both data and vanadium
        # self.applyCotainerMask()
        # 8 CreateGroupWorkspace      TODO: Assess performance, use alternative Andrei came up with that is faster
        groupingworkspace = self.mantidSnapper.CreateGroupWorkspace(
            "Create Group Workspace...",
            reductionIngredients.reductionState.stateConfig,
            reductionIngredients.reductionState.instrumentConfig.name,
        )

        # 3 ApplyDiffCal  -- just apply to data
        diffCalPrefix = self.mantidSnapper.LoadDiffCal(
            "Load Diff Cal...", Filename=diffCalPath, WorkspaceName="diffcal"
        )

        # 6 Apply Calibration Mask to Raw Vanadium and Data output from SumNeighbours
        #              -- done to both data, can be applied to vanadium per state
        self.mantidSnapper.MaskDetectors(
            "Apply Calibration Pixle Mask...", Workspace=raw_data, MaskedWorkspace=diffCalPrefix + "_mask"
        )
        self.mantidSnapper.MaskDetectors(
            "Apply Calibration Pixle Mask...", Workspace=vanadium, MaskedWorkspace=diffCalPrefix + "_mask"
        )

        self.mantidSnapper.ApplyDiffCal(
            "Apply Diff Cal...", InstrumentWorkspace=raw_data, CalibrationWorkspace=diffCalPrefix + "_cal"
        )

        self.mantidSnapper.DeleteWorkspace("Freeing workspace...", Workspace=diffCalPrefix + "_mask")
        self.mantidSnapper.DeleteWorkspace("Freeing workspace...", Workspace=diffCalPrefix + "_cal")
        self.mantidSnapper.DeleteWorkspace("Freeing workspace...", Workspace="idf")

        # 9 Does it have a container? Apply Container Attenuation Correction
        data = self.mantidSnapper.ConvertUnits(
            "Converting Units...",
            InputWorkspace=raw_data,
            EMode="Elastic",
            Target="dSpacing",
            OutputWorkspace="data",
            ConvertFromPointData=True,
        )
        vanadium = self.mantidSnapper.ConvertUnits(
            "Converting Units...",
            InputWorkspace=vanadium,
            EMode="Elastic",
            Target="dSpacing",
            OutputWorkspace="vanadium_dspacing",
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
        data = self.mantidSnapper.DiffractionFocusing(
            "Diffraction Focussing...",
            InputWorkspace=data,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="focused_data",
        )
        vanadium = self.mantidSnapper.DiffractionFocusing(
            "Diffraction Focussing...",
            InputWorkspace=vanadium,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="diffraction_focused_vanadium",
        )

        # 2 NormalizeByCurrent -- just apply to data
        self.mantidSnapper.NormaliseByCurrent("Normalize by current...", InputWorkspace=data, OutputWorkspace=data)

        # self.deleteWorkspace(Workspace=rebinned_data_before_focus)
        self.mantidSnapper.DeleteWorkspace("Freeing workspace...", Workspace="CommonRed")

        # compress data
        # data = self.compressEvents(InputWorkspace=data, OutputWorkspace='event_compressed_data')

        # sum chunks if files are large
        # TODO: Implement New Strip Peaks that allows for multiple FWHM, one per group,
        # for now just grab the first one to get it to run
        peakPositions = ",".join(
            str(s) for s in reductionIngredients.reductionState.stateConfig.normalizationCalibrant.peaks
        )

        vanadium = self.mantidSnapper.StripPeaks(
            "Stripping Peaks...",
            InputWorkspace=vanadium,
            FWHM=reductionIngredients.reductionState.stateConfig.focusGroups[0].FWHM[0],
            PeakPositions=peakPositions,
            OutputWorkspace="peaks_stripped_vanadium",
        )
        vanadium = self.mantidSnapper.SmoothData(
            "Smoothing Data ...",
            InputWorkspace=vanadium,
            NPoints=reductionIngredients.reductionState.stateConfig.normalizationCalibrant.smoothPoints,
            OutputWorkspace="smoothed_data_vanadium",
        )

        data = self.mantidSnapper.RebinToWorkspace(
            "Rebinning to workspace...",
            WorkspaceToRebin=data,
            WorkspaceToMatch=vanadium,
            OutputWorkspace="rebinned_data",
            PreserveEvents=False,
        )
        data = self.mantidSnapper.Divide(
            LHSWorkspace=data, RHSWorkspace=vanadium, OutputWorkspace="data_minus_vanadium"
        )

        # TODO: Refactor so excute only needs to be called once
        self.mantidSnapper.executeQueue()

        groupedData = data
        for workspaceIndex in range(len(focusGroups)):
            data = self.mantidSnapper.RebinRagged(
                "Rebinning ragged bins...",
                InputWorkspace=self.mantidSnapper.mtd[groupedData].getItem(workspaceIndex),
                XMin=focusGroups[workspaceIndex].dMin,
                XMax=focusGroups[workspaceIndex].dMax,
                Delta=focusGroups[workspaceIndex].dBin,
                OutputWorkspace="data_rebinned_ragged_" + str(focusGroups[workspaceIndex].name),
            )
        self.mantidSnapper.deleteWorkspace(Workspace="data_minus_vanadium")
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
