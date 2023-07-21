import json

from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceGroup, mtd
from mantid.kernel import Direction

from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.algorithm.CustomGroupWorkspace import CustomGroupWorkspace
from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "VanadiumFocussedReductionAlgorithm"


class VanadiumFocussedReductionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("SmoothDataIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspaceGroup", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        reductionIngredients = ReductionIngredients(**json.loads(self.getProperty("ReductionIngredients").value))
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        smoothIngredients = SmoothDataExcludingPeaksIngredients(
            **json.loads(self.getProperty("SmoothDataIngredients").value)
        )

        # run the algo
        self.log().notice("Execution of VanadiumFocussedReduction START!")

        vanadiumFilePath = reductionIngredients.reductionState.stateConfig.vanadiumFilePath
        diffCalPath = reductionIngredients.reductionState.stateConfig.diffractionCalibrant.diffCalPath

        vanadium = self.mantidSnapper.LoadNexus(
            "Loading Nexus for {}...".format(vanadiumFilePath),
            Filename=vanadiumFilePath,
            OutputWorkspace="vanadium",
        )

        groupingworkspace = self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=reductionIngredients.reductionState.stateConfig.json(),
            InstrumentName=reductionIngredients.reductionState.instrumentConfig.name,
            OutputWorkSpace="CommonRed",
        )

        # diffCalPrefix = "diffcal"
        # self.mantidSnapper.LoadDiffcal(
        #     "Loading Diffcal for {} ...".format(diffCalPath),
        #     InstrumentFilename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
        #     MakeGroupingWorkspace=False,
        #     MakeMaskWorkspace=True,
        #     Filename=diffCalPrefix,
        #     WorkspaceName=diffCalPrefix,
        # )

        # self.mantidSnapper.MaskDetectors(
        #     "Applying Pixel Mask...",
        #     Workspace=vanadium,
        #     MaskedWorkspace=diffCalPrefix + "_mask",
        # )

        vanadium = self.mantidSnapper.ConvertUnits(
            "Converting to Units of dSpacing...",
            InputWorkspace=vanadium,
            EMode="Elastic",
            Target="dSpacing",
            OutputWorkspace="vanadium_dspacing",
            ConvertFromPointData=True,
        )

        vanadium = self.mantidSnapper.DiffractionFocussing(
            "Applying Diffraction Focussing...",
            InputWorkspace=vanadium,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="diffraction_focused_vanadium",
        )

        # focussedVanadium = self.mantidSnapper.AlignAndFocusPowder(
        #     "Diffraction Focussing Vanadium...",
        #     InputWorkspace=vanadium,
        #     CalFileName=diffCalPath,
        #     OutputWorkspace="focussed_vanadium",
        # )

        # smoothedVanadium = self.mantidSnapper.SmoothDataExcludingPeaks(
        #     "Fit and Smooth Peaks...",
        #     InputWorkspace=vanadium,
        #     SmoothDataExcludingPeaksIngredients=smoothIngredients.json(),
        #     OutputWorkspace="smooth_vanadium",
        # )

        self.mantidSnapper.executeQueue()
        # ws_group = WorkspaceGroup()
        # mtd.add('vanadiumFocussedWSGroup', ws_group)
        # ws_group.add(vanadium)
        # ws_group.add(smoothedVanadium)
        self.log().notice("Execution of VanadiumFocussedReduction COMPLETE!")
        self.setProperty("OutputWorkspaceGroup", ws_group.name())
        return ws_group


AlgorithmFactory.subscribe(VanadiumFocussedReductionAlgorithm)
