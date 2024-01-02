# TODO this is duplicated by other normalization calibration work -- delete?

import json

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import (
    ReductionIngredients,
    SmoothDataExcludingPeaksIngredients,
)
from snapred.backend.recipe.algorithm.CustomGroupWorkspace import CustomGroupWorkspace  # noqa F401
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo  # noqa F401


class VanadiumFocussedReductionAlgorithm(PythonAlgorithm):
    """
    This algorithm creates focussed vanadium to be used during the calibration process. This will allow
    optimizing the parameters that are used by inspection of the output workspaces created.
    Inputs:

        ReductionIngredients: str -- JSON string of ReductionIngredients object
        SmoothDataIngredients: str -- JSON string of SmoothDataExcludingPeaksIngredients object

    Output:

        OutputWorkspaceGroup: str -- the name of the Output Workspace group (diffraction_focused_vanadium)
    """

    def category(self):
        return "SNAPRed Normalization Calibration"

    def PyInit(self):
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("SmoothDataIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspaceGroup", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def PyExec(self):
        reductionIngredients = ReductionIngredients(**json.loads(self.getProperty("ReductionIngredients").value))
        smoothIngredients = SmoothDataExcludingPeaksIngredients(
            **json.loads(self.getProperty("SmoothDataIngredients").value)
        )

        # run the algo
        self.log().notice("Execution of VanadiumFocussedReduction START!")

        vanadiumFilePath = reductionIngredients.reductionState.stateConfig.vanadiumFilePath

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
        self.mantidSnapper.executeQueue()

        diff_group = self.mantidSnapper.mtd["diffraction_focused_vanadium"]
        ws_list = list(diff_group.getNames())

        for idx, ws in enumerate(ws_list):
            self.mantidSnapper.SmoothDataExcludingPeaksAlgo(
                "Fit and Smooth Peaks...",
                InputWorkspace=ws,
                SmoothDataExcludingPeaksIngredients=smoothIngredients.json(),
                OutputWorkspace="smooth_ws",
            )

        self.mantidSnapper.WashDishes(
            "Clean up workspaces...",
            WorkspaceList=["idf", "weight_ws", "vanadium", "vanadium_dspacing", "CommonRed"],
        )
        self.mantidSnapper.executeQueue()

        self.log().notice("Execution of VanadiumFocussedReduction COMPLETE!")
        self.setProperty("OutputWorkspaceGroup", "diffraction_focused_vanadium")
        return diff_group


AlgorithmFactory.subscribe(VanadiumFocussedReductionAlgorithm)
