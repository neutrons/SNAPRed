import json

from mantid.api import AlgorithmFactory, PythonAlgorithm, mtd
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import (
    ReductionIngredients,
    SmoothDataExcludingPeaksIngredients,
)
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks  # noqa F401

name = "CalibrationNormalizationAlgo"


class CalibrationNormalization(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("FocusWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("SmoothWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("SmoothDataIngredients", defaultValue="", direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        reductionIngredients = ReductionIngredients.parse_raw(self.getProperty("ReductionIngredients").value)
        smoothIngredients = SmoothDataExcludingPeaksIngredients(
            **json.loads(self.getProperty("SmoothDataIngredients").value)
        )
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        # run the algo
        self.log().notice("Execution of CalibrationNormalizationAlgo START!")

        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
        raw_data = self.mantidSnapper.loadEventNexus(Filename=rawDataPath, OutputWorkspace="raw_data")

        groupingworkspace = self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=reductionIngredients.reductionState.stateConfig.json(),
            InputWorkspace=raw_data,
            OutputWorkspace="CommonRed",
        )

        for workspaceIndex in range(len(focusGroups)):
            data = self.mantidSnapper.RebinRagged(
                "Rebinning ragged bins...",
                InputWorkspace=mtd[raw_data].getItem(workspaceIndex),
                XMin=focusGroups[workspaceIndex].dMin,
                XMax=focusGroups[workspaceIndex].dMax,
                Delta=focusGroups[workspaceIndex].dBin,
                OutputWorkspace="data_rebinned_ragged_" + str(focusGroups[workspaceIndex].name),
                PreserveEvents=False,
            )

        focused_data = self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=data,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="focused_data",
            PreserveEvents=True,
        )

        ws = "cloneFocusDataWS"
        # clone the workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            InputWorkspace=focused_data,
            OutputWorkspace=ws,
        )

        ws = mtd[ws]

        smooth_ws = self.mantidSnapper.SmoothDataExcludingPeaks(
            "Fit and Smooth Peaks...",
            InputWorkspace=focused_data,
            SmoothDataExcludingPeaksIngredients=smoothIngredients.json(),
            OutputWorkspace="smooth_ws",
        )

        self.mantidSnapper.executeQueue()

        self.setProperty("FocusWorkspace", "ws")
        self.setProperty("SmoothWorkspace", "smooth_ws")

        return ws, smooth_ws


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationNormalization)
