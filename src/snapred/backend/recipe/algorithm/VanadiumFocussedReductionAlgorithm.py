import json

from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceGroup, mtd
from mantid.kernel import Direction

from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "VanadiumFocussedReductionAlgorithm"


class VanadiumFocussedReductionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("VanadiumFilePath", defaultValue="", direction=Direction.Input)
        self.declareProperty("CalibrationFilePath", defaultValue="", direction=Direction.Input)
        self.declareProperty("SmoothDataIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        vanadiumFilePath = self.getProperty("VanadiumFilePath").value
        diffCalPath = self.getProperty("CalibrationFilePath").value
        smoothIngredients = SmoothDataExcludingPeaksIngredients(
            **json.loads(self.getProperty("SmoothDataIngredients").value)
        )
        # run the algo
        self.log().notice("Execution of VanadiumFocussedReduction START!")

        vanadium = self.mantidSnapper.LoadNexus("Load Nexus... ", Filename=vanadiumFilePath, OutputWorkspace="vanadium")

        ws_group = WorkspaceGroup()
        mtd.add("vanadiumFocussedReduction", ws_group)

        focussedVanadium = self.mantidSnapper.AlignAndFocusPowder(
            "Diffraction Focussing Vanadium...",
            InputWorkspace=vanadium,
            CalFileName=diffCalPath,
            OutputWorkspace="focussed_vanadium",
        )

        smoothedVanadium = self.mantidSnapper.SmoothDataExcludingPeaks(
            "Fit and Smooth Peaks...",
            InputWorkspace=focussedVanadium,
            SmoothDataExcludingPeaksIngredients=smoothIngredients,
            OutputWorkspace="smooth_vanadium",
        )

        self.mantidSnapper.executeQueue()
        ws_group.add(focussedVanadium)
        ws_group.add(smoothedVanadium)
        self.mantidSnapper.cleanup()
        self.log().notice("Execution of VanadiumFocussedReduction COMPLETE!")
        return


AlgorithmFactory.subscribe(VanadiumFocussedReductionAlgorithm)
