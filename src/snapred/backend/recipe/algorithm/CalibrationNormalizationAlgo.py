import json

from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import (
    ReductionIngredients,
    SmoothDataExcludingPeaksIngredients,
)
from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import (
    NormalizationCalibrationIngredients as Ingredients,
)
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks  # noqa F401
from snapred.meta.Config import Config

name = "CalibrationNormalizationAlgo"


class CalibrationNormalization(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "ws_in_normalization", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("BackgroundWorkspace", "ws_background", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium background data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "ws_out_normalization", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing corrected data; if none given, the InputWorkspace will be overwritten",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("FocusWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("SmoothWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients: Ingredients):
        self.run = ingredients.run
        self.backgroundRun = ingredients.backgroundRun
        self.reductionIngredients = ingredients.reductionIngredients
        self.smoothDataIngredients = ingredients.smoothDataIngredients
        self.calibrationRecord = ingredients.calibrationRecord
        self.calibrantSample = ingredients.calibrantSample
        self.calibrationWorkspace = ingredients.calibrationWorkspace
        self.focusGroup = ingredients.focusGroup
        yield

    def PyExec(self):
        ingredients = Ingredients(**json.loads(self.getProperty("Ingredients").value))
        self.chopIngredients(ingredients)

        inputWS = self.getProperty("InputWorkspace").value
        inputWSName = str(inputWS)

        backgroundWS = self.getProperty("BackgroundWorkspace").value
        backgroundWSName = str(backgroundWS)

        self.mantidSnapper.mtd[inputWSName]
        self.mantidSnapper.mtd[backgroundWSName]

        # run the algo
        self.log().notice("Execution of CalibrationNormalizationAlgo START!")

        self.mantidSnapper.RawVanadiumCorrectionAlgorithm(
            "Correcting Vanadium Data...",
            InputWorkspace=inputWSName,
            BackgroundWorkspace=backgroundWSName,
            CalibrationWorkspace=self.calibrationWorkspace.json(),
            Ingredients=self.reductionIngredients.json(),
            CalibrantSample=self.calibrantSample.json(),
            OutputWorkspace=f"{inputWSName}_raw_vanadium_data",
        )

        self.mantidSnapper.LoadGroupingDefinition(
            f"Loading grouping file for focus group {self.focusGroup.name}...",
            GroupingFilename=self.focusGroup.definition,
            InstrumentDonor=f"{inputWSName}_raw_vanadium_data",
            OutputWorkspace=f"{inputWSName}{self.focusGroup.name}_grouping_WS",
        )

        focusedWSName = f"{inputWSName}_{self.focusGroup.name}_focused_data"
        groupingWSName = f"{inputWSName}_{self.focusGroup.name}_grouping_WS"
        self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=f"{inputWSName}_raw_vanadium_data",
            GroupingWorkspace=groupingWSName,
            OutputWorkspace=focusedWSName,
            PreserveEvents=True,
        )
        self.mantidSnapper.executeQueue()

        self.mantidSnapper.RebinRagged(
            "Rebinning ragged bins...",
            InputWorkspace=focusedWSName,
            XMin=self.focusGroup.dMin,
            XMax=self.focusGroup.dMax,
            Delta=self.focusGroup.dBin,
            OutputWorkspace=f"data_rebinned_ragged_{focusedWSName}",
            PreserveEvents=True,
        )

        ws = f"{focusedWSName}_clone_focused_data"
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            InputWorkspace=f"data_rebinned_ragged_{focusedWSName}",
            OutputWorkspace=ws,
        )

        self.mantidSnapper.SmoothDataExcludingPeaks(
            "Fit and Smooth Peaks...",
            InputWorkspace=ws,
            SmoothDataExcludingPeaksIngredients=self.smoothDataIngredients.json(),
            OutputWorkspace=f"{focusedWSName}_smooth_ws",
        )

        self.mantidSnapper.executeQueue()

        focusedWS = mtd[f"data_rebinned_ragged_{focusedWSName}"]
        smoothedWS = mtd[f"{focusedWSName}_smooth_ws"]

        self.setProperty("FocusWorkspace", f"data_rebinned_ragged_{focusedWSName}")
        self.setProperty("SmoothWorkspace", f"{focusedWSName}_smooth_ws")

        return focusedWS, smoothedWS


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationNormalization)
