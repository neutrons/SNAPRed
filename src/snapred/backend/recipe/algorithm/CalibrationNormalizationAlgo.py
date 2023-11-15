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

from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import (
    NormalizationCalibrationIngredients as Ingredients,
)
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks  # noqa F401

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
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients: Ingredients):
        self.run = ingredients.run
        self.backgroundRun = ingredients.backgroundRun
        self.reductionIngredients = ingredients.reductionIngredients
        self.calibrationRecord = ingredients.calibrationRecord
        self.calibrantSample = ingredients.calibrantSample
        self.calibrationWorkspace = ingredients.calibrationWorkspace
        self.instrumentState = ingredients.instrumentState
        self.focusGroup = ingredients.focusGroup
        yield

    def PyExec(self):
        ingredients = Ingredients(**json.loads(self.getProperty("Ingredients").value))
        self.chopIngredients(ingredients)

        self.setProperty("InputWorkspace", f"{self.run.runNumber}_input_WS")
        self.setProperty("BackgroundWorkspace", f"{self.backgroundRun.runNumber}_background_input_WS")

        inputWSName = str(self.getProperty("InputWorkspace").value)
        backgroundWSName = str(self.getProperty("BackgroundWorkspace").value)

        # run the algo
        self.log().notice("Execution of CalibrationNormalizationAlgo START!")

        self.mantidSnapper.RawVanadiumCorrectionAlgorithm(
            "Correcting Vanadium Data...",
            InputWorkspace=inputWSName,
            BackgroundWorkspace=backgroundWSName,
            CalibrationWorkspace=self.calibrationWorkspace.json(),  # remove this
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

        rebinRaggedFocussedWSName = f"data_rebinned_ragged_{inputWSName}_{self.focusGroup.name}"
        self.mantidSnapper.RebinRagged(
            "Rebinning ragged bins...",
            InputWorkspace=focusedWSName,
            XMin=self.instrumentState.PixelGroupingParameters.dResolution.minimum,
            XMax=self.instrumentState.PixelGroupingParameters.dResolution.maximum,
            Delta=(
                self.instrumentState.PixelGroupingParameters.dRelativeResolution
                / self.instrumentState.InstrumentConfig.NBins
            ),
            OutputWorkspace=rebinRaggedFocussedWSName,
            PreserveEvents=True,
        )

        self.mantidSnapper.WashDishes(
            "Washing dishes...",
            WorkspaceList=[
                f"{self.run.runNumber}_input_WS",
                f"{self.backgroundRun.runNumber}_background_input_WS",
                f"{inputWSName}_raw_vanadium_data",
                f"{inputWSName}_{self.focusGroup.name}_focused_data",
                f"{inputWSName}{self.focusGroup.name}_grouping_WS",
            ],
        )

        self.mantidSnapper.executeQueue()

        focusedWS = mtd[rebinRaggedFocussedWSName]

        self.setProperty("FocusWorkspace", rebinRaggedFocussedWSName)

        return focusedWS


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationNormalization)
