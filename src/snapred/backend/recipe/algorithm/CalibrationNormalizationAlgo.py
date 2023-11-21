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
            MatrixWorkspaceProperty("InputWorkspace", "ws_in_normalization", Direction.Input, PropertyMode.Optional),
            doc="Workspace containing the raw vanadium data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("BackgroundWorkspace", "ws_background", Direction.Input, PropertyMode.Optional),
            doc="Workspace containing the raw vanadium background data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "ws_out_normalization", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing corrected data; if none given, the InputWorkspace will be overwritten",
        )
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Table workspace with calibration data: cols detid, difc, difa, tzero",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("FocusWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("SmoothWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients: Ingredients):
        self.reductionIngredients = ingredients.reductionIngredients
        self.backgroundReductionIngredients = ingredients.backgroundReductionIngredients
        self.calibrantSample = ingredients.calibrantSample
        self.instrumentState = ingredients.instrumentState
        self.focusGroup = ingredients.focusGroup
        self.smoothDataIngredients = ingredients.smoothDataIngredients
        pass

    def PyExec(self):
        ingredients: Ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)

        inputWSName = str(self.getProperty("InputWorkspace").value)
        backgroundWSName = str(self.getProperty("BackgroundWorkspace").value)

        # run the algo
        self.log().notice("Execution of CalibrationNormalizationAlgo START!")

        rawVanadiumWSName = f"{inputWSName}_raw_vanadium_data"
        self.mantidSnapper.RawVanadiumCorrectionAlgorithm(
            "Correcting Vanadium Data...",
            InputWorkspace=inputWSName,
            BackgroundWorkspace=backgroundWSName,
            CalibrationWorkspace=self.getProperty("CalibrationWorkspace").value,
            Ingredients=self.reductionIngredients.json(),
            CalibrantSample=self.calibrantSample.json(),
            OutputWorkspace=rawVanadiumWSName,
        )

        groupingWSName = f"{inputWSName}_{self.focusGroup.name}_grouping_WS"
        self.mantidSnapper.LoadGroupingDefinition(
            f"Loading grouping file for focus group {self.focusGroup.name}...",
            GroupingFilename=self.focusGroup.definition,
            InstrumentDonor=rawVanadiumWSName,
            OutputWorkspace=groupingWSName,
        )
        dSpacingRawWSName = f"{rawVanadiumWSName}_dSpacing"
        self.mantidSnapper.ConvertUnits(
            "Converting to Units of dSpacing...",
            InputWorkspace=rawVanadiumWSName,
            Emode="Elastic",
            Target="dSpacing",
            OutputWorkspace=dSpacingRawWSName,
            ConvertFromPointData=True,
        )

        focusedWSName = f"{inputWSName}_{self.focusGroup.name}_focused_data"
        self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=dSpacingRawWSName,
            GroupingWorkspace=groupingWSName,
            OutputWorkspace=focusedWSName,
            PreserveEvents=True,
        )
        self.mantidSnapper.executeQueue()

        rebinRaggedFocussedWSName = f"data_rebinned_ragged_{inputWSName}_{self.focusGroup.name}"
        if (
            self.instrumentState.pixelGroupingInstrumentParameters is not None
            and len(self.instrumentState.pixelGroupingInstrumentParameters) > 0
        ):
            pixelGroupingParam = self.instrumentState.pixelGroupingInstrumentParameters

            dResolutionMin = [pgp.dResolution.minimum for pgp in pixelGroupingParam]
            dResolutionMax = [pgp.dResolution.maximum for pgp in pixelGroupingParam]
            dRelativeResolution = [
                (pgp.dRelativeResolution / self.instrumentState.instrumentConfig.NBins) for pgp in pixelGroupingParam
            ]

            self.mantidSnapper.RebinRagged(
                "Rebinning ragged bins...",
                InputWorkspace=focusedWSName,
                XMin=dResolutionMin,
                XMax=dResolutionMax,
                Delta=dRelativeResolution,
                OutputWorkspace=rebinRaggedFocussedWSName,
                PreserveEvents=True,
            )

        else:
            raise Exception("Pixel grouping instrument parameters not defined!")

        clonedWSName = f"{focusedWSName}_clone_focused_data"
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            InputWorkspace=rebinRaggedFocussedWSName,
            OutputWorkspace=clonedWSName,
        )

        smoothedWSName = f"{inputWSName}_{self.focusGroup.name}_smoothed_ws"
        self.mantidSnapper.SmoothDataExcludingPeaks(
            "Fit and Smooth Peaks...",
            InputWorkspace=clonedWSName,
            Ingredients=self.smoothDataIngredients.json(),
            OutputWorkspace=smoothedWSName,
        )

        self.mantidSnapper.WashDishes(
            "Washing dishes...",
            WorkspaceList=[
                inputWSName,
                backgroundWSName,
                rawVanadiumWSName,
                dSpacingRawWSName,
                groupingWSName,
                focusedWSName,
                clonedWSName,
            ],
        )

        self.mantidSnapper.executeQueue()

        focusedWS = mtd[rebinRaggedFocussedWSName]
        smoothedWS = mtd[smoothedWSName]

        self.setProperty("FocusWorkspace", rebinRaggedFocussedWSName)
        self.setProperty("SmoothWorkspace", smoothedWSName)

        return focusedWS, smoothedWS


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationNormalization)
