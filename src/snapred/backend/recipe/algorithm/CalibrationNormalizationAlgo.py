import json
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import (
    NormalizationCalibrationIngredients as Ingredients,
)
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo  # noqa F401


class CalibrationNormalizationAlgo(PythonAlgorithm):
    def category(self):
        return "SNAPRed Normalization Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Optional),
            doc="Workspace containg the raw vanadium data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("BackgroundWorkspace", "", Direction.Input, PropertyMode.Optional),
            doc="Workspace containing the raw vanadium background data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Optional),
            doc="Workspace definingthe grouping to use in normalization",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="The diffraction-focused, normalized data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("SmoothedOutput", "", Direction.Output, PropertyMode.Optional),
            doc="The diffraction-focused and smoothed normalized data",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients):
        self.reductionIngredients = ingredients.reductionIngredients

        self.calibrantSample = ingredients.calibrantSample
        self.smoothDataIngredients = ingredients.smoothDataIngredients

        pixelGroupingParam = self.reductionIngredients.pixelGroupingParameters
        self.dResolutionMin = [pgp.dResolution.minimum for pgp in pixelGroupingParam]
        self.dResolutionMax = [pgp.dResolution.maximum for pgp in pixelGroupingParam]
        instrumentConfig = self.reductionIngredients.reductionState.instrumentConfig
        self.dRelativeResolution = [
            -abs(pgp.dRelativeResolution / instrumentConfig.NBins) for pgp in pixelGroupingParam
        ]
        pass

    def unbagGroceries(self):
        # TODO useworkspace name generator
        self.inputWSName = self.getPropertyValue("InputWorkspace")
        self.backgroundWSName = self.getPropertyValue("BackgroundWorkspace")
        self.groupingWSName = self.getPropertyValue("GroupingWorkspace")
        self.rawVanadiumWSName = self.getPropertyValue("OutputWorkspace")
        if self.getProperty("OutputWorkspace").isDefault:
            self.smoothRawVanadiumWSName = self.rawVanadiumWSName + "_smoothed"
        else:
            self.smoothRawVanadiumWSName = self.getPropertyValue("OutputWorkspace")
        pass

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        if self.getProperty("OutputWorkspace").isDefault:
            errors["OutputWorkspace"] = "Output workspace is required"

        # TODO these validation checks should be redundant now with the extra validator
        reductionIngredientJSON = json.loads(self.getPropertyValue("Ingredients"))["reductionIngredients"]
        if reductionIngredientJSON.get("pixelGroupingParameters") is None:
            errors["Ingredients"] = "Pixel grouping parameters must be specified"
        elif len(reductionIngredientJSON.get("pixelGroupingParameters")) == 0:
            errors["Ingredients"] = "Pixel grouping parameters must have at least one set of parameters"

        return errors

    def PyExec(self):
        ingredients: Ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.unbagGroceries()

        # run the algo
        self.log().notice("Execution of CalibrationNormalizationAlgo START!")

        self.mantidSnapper.RawVanadiumCorrectionAlgorithm(
            "Correcting Vanadium Data...",
            InputWorkspace=self.inputWSName,
            BackgroundWorkspace=self.backgroundWSName,
            Ingredients=self.reductionIngredients.json(),
            CalibrantSample=self.calibrantSample.json(),
            OutputWorkspace=self.rawVanadiumWSName,
        )
        self.mantidSnapper.MakeDirtyDish(
            "Save copy of raw vanadium correction for CIS inspection",
            Inputworkspace=self.rawVanadiumWSName,
            OutputWorkspace=self.rawVanadiumWSName + "_corr",
        )

        # now convert to d-spacing and diffraction focus and rebin ragged
        self.mantidSnapper.ConvertUnits(
            "Converting to Units of dSpacing...",
            InputWorkspace=self.rawVanadiumWSName,
            Emode="Elastic",
            Target="dSpacing",
            OutputWorkspace=self.rawVanadiumWSName,
            ConvertFromPointData=True,
        )
        self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=self.rawVanadiumWSName,
            GroupingWorkspace=self.groupingWSName,
            OutputWorkspace=self.rawVanadiumWSName,
            PreserveEvents=True,
        )
        self.mantidSnapper.MakeDirtyDish(
            "Save a copy of diffraction focused raw vanadium before ragged rebin",
            InputWorkspace=self.rawVanadiumWSName,
            OutputWorkspace=self.rawVanadiumWSName + "_beforeRebin",
        )
        self.mantidSnapper.RebinRagged(
            "Rebinning ragged bins...",
            InputWorkspace=self.rawVanadiumWSName,
            XMin=self.dResolutionMin,
            XMax=self.dResolutionMax,
            Delta=self.dRelativeResolution,
            OutputWorkspace=self.rawVanadiumWSName,
            PreserveEvents=False,
        )

        # make a copy of the diffraction focused data with peaks smoothed
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            InputWorkspace=self.rawVanadiumWSName,
            OutputWorkspace=self.smoothRawVanadiumWSName,
        )
        self.mantidSnapper.SmoothDataExcludingPeaksAlgo(
            "Fit and Smooth Peaks...",
            InputWorkspace=self.smoothRawVanadiumWSName,
            Ingredients=self.smoothDataIngredients.json(),
            OutputWorkspace=self.smoothRawVanadiumWSName,
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationNormalizationAlgo)
