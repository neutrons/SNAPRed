from typing import Dict, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.recipe.algorithm.MakeDirtyDish import MakeDirtyDish
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class RawVanadiumCorrectionAlgorithm(PythonAlgorithm):
    def category(self):
        return "SNAPRed Normalization Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("BackgroundWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium background data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing corrected data; if none given, the InputWorkspace will be overwritten",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("CalibrantSample", defaultValue="", direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients, sample: CalibrantSamples) -> None:
        stateConfig = ingredients.reductionState.stateConfig
        self.TOFPars: Tuple[float, float, float] = (stateConfig.tofMin, stateConfig.tofBin, stateConfig.tofMax)

        self.geometry = sample.geometry
        self.material = sample.material
        self.sampleShape = self.geometry.shape

    def unbagGroceries(self) -> None:
        self.inputVanadiumWS = self.getPropertyValue("InputWorkspace")
        self.inputBackgroundWS = self.getPropertyValue("BackgroundWorkspace")
        self.outputVanadiumWS = self.getPropertyValue("OutputWorkspace")
        self.absorptionWS = "raw_vanadium_absorption_factor"
        self.outputBackgroundWS = self.inputBackgroundWS + "_out"

    def chopNeutronData(self, inputWS: str, outputWS: str) -> None:
        self.mantidSnapper.MakeDirtyDish(
            "make a copy of data before chop",
            InputWorkspace=inputWS,
            Outputworkspace=inputWS + "_beforeChop",
        )

        self.mantidSnapper.ConvertUnits(
            "Ensure workspace is in TOF units",
            InputWorkspace=inputWS,
            Outputworkspace=outputWS,
            Target="TOF",
        )
        self.mantidSnapper.CropWorkspace(
            "Filter the workspace within bounds",
            InputWorkspace=outputWS,
            OutputWorkspace=outputWS,
            XMin=self.TOFPars[0],
            XMax=self.TOFPars[2],
        )

        self.mantidSnapper.Rebin(
            "Rebin in log TOF",
            InputWorkspace=outputWS,
            Params=self.TOFPars,
            PreserveEvents=True,
            OutputWorkspace=outputWS,
            BinningMode="Logarithmic",
        )
        self.mantidSnapper.NormaliseByCurrent(
            "Normalize by current",
            InputWorkspace=outputWS,
            OutputWorkspace=outputWS,
        )
        self.mantidSnapper.MakeDirtyDish(
            "make a copy of data after chop",
            InputWorkspace=outputWS,
            Outputworkspace=outputWS + "_afterChop",
        )
        self.mantidSnapper.executeQueue()

    def shapedAbsorption(self, inputWS: str, absorptionWS: str, sampleShape: str):
        if sampleShape == "Cylinder":
            self.mantidSnapper.CylinderAbsorption(
                "Create cylinder absorption data",
                InputWorkspace=inputWS,
                OutputWorkspace=absorptionWS,
            )
        elif sampleShape == "Sphere":
            self.mantidSnapper.SphericalAbsorption(
                "Create spherical absorption data",
                InputWorkspace=inputWS,
                OutputWorkspace=absorptionWS,
            )
        else:
            raise RuntimeError("Must use cylindrical or spherical calibrant samples\n")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        if self.getProperty("OutputWorkspace").isDefault:
            errors["OutputWorkspace"] = "The output workspace for the raw vanadium correction must be specified"
        return errors

    def PyExec(self):
        # Load and pre-process vanadium and empty datasets
        ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        sample = CalibrantSamples.parse_raw(self.getProperty("CalibrantSample").value)
        self.chopIngredients(ingredients, sample)
        self.unbagGroceries()

        # Process the raw vanadium and background data
        self.chopNeutronData(self.inputVanadiumWS, self.outputVanadiumWS)
        self.chopNeutronData(self.inputBackgroundWS, self.outputBackgroundWS)

        # take difference
        self.mantidSnapper.Minus(
            "Subtract off empty background",
            LHSWorkspace=self.outputVanadiumWS,
            RHSWorkspace=self.outputBackgroundWS,
            OutputWorkspace=self.outputVanadiumWS,
        )
        self.mantidSnapper.MakeDirtyDish(
            "create record of state after subtraction",
            InputWorkspace=self.outputVanadiumWS,
            OutputWorkspace=self.outputVanadiumWS + "_minusBackground",
        )
        self.mantidSnapper.WashDishes(
            "Remove local vanadium background copy",
            Workspace=self.outputBackgroundWS,
        )

        # calculate and apply cylindrical absorption
        self.mantidSnapper.ConvertUnits(
            "Convert to wavelength",
            InputWorkspace=self.outputVanadiumWS,
            OutputWorkspace=self.outputVanadiumWS,
            Target="Wavelength",
        )
        # set the workspace's sample
        self.mantidSnapper.SetSample(
            "Setting workspace with calibrant sample",
            InputWorkspace=self.outputVanadiumWS,
            Geometry=self.geometry.dict(),
            Material=self.material.dict(),
        )
        self.shapedAbsorption(
            self.outputVanadiumWS,
            self.absorptionWS,
            self.sampleShape,
        )
        self.mantidSnapper.Divide(
            "Divide out the cylinder absorption data",
            LHSWorkspace=self.outputVanadiumWS,
            RHSWorkspace=self.absorptionWS,
            OutputWorkspace=self.outputVanadiumWS,
        )
        self.mantidSnapper.WashDishes(
            "Delete cluttering workspace",
            Workspace=self.absorptionWS,
        )
        self.mantidSnapper.ConvertUnits(
            "Convert units back to TOF",
            InputWorkspace=self.outputVanadiumWS,
            OutputWorkspace=self.outputVanadiumWS,
            Target="TOF",
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(RawVanadiumCorrectionAlgorithm)
