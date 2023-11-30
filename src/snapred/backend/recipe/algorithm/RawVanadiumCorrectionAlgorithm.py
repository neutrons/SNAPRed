from typing import Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
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
            ITableWorkspaceProperty("CalibrationWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Table workspace with calibration data: cols detid, difc, difa, tzero",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing corrected data; if none given, the InputWorkspace will be overwritten",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("CalibrantSample", defaultValue="", direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients) -> None:
        stateConfig = ingredients.reductionState.stateConfig
        self.TOFPars: Tuple[float, float, float] = (stateConfig.tofMin, stateConfig.tofBin, stateConfig.tofMax)

    def chopNeutronData(self, wsName: str) -> None:
        self.mantidSnapper.MakeDirtyDish(
            "make a copy of data before chop",
            InputWorkspace=wsName,
            Outputworkspace=wsName + "_beforeChop",
        )

        self.mantidSnapper.ConvertUnits(
            "Ensure workspace is in TOF units",
            InputWorkspace=wsName,
            Outputworkspace=wsName,
            Target="TOF",
        )
        self.mantidSnapper.CropWorkspace(
            "Filter the workspace within bounds",
            InputWorkspace=wsName,
            OutputWorkspace=wsName,
            XMin=self.TOFPars[0],
            XMax=self.TOFPars[2],
        )
        self.mantidSnapper.NormaliseByCurrent(
            "Normalize by current",
            InputWorkspace=wsName,
            OutputWorkspace=wsName,
        )
        self.mantidSnapper.ApplyDiffCal(
            "Apply diffraction calibration from geometry file",
            InstrumentWorkspace=wsName,
            CalibrationWorkspace=self.getProperty("CalibrationWorkspace").value,
        )

        self.mantidSnapper.Rebin(
            "Rebin in log TOF",
            InputWorkspace=wsName,
            Params=self.TOFPars,
            PreserveEvents=False,
            OutputWorkspace=wsName,
            BinningMode="Logarithmic",
        )
        self.mantidSnapper.MakeDirtyDish(
            "make a copy of data before chop",
            InputWorkspace=wsName,
            Outputworkspace=wsName + "_afterChop",
        )
        self.mantidSnapper.executeQueue()

    def shapedAbsorption(self, outputWS: str, wsName_cylinder: str):
        if self.sampleShape == "Cylinder":
            self.mantidSnapper.CylinderAbsorption(
                "Create cylinder absorption data",
                InputWorkspace=outputWS,
                OutputWorkspace=wsName_cylinder,
            )
        elif self.sampleShape == "Sphere":
            self.mantidSnapper.SphericalAbsorption(
                "Create spherical absorption data",
                InputWorkspace=outputWS,
                OutputWorkspace=wsName_cylinder,
            )
        else:
            raise RuntimeError("Must use cylindrical or spherical calibrant samples\n")

    def PyExec(self):
        wsNameV = self.getPropertyValue("InputWorkspace")
        wsNameVB = self.getPropertyValue("BackgroundWorkspace")
        wsName_cylinder = "cylAbsCalc"
        outputWS = self.getPropertyValue("OutputWorkspace")
        outputWSVB = wsNameVB + "_out"

        # if output is same as input, overwrite it
        # if output is not the same, clone copy of input and continue
        # else set the name so the workspace is overwritten
        if wsNameV != outputWS:
            self.mantidSnapper.CloneWorkspace(
                "Copying over input data for output",
                InputWorkspace=wsNameV,
                OutputWorkspace=outputWS,
            )
        elif outputWS == "":
            outputWS = wsNameV

        # do not mutate the background workspace
        # instead clone it, then delete later
        self.mantidSnapper.CloneWorkspace(
            "Copying over background data",
            InputWorkspace=wsNameVB,
            OutputWorkspace=outputWSVB,
        )

        # Load and pre-process vanadium and empty datasets
        ingredients: Ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)

        # Process the raw vanadium and background data
        self.chopNeutronData(outputWS)
        self.chopNeutronData(outputWSVB)

        # take difference
        self.mantidSnapper.Minus(
            "Subtract off empty background",
            LHSWorkspace=outputWS,
            RHSWorkspace=outputWSVB,
            OutputWorkspace=outputWS,
        )
        self.mantidSnapper.MakeDirtyDish(
            "create record of state after subtraction",
            InputWorkspace=outputWS,
            OutputWorkspace=outputWS + "_minusBackground",
        )
        self.mantidSnapper.WashDishes(
            "Remove local vanadium background copy",
            Workspace=outputWSVB,
        )

        # calculate and apply cylindrical absorption
        self.mantidSnapper.ConvertUnits(
            "Convert to wavelength",
            InputWorkspace=outputWS,
            OutputWorkspace=outputWS,
            Target="Wavelength",
        )
        # set the workspace's sample
        sample = CalibrantSamples.parse_raw(self.getProperty("CalibrantSample").value)
        self.sampleShape = sample.geometry.shape
        self.mantidSnapper.SetSample(
            "Setting workspace with calibrant sample",
            InputWorkspace=outputWS,
            Geometry=sample.geometry.dict(),
            Material=sample.material.dict(),
        )
        self.shapedAbsorption(outputWS, wsName_cylinder)
        self.mantidSnapper.Divide(
            "Divide out the cylinder absorption data",
            LHSWorkspace=outputWS,
            RHSWorkspace=wsName_cylinder,
            OutputWorkspace=outputWS,
        )
        self.mantidSnapper.WashDishes(
            "Delete cluttering workspace",
            Workspace=wsName_cylinder,
        )
        self.mantidSnapper.ConvertUnits(
            "Convert units back to TOF",
            InputWorkspace=outputWS,
            OutputWorkspace=outputWS,
            Target="TOF",
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(RawVanadiumCorrectionAlgorithm)
