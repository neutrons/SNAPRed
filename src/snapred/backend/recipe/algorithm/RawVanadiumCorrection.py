import json
from typing import Any, Dict, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
)
from mantid.kernel import Direction
from scipy.interpolate import make_smoothing_spline, splev

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "UnfocusedNormalizationCorrection"


# TODO: Rename so it matches filename
class UnfocusedNormalizationCorrection(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("CalibrantSample", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="vanadiumrawcorr_out", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients: Ingredients) -> None:
        stateConfig = ingredients.reductionState.stateConfig
        self.liteMode: bool = stateConfig.isLiteMode
        self.vanadiumRunNumber: str = ingredients.runConfig.runNumber
        self.vanadiumBackgroundRunNumber: str = stateConfig.emptyInstrumentRunNumber
        self.geomCalibFile: str = stateConfig.geometryCalibrationFileName
        self.rawVFile: str = stateConfig.rawVanadiumCorrectionFileName
        self.TOFPars: Tuple[float, float, float] = (stateConfig.tofMin, stateConfig.tofBin, stateConfig.tofMax)

    def chopCalibrantSample(self, sample: CalibrantSamples) -> Dict[str, Any]:
        self.sampleShape = sample.geometry.shape
        return {
            "geometry": sample.geometry.geometryDictionary,
            "material": sample.material.materialDictionary,
        }

    def raidPantry(self, wsName: str, filename: str) -> None:
        if self.liteMode:
            pass
        else:
            pass

        self.mantidSnapper.LoadEventNexus(
            "Load the indicated data",
            Filename=filename,
            OutputWorkspace=wsName,
            NumberOfBins=1,
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
            CalibrationFile=self.geomCalibFile,
        )

        self.mantidSnapper.Rebin(
            "Rebin in log TOF",
            InputWorkspace=wsName,
            Params=self.TOFPars,
            PreserveEvents=False,
            OutputWorkspace=wsName,
            BinningMode="Logarithmic",
        )
        self.mantidSnapper.executeQueue()

    def restockPantry(self, wsName: str, filename: str) -> None:
        self.mantidSnapper.SaveNexus(
            "Saving results",
            InputWorkspace=wsName,
            Filename=filename,
        )

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
        wsNameV = "TOF_V"
        wsNameVB = "TOF_VB"
        wsName_cylinder = "cylAbsCalc"
        outputWS = self.getProperty("OutputWorkspace").value

        # Load and pre-process vanadium and empty datasets
        ingredients: Ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.IPTSLoc = self.mantidSnapper.GetIPTS(
            "Retrieve IPTS location",
            RunNumber=self.vanadiumRunNumber,
            Instrument="SNAP",
        )
        self.mantidSnapper.executeQueue()

        self.raidPantry(wsNameV, self.rawVFile)
        self.raidPantry(wsNameVB, self.vanadiumBackgroundRunNumber)

        # take difference
        self.mantidSnapper.Minus(
            "Subtract off empty background",
            LHSWorkspace=wsNameV,
            RHSWorkspace=wsNameVB,
            OutputWorkspace=outputWS,
        )
        self.mantidSnapper.DeleteWorkspaces(
            "Remove cluttering workspaces",
            WorkspaceList=[wsNameV, wsNameVB],
        )

        if not self.liteMode:
            self.mantidSnapper.SumNeighbours(
                "Add neighboring pixels",
                InputWorkspace=outputWS,
                SumX=8,  # TODO: extract this from SNAPLite definition
                SumY=8,  # TODO: extract this from SNAPLite definition
                OutputWorkspace=outputWS,
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
            Geometry=sample.geometry.geometryDictionary,
            Material=sample.material.materialDictionary,
        )
        self.shapedAbsorption(outputWS, wsName_cylinder)
        self.mantidSnapper.Divide(
            "Divide out the cylinder absorption data",
            LHSWorkspace=outputWS,
            RHSWorkspace=wsName_cylinder,
            OutputWorkspace=outputWS,
        )
        self.mantidSnapper.DeleteWorkspace(
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

        # save results
        try:
            self.restockPantry(outputWS, self.rawVFile)
        except:  # noqa: E722
            raise Warning("Unable to save output to file")


# Register algorithm with Mantid
AlgorithmFactory.subscribe(UnfocusedNormalizationCorrection)
