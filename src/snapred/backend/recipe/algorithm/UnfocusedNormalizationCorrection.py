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
    VANADIUM_CYLINDER = {
        "attenuationXSection": 4.878,
        "scatteringXSection": 5.159,
        "sampleNumberDensity": 0.070,
        "cylinderSampleHeight": 0.30,  # cm
        "cylinderSampleRadius": 0.15,  # cm
        "numberOfSlices": 10,
        "numberOfAnnuli": 10,
    }

    def PyInit(self):
        # declare properties
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("CalibrantSample", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="vanadiumrawcorr_out", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients: Ingredients) -> None:
        stateConfig = ingredients.reductionState.stateConfig
        self.liteMode = stateConfig.isLiteMode
        self.vanadiumRunNumber = ingredients.runConfig.runNumber
        self.vanadiumBackgroundRunNumber = stateConfig.emptyInstrumentRunNumber
        self.geomCalibFile: str = stateConfig.geometryCalibrationFileName
        # iPrm['calibrationDirectory'] + sPrm['stateID'] +'/057514/'+ f'RVMB{VRun}
        self.rawVFile = stateConfig.rawVanadiumCorrectionFileName
        self.TOFPars: Tuple[float, float, float] = (stateConfig.tofMin, stateConfig.tofBin, stateConfig.tofMax)

    def chopCalibrantSample(self, sample: CalibrantSamples) -> Dict[str, Any]:
        self.sampleForm = sample.geometry.form
        geometry = {
            # SNAPRed specifies form as lowercase, mantid wants init cap
            "Shape": f"{self.sampleForm[0].upper()}{self.sampleForm[1:]}",
            "Radius": sample.geometry.radius,
            "Center": [0, 0, 0],  # @mguthrime confirms this is always true
        }

        # make the geometry dictionary
        if geometry["Shape"] == "Cylinder":
            geometry["Height"] = sample.geometry.total_height
            geometry["Axis"] = [0, 1, 0]  # @mguthriem confirms this is always true
        elif geometry["Shape"] != "Sphere":
            raise RuntimeError(f"Calibrant sample has shape {geometry['Shape']}: must be Cylinder or Sphere\n")

        # make the material dictionary
        material = {
            "ChemicalFormula": sample.material.chemical_composition,
        }

        return {
            "geometry": geometry,
            "material": material,
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
            FilterByTofMin=self.TOFPars[0],
            FilterByTofMax=self.TOFPars[2],
            NumberOfBins=1,
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
        if self.sampleForm == "cylinder":
            self.mantidSnapper.CylinderAbsorption(
                "Create cylinder absorption data",
                InputWorkspace=outputWS,
                OutputWorkspace=wsName_cylinder,
            )
        elif self.sampleForm == "sphere":
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
        toSet = self.chopCalibrantSample(sample)
        self.mantidSnapper.SetSample(
            "Setting workspace with calibrant sample",
            InputWorkspace=outputWS,
            Geometry=toSet["geometry"],
            Material=toSet["material"],
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

        self.restockPantry(outputWS, self.rawVFile)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(UnfocusedNormalizationCorrection)
