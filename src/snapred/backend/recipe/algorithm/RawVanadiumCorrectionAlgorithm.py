from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceUnitValidator,
)
from mantid.kernel import Direction, StringMandatoryValidator

from snapred.backend.dao.ingredients import NormalizationIngredients as Ingredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class RawVanadiumCorrectionAlgorithm(PythonAlgorithm):
    def category(self):
        return "SNAPRed Normalization Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty(
                "InputWorkspace", "", Direction.Input, PropertyMode.Mandatory, validator=WorkspaceUnitValidator("TOF")
            ),
            doc="Workspace containing the raw vanadium data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "BackgroundWorkspace",
                "",
                Direction.Input,
                PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("TOF"),
            ),
            doc="Workspace containing the raw vanadium background data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory, validator=WorkspaceUnitValidator("TOF")
            ),
            doc="Workspace containing corrected data; if none given, the InputWorkspace will be overwritten",
        )
        self.declareProperty(
            "Ingredients", defaultValue="", validator=StringMandatoryValidator(), direction=Direction.Input
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients) -> None:
        self.TOFPars = ingredients.pixelGroup.timeOfFlight.params

        self.geometry = ingredients.calibrantSample.geometry
        self.material = ingredients.calibrantSample.material
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

        self.mantidSnapper.Rebin(
            "Rebin in log TOF",
            InputWorkspace=outputWS,
            Params=self.TOFPars,
            PreserveEvents=True,
            OutputWorkspace=outputWS,
            BinningMode="Logarithmic",
        )

        self.mantidSnapper.MakeDirtyDish(
            "make a copy of data after chop",
            InputWorkspace=outputWS,
            Outputworkspace=outputWS + "_afterChop",
        )

        self.mantidSnapper.executeQueue()

    def shapedAbsorption(self, inputWS: str, absorptionWS: str, sampleShape: str):
        numberOfSlices = Config["constants.RawVanadiumCorrection.numberOfSlices"]
        numberOfAnnuli = Config["constants.RawVanadiumCorrection.numberOfAnnuli"]

        if sampleShape == "Cylinder":
            self.mantidSnapper.CylinderAbsorption(
                "Create cylinder absorption data",
                InputWorkspace=inputWS,
                OutputWorkspace=absorptionWS,
                NumberOfSlices=numberOfSlices,
                NumberOfAnnuli=numberOfAnnuli,
            )
        elif sampleShape == "Sphere":
            self.mantidSnapper.SphericalAbsorption(
                "Create spherical absorption data",
                InputWorkspace=inputWS,
                OutputWorkspace=absorptionWS,
                NumberOfSlices=numberOfSlices,
                NumberOfAnnuli=numberOfAnnuli,
            )
        else:
            raise RuntimeError("Must use cylindrical or spherical calibrant samples\n")

    def PyExec(self):
        # Load and pre-process vanadium and empty datasets
        ingredients = Ingredients.model_validate_json(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.unbagGroceries()

        # Process the raw vanadium and background data
        self.chopNeutronData(self.inputVanadiumWS, self.outputVanadiumWS)
        self.chopNeutronData(self.inputBackgroundWS, self.outputBackgroundWS)

        pcV = self.mantidSnapper.mtd[self.outputVanadiumWS].run().getProtonCharge()
        pcB = self.mantidSnapper.mtd[self.outputBackgroundWS].run().getProtonCharge()
        protonCharge = pcV / pcB

        self.mantidSnapper.Scale(
            "Scale entire workspace by factor value",
            InputWorkspace=self.outputBackgroundWS,
            Outputworkspace=self.outputBackgroundWS,
            Factor=protonCharge,
        )

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
        self.setPropertyValue("OutputWorkspace", self.outputVanadiumWS)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(RawVanadiumCorrectionAlgorithm)
