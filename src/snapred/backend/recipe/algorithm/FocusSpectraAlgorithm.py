from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction, StringMandatoryValidator

from snapred.backend.dao.state.PixelGroup import PixelGroup as Ingredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class FocusSpectraAlgorithm(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing values at each pixel",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace defining the grouping for diffraction focusing",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory),
            doc="The diffraction-focused data",
        )
        self.declareProperty(
            "Ingredients",
            defaultValue="",
            validator=StringMandatoryValidator(),
            direction=Direction.Input,
        )
        self.declareProperty(
            "RebinOutput",
            defaultValue=True,
            direction=Direction.Input,
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients):
        self.dMin = ingredients.dMin()
        self.dMax = ingredients.dMax()
        self.dBin = ingredients.dBin()
        pass

    def unbagGroceries(self):
        self.inputWSName = self.getPropertyValue("InputWorkspace")
        self.groupingWSName = self.getPropertyValue("GroupingWorkspace")
        self.outputWSName = self.getPropertyValue("OutputWorkspace")
        self.rebinOutput = self.getProperty("RebinOutput").value

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        # make sure the input workspace can be reduced by this grouping workspace
        inWS = self.mantidSnapper.mtd[self.getPropertyValue("InputWorkspace")]
        groupWS = self.mantidSnapper.mtd[self.getPropertyValue("GroupingWorkspace")]
        if "Grouping" not in groupWS.id():
            errors["GroupingWorkspace"] = "Grouping workspace must be an actual GroupingWorkspace"
        elif inWS.getNumberHistograms() == len(groupWS.getGroupIDs()):
            msg = "The data appears to have already been diffraction focused"
            errors["InputWorkspace"] = msg
            errors["GroupingWorkspace"] = msg
        elif inWS.getNumberHistograms() != groupWS.getNumberHistograms():
            msg = f"""
                The workspaces {self.getPropertyValue("InputWorkspace")}
                and {self.getPropertyValue("GroupingWorkspace")}
                have inconsistent number of spectra
                """
            errors["InputWorkspace"] = msg
            errors["GroupingWorkspace"] = msg
        return errors

    def PyExec(self):
        ingredients: Ingredients = Ingredients.parse_raw(self.getPropertyValue("Ingredients"))
        self.chopIngredients(ingredients)
        self.unbagGroceries()

        self.log().notice("Execute of FocusSpectra START!")

        # convert to d-spacing and diffraction focus and rebin ragged
        self.mantidSnapper.ConvertUnits(
            "Converting to Units of dSpacing...",
            InputWorkspace=self.inputWSName,
            Emode="Elastic",
            Target="dSpacing",
            OutputWorkspace=self.outputWSName,
            ConvertFromPointData=True,
        )
        self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=self.outputWSName,
            GroupingWorkspace=self.groupingWSName,
            OutputWorkspace=self.outputWSName,
            PreserveEvents=True,
        )
        # TODO: Compress events here if preserving events
        if self.rebinOutput is True:
            self.mantidSnapper.RebinRagged(
                "Rebinning ragged bins...",
                InputWorkspace=self.outputWSName,
                XMin=self.dMin,
                XMax=self.dMax,
                Delta=self.dBin,
                OutputWorkspace=self.outputWSName,
                PreserveEvents=False,
            )

        self.mantidSnapper.executeQueue()
        self.setPropertyValue("OutputWorkspace", self.outputWSName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(FocusSpectraAlgorithm)
