import json
from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients as Ingredients
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
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="The diffraction-focused data",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients):
        pixelGroupingParam = self.ingredients.pixelGroupingParameters
        self.dResolutionMin = [pgp.dResolution.minimum for pgp in pixelGroupingParam]
        self.dResolutionMax = [pgp.dResolution.maximum for pgp in pixelGroupingParam]
        instrumentConfig = ingredients.reductionState.instrumentConfig
        self.dRelativeResolution = [
            -abs(pgp.dRelativeResolution / instrumentConfig.NBins) for pgp in pixelGroupingParam
        ]
        pass

    def unbagGroceries(self):
        self.inputWSName = self.getPropertyValue("InputWorkspace")
        self.groupingWSName = self.getPropertyValue("GroupingWorkspace")
        self.outputWSName = self.getPropertyValue("OutputWorkspace")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        if self.getProperty("OutputWorkspace").isDefault:
            errors["OutputWorkspace"] = "You must specify the output workspace for focused data"

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
            errors["GroupingpWorkspace"] = msg
        return errors

    def PyExec(self):
        ingredients: Ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
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
        self.mantidSnapper.RebinRagged(
            "Rebinning ragged bins...",
            InputWorkspace=self.outputWSName,
            XMin=self.dResolutionMin,
            XMax=self.dResolutionMax,
            Delta=self.dRelativeResolution,
            OutputWorkspace=self.outputWSName,
            PreserveEvents=True,
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(FocusSpectraAlgorithm)
