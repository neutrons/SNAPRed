from typing import Dict

from mantid.api import AlgorithmFactory, ITableWorkspaceProperty, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


#######################################################
# ATTENTION: Could be replaced by alignAndFocusPowder #
# please confirm that attenuation correction before   #
# and after is equivalent                             #
#######################################################
class ReductionAlgorithm(PythonAlgorithm):
    """
    NOTE: this was originally written to work with a list of grouping workspaces,
    and apply the reudction operations on each memberof the list.
    This will now work with a single grouping workspace at a time.
    """

    def category(self):
        return "SNAPRed Reduction"

    def PyInit(self):
        # declare properties
        self.declareProperty(MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("VanadiumWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("MaskWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationWorkspace", "", Direction.Output, PropertyMode.Mandatory)
        )
        self.declareProperty("Ingredients", "", Direction.Input)
        self.declareProperty(MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory))
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        return {}

    def chopIngredients(self, ingredients: ReductionIngredients):
        self.dMin = ingredients.pixelGroup.dMin()
        self.dMax = ingredients.pixelGroup.dMax()
        self.dBin = ingredients.pixelGroup.dBin()

    def unbagGroceries(self):
        self.outputWorkspace = self.getPropertyValue("OutputWorkspace")
        self.vanadiumWorkspace = "tmp_vanadium"  # TODO use unique name genetator
        self.mantidSnapper.CloneWorkspace(
            "Clone copy of input to serve as output",
            InputWorkspace=self.getPropertyValue("InputWorkspace"),
            OutputWorkspace=self.outputWorkspace,
        )
        self.mantidSnapper.CloneWorkspace(
            "clone copy of vanadium for editing",
            InputWorkspace=self.getPropertyValue("VanadiumWorkspace"),
            OutputWorkspace=self.vanadiumWorkspace,
        )
        self.groupingWorkspace = self.getPropertyValue("GroupingWorkspace")
        self.maskWorkspace = self.getPropertyValue("MaskWorkspace")
        self.calibrationTable = self.getPropertyValue("CalibrationWorkspace")

    def PyExec(self):
        reductionIngredients = ReductionIngredients.parse_raw(self.getPropertyValue("Ingredients"))
        self.chopIngredients(reductionIngredients)
        self.unbagGroceries()
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")

        # the data, and only data, should be normalized by current
        # TODO ask Malcolm why
        self.mantidSnapper.NormaliseByCurrent(
            "Normalizing Current ...",
            InputWorkspace=self.outputWorkspace,
            OutputWorkspace=self.outputWorkspace,
        )

        # prepare the data, vanadium with all same applications
        for ws in [self.outputWorkspace, self.vanadiumWorkspace]:
            self.mantidSnapper.MaskDetectors(
                "Applying Pixel Mask...",
                Workspace=ws,
                MaskedWorkspace=self.maskWorkspace,
            )
            self.mantidSnapper.ApplyDiffCal(
                "Applying Diffcal...",
                InstrumentWorkspace=ws,
                CalibrationWorkspace=self.calibrationTable,
            )
            self.mantidSnapper.ConvertUnits(
                "Converting to Units of dSpacing...",
                InputWorkspace=ws,
                EMode="Elastic",
                Target="dSpacing",
                OutputWorkspace=ws,
                ConvertFromPointData=True,
            )
            self.mantidSnapper.DiffractionFocussing(
                "Applying Diffraction Focussing...",
                InputWorkspace=ws,
                GroupingWorkspace=self.groupingWorkspace,
                OutputWorkspace=ws,
            )
        # rebin vanadium to match data for division
        self.mantidSnapper.RebinToWorkspace(
            "Rebin vanadium to match",
            WorkspaceToMatch=self.outputWorkspace,
            WorkspaceToRebin=self.vanadiumWorkspace,
            OutputWorkspace=self.vanadiumWorkspace,
        )
        # divide the data by the vanadium
        # NOTE this must be done before ragged rebinning, since Divide cannot handle unequal axes
        self.mantidSnapper.Divide(
            "Dividing data by vanadium...",
            LHSWorkspace=self.outputWorkspace,
            RHSWorkspace=self.vanadiumWorkspace,
            OutputWorkspace=self.outputWorkspace,
        )
        # ragged-rebin the data
        self.mantidSnapper.RebinRagged(
            "Rebinning ragged bins...",
            InputWorkspace=self.outputWorkspace,
            XMin=self.dMin,
            XMax=self.dMax,
            Delta=self.dBin,
            OutputWorkspace=self.outputWorkspace,
        )
        self.mantidSnapper.WashDishes(
            "Freeing workspace...",
            Workspace=self.vanadiumWorkspace,
        )
        self.mantidSnapper.executeQueue()
        self.setPropertyValue("OutputWorkspace", self.outputWorkspace)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReductionAlgorithm)
