from typing import Dict

from mantid.api import AlgorithmFactory, ITableWorkspaceProperty, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class AlignAndFocusReductionAlgorithm(PythonAlgorithm):
    def category(self):
        return "SNAPRed Reduction"

    def PyInit(self):
        # declare properties
        # NOTE this must have identical properts to ReductionAlgorithm
        self.declareProperty(MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("VanadiumWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("MaskWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationWorkspace", "", Direction.Output, PropertyMode.Mandatory)
        )
        self.declareProperty("Ingredients", default="", direction=Direction.Input, optional=PropertyMode.Mandatory)
        self.declareProperty(MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory))
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        return {}

    def chopIngredients(self, ingredients: ReductionIngredients):
        self.stateConfig = ingredients.reductionState.stateConfig
        self.groupIDs = ingredients.pixelGroup.groupIDs
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
        ingredients = ReductionIngredients.parse_raw(self.getPropertyValue("ReductionIngredients"))
        self.chopIngredients(ingredients)
        self.unbagGroceries()
        # run the algo
        self.log().notice("Execution of AlignAndFocusReductionAlgorithm START!")

        self.mantidSnapper.AlignAndFocusPowder(
            "Executing AlignAndFocusPowder...",
            InputWorkspace=self.outputWorkspace,
            MaxChunkSize=5,
            # UnfocussedWorkspace="?",
            GroupingWorkspace=self.groupingWorkspace,
            CalibrationWorkspace=self.calibrationTable,
            MaskWorkspace=self.maskWorkspace,
            #   Params="",
            DMin=self.dMin,
            DMax=self.dMax,
            DeltaRagged=self.dBin,
            #   ReductionProperties="?",
            OutputWorkspace=self.outputWorkspace,
        )
        self.mantidSnapper.executeQueue()
        self.setPropertyValue("OutputWorkspace", self.outputWorkspace)
        self.log().notice("Execution of AlignAndFocusReductionAlgorithm COMPLETE!")


# Register algorithm with Mantid
AlgorithmFactory.subscribe(AlignAndFocusReductionAlgorithm)
