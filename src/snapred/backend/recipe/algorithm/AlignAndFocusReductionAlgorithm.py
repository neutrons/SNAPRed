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
        self.declareProperty(MatrixWorkspaceProperty("InputWorkspace", "", Direction.InOut, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("VanadiumWorkspace", "", Direction.InOut, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(MatrixWorkspaceProperty("MaskWorkspace", "", Direction.Input, PropertyMode.Mandatory))
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationWorkspace", "", Direction.Input, PropertyMode.Mandatory)
        )
        self.declareProperty("Ingredients", default="", direction=Direction.Input, optional=PropertyMode.Mandatory)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        return {}

    def chopIngredients(self, ingredients: ReductionIngredients):
        self.stateConfig = ingredients.reductionState.stateConfig
        self.groupIDs = ingredients.pixelGroup.groupIDs
        # Warning: <pixel grouping parameters>.isMasked will be set for fully-masked groups
        #   "0.0" is used by 'Mantid::AlignAndFocusPowderFromFiles' as the _default_ value (=> a non-specified limit)
        self.dMin = ingredients.pixelGroup.dMin(default=0.0)
        self.dMax = ingredients.pixelGroup.dMax(default=0.0)
        self.dBin = ingredients.pixelGroup.dBin()

    def unbagGroceries(self):
        self.outputWorkspace = self.getPropertyValue("InputWorkspace")
        self.vanadiumWorkspace = self.getPropertyValue("VanadiumWorkspace")
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
        self.setPropertyValue("InputWorkspace", self.outputWorkspace)
        self.log().notice("Execution of AlignAndFocusReductionAlgorithm COMPLETE!")


# Register algorithm with Mantid
AlgorithmFactory.subscribe(AlignAndFocusReductionAlgorithm)
