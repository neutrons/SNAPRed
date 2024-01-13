from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class AlignAndFocusReductionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: ReductionIngredients):
        self.stateConfig = ingredients.reductionState.stateConfig
        self.groupIDs = ingredients.pixelGroup.groupIDs
        self.dMin = ingredients.pixelGroup.dMin()
        self.dMax = ingredients.pixelGroup.dMax()
        self.dBin = ingredients.pixelGroup.dBin()

    def unbagGroceries(self):
        pass

    def PyExec(self):
        ingredients = ReductionIngredients.parse_raw(self.getPropertyValue("ReductionIngredients"))
        self.chopIngredients(ingredients)
        # run the algo
        self.log().notice("Execution of AlignAndFocusReductionAlgorithm START!")

        # raw_data = self.loadEventNexus(Filename=rawDataPath, OutputWorkspace="raw_data")
        self.mantidSnapper.LoadNexus(
            "loading vanadium file...",
            Filename=vanadiumFilePath,
            OutputWorkspace="vanadium",
        )

        self.mantidSnapper.CustomGroupWorkspace(
            "Creating group workspace...",
            StateConfig=self.stateConfig.json(),
            InputWorkspace="vanadium",
        )

        # 3 ApplyDiffCal  -- just apply to data
        diffCalPrefix = "diffcal"
        self.mantidSnapper.LoadDiffCal(
            "Loading DiffCal for {} ...".format(diffCalPath),
            InstrumentFilename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
            MakeGroupingWorkspace=False,
            MakeMaskWorkspace=True,
            Filename=diffCalPath,
            WorkspaceName=diffCalPrefix,
        )

        self.mantidSnapper.executeQueue()

        self.mantidSnapper.AlignAndFocusPowderFromFiles(
            "Executing AlignAndFocusPowder...",
            Filename=rawDataPath,
            MaxChunkSize=5,
            # UnfocussedWorkspace="?",
            GroupingWorkspace="Column",
            CalibrationWorkspace=diffCalPrefix + "_cal",
            MaskWorkspace=diffCalPrefix + "_mask",
            #   Params="",
            DMin=self.dMin,
            DMax=self.dMax,
            DeltaRagged=self.dBin,
            #   ReductionProperties="?",
            OutputWorkspace="output",
        )
        self.mantidSnapper.executeQueue()

        self.log().notice("Execution of AlignAndFocusReductionAlgorithm COMPLETE!")


# Register algorithm with Mantid
AlgorithmFactory.subscribe(AlignAndFocusReductionAlgorithm)
