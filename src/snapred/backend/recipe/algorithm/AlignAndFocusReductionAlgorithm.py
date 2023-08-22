from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "AlignAndFocusReductionAlgorithm"


class AlignAndFocusReductionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        reductionIngredients = ReductionIngredients.parse_raw(self.getProperty("ReductionIngredients").value)
        # run the algo
        self.log().notice("Execution of AlignAndFocusReductionAlgorithm START!")

        # TODO: Reorg how filepaths are stored
        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
        vanadiumFilePath = reductionIngredients.reductionState.stateConfig.vanadiumFilePath
        diffCalPath = reductionIngredients.reductionState.stateConfig.diffractionCalibrant.diffCalPath

        # raw_data = self.loadEventNexus(Filename=rawDataPath, OutputWorkspace="raw_data")
        self.mantidSnapper.LoadNexus("loading vanadium file...", Filename=vanadiumFilePath, OutputWorkspace="vanadium")

        self.mantidSnapper.CustomGroupWorkspace(
            "Creating group workspace...",
            StateConfig=reductionIngredients.reductionState.stateConfig.json(),
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
        DMin = reductionIngredients.reductionState.stateConfig.focusGroups[0].dMin
        DMax = reductionIngredients.reductionState.stateConfig.focusGroups[0].dMax
        DeltaRagged = reductionIngredients.reductionState.stateConfig.focusGroups[0].dBin

        self.mantidSnapper.AlignAndFocusPowderFromFiles(
            "Executing AlignAndFocusPowder...",
            Filename=rawDataPath,
            MaxChunkSize=5,
            # UnfocussedWorkspace="?",
            GroupingWorkspace="Column",
            CalibrationWorkspace=diffCalPrefix + "_cal",
            MaskWorkspace=diffCalPrefix + "_mask",
            #   Params="",
            DMin=DMin,
            DMax=DMax,
            DeltaRagged=DeltaRagged,
            #   ReductionProperties="?",
            OutputWorkspace="output",
        )

        # self.enqueueAlgorithm("AlignAndFocusPowderFromFiles", "Executing AlignAndFocusPowder...", False,
        #                       Filename=vanadiumFilePath,
        #                       MaxChunkSize=5,
        #                     # UnfocussedWorkspace="?",
        #                       GroupingWorkspace="Column",
        #                       CalibrationWorkspace=diffCalPrefix+"_cal",
        #                       MaskWorkspace=diffCalPrefix+"_mask",
        #                     #   Params="",
        #                       DMin=DMin,
        #                       DMax=DMax,
        #                       DeltaRagged=DeltaRagged,
        #                     #   ReductionProperties="?",
        #                       OutputWorkspace="vanadium")

        self.mantidSnapper.executeQueue()

        self.log().notice("Execution of AlignAndFocusReductionAlgorithm COMPLETE!")
        # return data

        # set outputworkspace to data


# Register algorithm with Mantid
AlgorithmFactory.subscribe(AlignAndFocusReductionAlgorithm)
