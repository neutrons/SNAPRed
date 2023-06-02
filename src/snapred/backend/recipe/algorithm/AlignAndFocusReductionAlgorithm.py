from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ReductionIngredients import ReductionIngredients
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
        reductionIngredients = ReductionIngredients.parse_raw(self.getProperty("ReductionIngredients"))
        # run the algo
        self.log().notice("Execution of AlignAndFocusReductionAlgorithm START!")

        # TODO: Reorg how filepaths are stored
        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
        vanadiumFilePath = reductionIngredients.reductionState.stateConfig.vanadiumFilePath
        diffCalPath = reductionIngredients.reductionState.stateConfig.diffractionCalibrant.diffCalPath

        # raw_data = self.loadEventNexus(Filename=rawDataPath, OutputWorkspace="raw_data")
        self.mantidSnapper.LoadNexus("loading vanadium file...", Filename=vanadiumFilePath, OutputWorkspace="vanadium")

        self.mantidSnapper.CreateGroupWorkspace(
            "Creating group workspace...",
            StateConfig=reductionIngredients.reductionState.stateConfig,
            InstrumentName=reductionIngredients.reductionState.instrumentConfig.name,
        )

        # 3 ApplyDiffCal  -- just apply to data
        diffCalPrefix = self.mantidSnapper.LoadDiffCal(
            "Loading Diffcal...", Filename=diffCalPath, InputWorkspace="idf", WorkspaceName="diffcal"
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

        import yappi

        # Dont use wall time as Qt event loop runs for entire duration
        output_path = "./profile.out"
        yappi.set_clock_type("cpu")
        yappi.start(builtins=False, profile_threads=True, profile_greenlets=True)
        try:
            self.mantidSnapper.executeQueue()
        finally:
            # Qt will try to sys.exit so wrap in finally block before we go down
            yappi.stop()
            prof_data = yappi.get_func_stats()
            prof_data.save(output_path, type="callgrind")

        self.log().notice("Execution of AlignAndFocusReductionAlgorithm COMPLETE!")
        # return data

        # set outputworkspace to data


# Register algorithm with Mantid
AlgorithmFactory.subscribe(AlignAndFocusReductionAlgorithm)
