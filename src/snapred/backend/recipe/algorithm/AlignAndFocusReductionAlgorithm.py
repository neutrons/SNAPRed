from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.state import PixelGroupingParameters
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "AlignAndFocusReductionAlgorithm"


class AlignAndFocusReductionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("PixelGroupingParameters", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        reductionIngredients = ReductionIngredients.parse_raw(self.getProperty("ReductionIngredients").value)
        pixelGroupingParameters = PixelGroupingParameters.parse_raw(self.getProperty("PixelGroupingParameters").value)
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
        dMinList = []
        dMaxList = []
        dBinList = []
        for i in pixelGroupingParameters:
            dMinList.append(i.dResolution.minimum)
            dMaxList.append(i.dResolution.maximum)
            dBinList.append(i.dRelativeResolution)
        DMin: float = max(dMinList)
        DMax: float = min(dMaxList)
        DeltaRagged: float = min(dBinList) / reductionIngredients.reductionState.instrumentConfig.NBins

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
