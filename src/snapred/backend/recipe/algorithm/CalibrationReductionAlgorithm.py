import json

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CalibrationReductionAlgorithm"


class CalibrationReductionAlgorithm(PythonAlgorithm):
    mantidSnapper = MantidSnapper(name)

    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)

    def PyExec(self):
        reductionIngredients = ReductionIngredients(**json.loads(self.getProperty("ReductionIngredients").value))
        # run the algo
        self.log().notice("Execution of CalibrationReductionAlgorithm START!")

        # TODO: Reorg how filepaths are stored
        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
        calibrationDirectory = reductionIngredients.reductionState.instrumentConfig.calibrationDirectory
        stateId = reductionIngredients.reductionState.stateConfig.stateId
        rawVanadiumCorrectionFileName = reductionIngredients.reductionState.stateConfig.rawVanadiumCorrectionFileName
        calibrationDirectory + stateId + "/powder/" + rawVanadiumCorrectionFileName
        diffCalPath = (
            calibrationDirectory
            + stateId
            + "/powder/"
            + reductionIngredients.reductionState.stateConfig.diffractionCalibrant.filename
        )

        raw_data = self.mantidSnapper.LoadEventNexus(
            "Loading Event Nexus for {} ...".format(rawDataPath), Filename=rawDataPath, OutputWorkspace="raw_data"
        )

        # 4 Not Lite? SumNeighbours  -- just apply to data
        # self.sumNeighbours(InputWorkspace=raw_data, SumX=SuperPixEdge, SumY=SuperPixEdge, OutputWorkspace=raw_data)

        # 7 Does it have a container?
        # Apply Container Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data and vanadium
        # self.applyCotainerMask()
        # 8 CreateGroupWorkspace      TODO: Assess performance, use alternative Andrei came up with that is faster
        groupingworkspace = self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=reductionIngredients.reductionState.stateConfig.json(),
            InstrumentName=reductionIngredients.reductionState.instrumentConfig.name,
            OutputWorkspace="CommonRed",
        )

        # 3 ApplyDiffCal  -- just apply to data
        diffCalPrefix = self.mantidSnapper.LoadDiffCal(
            "Loading DiffCal for {} ...".format(diffCalPath), Filename=diffCalPath, WorkspaceName="diffcal"
        )

        # 6 Apply Calibration Mask to Raw Vanadium and Data output from SumNeighbours --
        # done to both data, can be applied to vanadium per state
        self.mantidSnapper.ApplyCalibrationPixelMask(
            "Applying Pixel Mask...", Workspace=raw_data, MaskedWorkspace=diffCalPrefix + "_mask"
        )

        self.mantidSnapper.ApplyDiffCal(
            "Applying DiffCal...", InstrumentWorkspace=raw_data, CalibrationWorkspace=diffCalPrefix + "_cal"
        )

        self.mantidSnapper.DeleteWorkspace("Deleting DiffCal Mask", Workspace=diffCalPrefix + "_mask")
        self.mantidSnapper.DeleteWorkspace("Deleting DiffCal Calibration", Workspace=diffCalPrefix + "_cal")
        self.mantidSnapper.DeleteWorkspace("Deleting IDF", Workspace="idf")

        # 9 Does it have a container? Apply Container Attenuation Correction
        data = self.mantidSnapper.ConvertUnits(
            "Converting to Units of dSpacing ...",
            InputWorkspace=raw_data,
            EMode="Elastic",
            Target="dSpacing",
            OutputWorkspace="data",
            ConvertFromPointData=True,
        )

        # TODO: May impact performance of lite mode data
        # TODO: Params is supposed to be smallest dmin, smalled dbin, largest dmax
        # self.enqueueAlgorithm('Rebin',
        # "Rebinning",
        # isChild=False,
        # InputWorkspace=data,
        # Params='0.338, -0.00086, 5.0',
        # PreserveEvents=False,
        # OutputWorkspace="rebinned_data_before_focus")

        # data = "rebinned_data_before_focus"
        # vanadium = self.enqueueAlgorithm('Rebin',
        # "Rebinning",
        # isChild=False,
        # InputWorkspace=vanadium,
        # Params='0.338, -0.00086, 5.0',
        # PreserveEvents=False,
        # OutputWorkspace="rebinned_vanadium_before_focus")
        # vanadium = "rebinned_vanadium_before_focus"
        # 11 For each Group (no for each loop, the algos apply things based on groups of group workspace)
        data = self.mantidSnapper.DiffractionFocusing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=data,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="focused_data",
        )

        self.mantidSnapper.executeQueue()

        self.log().notice("Execution of CalibrationReductionAlgorithm COMPLETE!")
        return data

        # set outputworkspace to data


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationReductionAlgorithm)
