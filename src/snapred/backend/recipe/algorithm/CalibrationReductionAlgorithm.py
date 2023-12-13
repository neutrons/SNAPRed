import json

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.recipe.algorithm.CustomGroupWorkspace import CustomGroupWorkspace  # noqa F401
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class CalibrationReductionAlgorithm(PythonAlgorithm):
    def category(self):
        return "SNAPRed Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def PyExec(self):
        reductionIngredients = ReductionIngredients.parse_raw(self.getProperty("ReductionIngredients").value)
        # run the algo
        self.log().notice("Execution of CalibrationReductionAlgorithm START!")

        # TODO: Reorg how filepaths are stored
        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
        diffCalPath = reductionIngredients.reductionState.stateConfig.diffractionCalibrant.diffCalPath

        raw_data = self.mantidSnapper.LoadEventNexus(
            "Loading Event Nexus for {} ...".format(rawDataPath),
            Filename=rawDataPath,
            FilterByTofMin=2000,
            FilterByTofMax=14500,
            OutputWorkspace="raw_data",
        )

        self.mantidSnapper.Rebin(
            "Rebinning to 2000 bins logarithmic...",
            InputWorkspace=raw_data,
            OutputWorkspace=raw_data,
            Params="2000,-0.001,14500",
        )

        groupingworkspace = self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=reductionIngredients.reductionState.stateConfig.json(),
            InputWorkspace=raw_data,
            OutputWorkspace="CommonRed",
        )

        diffCalPrefix = "diffcal"
        self.mantidSnapper.LoadDiffCal(
            "Loading DiffCal for {} ...".format(diffCalPath),
            InstrumentFilename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
            MakeGroupingWorkspace=False,
            MakeMaskWorkspace=True,
            Filename=diffCalPath,
            WorkspaceName=diffCalPrefix,
        )

        self.mantidSnapper.MaskDetectors(
            "Applying Pixel Mask...", Workspace=raw_data, MaskedWorkspace=diffCalPrefix + "_mask"
        )

        self.mantidSnapper.ApplyDiffCal(
            "Applying DiffCal...", InstrumentWorkspace=raw_data, CalibrationWorkspace=diffCalPrefix + "_cal"
        )

        self.mantidSnapper.WashDishes(
            "Deleting DiffCal Mask",
            WorkspaceList=[diffCalPrefix + "_mask", diffCalPrefix + "_cal"],
        )

        data = self.mantidSnapper.ConvertUnits(
            "Converting to Units of dSpacing ...",
            InputWorkspace=raw_data,
            EMode="Elastic",
            Target="dSpacing",
            OutputWorkspace="data",
            ConvertFromPointData=True,
        )
        self.mantidSnapper.WashDishes("Deleting Raw Data", Workspace=raw_data)

        focused_data = self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=data,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="focused_data",
            PreserveEvents=False,
        )
        self.mantidSnapper.NormaliseByCurrent(
            "Normalizing Current ...", InputWorkspace=focused_data, OutputWorkspace=focused_data
        )
        self.mantidSnapper.RemoveLogs("Removing Logs...", Workspace=focused_data)
        self.mantidSnapper.WashDishes(
            "Deleting Intermediate Data and Grouping Workspace",
            WorkspaceList=[data, groupingworkspace],
        )

        # Rename focused_data to runid_calibration_reduction_result
        outputNameFormat = Config["calibration.reduction.output.format"]
        self.mantidSnapper.RenameWorkspace(
            "Renaming calibration result...",
            InputWorkspace=focused_data,
            OutputWorkspace=outputNameFormat.format(reductionIngredients.runConfig.runNumber),
        )
        self.mantidSnapper.executeQueue()

        self.log().notice("Execution of CalibrationReductionAlgorithm COMPLETE!")
        return data

        # set outputworkspace to data


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationReductionAlgorithm)
