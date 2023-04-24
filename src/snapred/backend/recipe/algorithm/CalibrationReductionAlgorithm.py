import json

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CalibrationReductionAlgorithm"


class CalibrationReductionAlgorithm(PythonAlgorithm):

    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

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
        vanadiumFilePath = calibrationDirectory + "Powder/" + stateId + rawVanadiumCorrectionFileName
        diffCalPath = (
            calibrationDirectory
            + "Powder/" 
            + stateId
            + reductionIngredients.reductionState.stateConfig.diffractionCalibrant.filename
        )


        raw_data = self.mantidSnapper.LoadEventNexus(
            "Loading Event Nexus for {} ...".format(rawDataPath), Filename=rawDataPath, FilterByTofMin=2000, FilterByTofMax=14500, OutputWorkspace="raw_data"
        )

        self.mantidSnapper.Rebin("Rebinning to 2000 bins logarithmic...", InputWorkspace=raw_data, OutputWorkspace=raw_data, Params='2000,-0.001,14500')

        groupingworkspace = self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=reductionIngredients.reductionState.stateConfig.json(),
            InstrumentName=reductionIngredients.reductionState.instrumentConfig.name,
            OutputWorkspace="CommonRed",
        )

        diffCalPrefix = "diffcal"
        self.mantidSnapper.LoadDiffCal(
            "Loading DiffCal for {} ...".format(diffCalPath),
            InstrumentFilename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
            MakeGroupingWorkspace=False,
            MakeMaskWorkspace=True, 
            Filename=diffCalPath, 
            WorkspaceName=diffCalPrefix
        )

        self.mantidSnapper.MaskDetectors(
            "Applying Pixel Mask...", Workspace=raw_data, MaskedWorkspace=diffCalPrefix + "_mask"
        )

        self.mantidSnapper.ApplyDiffCal(
            "Applying DiffCal...", InstrumentWorkspace=raw_data, CalibrationWorkspace=diffCalPrefix + "_cal"
        )

        self.mantidSnapper.DeleteWorkspace("Deleting DiffCal Mask", Workspace=diffCalPrefix + "_mask")
        self.mantidSnapper.DeleteWorkspace("Deleting DiffCal Calibration", Workspace=diffCalPrefix + "_cal")
        self.mantidSnapper.DeleteWorkspace("Deleting IDF", Workspace="idf")

        data = self.mantidSnapper.ConvertUnits(
            "Converting to Units of dSpacing ...",
            InputWorkspace=raw_data,
            EMode="Elastic",
            Target="dSpacing",
            OutputWorkspace="data",
            ConvertFromPointData=True,
        )
        self.mantidSnapper.DeleteWorkspace("Deleting Raw Data", Workspace=raw_data)

        focused_data = self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=data,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="focused_data",
            PreserveEvents=False
        )
        self.mantidSnapper.NormaliseByCurrent("Normalizing Current ...", InputWorkspace=focused_data, OutputWorkspace=focused_data)
        self.mantidSnapper.RemoveLogs("Removing Logs...", Workspace=focused_data)
        self.mantidSnapper.DeleteWorkspace("Deleting Intermediate Data", Workspace=data)
        self.mantidSnapper.DeleteWorkspace("Deleting Grouping Workspace", Workspace=groupingworkspace)


        self.mantidSnapper.executeQueue()

        self.log().notice("Execution of CalibrationReductionAlgorithm COMPLETE!")
        return data

        # set outputworkspace to data


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationReductionAlgorithm)