from snapred.backend.dao.calibration import (
    CalibrationRecord,
)
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.GenericRecipe import (
    AppendSpectraRecipe,
    ConvertTableToMatrixWorkspaceRecipe,
    GenerateTableWorkspaceFromListOfDictRecipe,
)
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


@Singleton
class GenerateCalibrationMetricsWorkspacesRecipe:
    def __init__(self):
        self.mantidSnapper = MantidSnapper(self, __name__)

    def executeRecipe(self, calibrationRecord: CalibrationRecord):
        runId = calibrationRecord.runNumber
        calibrationVersion = calibrationRecord.version
        logger.info(f"Executing recipe for run: {runId} calibration version: {calibrationVersion}")
        outputWS = f"{runId}_calibrationMetrics_v{calibrationVersion}"
        tableWS = outputWS + "_table"
        try:
            GenerateTableWorkspaceFromListOfDictRecipe().executeRecipe(
                ListOfDict=list_to_raw(calibrationRecord.focusGroupCalibrationMetrics.calibrationMetric),
                OutputWorkspace=tableWS,
            )

            outputWS1 = ConvertTableToMatrixWorkspaceRecipe().executeRecipe(
                InputWorkspace=tableWS,
                ColumnX="twoThetaAverage",
                ColumnY="sigmaAverage",
                ColumnE="sigmaStandardDeviation",
                OutputWorkspace=outputWS + "_sigma",
            )

            outputWS2 = ConvertTableToMatrixWorkspaceRecipe().executeRecipe(
                InputWorkspace=tableWS,
                ColumnX="twoThetaAverage",
                ColumnY="strainAverage",
                ColumnE="strainStandardDeviation",
                OutputWorkspace=outputWS + "_strain",
            )
            AppendSpectraRecipe().executeRecipe(
                InputWorkspace1=outputWS2, InputWorkspace2=outputWS1, OutputWorkspace=outputWS + "_matrix"
            )

        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished generating calibration metrics workspace.")
