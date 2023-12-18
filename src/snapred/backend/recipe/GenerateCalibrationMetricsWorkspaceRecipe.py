from snapred.backend.dao.ingredients import CalibrationMetricsWorkspaceIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.GenericRecipe import (
    ConvertTableToMatrixWorkspaceRecipe,
    GenerateTableWorkspaceFromListOfDictRecipe,
)
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


@Singleton
class GenerateCalibrationMetricsWorkspaceRecipe:
    def __init__(self):
        self.mantidSnapper = MantidSnapper(self, self.__class__.__name__)

    def executeRecipe(self, ingredients: Ingredients):
        runId = ingredients.calibrationRecord.runNumber
        print("MY RUN ID: " + str(runId))

        if ingredients.timestamp is not None:
            logger.info(f"Executing recipe {__name__} for run: {runId} timestamp: {ingredients.timestamp}")
            outputWS_stem = f"{runId}_calibrationMetrics_ts{ingredients.timestamp}"
        else:
            calibrationVersion = ingredients.calibrationRecord.version
            logger.info(f"Executing recipe {__name__} for run: {runId} calibration version: {calibrationVersion}")
            outputWS_stem = f"{runId}_calibrationMetrics_v{calibrationVersion}"

        tableWS = outputWS_stem + "_table"
        try:
            # create a table workspace with all metrics
            GenerateTableWorkspaceFromListOfDictRecipe().executeRecipe(
                ListOfDict=list_to_raw(ingredients.calibrationRecord.focusGroupCalibrationMetrics.calibrationMetric),
                OutputWorkspace=tableWS,
            )

            # convert the table workspace to 2 matrix workspaces: sigma vs. twoTheta and strain vs. twoTheta
            for metric in ["sigma", "strain"]:
                ConvertTableToMatrixWorkspaceRecipe().executeRecipe(
                    InputWorkspace=tableWS,
                    ColumnX="twoThetaAverage",
                    ColumnY=metric + "Average",
                    ColumnE=metric + "StandardDeviation",
                    OutputWorkspace=outputWS_stem + "_" + metric,
                )

            self.mantidSnapper.DeleteWorkspace(f"Deleting workspace {tableWS}", Workspace=tableWS)
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished generating calibration metrics workspace.")
