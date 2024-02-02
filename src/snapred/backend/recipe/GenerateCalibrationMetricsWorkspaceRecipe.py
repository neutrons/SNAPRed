from mantid.simpleapi import (
    DeleteWorkspace,
)

from snapred.backend.dao.ingredients import CalibrationMetricsWorkspaceIngredients as Ingredients
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.GenericRecipe import (
    ConvertTableToMatrixWorkspaceRecipe,
    GenerateTableWorkspaceFromListOfDictRecipe,
)
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


@Singleton
class GenerateCalibrationMetricsWorkspaceRecipe:
    def __init__(self):
        self.mantidSnapper = MantidSnapper(self, self.__class__.__name__)

    def executeRecipe(self, ingredients: Ingredients):
        runId = ingredients.calibrationRecord.runNumber

        if ingredients.timestamp is not None:
            calibVersionOrTimeStamp = "ts" + str(ingredients.timestamp)
            logger.info(f"Executing recipe {__name__} for run: {runId} timestamp: {calibVersionOrTimeStamp}")
        else:
            calibVersionOrTimeStamp = wng.formatVersion(str(ingredients.calibrationRecord.version))
            logger.info(f"Executing recipe {__name__} for run: {runId} calibration version: {calibVersionOrTimeStamp}")

        ws_table = wng.diffCalMetrics().runNumber(runId).version(calibVersionOrTimeStamp).metricName("table").build()

        try:
            # create a table workspace with all metrics
            GenerateTableWorkspaceFromListOfDictRecipe().executeRecipe(
                ListOfDict=list_to_raw(ingredients.calibrationRecord.focusGroupCalibrationMetrics.calibrationMetric),
                OutputWorkspace=ws_table,
            )

            # convert the table workspace to 2 matrix workspaces: sigma vs. twoTheta and strain vs. twoTheta
            for metric in ["sigma", "strain"]:
                ws_metric = (
                    wng.diffCalMetrics().metricName(metric).runNumber(runId).version(calibVersionOrTimeStamp).build()
                )
                ConvertTableToMatrixWorkspaceRecipe().executeRecipe(
                    InputWorkspace=ws_table,
                    ColumnX="twoThetaAverage",
                    ColumnY=metric + "Average",
                    ColumnE=metric + "StandardDeviation",
                    OutputWorkspace=ws_metric,
                )
            DeleteWorkspace(Workspace=ws_table)
        except (RuntimeError, AlgorithmException) as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished generating calibration metrics workspace.")
