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
    """

    The GenerateCalibrationMetricsWorkspaceRecipe is designed to compile calibration metrics into Mantid
    workspaces for scientific data analysis. Leveraging the MantidSnapper for its operations and taking
    CalibrationMetricsWorkspaceIngredients as input, it constructs a table workspace of calibration metrics
    based on the run ID and either a timestamp or calibration version. The class logs its operations and
    proceeds to transform this table into matrix workspaces that graphically represent sigma and strain against
    twoTheta values, allowing for a detailed visualization of calibration quality. Upon successful conversion,
    it cleans up interim workspaces, ensuring a streamlined workflow.

    """

    def __init__(self):
        self.mantidSnapper = MantidSnapper(self, self.__class__.__name__)

    def executeRecipe(self, ingredients: Ingredients):
        runId = ingredients.calibrationRecord.runNumber

        if ingredients.timestamp is not None:
            timestamp = str(ingredients.timestamp)
            ws_table = wng.diffCalTimedMetric().runNumber(runId).timestamp(timestamp).metricName("table").build()
            logger.info(f"Executing recipe {__name__} for run: {runId} timestamp: {timestamp}")
        else:
            version = str(ingredients.calibrationRecord.version)
            ws_table = wng.diffCalMetric().runNumber(runId).version(version).metricName("table").build()
            logger.info(f"Executing recipe {__name__} for run: {runId} calibration version: {version}")

        try:
            # create a table workspace with all metrics
            GenerateTableWorkspaceFromListOfDictRecipe().executeRecipe(
                ListOfDict=list_to_raw(ingredients.calibrationRecord.focusGroupCalibrationMetrics.calibrationMetric),
                OutputWorkspace=ws_table,
            )
            outputs = []
            # convert the table workspace to 2 matrix workspaces: sigma vs. twoTheta and strain vs. twoTheta
            for metric in ["sigma", "strain"]:
                if ingredients.timestamp is not None:
                    ws_metric = (
                        wng.diffCalTimedMetric().metricName(metric).runNumber(runId).timestamp(timestamp).build()
                    )
                else:
                    ws_metric = wng.diffCalMetric().metricName(metric).runNumber(runId).version(version).build()
                ConvertTableToMatrixWorkspaceRecipe().executeRecipe(
                    InputWorkspace=ws_table,
                    ColumnX="twoThetaAverage",
                    ColumnY=metric + "Average",
                    ColumnE=metric + "StandardDeviation",
                    OutputWorkspace=ws_metric,
                )
                outputs.append(ws_metric)
            DeleteWorkspace(Workspace=ws_table)
            logger.info("Finished generating calibration metrics workspace.")
            return outputs
        except (RuntimeError, AlgorithmException) as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
