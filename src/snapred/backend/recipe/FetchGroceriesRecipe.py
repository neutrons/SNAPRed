from typing import Any, Dict, List

from mantid.api import AlgorithmManager

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import FetchGroceriesAlgorithm as FetchAlgo
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class FetchGroceriesRecipe:
    def __init__(self):
        self._loadedRuns: List[RunConfig] = []

    def _createFilenameFromRunConfig(self, runConfig: RunConfig) -> str:
        if runConfig.isLite:
            return f"{runConfig.IPTS}shared/lite/SNAP_{runConfig.runNumber}.lite.nxs.h5"
        else:
            return f"{runConfig.IPTS}nexus/SNAP_{runConfig.runNumber}.nxs.h5"

    def _createNexusWorkspaceName(self, runConfig) -> str:
        name: str = f"_TOF_RAW_{runConfig.runNumber}"
        if runConfig.isLite:
            name = name + "_lite"

    def fetchNexusData(self, runConfig: RunConfig, loader: str = "") -> Dict[str, Any]:
        workspaceName = self._createNexusWorkspaceName(runConfig)
        fileName = self._createFilenameFromRunConfig(runConfig)
        data: Dict[str, Any] = {
            "result": False,
            "loader": loader,
            "workspace": workspaceName,
        }
        if runConfig in self._loadedRuns:
            logger.info("Data already loaded... continuing")
            data["result"] = True
        else:
            logger.info("Fetching nexus data for run: %s at %s" % runConfig.runNumber, fileName)
            algo = AlgorithmManager.create(FetchAlgo.__name__)
            algo.setProperty("Filename", fileName)
            algo.setProperty("Workspace", workspaceName)
            algo.setProperty("LoaderType", loader)
            try:
                data["result"] = algo.execute()
                data["loader"] = algo.getProperty("LoaderType")
                data["workspace"] = workspaceName
            except RuntimeError as e:
                errorString = str(e)
                raise Exception(errorString.split("\n")[0])
            logger.info("Finished loading nexus data")
            self._loadedRuns.append(runConfig)
        return data

    def _createGroupingFilename(self, groupingScheme: str, isLite: bool = True):
        filepath = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/"
        if isLite:
            return f"{filepath}SNAPFocGroup_{groupingScheme}.lite.nxs"
        else:
            return f"{filepath}SNAPFocGroup_{groupingScheme}.xml"

    def _createGroupingWorkspaceName(self, groupingScheme: str, isLite: bool = True):
        if isLite:
            return f"SNAPLite_grouping_{groupingScheme}"
        else:
            return f"SNAP_grouping_{groupingScheme}"

    def fetchGroupingDefinition(self, groupingScheme: str, isLite: bool = True) -> str:
        workspaceName = self._createGroupingWorkspaceName(groupingScheme, isLite)
        fileName = self._createGroupingFilename(groupingScheme, isLite)
        logger.info("Fetching grouping definition: %s" % groupingScheme)
        data: Dict[str, Any] = {
            "result": False,
            "loader": "LoadGroupingDefinition",
            "workspaceName": workspaceName,
        }
        algo = AlgorithmManager.create(FetchAlgo.__name__)
        algo.setProperty("Filename", fileName)
        algo.setProperty("Workspace", workspaceName)
        algo.setProperty("LoaderType", "LoadGroupingDefinition")
        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished loading grouping")
        return data

    def executeRecipe(self, groceries: List[GroceryListItem]) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "result": True,
            "workspaces": [],
        }
        for item in groceries:
            if item.workspaceType == "nexus":
                res = self.fetchNexusData(item.runConfig, item.loader)
                data["result"] &= res["result"]
                data["workspaces"].append(res["workspaceName"])
            elif item.workspaceType == "grouping":
                res = self.fetchGroupingDefinition(item.groupingScheme, item.isLite)
                data["result"] &= res["result"]
                data["workspaces"].append(res["workspaceName"])
            return data
