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
    """
    You need some data?  Yeah, I can get that for you.  Just send me a list.
    """

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
        return name

    def fetchNexusData(self, runConfig: RunConfig, loader: str = "") -> Dict[str, Any]:
        """
        Fetch just a nexus data file
        inputs:
        - runConfig, a RunConfig object corresponf to the data desired
        - loader, the loading algorithm to use, if you think you already know
        outputs:
        - data, a dictionary with
          - "result": true if everything ran correctly
          - "loader": the loader that was used by the algorithm, use it next time
          - "workspace": the name of the workspace created in the ADS
        """

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
            data["alreadyLoaded"] = True
        else:
            logger.info(f"Fetching nexus data for run {runConfig.runNumber} at {fileName}")
            algo = FetchAlgo()
            algo.initialize()
            algo.setProperty("Filename", fileName)
            algo.setProperty("Workspace", workspaceName)
            algo.setProperty("LoaderType", loader)
            try:
                data["result"] = algo.execute()
                data["loader"] = algo.getPropertyValue("LoaderType")
            except RuntimeError as e:
                raise RuntimeError(str(e).split("\n")[0]) from e
            logger.info("Finished loading nexus data")
            self._loadedRuns.append(runConfig)
        return data

    def _createGroupingFilename(self, groupingScheme: str, isLite: bool = True):
        location = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/"
        if isLite:
            return f"{location}SNAPFocGroup_{groupingScheme}.lite.nxs"
        else:
            return f"{location}SNAPFocGroup_{groupingScheme}.xml"

    def _createGroupingWorkspaceName(self, groupingScheme: str, isLite: bool = True):
        if isLite:
            return f"SNAPLite_grouping_{groupingScheme}"
        else:
            return f"SNAP_grouping_{groupingScheme}"

    def fetchGroupingDefinition(self, item: GroceryListItem) -> str:
        """
        Fetch just a grouping definition.
        inputs:
        - item, a GroceryListItem
        outputs:
        - data, a dictionayr with
          - "result", true if everything ran correctly
          - "loader", just "LoadGroupingDefinition" with no apologies
          - "workspaceName", the name of the new grouping workspace in the ADS
        """

        workspaceName = self._createGroupingWorkspaceName(item.groupingScheme, item.isLite)
        fileName = self._createGroupingFilename(item.groupingScheme, item.isLite)
        logger.info("Fetching grouping definition: %s" % item.groupingScheme)
        data: Dict[str, Any] = {
            "result": False,
            "loader": "LoadGroupingDefinition",
            "workspace": workspaceName,
        }
        algo = FetchAlgo()
        algo.initialize()
        algo.setProperty("Filename", fileName)
        algo.setProperty("Workspace", workspaceName)
        algo.setProperty("LoaderType", "LoadGroupingDefinition")
        algo.setProperty(item.instrumentPropertySource, item.instrumentSource)
        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            raise RuntimeError(str(e).split("\n")[0]) from e
        logger.info("Finished loading grouping")
        return data

    def executeRecipe(self, groceries: List[GroceryListItem]) -> Dict[str, Any]:
        """
        inputs:
        - groceries, a list of data files to create
        outputs:
        - data, a dictionary with:
          - "result" (True if everything good, otherwise False)
          - "workspaces" a list of strings with all workspace names created in the ADS
        """

        data: Dict[str, Any] = {
            "result": True,
            "workspaces": [],
        }
        for item in groceries:
            if item.workspaceType == "nexus":
                res = self.fetchNexusData(item.runConfig, item.loader)
                data["result"] &= res["result"]
                data["workspaces"].append(res["workspace"])
            elif item.workspaceType == "grouping":
                # if we intend to use the previously loaded workspace as the donor
                if item.instrumentSource == "prev":
                    item.instrumentPropertySource = "InstrumentDonor"
                    item.instrumentSource = data["workspaces"][-1]
                res = self.fetchGroupingDefinition(item)
                data["result"] &= res["result"]
                data["workspaces"].append(res["workspace"])
        return data
