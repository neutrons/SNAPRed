from typing import Any, Dict, List

from mantid.simpleapi import CloneWorkspace

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import FetchGroceriesAlgorithm as FetchAlgo
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class FetchGroceriesRecipe:
    """
    You need some data?  Yeah, I can get that for you.  Just send me a list.
    """

    def __init__(self):
        self._loadedRuns: Dict[(str, str, bool), int] = {}

    def _createFilenameFromRunConfig(self, runConfig: RunConfig) -> str:
        instr = "nexus.lite" if runConfig.isLite else "nexus.native"
        home = instr + ".home"
        pre = instr + ".prefix"
        ext = instr + ".extension"
        return Config[home] + Config[pre] + str(runConfig.runNumber) + Config[ext]

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
        outputsa dictionary with
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
        thisKey = (runConfig.runNumber, runConfig.IPTS, runConfig.isLite)
        if self._loadedRuns.get(thisKey, 0) > 0:
            newWorkspaceName = f"{workspaceName}_{self._loadedRuns[thisKey]}"
            self._loadedRuns[thisKey] += 1
            logger.info("Data already loaded... continuing")
            # TODO: does this really need to be cloned?  If algo's always clone, not necessary here
            CloneWorkspace(
                InputWorkspace=workspaceName,
                OutputWorkspace=newWorkspaceName,
            )
            data["result"] = True
            data["workspace"] = newWorkspaceName
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
            self._loadedRuns[thisKey] = 1
        return data

    def _createGroupingFilename(self, groupingScheme: str, isLite: bool = True):
        instr = "lite" if isLite else "native"
        home = "instrument.calibration.powder.grouping.home"
        pre = "grouping.filename.prefix"
        ext = "grouping.filename." + instr + ".extension"
        return Config[home] + Config[pre] + groupingScheme + Config[ext]

    def _createGroupingWorkspaceName(self, groupingScheme: str, isLite: bool = True):
        instr = "lite" if isLite else "native"
        return Config["grouping.workspacename." + instr] + groupingScheme

    def fetchGroupingDefinition(self, item: GroceryListItem) -> str:
        """
        Fetch just a grouping definition.
        inputs:
        - item, a GroceryListItem
        outputs a dictionayr with
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
        outputs a dictionary with:
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
