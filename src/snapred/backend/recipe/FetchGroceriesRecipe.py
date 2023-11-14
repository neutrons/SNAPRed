from typing import Any, Dict, List, Tuple

from mantid.simpleapi import CloneWorkspace

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import FetchGroceriesAlgorithm as FetchAlgo
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import NameBuilder
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)


@Singleton
class FetchGroceriesRecipe:
    """
    You need some data?  Yeah, I can get that for you.  Just send me a list.
    """

    def __init__(self):
        self._loadedRuns: Dict[(str, str, bool), int] = {}

    def _runKey(self, runConfig: RunConfig) -> Tuple[str, str, bool]:
        return (runConfig.runNumber, runConfig.IPTS, runConfig.isLite)

    def _createFilenameFromRunConfig(self, runConfig: RunConfig) -> str:
        instr = "nexus.lite" if runConfig.isLite else "nexus.native"
        pre = instr + ".prefix"
        ext = instr + ".extension"
        return runConfig.IPTS + Config[pre] + str(runConfig.runNumber) + Config[ext]

    def _createNexusWorkspaceNameBuilder(self, runConfig: RunConfig) -> NameBuilder:
        runNameBuilder = wng.run().runNumber(runConfig.runNumber)
        if runConfig.isLite:
            runNameBuilder.lite(wng.Lite.TRUE)

    def _createNexusWorkspaceName(self, runConfig: RunConfig) -> str:
        return self._createNexusWorkspaceNameBuilder(runConfig).build()

    def _createRawNexusWorkspaceName(self, runConfig: RunConfig) -> str:
        return self._createNexusWorkspaceNameBuilder(runConfig).auxilary("Raw").build()

    def _createCopyNexusWorkspaceName(self, runConfig: RunConfig, numCopies: int) -> str:
        return self._createNexusWorkspaceNameBuilder(runConfig).auxilary(f"Copy{numCopies}").build()

    def _fetch(self, filename: str, workspace: str, loader: str = "") -> Dict[str, Any]:
        """
        Wraps the fetch groceries algorithm
        inputs:
        - filename, the path to the file where data is to be fetched
        - workspace, a string name for the workspace to load data into
        - loader, the loading algorithm to use, if you think you already know
        outputs a dictionary with
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time (is blank if loading skipped)
        - "workspace": the name of the workspace created in the ADS
        """
        data: Dict[str, Any] = {
            "result": False,
            "loader": loader,
        }
        logger.info(f"Fetching nexus data from {filename} into {workspace}")
        algo = FetchAlgo()
        algo.initialize()
        algo.setProperty("Filename", filename)
        algo.setProperty("OutputWorkspace", workspace)
        algo.setProperty("LoaderType", loader)
        try:
            data["result"] = algo.execute()
            data["loader"] = algo.getPropertyValue("LoaderType")
            data["workspace"] = workspace
        except RuntimeError as e:
            raise RuntimeError(str(e).split("\n")[0]) from e
        logger.info(f"Finished fetching nexus data into {workspace}")
        return data

    def fetchCleanNexusData(self, runConfig: RunConfig, loader: str = "") -> Dict[str, Any]:
        """
        Fetch a nexus data file, only if it is not loaded; otherwise clone a copy
        inputs:
        - runConfig, a RunConfig object corresponf to the data desired
        - loader, the loading algorithm to use, if you think you already know
        outputs a dictionary with
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time
        - "workspace": the name of the workspace created in the ADS
        """
        filename: str = self._createFilenameFromRunConfig(runConfig)
        rawWorkspaceName: str = self._createRawNexusWorkspaceName(runConfig)
        data: Dict[str, Any] = {
            "result": False,
            "loader": loader,
        }
        thisKey = self._runKey(runConfig)
        if self._loadedRuns.get(thisKey, 0) == 0:
            subdata = self._fetch(filename, rawWorkspaceName, loader)
            data["result"] = subdata["result"]
            data["loader"] = subdata["loader"]
            self._loadedRuns[thisKey] = 1
        else:
            logger.info(f"Data for run {runConfig.runNumber} already loaded... continuing")
            self._loadedRuns[thisKey] += 1
            data["loader"] = ""
            data["result"] = True
        workspaceName = self._createCopyNexusWorkspaceName(runConfig, self._loadedRuns[thisKey])
        CloneWorkspace(
            InputWorkspace=rawWorkspaceName,
            OutputWorkspace=workspaceName,
        )
        data["workspace"] = workspaceName
        return data

    def fetchDirtyNexusData(self, runConfig: RunConfig, loader: str = "") -> Dict[str, Any]:
        """
        Fetch a nexus data file to immediately use, without copy-protection
        inputs:
        - runConfig, a RunConfig object for the data desired
        - loader, the loading algorithm to use, if you think you already know
        outputs a dictionary with
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time
        - "workspace": the name of the workspace created in the ADS
        """
        filename: str = self._createFilenameFromRunConfig(runConfig)
        workspaceName: str = self._createNexusWorkspaceName(runConfig)
        data: Dict[str, Any] = {
            "result": False,
            "loader": loader,
            "workspace": workspaceName,
        }
        thisKey = self._runKey(runConfig)
        # check if a raw workspace exists, and clone it if so
        if self._loadedRuns.get(thisKey, 0) > 0:
            CloneWorkspace(
                InputWorkspace=self._createRawNexusWorkspaceName(runConfig),
                OutputWorkspace=workspaceName,
            )
            data["loader"] = ""  # unset the loader as a sign none was used
        # otherwise fetch the data
        else:
            subdata = self._fetch(filename, workspaceName, loader)
            data["result"] = subdata["result"]
            data["loader"] = subdata["loader"]
        return data

    def _createGroupingFilename(self, groupingScheme: str, isLite: bool = True):
        instr = "lite" if isLite else "native"
        home = "instrument.calibration.powder.grouping.home"
        pre = "grouping.filename.prefix"
        ext = "grouping.filename." + instr + ".extension"
        return Config[home] + "/" + Config[pre] + groupingScheme + Config[ext]

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
        algo.setProperty("OutputWorkspace", workspaceName)
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
                if item.keepItClean:
                    res = self.fetchCleanNexusData(item.runConfig, item.loader)
                else:
                    res = self.fetchDirtyNexusData(item.runConfig, item.loader)
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
