import os
from typing import Any, Dict, List, Tuple

from mantid.simpleapi import CloneWorkspace

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import FetchGroceriesAlgorithm as FetchAlgo
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo as LiteDataAlgo
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import NameBuilder
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)


@Singleton
class FetchGroceriesRecipe:
    """
    You need some data?
    Yeah, I can get that for you.
    Just send me a list.
    """

    def __init__(self):
        self._loadedRuns: Dict[(str, bool), int] = {}
        self._loadedGroupings: Dict[(str, bool), str] = {}

    def _runKey(self, runConfig: RunConfig) -> Tuple[str, bool]:
        return (runConfig.runNumber, runConfig.useLiteMode)

    def _createFilenameFromRunConfig(self, runConfig: RunConfig) -> str:
        instr = "nexus.lite" if runConfig.useLiteMode else "nexus.native"
        pre = instr + ".prefix"
        ext = instr + ".extension"
        return runConfig.IPTS + Config[pre] + str(runConfig.runNumber) + Config[ext]

    def _createNexusWorkspaceNameBuilder(self, runConfig: RunConfig) -> NameBuilder:
        runNameBuilder = wng.run().runNumber(runConfig.runNumber)
        if runConfig.useLiteMode:
            runNameBuilder.lite(wng.Lite.TRUE)
        return runNameBuilder

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

    def _makeLite(self, workspacename: str):
        liteMapKey = ("Lite", False)
        liteAlgo = LiteDataAlgo()
        liteAlgo.initialize()
        liteAlgo.setPropertyValue("InputWorkspace", workspacename)
        liteAlgo.setPropertyValue("OutputWorkspace", workspacename)
        if self._loadedGroupings.get(liteMapKey) is None:
            self.fetchGroupingDefinition(
                GroceryListItem(
                    workspaceType="grouping",
                    groupingScheme="Lite",
                    useLiteMode=False,
                    instrumentPropertySource="InstrumentFilename",
                    instrumentSource=str(Config["instrument.native.definition.file"]),
                )
            )
        liteAlgo.setPropertyValue("LiteDataMapWorkspace", self._loadedGroupings[liteMapKey])
        liteAlgo.execute()

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

        data: Dict[str, Any] = {
            "result": False,
            "loader": loader,
        }
        # treat Lite data specially -- this easier than extending the if/else block here
        if runConfig.useLiteMode:
            data = self.fetchCleanNexusDataLite(runConfig, loader)

        # if not Lite data, continue as below
        else:
            filename: str = self._createFilenameFromRunConfig(runConfig)
            rawWorkspaceName: str = self._createRawNexusWorkspaceName(runConfig)
            thisKey = self._runKey(runConfig)
            # if the raw data has not been loaded, first load it
            # if it has been loaded, increment its counts
            if self._loadedRuns.get(thisKey, 0) == 0:
                data = self._fetch(filename, rawWorkspaceName, loader)
                self._loadedRuns[thisKey] = 1
            else:
                logger.info(f"Data for run {runConfig.runNumber} already loaded... continuing")
                self._loadedRuns[thisKey] += 1
                data["loader"] = ""  # unset to indicate no loading occurred
                data["result"] = True
            # create a copy of the raw data for use
            workspaceName = self._createCopyNexusWorkspaceName(runConfig, self._loadedRuns[thisKey])
            CloneWorkspace(
                InputWorkspace=rawWorkspaceName,
                OutputWorkspace=workspaceName,
            )
            data["workspace"] = workspaceName
        return data

    def fetchCleanNexusDataLite(self, runConfig: RunConfig, loader: str = "") -> Dict[str, Any]:
        """
        Fetch a Lite nexus data file.
        In the SNAP instrument, Lite data is formed by summing 8x8 grids of pixels
        Leading to reduced resolution but enhanced imaging in the superpixels, and faster run times
        Lite data is formed by grouping and remapping pixels in the native resolution data, and is
        sometimes saved to disk.
        If the Lite data exists in cache, it will clone from it.
        If a Lite file exists, it will open that.
        If a Lite file does not exist, but the non-Lite version is already loaded, it will reduce that
        If a Lite file does not exist and non-Lite version not loaded, it will load the native data and reduce it
        inputs:
        - runConfig, a RunConfig object corresponf to the data desired
        - loader, the loading algorithm to use, if you think you already know
        outputs a dictionary with
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time
        - "workspace": the name of the workspace created in the ADS
        """

        data: Dict[str, Any] = {
            "result": False,
            "loader": loader,
        }

        rawWorkspaceName: str = self._createRawNexusWorkspaceName(runConfig)
        filename: str = self._createFilenameFromRunConfig(runConfig)
        key = self._runKey(runConfig)

        loadedFromNative: bool = False

        # if the lite data exists in cache, copy it
        if self._loadedRuns.get(key, 0) > 0:
            data["result"] = True
            data["loader"] = ""  # unset to indicate no loading occurred
            self._loadedRuns[key] += 1

        # the lite data is not cached, but the lite file exists
        # then load the lite file
        elif os.path.isfile(filename):
            data = self._fetch(filename, rawWorkspaceName, loader)
            self._loadedRuns[key] = 1

        # the lite data is not cached and lite file does not exist,
        # but native data exists in cache, clone it then reduce it
        elif self._loadedRuns.get((runConfig, False), 0) > 0:
            # this can be accomplished by changing rawWorkspaceName to name of native workspace
            nativeRunConfig = RunConfig(
                runNumber=runConfig.runNumber,
                IPTS=runConfig.IPTS,
                useLiteMode=False,
            )
            # use the native raw workspace as the raw workspace to copy
            rawWorkspaceName = self._createRawNexusWorkspaceName(nativeRunConfig)
            data["result"] = True
            data["loader"] = ""
            self._loadedRuns[(runConfig, False)] += 1
            loadedFromNative = True

        # neither lite nor native data in cache and lite file does not exist
        # then load native data and reduce it
        else:
            nativeRunConfig = RunConfig(
                runNumber=runConfig.runNumber,
                IPTS=runConfig.IPTS,
                useLiteMode=False,
            )
            # use the native resolution data
            rawWorkspaceName = self._createRawNexusWorkspaceName(nativeRunConfig)
            filename = self._createFilenameFromRunConfig(nativeRunConfig)
            data = self._fetch(filename, rawWorkspaceName, loader)
            loadedFromNative = True

        workspaceName = self._createCopyNexusWorkspaceName(runConfig, self._loadedRuns[key])
        CloneWorkspace(
            InputWorkspace=rawWorkspaceName,
            OutputWorkspace=workspaceName,
        )
        # if lite data is specified, create a lite workspace
        if loadedFromNative:
            self._makeLite(workspaceName)
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
        # check if a raw workspace exists, and clone it if so
        if self._loadedRuns.get(self._runKey(runConfig), 0) > 0:
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
        # if lite data is specified, create a lite workspace
        if runConfig.useLiteMode:
            self._makeLite(workspaceName)
        return data

    def _createGroupingFilename(self, groupingScheme: str, useLiteMode: bool = True):
        if groupingScheme == "Lite":
            return str(Config["instrument.lite.map.file"])
        instr = "lite" if useLiteMode else "native"
        home = "instrument.calibration.powder.grouping.home"
        pre = "grouping.filename.prefix"
        ext = "grouping.filename." + instr + ".extension"
        return Config[home] + "/" + Config[pre] + groupingScheme + Config[ext]

    def _createGroupingWorkspaceName(self, groupingScheme: str, useLiteMode: bool = True):
        if groupingScheme == "Lite":
            return "lite_grouping_map"
        instr = "lite" if useLiteMode else "native"
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
        workspaceName = self._createGroupingWorkspaceName(item.groupingScheme, item.useLiteMode)
        fileName = self._createGroupingFilename(item.groupingScheme, item.useLiteMode)
        logger.info("Fetching grouping definition: %s" % item.groupingScheme)
        groupingLoader = "LoadGroupingDefinition"
        data: Dict[str, Any] = {
            "result": False,
            "loader": groupingLoader,
            "workspace": workspaceName,
        }

        key = (item.groupingScheme, item.useLiteMode)
        if self._loadedGroupings.get(key) is None:
            algo = FetchAlgo()
            algo.initialize()
            algo.setPropertyValue("Filename", fileName)
            algo.setPropertyValue("OutputWorkspace", workspaceName)
            algo.setPropertyValue("LoaderType", "LoadGroupingDefinition")
            algo.setPropertyValue(str(item.instrumentPropertySource), item.instrumentSource)
            try:
                data["result"] = algo.execute()
                self._loadedGroupings[key] = workspaceName
            except RuntimeError as e:
                raise RuntimeError(str(e).split("\n")[0]) from e
        else:
            data["result"] = True
            data["loader"] = ""  # unset the loader to indicate it was unused
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
        prev: str = ""
        for item in groceries:
            if item.workspaceType == "nexus":
                if item.keepItClean:
                    res = self.fetchCleanNexusData(item.runConfig, item.loader)
                else:
                    res = self.fetchDirtyNexusData(item.runConfig, item.loader)
                data["result"] &= res["result"]
                data["workspaces"].append(res["workspace"])
                prev = res["workspace"]
            elif item.workspaceType == "grouping":
                # if we intend to use the previously loaded workspace as the donor
                if item.instrumentSource == "prev":
                    item.instrumentPropertySource = "InstrumentDonor"
                    item.instrumentSource = prev
                res = self.fetchGroupingDefinition(item)
                data["result"] &= res["result"]
                data["workspaces"].append(res["workspace"])
        return data
