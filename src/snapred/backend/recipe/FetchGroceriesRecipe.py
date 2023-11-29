import os
from typing import Any, Dict, List, Tuple

from mantid.simpleapi import CloneWorkspace

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
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

    def _key(self, item: GroceryListItem) -> Tuple[str, bool]:
        if item.workspaceType == "nexus":
            return (item.runNumber, item.useLiteMode)
        elif item.workspaceType == "grouping":
            return (item.groupingScheme, item.useLiteMode)

    ## FILENAME METHODS

    def _createNexusFilename(self, item: GroceryListItem) -> str:
        instr = "nexus.lite" if item.useLiteMode else "nexus.native"
        pre = instr + ".prefix"
        ext = instr + ".extension"
        return item.IPTS + Config[pre] + str(item.runNumber) + Config[ext]

    def _createGroupingFilename(self, item: GroceryListItem):
        if item.groupingScheme == "Lite":
            return str(Config["instrument.lite.map.file"])
        instr = "lite" if item.useLiteMode else "native"
        home = "instrument.calibration.powder.grouping.home"
        pre = "grouping.filename.prefix"
        ext = "grouping.filename." + instr + ".extension"
        return Config[home] + "/" + Config[pre] + item.groupingScheme + Config[ext]

    ## WORKSPACE NAME METHODS

    def _createNexusWorkspaceNameBuilder(self, item: GroceryListItem) -> NameBuilder:
        runNameBuilder = wng.run().runNumber(item.runNumber)
        if item.useLiteMode:
            runNameBuilder.lite(wng.Lite.TRUE)
        return runNameBuilder

    def _createNexusWorkspaceName(self, item: GroceryListItem) -> str:
        return self._createNexusWorkspaceNameBuilder(item).build()

    def _createRawNexusWorkspaceName(self, item: GroceryListItem) -> str:
        return self._createNexusWorkspaceNameBuilder(item).auxilary("Raw").build()

    def _createCopyNexusWorkspaceName(self, item: GroceryListItem, numCopies: int) -> str:
        return self._createNexusWorkspaceNameBuilder(item).auxilary(f"Copy{numCopies}").build()

    def _createGroupingWorkspaceName(self, item: GroceryListItem):
        if item.groupingScheme == "Lite":
            return "lite_grouping_map"
        instr = "lite" if item.useLiteMode else "native"
        return Config["grouping.workspacename." + instr] + item.groupingScheme

    ## FETCH METHODS

    def executeRecipe(self, groceries: List[GroceryListItem]) -> Dict[str, Any]:
        """
        inputs:
        - groceries -- a list of GroceryListItems indicating the workspaces to create
        outputs a dictionary with keys:
        - "result": bool, True if everything good, otherwise False
        - "workspaces": a list of strings workspace names created in the ADS
        """

        data: Dict[str, Any] = {
            "result": True,
            "workspaces": [],
        }
        prev: str = ""
        for item in groceries:
            if item.workspaceType == "nexus":
                if item.keepItClean:
                    res = self.fetchCleanNexusData(item)
                else:
                    res = self.fetchDirtyNexusData(item)
                data["result"] &= res["result"]
                data["workspaces"].append(res["workspace"])
                # save the most recently-loaded nexus data as a possible instrument donor for groupings
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

    def _fetch(self, filename: str, workspace: str, loader: str = "") -> Dict[str, Any]:
        """
        Wraps the fetch groceries algorithm
        inputs:
        - filename -- the path to the file where data is to be fetched
        - workspace -- a string name for the workspace to load data into
        - loader -- the loading algorithm to use, if you think you already know
        outputs a dictionary with keys
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time (is blank if loading skipped)
        - "workspace": the name of the workspace created in the ADS
        """
        data: Dict[str, Any] = {
            "result": False,
            "loader": "",
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

    def fetchCleanNexusData(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch a nexus data file, only if it is not loaded; otherwise clone a copy
        inputs:
        - item -- a GroceryListItem specifying the run number of the data to load
        outputs a dictionary with keys
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time
        - "workspace": the name of the workspace created in the ADS
        """

        data: Dict[str, Any] = {
            "result": False,
            "loader": "",
        }
        # treat Lite data specially -- this is easier than extending the if/else block here
        if item.useLiteMode:
            data = self.fetchCleanNexusDataLite(item)

        # if not Lite data, continue as below
        else:
            filename: str = self._createNexusFilename(item)
            rawWorkspaceName: str = self._createRawNexusWorkspaceName(item)
            thisKey = self._key(item)
            # if the raw data has not been loaded, first load it
            # if it has been loaded, increment its counts
            if self._loadedRuns.get(thisKey) is None:
                data = self._fetch(filename, rawWorkspaceName, item.loader)
                self._loadedRuns[thisKey] = 1
            else:
                logger.info(f"Data for run {item.runNumber} already loaded... continuing")
                self._loadedRuns[thisKey] += 1
                data["loader"] = "cached"  # unset to indicate no loading occurred
                data["result"] = True
            # create a copy of the raw data for use
            workspaceName = self._createCopyNexusWorkspaceName(item, self._loadedRuns[thisKey])
            CloneWorkspace(
                InputWorkspace=rawWorkspaceName,
                OutputWorkspace=workspaceName,
            )
            data["workspace"] = workspaceName
        return data

    def fetchDirtyNexusData(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch a nexus data file to immediately use, without copy-protection
        inputs:
        - item -- a GroceryListItem with the run number of the data to load
        outputs a dictionary with keys
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time
        - "workspace": the name of the workspace created in the ADS
        """

        # if keepItClean was set to True, do not use this method, but the clean version
        if item.keepItClean:
            return self.fetchCleanNexusData(item)

        filename: str = self._createNexusFilename(item)
        workspaceName: str = self._createNexusWorkspaceName(item)
        data: Dict[str, Any] = {
            "result": False,
            "loader": "",
            "workspace": workspaceName,
        }
        # check if a raw workspace exists, and clone it if so
        if self._loadedRuns.get(self._key(item), 0) > 0:
            CloneWorkspace(
                InputWorkspace=self._createRawNexusWorkspaceName(item),
                OutputWorkspace=workspaceName,
            )
            data["loader"] = "cached"  # unset the loader as a sign none was used
        # otherwise fetch the data
        else:
            subdata = self._fetch(filename, workspaceName, item.loader)
            data["result"] = subdata["result"]
            data["loader"] = subdata["loader"]
        # if lite data is specified, create a lite workspace
        if item.useLiteMode:
            self._makeLite(workspaceName)
        return data

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
        workspaceName = self._createGroupingWorkspaceName(item)
        fileName = self._createGroupingFilename(item)
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
            data["loader"] = "cached"  # unset the loader to indicate it was unused
        logger.info("Finished loading grouping")
        return data

    ## FOR FETCHING LITE MODE DATA

    def fetchCleanNexusDataLite(self, item: GroceryListItem) -> Dict[str, Any]:
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
        - "item": a GroceryListItem specifying the run number of the nexus data to load
        outputs a dictionary with
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time
        - "workspace": the name of the workspace created in the ADS
        """

        data: Dict[str, Any] = {
            "result": False,
            "loader": "",
        }

        # if this somehow was errantly called when we don't want lite data, return
        if not item.useLiteMode:
            return data

        rawWorkspaceName: str = self._createRawNexusWorkspaceName(item)
        filename: str = self._createNexusFilename(item)
        key = self._key(item)

        loadedFromNative: bool = False

        # if the lite data exists in cache, copy it
        if self._loadedRuns.get(key) is not None:
            data["result"] = True
            data["loader"] = "cached"  # unset to indicate no loading occurred
            self._loadedRuns[key] += 1

        # the lite data is not cached, but the lite file exists
        # then load the lite file
        elif os.path.isfile(filename):
            data = self._fetch(filename, rawWorkspaceName, item.loader)
            self._loadedRuns[key] = 1

        # the lite data is not cached and lite file does not exist,
        # but native data exists in cache, clone it, then reduce it
        elif self._loadedRuns.get((item.runNumber, False)) is not None:
            # this can be accomplished by changing rawWorkspaceName to name of native workspace
            goingNative = GroceryListItem.makeNativeNexusItem(item.runNumber)
            nativeRawWorkspaceName = self._createRawNexusWorkspaceName(goingNative)
            data["result"] = True
            data["loader"] = "cached"
            loadedFromNative = True

        # neither lite nor native data in cache and lite file does not exist
        # then load native data, clone it, and reduce it
        else:
            # use the native resolution data
            goingNative = GroceryListItem.makeNativeNexusItem(item.runNumber)
            nativeRawWorkspaceName = self._createRawNexusWorkspaceName(goingNative)
            filename = self._createNexusFilename(goingNative)
            data = self._fetch(filename, nativeRawWorkspaceName, "")
            self._loadedRuns[(item.runNumber, False)] = 0
            loadedFromNative = True

        if loadedFromNative:
            CloneWorkspace(
                InputWorkspace=nativeRawWorkspaceName,
                OutputWorkspace=rawWorkspaceName,
            )
            data["loader"] = "cached"
            self._loadedRuns[key] = 1
            self._makeLite(rawWorkspaceName)

        workspaceName = self._createCopyNexusWorkspaceName(item, self._loadedRuns[key])
        CloneWorkspace(
            InputWorkspace=rawWorkspaceName,
            OutputWorkspace=workspaceName,
        )
        data["workspace"] = workspaceName
        return data

    def _makeLite(self, workspacename: str):
        """
        Wraps the lite data creation algorithm
        inputs:
        workspacename -- the name of the workspace to convert to lite mode, used as input and output
        """
        liteMapKey = ("Lite", False)
        liteAlgo = LiteDataAlgo()
        liteAlgo.initialize()
        liteAlgo.setPropertyValue("InputWorkspace", workspacename)
        liteAlgo.setPropertyValue("OutputWorkspace", workspacename)
        # if the lite data map is not already loaded, then load it
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
