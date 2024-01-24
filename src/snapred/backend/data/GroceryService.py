import os
from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager, mtd

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.recipe.algorithm.SaveGroupingDefinition import SaveGroupingDefinition
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import NameBuilder, WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.redantic import list_to_raw_pretty


@Singleton
class GroceryService:
    """
    You need some data?
    Yeah, I can get that for you.
    Just send me a list.
    """

    def __init__(self):
        self._loadedRuns: Dict[Tuple[str, bool], int] = {}
        self._loadedGroupings: Dict[Tuple[str, bool], str] = {}
        self.grocer = FetchGroceriesRecipe()

    def _key(self, using: str, useLiteMode: bool) -> Tuple[str, bool]:
        """
        Creates keys used for accessing workspaces from the cache
        """
        return (using, useLiteMode)

    def rebuildCache(self):
        self.rebuildNeutronCache()
        self.rebuildGroupingCache()

    def rebuildNeutronCache(self):
        for loadedRun in self._loadedRuns.copy():
            self._updateNeutronCacheFromADS(*loadedRun)

    def rebuildGroupingCache(self):
        for loadedGrouping in self._loadedGroupings.copy():
            self._updateGroupingCacheFromADS(loadedGrouping, self._loadedGroupings[loadedGrouping])

    ## FILENAME METHODS

    def getIPTS(self, runNumber: str, instrumentName: str = Config["instrument.name"]) -> str:
        from mantid.simpleapi import GetIPTS

        ipts = GetIPTS(runNumber, instrumentName)
        return str(ipts)

    def _createNeutronFilename(self, runNumber: str, useLiteMode: bool) -> str:
        instr = "nexus.lite" if useLiteMode else "nexus.native"
        pre = instr + ".prefix"
        ext = instr + ".extension"
        return self.getIPTS(runNumber) + Config[pre] + str(runNumber) + Config[ext]

    def _createGroupingFilename(self, groupingScheme: str, useLiteMode: bool) -> str:
        if groupingScheme == "Lite":
            return str(Config["instrument.lite.map.file"])
        instr = "lite" if useLiteMode else "native"
        home = "instrument.calibration.powder.grouping.home"
        pre = "grouping.filename.prefix"
        ext = "grouping.filename." + instr + ".extension"
        return Config[home] + "/" + Config[pre] + groupingScheme + Config[ext]

    ## WORKSPACE NAME METHODS

    def _createNeutronWorkspaceNameBuilder(self, runNumber: str, useLiteMode: bool) -> NameBuilder:
        runNameBuilder = wng.run().runNumber(runNumber)
        if useLiteMode:
            runNameBuilder.lite(wng.Lite.TRUE)
        return runNameBuilder

    def _createNeutronWorkspaceName(self, runId: str, useLiteMode: bool) -> str:
        return self._createNeutronWorkspaceNameBuilder(runId, useLiteMode).build()

    def _createRawNeutronWorkspaceName(self, runId: str, useLiteMode: bool) -> str:
        return self._createNeutronWorkspaceNameBuilder(runId, useLiteMode).auxilary("Raw").build()

    def _createCopyNeutronWorkspaceName(self, runId: str, useLiteMode: bool, numCopies: int) -> str:
        return self._createNeutronWorkspaceNameBuilder(runId, useLiteMode).auxilary(f"Copy{numCopies}").build()

    def _createGroupingWorkspaceName(self, groupingScheme: str, useLiteMode: bool):
        if groupingScheme == "Lite":
            return "lite_grouping_map"
        instr = "lite" if useLiteMode else "native"
        return Config["grouping.workspacename." + instr] + groupingScheme

    def _createDiffcalWorkspaceName(self, runId: str):
        return wng.diffCal().runNumber(runId).build()

    def _createDiffcalOutputWorkspaceName(self, runId: str):
        return wng.diffCalOutput().runNumber(runId).build()

    def _createDiffcalTableWorkspaceName(self, runId: str):
        return wng.diffCalTable().runNumber(runId).build()

    def _createDiffcalMaskWorkspaceName(self, runId: str):
        return wng.diffCalMask().runNumber(runId).build()

    ## WRITING TO DISK

    def writeWorkspace(self, path: str, name: WorkspaceName):
        """
        Writes a Mantid Workspace to disk.
        """
        saveAlgo = AlgorithmManager.create("SaveNexus")
        saveAlgo.setProperty("InputWorkspace", name)
        saveAlgo.setProperty("Filename", path + name)
        saveAlgo.execute()

    def writeGrouping(self, path: str, name: WorkspaceName):
        """
        Writes a Mantid Workspace to disk.
        """
        saveAlgo = AlgorithmManager.create("SaveGroupingDefinition")
        saveAlgo.setProperty("GroupingWorkspace", name)
        saveAlgo.setProperty("OutputFilename", path + name)
        saveAlgo.execute()

    def writeDiffCalTable(self, path: str, name: WorkspaceName, grouping: WorkspaceName = "", mask: WorkspaceName = ""):
        """
        Writes a diffcal table from a Mantid TableWorkspace to disk.
        """
        saveAlgo = AlgorithmManager.create("SaveDiffCal")
        saveAlgo.setPropertyValue("CalibrationWorkspace", name)
        saveAlgo.setPropertyValue("GroupingWorkspace", grouping)
        saveAlgo.setPropertyValue("MaskWorkspace", mask)
        saveAlgo.setPropertyValue("Filename", path + name)
        saveAlgo.execute()

    ## ACCESSING WORKSPACES
    """
    These methods are for acccessing Mantid's ADS at the service layer
    """

    def workspaceDoesExist(self, name: WorkspaceName):
        return mtd.doesExist(name)

    def getWorkspaceForName(self, name: WorkspaceName):
        """
        Simple wrapper of mantid's ADS for the service layer.
        Returns a pointer to a workspace, if it exists.
        If you need this method, you are probably doing something wrong.
        """
        if self.workspaceDoesExist(name):
            return mtd[name]
        else:
            return None

    def getCloneOfWorkspace(self, name: WorkspaceName, copy: WorkspaceName):
        """
        Simple wrapper to clone a workspace, for the service layer.
        Returns a pointer to the cloned workspace, if the original workspace exists.
        If you are doing anything with the pointer beyond checking it is non-null,
        you are probably doing something wrong.
        """
        from mantid.simpleapi import CloneWorkspace

        if self.workspaceDoesExist(name):
            ws = CloneWorkspace(InputWorkspace=name, OutputWorkspace=copy)
        else:
            ws = None
        return ws

    def _updateNeutronCacheFromADS(self, runId: str, useLiteMode: bool):
        """
        If the workspace has been loaded, but is not represented in the cache
        then update the cache to represent this condition
        """
        workspace = self._createRawNeutronWorkspaceName(runId, useLiteMode)
        key = self._key(runId, useLiteMode)
        # if the raw data has been loaded but not represent in cache
        if self.workspaceDoesExist(workspace) and self._loadedRuns.get(key) is None:
            self._loadedRuns[key] = 0
        # if the raw data has not been loaded but the cache thinks it has
        if self._loadedRuns.get(key) is not None and not self.workspaceDoesExist(workspace):
            del self._loadedRuns[key]

    def _updateGroupingCacheFromADS(self, key, workspace):
        """
        If the workspace has been loaded, but is not represented in the cache
        then update the cache to represent this condition
        """
        if self.workspaceDoesExist(workspace) and self._loadedGroupings.get(key) is None:
            self._loadedGroupings[key] = workspace
        if self._loadedGroupings.get(key) is not None and not self.workspaceDoesExist(workspace):
            del self._loadedGroupings[key]

    ## FETCH METHODS
    """
    The fetch methods orchestrate finding datas files, loading them into workspaces,
    and preserving a cache to prevent re-loading the same data files.
    """

    def fetchWorkspace(self, path: str, name: WorkspaceName, loader: str = "") -> WorkspaceName:
        """
        Will fetch a workspace given a name and a path.
        Returns the same workspace name, if the workspace exists or can be loaded.
        """
        if self.workspaceDoesExist(name):
            return name
        else:
            try:
                res = self.grocer.executeRecipe(path, name, loader)
            except RuntimeError:
                raise RuntimeError(f"Failed to load workspace {name} from {path}")
            if res["result"] is True:
                return res["workspace"]
            else:
                raise RuntimeError(f"Failed to load workspace {name} from {path}")

    def fetchNeutronDataSingleUse(self, runId: str, useLiteMode: bool, loader: str = "") -> Dict[str, Any]:
        """
        Fetch a neutron data file, without copy-protection.
        If the workspace is truly only needed once, this saves time and memory.
        If the workspace needs to be loaded more than once, used cached method.
        inputs:
        - runNumber -- the neutron data run number
        - useLiteMode -- whether to reduce to the instrument's Lite mode
        - loader -- (optional) the loader algorithm to use to load the data
        outputs a dictionary with keys
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm; use it next time
        - "workspace": the name of the workspace created in the ADS
        """

        filename: str = self._createNeutronFilename(runId, useLiteMode)
        workspaceName: str = self._createNeutronWorkspaceName(runId, useLiteMode)

        self._updateNeutronCacheFromADS(runId, useLiteMode)

        # check if a raw workspace exists, and clone it if so
        if self._loadedRuns.get(self._key(runId, useLiteMode)) is not None:
            self.getCloneOfWorkspace(self._createRawNeutronWorkspaceName(runId, useLiteMode), workspaceName)
            data = {
                "result": True,
                "loader": "cached",
                "workspace": workspaceName,
            }
        # otherwise fetch the data
        else:
            data = self.grocer.executeRecipe(filename, workspaceName, loader)

        if useLiteMode:
            self.convertToLiteMode(workspaceName)

        return data

    def fetchNeutronDataCached(self, runId: str, useLiteMode: bool, loader: str = "") -> Dict[str, Any]:
        """
        Fetch a nexus data file using a cache system to prevent double-loading from disk
        inputs:
        - runNumber -- the neutron data run number
        - useLiteMode -- whether to reduce to the instrument's Lite mode
        - loader -- (optional) the loader algorithm to use to load the data
        outputs a dictionary with keys
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time
        - "workspace": the name of the workspace created in the ADS
        """
        key = self._key(runId, useLiteMode)
        rawWorkspaceName: WorkspaceName = self._createRawNeutronWorkspaceName(runId, useLiteMode)
        filename: str = self._createNeutronFilename(runId, useLiteMode)

        loadedFromNative: bool = False

        self._updateNeutronCacheFromADS(runId, useLiteMode)

        # if the raw data has already been loaded, clone it
        if self._loadedRuns.get(key) is not None:
            data = {"loader": "cached"}
        # if the data is not cached, but the file exists
        elif os.path.isfile(filename):
            data = self.grocer.executeRecipe(filename, rawWorkspaceName, loader)
            self._loadedRuns[key] = 0
        # if the file does not exist, and this is native resolution data, this represents an error condition
        elif useLiteMode is False:
            raise RuntimeError(f"Could not load run {runId} from file {filename}")
        # if in Lite mode, and no raw workspace and no file exists, look if native data has been loaded from cache
        # if so, then clone the native data and reduce it
        elif self._loadedRuns.get(self._key(runId, False)) is not None:
            nativeRawWorkspaceName = self._createRawNeutronWorkspaceName(runId, False)
            data = {"loader": "cached"}
            loadedFromNative = True
        # neither lite nor native data in cache and lite file does not exist
        # then load native data, clone it, and reduce it
        elif os.path.isfile(self._createNeutronFilename(runId, False)):
            # load the native resolution data
            goingNative = (runId, False)
            nativeRawWorkspaceName = self._createRawNeutronWorkspaceName(*goingNative)
            nativeFilename = self._createNeutronFilename(*goingNative)
            data = self.grocer.executeRecipe(nativeFilename, nativeRawWorkspaceName, loader="")
            # keep track of the loaded raw native data
            self._loadedRuns[self._key(*goingNative)] = 0
            loadedFromNative = True
        # the data cannot be loaded -- this is an error condition
        else:
            raise RuntimeError(f"Could not load run {runId} from file {filename}")

        if loadedFromNative:
            # clone the native raw workspace
            # then reduce its resolution to make the lite raw workspace
            self.getCloneOfWorkspace(nativeRawWorkspaceName, rawWorkspaceName)
            self._loadedRuns[key] = 0
            self.convertToLiteMode(rawWorkspaceName)

        # create a copy of the raw data for use
        workspaceName = self._createCopyNeutronWorkspaceName(runId, useLiteMode, self._loadedRuns[key] + 1)
        data["result"] = self.getCloneOfWorkspace(rawWorkspaceName, workspaceName) is not None
        data["workspace"] = workspaceName
        self._loadedRuns[key] += 1
        return data

    def fetchLiteDataMap(self) -> WorkspaceName:
        item = GroceryListItem.builder().grouping("Lite").build()
        return self.fetchGroupingDefinition(item)["workspace"]

    def fetchGroupingDefinition(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch a grouping definition.
        inputs:
        - item, a GroceryListItem
        outputs a dictionary with
        - "result", true if everything ran correctly
        - "loader", either "LoadGroupingDefinition" or "cached"
        - "workspace", the name of the new grouping workspace in the ADS
        """
        itemTuple = (item.groupingScheme, item.useLiteMode)
        workspaceName = self._createGroupingWorkspaceName(*itemTuple)
        filename = self._createGroupingFilename(*itemTuple)
        groupingLoader = "LoadGroupingDefinition"
        key = self._key(*itemTuple)

        groupingIsLoaded = self._loadedGroupings.get(key) is not None and self.workspaceDoesExist(
            self._loadedGroupings.get(key)
        )

        if groupingIsLoaded:
            data = {
                "result": True,
                "loader": "cached",
                "workspace": workspaceName,
            }
        else:
            if self._loadedGroupings.get(key) is not None:
                self.rebuildCache()
            data = self.grocer.executeRecipe(
                filename=filename,
                workspace=workspaceName,
                loader=groupingLoader,
                instrumentPropertySource=item.instrumentPropertySource,
                instrumentSource=item.instrumentSource,
            )
            self._loadedGroupings[key] = data["workspace"]

        return data

    def fetchGroceryList(self, groceryList: List[GroceryListItem]) -> List[WorkspaceName]:
        """
        inputs:
        - groceryList -- a list of GroceryListItems indicating the workspaces to create
        output:
        - the names of the workspaces, in the same order as items in the grocery list
        """
        groceries = []
        prev: WorkspaceName = ""
        for item in groceryList:
            match item.workspaceType:
                # for neutron data stored in a nexus file
                case "neutron":
                    if item.keepItClean:
                        res = self.fetchNeutronDataCached(item.runNumber, item.useLiteMode, item.loader)
                    else:
                        res = self.fetchNeutronDataSingleUse(item.runNumber, item.useLiteMode, item.loader)
                    # save the most recently-loaded neutron data as a possible instrument donor
                    prev = res["workspace"]
                # for grouping definitions
                case "grouping":
                    # this flag indicates to use most recently-loaded nexus data as the instrument donor
                    if item.instrumentSource == "prev":
                        item.instrumentPropertySource = "InstrumentDonor"
                        item.instrumentSource = prev
                    res = self.fetchGroupingDefinition(item)
                case "diffcal":
                    res = {"result": True, "workspace": self._createDiffcalWorkspaceName(item.runNumber)}
                    raise RuntimeError(
                        f"not implemented: no path available to fetch diffcal " + 
                        f"input table workspace: \'{res['workspace']}\'"
                    )
                # for output (i.e. special-order) workspaces
                case "diffcal_output":
                    res = {"result": True, "workspace": self._createDiffcalOutputWorkspaceName(item.runNumber)}
                case "diffcal_table":
                    res = {"result": True, "workspace": self._createDiffcalTableWorkspaceName(item.runNumber)}
                case "diffcal_mask":
                    res = {"result": True, "workspace": self._createDiffcalMaskWorkspaceName(item.runNumber)}
                case _:
                    raise RuntimeError(f"unrecognized 'workspaceType': '{item.workspaceType}'")
            # check that the fetch operation succeeded and if so append the workspace
            if res["result"] is True:
                groceries.append(res["workspace"])
            else:
                raise RuntimeError(f"Error fetching item {item.json(indent=2)}")
        return groceries

    def fetchGroceryDict(self, groceryDict: Dict[str, GroceryListItem], **kwargs) -> Dict[str, WorkspaceName]:
        """
        This is the primary method you should use for fetching groceries, in almost all cases.
        Will create a dictionary matching property names to workspaces for those properties,
        for easiest use in recipes and algorithms.
        inputs:
        - groceryDict -- a dictionary of GroceryListItems keyed to their property name
        output:
        - a dictionary where each key is a property name and each value the workspace name for that property
        """
        groceryList = groceryDict.values()
        groceryNames = groceryDict.keys()
        groceries = self.fetchGroceryList(groceryList)
        data = dict(zip(groceryNames, groceries))
        data.update(kwargs)
        return data

    def convertToLiteMode(self, workspace: WorkspaceName):
        from snapred.backend.service.LiteDataService import LiteDataService

        LiteDataService().reduceLiteData(workspace, workspace)

    ## CLEANUP METHODS

    def deleteWorkspace(self, name: WorkspaceName):
        """
        Deletes a workspace from Mantid.
        Mostly for cleanup at the Service Layer.
        """
        deleteAlgo = AlgorithmManager.create("WashDishes")
        deleteAlgo.setProperty("Workspace", name)
        deleteAlgo.execute()

    def deleteWorkspaceUnconditional(self, name: WorkspaceName):
        """
        Deletes a workspace from Mantid, regardless of CIS mode.
        Use when a workspace MUST be deleted for proper behavior.
        """
        if self.workspaceDoesExist(name):
            deleteAlgo = AlgorithmManager.create("DeleteWorkspace")
            deleteAlgo.setProperty("Workspace", name)
            deleteAlgo.execute()
        else:
            pass
