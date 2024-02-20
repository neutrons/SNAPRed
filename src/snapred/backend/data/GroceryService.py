import os
from functools import cache
from typing import Any, Dict, List, Tuple
from pathlib import Path
import json

from mantid.api import AlgorithmManager, mtd

from snapred.backend.dao.state import DetectorState
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.dao.ingredients import GroceryListItem
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
    dataService: "LocalDataService"

    def __init__(self, dataService: LocalDataService = None):
        self.dataService = self._defaultClass(dataService, LocalDataService)

        # _loadedRuns caches a count of the number of copies made from the neutron-data workspace
        #   corresponding to a given (runId, isLiteMode) key:
        #   This count is:
        #     None: if a workspace is not loaded;
        #        0: if it is loaded, but no copies have been made;
        #       >0: if any copies have been made, since loading
        self._loadedRuns: Dict[Tuple[str, bool], int] = {}

        # Cache maps to workspace names, for various purposes
        self._loadedGroupings: Dict[Tuple[str, str, bool], str] = {}
        self._loadedInstruments: Dict[Tuple[str, bool], str] = {}

        self.grocer = FetchGroceriesRecipe()

    def _defaultClass(self, val, clazz):
        if val is None:
            val = clazz()
        return val

    def _key(self, *tokens: Tuple[Any, ...]) -> Tuple[Any, ...]:
        """
        Creates keys used for accessing workspaces from the various cache maps
        """
        # Each token needs to be separately hashable, but beyond that,
        #   enforcing key consistency over distinct cache maps seemed to be overkill.
        return tokens

    def rebuildCache(self):
        self.rebuildNeutronCache()
        self.rebuildGroupingCache()
        self.rebuildInstrumentCache()

    def rebuildNeutronCache(self):
        for loadedRun in self._loadedRuns.copy():
            self._updateNeutronCacheFromADS(*loadedRun)

    def rebuildGroupingCache(self):
        for loadedGrouping in self._loadedGroupings.copy():
            self._updateGroupingCacheFromADS(loadedGrouping, self._loadedGroupings[loadedGrouping])

    def rebuildInstrumentCache(self):
        for loadedInstrument in self._loadedInstruments.copy():
            self._updateInstrumentCacheFromADS(*loadedInstrument)

    def getCachedWorkspaces(self):
        """
        Returns a list of all workspaces cached in GroceryService
        """
        cachedWorkspaces = set()
        cachedWorkspaces.update(
            [self._createRawNeutronWorkspaceName(runId, useLiteMode) for runId, useLiteMode in self._loadedRuns.keys()]
        )
        cachedWorkspaces.update(self._loadedGroupings.values())
        cachedWorkspaces.update(self._loadedInstruments.values())

        return list(cachedWorkspaces)

    def _updateNeutronCacheFromADS(self, runId: str, useLiteMode: bool):
        """
        If the workspace has been loaded, but is not represented in the cache
        then update the cache to represent this condition
        """
        workspace = self._createRawNeutronWorkspaceName(runId, useLiteMode)
        key = self._key(runId, useLiteMode)
        if self.workspaceDoesExist(workspace) and self._loadedRuns.get(key) is None:
            # 0 => loaded with no copies
            self._loadedRuns[key] = 0
        elif self._loadedRuns.get(key) is not None and not self.workspaceDoesExist(workspace):
            del self._loadedRuns[key]

    def _updateGroupingCacheFromADS(self, key, workspace):
        """
        Ensure cache consistency for a single grouping workspace.
        """
        if self.workspaceDoesExist(workspace) and self._loadedGroupings.get(key) is None:
            self._loadedGroupings[key] = workspace
        elif self._loadedGroupings.get(key) is not None and not self.workspaceDoesExist(workspace):
            del self._loadedGroupings[key]

    def _updateInstrumentCacheFromADS(self, runId: str, useLiteMode: bool):
        """
        Ensure cache consistency for a single instrument-donor workspace.
        """
        # The workspace name in the cache may be either a neutron data workspace or an empty-instrument workspace.
        workspace = self._createRawNeutronWorkspaceName(runId, useLiteMode)
        key = self._key(runId, useLiteMode)
        if self.workspaceDoesExist(workspace) and self._loadedInstruments.get(key) is None:
            self._loadedInstruments[key] = workspace
        elif self._loadedInstruments.get(key) is not None:
            workspace = self._loadedInstruments.get(key)
            if not self.workspaceDoesExist(workspace):
                del self._loadedInstruments[key]

    ## FILENAME METHODS

    def getIPTS(self, runNumber: str, instrumentName: str = Config["instrument.name"]) -> str:
        ipts = self.dataService.getIPTS(runNumber, instrumentName)
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
        return f"{Config[home]}/{Config[pre]}{groupingScheme}{Config[ext]}"

    ## WORKSPACE NAME METHODS

    def _createNeutronWorkspaceNameBuilder(self, runNumber: str, useLiteMode: bool) -> NameBuilder:
        runNameBuilder = wng.run().runNumber(runNumber)
        if useLiteMode:
            runNameBuilder.lite(wng.Lite.TRUE)
        return runNameBuilder

    def _createNeutronWorkspaceName(self, runId: str, useLiteMode: bool) -> WorkspaceName:
        return self._createNeutronWorkspaceNameBuilder(runId, useLiteMode).build()

    def _createRawNeutronWorkspaceName(self, runId: str, useLiteMode: bool) -> WorkspaceName:
        return self._createNeutronWorkspaceNameBuilder(runId, useLiteMode).auxiliary("Raw").build()

    def _createCopyNeutronWorkspaceName(self, runId: str, useLiteMode: bool, numCopies: int) -> WorkspaceName:
        return self._createNeutronWorkspaceNameBuilder(runId, useLiteMode).auxiliary(f"Copy{numCopies}").build()

    def _createGroupingWorkspaceName(self, groupingScheme: str, runId: str, useLiteMode: bool) -> WorkspaceName:
        # TODO: use WNG here!
        if groupingScheme == "Lite":
            return f"lite_grouping_map_{runId}"
        instr = "lite" if useLiteMode else "native"
        return f"{Config['grouping.workspacename.' + instr]}_{groupingScheme}_{runId}"

    def _createDiffcalInputWorkspaceName(self, runId: str) -> WorkspaceName:
        return wng.diffCalInput().runNumber(runId).build()

    def _createDiffcalOutputWorkspaceName(self, runId: str) -> WorkspaceName:
        return wng.diffCalOutput().runNumber(runId).build()

    def _createDiffcalOutputWorkspaceFilename(self, runId: str, version: str) -> str:
        return str(
            Path(self._getCalibrationDataPath(runId, version))
            / (self._createDiffcalOutputWorkspaceName(runId) + ".nxs")
        )

    def _createDiffcalTableWorkspaceName(self, runId: str) -> WorkspaceName:
        return wng.diffCalTable().runNumber(runId).build()

    def _createDiffcalMaskWorkspaceName(self, runId: str) -> WorkspaceName:
        return wng.diffCalMask().runNumber(runId).build()

    def _createDiffcalTableFilename(self, runId: str, version: str) -> str:
        return str(
            Path(self._getCalibrationDataPath(runId, version)) / (self._createDiffcalTableWorkspaceName(runId) + ".h5")
        )

    ## ACCESSING WORKSPACES
    """
    These methods are for acccessing Mantid's ADS at the service layer
    """

    def uniqueHiddenName(self):
        return mtd.unique_hidden_name()

    def workspaceDoesExist(self, name: WorkspaceName):
        return mtd.doesExist(name)

    def renameWorkspace(self, oldName: WorkspaceName, newName: WorkspaceName):
        """
        Renames a workspace in Mantid's ADS.
        """
        renameAlgo = AlgorithmManager.create("RenameWorkspace")
        renameAlgo.setProperty("InputWorkspace", oldName)
        renameAlgo.setProperty("OutputWorkspace", newName)
        renameAlgo.execute()

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

    ## FETCH METHODS
    """
    The fetch methods orchestrate finding data files, loading them into workspaces,
    and preserving a cache to prevent re-loading the same data files.
    """

    def _fetchInstrumentDonor(self, runId: str, useLiteMode: bool) -> WorkspaceName:
        self._updateInstrumentCacheFromADS(runId, useLiteMode)

        key = self._key(runId, useLiteMode)
        if self._loadedInstruments.get(key) is not None:
            return self._loadedInstruments.get(key)

        wsName = None
        self._updateNeutronCacheFromADS(runId, useLiteMode)
        if self._loadedRuns.get(key) is not None:
            # If possible, use a cached neutron-data workspace as an instrument donor
            wsName = self._createRawNeutronWorkspaceName(runId, useLiteMode)
        else:
            # Otherwise, create an instrument donor.
            #   Alternatively, depending on performance, loading the corresponding neutron-data workspace
            #   could also be triggered here.

            wsName = self.uniqueHiddenName()

            # Load the bare instrument:
            instrumentFilename = (
                Config["instrument.lite.definition.file"]
                if useLiteMode
                else Config["instrument.native.definition.file"]
            )
            loadEmptyInstrument = AlgorithmManager.create("LoadEmptyInstrument")
            loadEmptyInstrument.setProperty("Filename", instrumentFilename)
            loadEmptyInstrument.setProperty("OutputWorkspace", wsName)
            loadEmptyInstrument.execute()

            # Initialize the instrument parameters
            # (Reserved IDs will use the unmodified instrument.)
            if runId != GroceryListItem.RESERVED_NATIVE_RUNID and runId != GroceryListItem.RESERVED_LITE_RUNID:
                self._updateInstrumentParameters(runId, wsName)
        self._loadedInstruments[key] = wsName
        return wsName

    def _updateInstrumentParameters(self, runNumber: str, wsName: str):
        """
        Update mutable instrument parameters
        """
        # Moved from `PixelGroupingParametersCalculationAlgorithm` for more general application here.

        detectorState: DetectorState = self._getDetectorState(runNumber)

        # Add sample logs with detector "arc" and "lin" parameters to the workspace
        # NOTE after adding the logs, it is necessary to update the instrument to
        #  factor in these new parameters, or else calculations will be inconsistent.
        #  This is done with a call to `ws->populateInstrumentParameters()` from within mantid.
        #  This call only needs to happen with the last log
        logsAdded = 0
        for paramName in ("arc", "lin"):
            for index in range(2):
                addSampleLog = AlgorithmManager.create("AddSampleLog")
                addSampleLog.setProperty("Workspace", wsName)
                addSampleLog.setProperty("LogName", "det_" + paramName + str(index + 1))
                addSampleLog.setProperty("LogText", str(getattr(detectorState, paramName)[index]))
                addSampleLog.setProperty("LogType", "Number Series")
                addSampleLog.setProperty("UpdateInstrumentParameters", (logsAdded >= 3))
                addSampleLog.execute()
                logsAdded += 1

    def _getDetectorState(self, runNumber: str) -> DetectorState:
        """
        Get the `DetectorState` associated with a given run number
        """
        # This method is provided to facilitate workspace loading with a _complete_ instrument state
        return self.dataService.readDetectorState(runNumber)

    def _getCalibrationDataPath(self, runNumber: str, version: str) -> str:
        return self.dataService._constructCalibrationDataPath(runNumber, version)

    def fetchWorkspace(self, filePath: str, name: WorkspaceName, loader: str = "") -> WorkspaceName:
        """
        Will fetch a workspace given a name and a path.
        inputs:
        - "filePath": complete path to workspace file
        - "name": workspace name
        - "loader" -- (optional) the loader algorithm to use to load the data
        outputs a dictionary with keys
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm; use it next time
        - "workspace": the name of the workspace created in the ADS
        """
        data = None
        if self.workspaceDoesExist(name):
            data = {
                "result": True,
                "loader": "cached",
                "workspace": name,
            }
        else:
            try:
                data = self.grocer.executeRecipe(filePath, name, loader)
            except RuntimeError:
                # Mantid's error message is not particularly useful, although it's logged in any case
                data = {"result": False}
            if not data["result"]:
                raise RuntimeError(f"unable to load workspace {name} from {filePath}")
        return data

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

    def fetchLiteDataMap(self, runId: str) -> WorkspaceName:
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
        key = self._key(item.groupingScheme, item.runNumber, item.useLiteMode)
        workspaceName = self._createGroupingWorkspaceName(item.groupingScheme, item.runNumber, item.useLiteMode)

        self._updateGroupingCacheFromADS(key, workspaceName)
        groupingIsLoaded = self._loadedGroupings.get(key) is not None

        if groupingIsLoaded:
            data = {
                "result": True,
                "loader": "cached",
                "workspace": workspaceName,
            }
        else:
            filename = self._createGroupingFilename(item.groupingScheme, item.useLiteMode)
            groupingLoader = "LoadGroupingDefinition"

            # Unless overridden: use a cached workspace as the instrument donor.
            instrumentPropertySource, instrumentSource = (
                ("InstrumentDonor", self._fetchInstrumentDonor(item.runNumber, item.useLiteMode))
                if not item.instrumentPropertySource
                else (item.instrumentPropertySource, item.instrumentSource)
            )
            data = self.grocer.executeRecipe(
                filename=filename,
                workspace=workspaceName,
                loader=groupingLoader,
                instrumentPropertySource=instrumentPropertySource,
                instrumentSource=instrumentSource,
            )
            self._loadedGroupings[key] = data["workspace"]

        return data

    def fetchCalibrationWorkspaces(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch diffraction-calibration table and mask workspaces
        inputs:
        - item, a GroceryListItem
        outputs a dictionary with
        - "result", true if everything ran correctly
        - "loader", either "LoadDiffractionCalibrationWorkspaces" or "cached"
        - "workspace", the name of the new workspace in the ADS:
          this defaults to the name of the calibration-table workspace, but the
          mask workspace will be loaded as well
        """

        runNumber, version, useLiteMode = item.runNumber, item.version, item.useLiteMode
        tableWorkspaceName = self._createDiffcalTableWorkspaceName(runNumber)
        maskWorkspaceName = self._createDiffcalMaskWorkspaceName(runNumber)

        if self.workspaceDoesExist(tableWorkspaceName):
            data = {
                "result": True,
                "loader": "cached",
                "workspace": tableWorkspaceName,
            }
        else:
            # table + mask are in the same hdf5 file:
            filename = self._createDiffcalTableFilename(runNumber, version)

            # Unless overridden: use a cached workspace as the instrument donor.
            instrumentPropertySource, instrumentSource = (
                ("InstrumentDonor", self._fetchInstrumentDonor(runNumber, useLiteMode))
                if not item.instrumentPropertySource
                else (item.instrumentPropertySource, item.instrumentSource)
            )
            data = self.grocer.executeRecipe(
                filename=filename,
                # IMPORTANT: Both table and mask workspaces will be loaded,
                #   however, the 'workspace' property needs to return
                #   a `MatrixWorkspace`-derived property, otherwise Mantid gets confused.
                workspace=maskWorkspaceName,
                loader="LoadCalibrationWorkspaces",
                instrumentPropertySource=instrumentPropertySource,
                instrumentSource=instrumentSource,
                loaderArgs=json.dumps({"CalibrationTable": tableWorkspaceName, "MaskWorkspace": maskWorkspaceName}),
            )
            data["workspace"] = tableWorkspaceName

        return data

    def fetchGroceryList(self, groceryList: List[GroceryListItem]) -> List[WorkspaceName]:
        """
        inputs:
        - groceryList -- a list of GroceryListItems indicating the workspaces to create
        output:
        - the names of the workspaces, in the same order as items in the grocery list
        """
        groceries = []
        for item in groceryList:
            match item.workspaceType:
                # for neutron data stored in a nexus file
                case "neutron":
                    if item.keepItClean:
                        res = self.fetchNeutronDataCached(item.runNumber, item.useLiteMode, item.loader)
                    else:
                        res = self.fetchNeutronDataSingleUse(item.runNumber, item.useLiteMode, item.loader)
                # for grouping definitions
                case "grouping":
                    res = self.fetchGroupingDefinition(item)
                case "diffcal":
                    res = {"result": False, "workspace": self._createDiffcalInputWorkspaceName(item.runNumber)}
                    raise RuntimeError(
                        "not implemented: no path available to fetch diffcal "
                        + f"input table workspace: '{res['workspace']}'"
                    )
                # for diffraction-calibration workspaces
                case "diffcal_output":
                    diffcalOutputWorkspaceName = self._createDiffcalOutputWorkspaceName(item.runNumber)
                    if item.isOutput:
                        res = {"result": True, "workspace": diffcalOutputWorkspaceName}
                    else:
                        res = self.fetchWorkspace(
                            self._createDiffcalOutputWorkspaceFilename(item.runNumber, item.version),
                            diffcalOutputWorkspaceName,
                        )
                case "diffcal_table":
                    tableWorkspaceName = self._createDiffcalTableWorkspaceName(item.runNumber)
                    if item.isOutput:
                        res = {"result": True, "workspace": tableWorkspaceName}
                    else:
                        res = self.fetchCalibrationWorkspaces(item)
                        res["workspace"] = tableWorkspaceName
                case "diffcal_mask":
                    maskWorkspaceName = self._createDiffcalMaskWorkspaceName(item.runNumber)
                    if item.isOutput:
                        res = {"result": True, "workspace": self._createDiffcalMaskWorkspaceName(item.runNumber)}
                    else:
                        res = self.fetchCalibrationWorkspaces(item)
                        res["workspace"] = maskWorkspaceName
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

    def clearADS(self, exclude: List[str] = []):
        """
        Clears ads of all workspaces except those in the exclude list and cache.
        """
        workspacesToClear = mtd.getObjectNames()
        # filter exclude
        workspacesToClear = [w for w in workspacesToClear if w not in exclude]
        # filter caches
        workspaceCache = self.getCachedWorkspaces()
        workspacesToClear = [w for w in workspacesToClear if w not in workspaceCache]
        # clear the workspaces
        for workspace in workspacesToClear:
            self.deleteWorkspaceUnconditional(workspace)
