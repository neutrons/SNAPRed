# ruff: noqa: F811
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from mantid.simpleapi import mtd
from pydantic import validate_arguments

from snapred.backend.dao.indexing.Record import Nonrecord
from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.state import DetectorState
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import NameBuilder, WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)

Version = Union[int, Literal["*"]]


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
        self.workspaceMetadataService = WorkspaceMetadataService()

        # _loadedRuns caches a count of the number of copies made from the neutron-data workspace
        #   corresponding to a given (runNumber, isLiteMode) key:
        #   This count is:
        #     None: if a workspace is not loaded;
        #        0: if it is loaded, but no copies have been made;
        #       >0: if any copies have been made, since loading
        self._loadedRuns: Dict[Tuple[str, bool], int] = {}

        # Cache maps to workspace names, for various purposes
        self._loadedGroupings: Dict[Tuple[str, str, bool], str] = {}
        self._loadedInstruments: Dict[Tuple[str, bool], str] = {}

        self.grocer = FetchGroceriesRecipe()
        self.mantidSnapper = MantidSnapper(None, "Utensils")

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
        """
        Recreate all caches to match what is in the ADS
        """
        self.rebuildNeutronCache()
        self.rebuildGroupingCache()
        self.rebuildInstrumentCache()

    def rebuildNeutronCache(self):
        """
        Rebuild the neutron data cache
        """
        for loadedRun in self._loadedRuns.copy():
            self._updateNeutronCacheFromADS(*loadedRun)

    def rebuildGroupingCache(self):
        """
        Rebuild the grouping cache
        """
        for loadedGrouping in self._loadedGroupings.copy():
            self._updateGroupingCacheFromADS(loadedGrouping, self._loadedGroupings[loadedGrouping])

    def rebuildInstrumentCache(self):
        """
        Rebuild the instrument cache
        """
        for loadedInstrument in self._loadedInstruments.copy():
            self._updateInstrumentCacheFromADS(*loadedInstrument)

    def getCachedWorkspaces(self):
        """
        :return: a list of all workspaces cached in GroceryService
        :rtype: List[WorkspaceName]
        """
        cachedWorkspaces = set()
        cachedWorkspaces.update(
            [
                self._createRawNeutronWorkspaceName(runNumber, useLiteMode)
                for runNumber, useLiteMode in self._loadedRuns.keys()
            ]
        )
        cachedWorkspaces.update(self._loadedGroupings.values())
        cachedWorkspaces.update(self._loadedInstruments.values())

        return list(cachedWorkspaces)

    def _updateNeutronCacheFromADS(self, runNumber: str, useLiteMode: bool):
        """
        If the workspace has been loaded, but is not represented in the cache
        then update the cache to represent this condition

        :param runNumber: the run number of the desired data
        :type runNumber: str
        :param useLiteMode: whether to use lite or native resolution
        :type useLiteMode: bool
        """
        workspace = self._createRawNeutronWorkspaceName(runNumber, useLiteMode)
        key = self._key(runNumber, useLiteMode)
        if self.workspaceDoesExist(workspace) and self._loadedRuns.get(key) is None:
            # 0 => loaded with no copies
            self._loadedRuns[key] = 0
        elif self._loadedRuns.get(key) is not None and not self.workspaceDoesExist(workspace):
            del self._loadedRuns[key]

    def _updateGroupingCacheFromADS(self, key, workspace):
        """
        Ensure cache consistency for a single grouping workspace.

        :param key: the cache key fof the grouping workspace
        :type key: tuple, generated by _key()
        :param workspace: the purported workspace name in the ADS
        :type workspace: WorkspaceName
        """
        if self.workspaceDoesExist(workspace) and self._loadedGroupings.get(key) is None:
            self._loadedGroupings[key] = workspace
        elif self._loadedGroupings.get(key) is not None and not self.workspaceDoesExist(workspace):
            del self._loadedGroupings[key]

    def _updateInstrumentCacheFromADS(self, runNumber: str, useLiteMode: bool):
        """
        Ensure cache consistency for a single instrument-donor workspace.

        :param runNumber: a run number to a workspace being used as an instrument donor
        :type runNumber: str
        :param useLiteMode: whether to use lite or native resolution
        :type useLiteMode: bool
        """
        # The workspace name in the cache may be either a neutron data workspace or an empty-instrument workspace.
        workspace = self._createRawNeutronWorkspaceName(runNumber, useLiteMode)
        key = self._key(runNumber, useLiteMode)
        if self.workspaceDoesExist(workspace) and self._loadedInstruments.get(key) is None:
            self._loadedInstruments[key] = workspace
        elif self._loadedInstruments.get(key) is not None:
            workspace = self._loadedInstruments.get(key)
            if not self.workspaceDoesExist(workspace):
                del self._loadedInstruments[key]

    ## FILENAME METHODS

    def getIPTS(self, runNumber: str, instrumentName: str = Config["instrument.name"]) -> str:
        """
        Find the approprate IPTS folder for a run number.

        :param runNumber: the data run number
        :type runNumber: str
        :param instrumentName: the name of the instrument, defaults to instrument defined in application.yml
        :type instrumentName: str
        :return: The IPTS directory
        :rtype: str
        """
        ipts = self.dataService.getIPTS(runNumber, instrumentName)
        return str(ipts)

    def _createNeutronFilename(self, runNumber: str, useLiteMode: bool) -> str:
        instr = "nexus.lite" if useLiteMode else "nexus.native"
        pre = instr + ".prefix"
        ext = instr + ".extension"
        return self.getIPTS(runNumber) + Config[pre] + str(runNumber) + Config[ext]

    @validate_arguments
    def _createGroupingFilename(self, runNumber: str, groupingScheme: str, useLiteMode: bool) -> str:
        if groupingScheme == "Lite":
            path = str(Config["instrument.lite.map.file"])
        else:
            groupingMap = self.dataService.readGroupingMap(runNumber)
            path = groupingMap.getMap(useLiteMode)[groupingScheme].definition
        return str(path)

    @validate_arguments
    def _createDiffcalOutputWorkspaceFilename(self, item: GroceryListItem) -> str:
        ext = Config["calibration.diffraction.output.extension"]
        return str(
            Path(self._getCalibrationDataPath(item.runNumber, item.useLiteMode, item.version))
            / (self._createDiffcalOutputWorkspaceName(item) + ext)
        )

    @validate_arguments
    def _createDiffcalDiagnosticWorkspaceFilename(self, item: GroceryListItem) -> str:
        ext = Config["calibration.diffraction.diagnostic.extension"]
        return str(
            Path(self._getCalibrationDataPath(item.runNumber, item.useLiteMode, item.version))
            / (self._createDiffcalOutputWorkspaceName(item) + ext)
        )

    @validate_arguments
    def _createDiffcalTableFilename(self, runNumber: str, useLiteMode: bool, version: Optional[Version]) -> str:
        return str(
            Path(self._getCalibrationDataPath(runNumber, useLiteMode, version))
            / (self._createDiffcalTableWorkspaceName(runNumber, useLiteMode, version) + ".h5")
        )

    @validate_arguments
    def _createNormalizationWorkspaceFilename(self, runNumber: str, useLiteMode: bool, version: Optional[int]) -> str:
        return str(
            Path(self._getNormalizationDataPath(runNumber, useLiteMode, version))
            / (
                self._createNormalizationWorkspaceName(runNumber, useLiteMode, version)
                + Config["calibration.normalization.output.ws.extension"]
            )
        )

    ## WORKSPACE NAME METHODS

    def _createNeutronWorkspaceNameBuilder(self, runNumber: str, useLiteMode: bool) -> NameBuilder:
        runNameBuilder = wng.run().runNumber(runNumber)
        if useLiteMode:
            runNameBuilder.lite(wng.Lite.TRUE)
        return runNameBuilder

    def _createNeutronWorkspaceName(self, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        return self._createNeutronWorkspaceNameBuilder(runNumber, useLiteMode).build()

    def _createRawNeutronWorkspaceName(self, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        return self._createNeutronWorkspaceNameBuilder(runNumber, useLiteMode).auxiliary("Raw").build()

    def _createCopyNeutronWorkspaceName(self, runNumber: str, useLiteMode: bool, numCopies: int) -> WorkspaceName:
        return self._createNeutronWorkspaceNameBuilder(runNumber, useLiteMode).auxiliary(f"Copy{numCopies}").build()

    def _createGroupingWorkspaceName(self, groupingScheme: str, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        # TODO: use WNG here!
        if groupingScheme == "Lite":
            return "lite_grouping_map"
        instr = "lite" if useLiteMode else "native"
        return f"{Config['grouping.workspacename.' + instr]}_{groupingScheme}_{runNumber}"

    def _createDiffcalInputWorkspaceName(self, runNumber: str) -> WorkspaceName:
        return wng.diffCalInput().runNumber(runNumber).build()

    def _createDiffcalOutputWorkspaceName(self, item: GroceryListItem) -> WorkspaceName:
        return (
            wng.diffCalOutput()
            .unit(item.unit)
            .runNumber(item.runNumber)
            .version(item.version)
            .group(item.groupingScheme)
            .build()
        )

    @validate_arguments
    def _createDiffcalTableWorkspaceName(
        self,
        runNumber: str,
        useLiteMode: bool,  # noqa: ARG002
        version: Optional[int],
    ) -> WorkspaceName:
        return wng.diffCalTable().runNumber(runNumber).version(version).build()

    @validate_arguments
    def _createDiffcalMaskWorkspaceName(
        self,
        runNumber: str,
        useLiteMode: bool,  # noqa: ARG002
        version: Optional[int],
    ) -> WorkspaceName:
        return wng.diffCalMask().runNumber(runNumber).version(version).build()

    def _createNormalizationWorkspaceName(
        self,
        runNumber: str,
        useLiteMode: bool,  # noqa: ARG002
        version: Optional[int],
    ) -> WorkspaceName:
        return wng.rawVanadium().runNumber(runNumber).version(version).build()

    ## ACCESSING WORKSPACES
    """
    These methods are for acccessing Mantid's ADS at the service layer
    """

    def uniqueHiddenName(self) -> WorkspaceName:
        """
        Return a unique hidden workspace name using mantid's built-in method.

        :return: the unqiue hidden name
        :rtype: WorkspaceName
        """
        return mtd.unique_hidden_name()

    def workspaceDoesExist(self, name: WorkspaceName):
        """
        Check if the workspace exists in the ADS.

        :param name: the name of the workspace to check for existence
        :type name: WorkspaceName
        :return: True if the workspace exists, False if it does not exist
        :rtype: bool
        """
        return mtd.doesExist(name)

    def renameWorkspace(self, oldName: WorkspaceName, newName: WorkspaceName):
        """
        Renames a workspace in Mantid's ADS.

        :param oldName: the original name of the workspace in the ADS
        :type oldName: WorkspaceName
        :param newName: the name to replace the workspace name in the ADS
        :type newName: WorkspaceName
        """
        self.mantidSnapper.RenameWorkspace(
            f"Renaming {oldName} to {newName}",
            InputWorkspace=oldName,
            OutputWorkspace=newName,
        )
        self.mantidSnapper.executeQueue()

    def getWorkspaceForName(self, name: WorkspaceName):
        """
        Simple wrapper of mantid's ADS for the service layer.
        Returns a pointer to a workspace, if it exists.
        If you need this method, you are probably doing something wrong.

        :param name: the name of the workspace in the ADS
        :type name: WorkspaceName
        :return: a pointer to the workspace in the ADS corresponding to name
        :rtype: a C++ shared pointer to a MatrixWorkspace
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

        :param name: the name of the workspace to clone in the ADS
        :type name: WorkspaceName
        :param copy: the name of the resulting cloned workspace
        :type copy: WorkspaceName
        :return: a pointer to the cloned workspace in the ADS
        :rtype: a C++ shared pointer to a MatrixWorkspace
        """
        from mantid.simpleapi import CloneWorkspace

        if self.workspaceDoesExist(name):
            ws = CloneWorkspace(InputWorkspace=name, OutputWorkspace=copy)
        else:
            ws = None
        return ws

    def getWorkspaceTag(self, workspaceName: str, logname: str):
        """
        Simple wrapper to get a workspace metadata tag, for the service layer.
        Returns a tag for a given workspace. Raise an error if the workspace
        does not exist.

        :param workspaceName: the name of the workspace containing the tag
        :type workspaceName: string
        :param logname: the name of the log, usually for diffcal or normalization
        :type logname: string
        :return: string of the tag, default value is "unset"
        """
        if self.workspaceDoesExist(workspaceName):
            return self.workspaceMetadataService.readMetadataTag(workspaceName, logname)
        else:
            raise RuntimeError(f"Workspace {workspaceName} does not exist")

    def setWorkspaceTag(self, workspaceName: str, logname: str, logvalue: str):
        """
        Simple wrapper to set a workspace metadata tag, for the service layer.
        Sets a tag for a given workspace. Raise an error if the workspace
        does not exist.

        :param workspaceName: the name of the workspace containing the tag
        :type workspaceName: string
        :param logname: the name of the log, usually for diffcal or normalization
        :type logname: string
        :param logvalue: tag value to be set, must exist in the WorkspaceMetadata dao
        :type logvalue: string
        """
        if self.workspaceDoesExist(workspaceName):
            self.workspaceMetadataService.writeMetadataTag(workspaceName, logname, logvalue)
        else:
            raise RuntimeError(f"Workspace {workspaceName} does not exist")

    ## FETCH METHODS
    """
    The fetch methods orchestrate finding data files, loading them into workspaces,
    and preserving a cache to prevent re-loading the same data files.
    """

    def _fetchInstrumentDonor(self, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        """
        The grouping workspaces require an instrument definition, and do not always have their own instrument definition
        saved with them (such as XML groupings).  Therefore, when loading groupings, it is necessary to match them to
        the proper instrument definition *with the proper instrument state params*.
        This uses the run number (and lite mode)to locate the proper state, and from that the proper instrument
        definition with instrument params for that state.

        :param runNumber: a run number that was taken in the desired instrument state
        :type runNumber: str
        :param useLiteMode: whether to use lite or native resolution
        :type useLiteMode: bool
        :return: the name of a workspace in the ADS with a correctly updated instrument
        :rtype: WorkspaceName
        """
        self._updateInstrumentCacheFromADS(runNumber, useLiteMode)

        key = self._key(runNumber, useLiteMode)
        wsName = self._loadedInstruments.get(key)
        if wsName is None:
            self._updateNeutronCacheFromADS(runNumber, useLiteMode)
            if self._loadedRuns.get(key) is not None:
                # If possible, use a cached neutron-data workspace as an instrument donor
                wsName = self._createRawNeutronWorkspaceName(runNumber, useLiteMode)
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
                self.mantidSnapper.LoadEmptyInstrument(
                    f"Loading instrument at {instrumentFilename} to {wsName}",
                    Filename=instrumentFilename,
                    OutputWorkspace=wsName,
                )
                self.mantidSnapper.executeQueue()

                # Initialize the instrument parameters
                # (Reserved run-numbers will use the unmodified instrument.)
                if (
                    runNumber != GroceryListItem.RESERVED_NATIVE_RUNNUMBER
                    and runNumber != GroceryListItem.RESERVED_LITE_RUNNUMBER
                ):
                    detectorState: DetectorState = self._getDetectorState(runNumber)
                    self.updateInstrumentParameters(wsName, detectorState)
            self._loadedInstruments[key] = wsName
        return wsName

    def updateInstrumentParameters(self, wsName: WorkspaceName, detectorState: DetectorState):
        """
        The SNAP instrument has moveable panels, and certain properties of a calculation will depend
        on how the panels are oriented (the instrument state).
        For mantid algorithms to correctly use the instrument state, the parameters
        specifying the positions of the panels must be updated within the workspace's logs.
        -- this public method allows override during algorithm testing
        separately from the recipe system (i.e. not using `GroceryService` loading).

        :param wsName: the workspace with the instrument to be updated
        :type wsName: WorkspaceName
        :param detectorState: the detector state which contains the mutable parameters to be applied
        :type detectorSttate: DetectorState
        """
        # Moved from `PixelGroupingParametersCalculationAlgorithm` for more general application here.

        # Add sample logs with detector "arc" and "lin" parameters to the workspace
        # NOTE after adding the logs, it is necessary to update the instrument to
        #  factor in these new parameters, or else calculations will be inconsistent.
        #  This is done with a call to `ws->populateInstrumentParameters()` from within mantid.
        #  This call only needs to happen with the last log
        logsAdded = 0
        for paramName in ("arc", "lin"):
            for index in range(2):
                self.mantidSnapper.AddSampleLog(
                    f"Updating parameter {paramName}{str(index + 1)}",
                    Workspace=wsName,
                    LogName=f"det_{paramName}{str(index + 1)}",
                    LogText=str(getattr(detectorState, paramName)[index]),
                    LogType="Number Series",
                    UpdateInstrumentParameters=(logsAdded >= 3),
                )
                logsAdded += 1
        self.mantidSnapper.executeQueue()

    def _getDetectorState(self, runNumber: str) -> DetectorState:
        """
        Get the `DetectorState` associated with a given run number

        :param runNumber: a run number, whose state is to be returned
        :type runNumber: str
        :return: detector state object corresponding to the run number
        :rtype: DetectorState


        """
        # This method is provided to facilitate workspace loading with a _complete_ instrument state
        return self.dataService.readDetectorState(runNumber)

    @validate_arguments
    def _getCalibrationDataPath(self, runNumber: str, useLiteMode: bool, version: Optional[Version]) -> str:
        """
        Get a path to the directory with the calibration data

        :param runNumber: a run number, whose state will be looked up
        :type runNumber: str
        :param version: the calibration version to use in the lookup
        :type version: int
        """
        return self.dataService.calibrationIndexor(runNumber, useLiteMode).versionPath(version)

    @validate_arguments
    def _getNormalizationDataPath(self, runNumber: str, useLiteMode: bool, version: Optional[Version]) -> str:
        """
        Get a path to the directory with the normalization data

        :param runNumber: a run number, whose state will be looked up
        :type runNumber: str
        :param version: the normalization version to use in the lookup
        :type version: int
        """
        return self.dataService.normalizationIndexor(runNumber, useLiteMode).versionPath(version)

    def fetchWorkspace(self, filePath: str, name: WorkspaceName, loader: str = "") -> Dict[str, Any]:
        """
        Will fetch a workspace given a name and a path.

        :param filePath: complete path to workspace file
        :type filePath: str
        :param name: the name to use for the workspace in the ADS
        :type name: WorkspaceName
        :param loader: the loader algorithm to use to load the data, optional
        :type loader: str
        :return: a dictionary with keys

            - "result": true if everything ran correctly
            - "loader": the loader that was used by the algorithm; use it next time
            - "workspace": the name of the workspace created in the ADS

        :rtype: Dict[str, Any]
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

    def fetchNeutronDataSingleUse(self, runNumber: str, useLiteMode: bool, loader: str = "") -> Dict[str, Any]:
        """
        Fetch a neutron data file, without copy-protection.
        If the workspace is truly only needed once, this saves time and memory.
        If the workspace needs to be loaded more than once, used cached method.

        :param runNumber: the neutron data run number
        :type runNumber: str
        :param useLiteMode: whether to use lite or native resolution
        :type useLiteMode: bool
        :param loader: the loader algorithm to use to load the data, optional
        :type loader: str
        :return: a dictionary with the following keys

            - "result": true if everything ran correctly
            - "loader": the loader that was used by the algorithm; use it next time
            - "workspace": the name of the workspace created in the ADS

        :rtype: Dict[str, Any]
        """

        filename: str = self._createNeutronFilename(runNumber, useLiteMode)
        workspaceName: str = self._createNeutronWorkspaceName(runNumber, useLiteMode)

        self._updateNeutronCacheFromADS(runNumber, useLiteMode)

        # check if a raw workspace exists, and clone it if so
        if self._loadedRuns.get(self._key(runNumber, useLiteMode)) is not None:
            self.getCloneOfWorkspace(self._createRawNeutronWorkspaceName(runNumber, useLiteMode), workspaceName)
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

    def fetchNeutronDataCached(self, runNumber: str, useLiteMode: bool, loader: str = "") -> Dict[str, Any]:
        """
        Fetch a nexus data file using a cache system to prevent double-loading from disk

        :param runNumber: the neutron data run number
        :type runNumber: str
        :param useLiteMode: whether to reduce to the instrument's Lite mode
        :type useLiteMode: bool
        :param loader: the loader algorithm to use to load the data, optional
        :type loader: str
        :return: a dictionary with the following keys

            - "result": true if everything ran correctly
            - "loader": the loader that was used by the algorithm, use it next time
            - "workspace": the name of the workspace created in the ADS

        :rtype: Dict[str, Any]
        """
        key = self._key(runNumber, useLiteMode)
        rawWorkspaceName: WorkspaceName = self._createRawNeutronWorkspaceName(runNumber, useLiteMode)
        filename: str = self._createNeutronFilename(runNumber, useLiteMode)

        loadedFromNative: bool = False

        self._updateNeutronCacheFromADS(runNumber, useLiteMode)

        # if the raw data has already been loaded, clone it
        if self._loadedRuns.get(key) is not None:
            data = {"loader": "cached"}
        # if the data is not cached, but the file exists
        elif os.path.isfile(filename):
            data = self.grocer.executeRecipe(filename, rawWorkspaceName, loader)
            self._loadedRuns[key] = 0
        # if the file does not exist, and this is native resolution data, this represents an error condition
        elif useLiteMode is False:
            raise RuntimeError(f"Could not load run {runNumber} from file {filename}")
        # if in Lite mode, and no raw workspace and no file exists, look if native data has been loaded from cache
        # if so, then clone the native data and reduce it
        elif self._loadedRuns.get(self._key(runNumber, False)) is not None:
            nativeRawWorkspaceName = self._createRawNeutronWorkspaceName(runNumber, False)
            data = {"loader": "cached"}
            loadedFromNative = True
        # neither lite nor native data in cache and lite file does not exist
        # then load native data, clone it, and reduce it
        elif os.path.isfile(self._createNeutronFilename(runNumber, False)):
            # load the native resolution data
            goingNative = (runNumber, False)
            nativeRawWorkspaceName = self._createRawNeutronWorkspaceName(*goingNative)
            nativeFilename = self._createNeutronFilename(*goingNative)
            data = self.grocer.executeRecipe(nativeFilename, nativeRawWorkspaceName, loader="")
            # keep track of the loaded raw native data
            self._loadedRuns[self._key(*goingNative)] = 0
            loadedFromNative = True
        # the data cannot be loaded -- this is an error condition
        else:
            raise RuntimeError(f"Could not load run {runNumber} from file {filename}")

        if loadedFromNative:
            # clone the native raw workspace
            # then reduce its resolution to make the lite raw workspace
            self.getCloneOfWorkspace(nativeRawWorkspaceName, rawWorkspaceName)
            self._loadedRuns[key] = 0
            self.convertToLiteMode(rawWorkspaceName)

        # create a copy of the raw data for use
        workspaceName = self._createCopyNeutronWorkspaceName(runNumber, useLiteMode, self._loadedRuns[key] + 1)
        data["result"] = self.getCloneOfWorkspace(rawWorkspaceName, workspaceName) is not None
        data["workspace"] = workspaceName
        self._loadedRuns[key] += 1

        return data

    def fetchLiteDataMap(self) -> WorkspaceName:
        """
        The lite data map is a special grouping workspace which associates groups of nearby pixels to create Lite mode.

        :return: the name of the loaded lite data map
        :rtype: WorkspaceName
        """
        item = GroceryListItem.builder().grouping("Lite").build()
        return self.fetchGroupingDefinition(item)["workspace"]

    def fetchGroupingDefinition(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch a single grouping definition.

        :param item: a GroceryListItem corresponding to the grouping desired
        :type item: GroceryListItem
        :return: a dictionary with keys

            - "result", true if everything ran correctly
            - "loader", either "LoadGroupingDefinition" or "cached"
            - "workspace", the name of the new grouping workspace in the ADS

        :rtype: Dict[str, Any]
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
            filename = self._createGroupingFilename(item.runNumber, item.groupingScheme, item.useLiteMode)
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

        :param item: a GroceryListItem corresponding to the calibration table
        :type item: GroceryListItem
        :return: a dictionary with

            - "result", true if everything ran correctly
            - "loader", either "LoadDiffractionCalibrationWorkspaces" or "cached"
            - "workspace", the name of the new workspace in the ADS;
                this defaults to the name of the calibration-table workspace,
                but the mask workspace will be loaded as well

        :rtype: Dict[str, Any]
        """
        runNumber, version, useLiteMode = item.runNumber, item.version, item.useLiteMode
        tableWorkspaceName = self._createDiffcalTableWorkspaceName(runNumber, useLiteMode, version)
        maskWorkspaceName = self._createDiffcalMaskWorkspaceName(runNumber, useLiteMode, version)

        if self.workspaceDoesExist(tableWorkspaceName):
            data = {
                "result": True,
                "loader": "cached",
                "workspace": tableWorkspaceName,
            }
        else:
            # table + mask are in the same hdf5 file:
            filename = self._createDiffcalTableFilename(runNumber, useLiteMode, version)

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

    @validate_arguments
    def fetchDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool, version: Version) -> WorkspaceName:
        tableWorkspaceName = self._createDiffcalTableWorkspaceName("default", useLiteMode, version)
        self.mantidSnapper.CalculateDiffCalTable(
            "Generate the default diffcal table",
            InputWorkspace=self._fetchInstrumentDonor(runNumber, useLiteMode),
            CalibrationTable=tableWorkspaceName,
        )
        self.mantidSnapper.executeQueue()
        if self.workspaceDoesExist(tableWorkspaceName):
            return tableWorkspaceName
        else:
            raise RuntimeError(f"Could not create a default diffcal file for run {runNumber}")

    def fetchNormalizationWorkspaces(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch normalization workspaces

        :param item: a GroceryListItem corresponding to the normalization workspaces
        :type item: GroceryListItem
        :return: a dictionary with

            - "result", true if everything ran correctly
            - "loader", either "LoadNexusProcessed" or "cached"
            - "workspace", the name of the new workspace in the ADS;
                this defaults to the name of the normalization workspace,

        :rtype: Dict[str, Any]
        """

        runNumber, useLiteMode, version = item.runNumber, item.useLiteMode, item.version
        workspaceName = self._createNormalizationWorkspaceName(runNumber, useLiteMode, version)

        if self.workspaceDoesExist(workspaceName):
            data = {
                "result": True,
                "loader": "cached",
                "workspace": workspaceName,
            }
        else:
            filename = self._createNormalizationWorkspaceFilename(runNumber, useLiteMode, version)

            # Unless overridden: use a cached workspace as the instrument donor.
            instrumentPropertySource, instrumentSource = (
                ("InstrumentDonor", self._fetchInstrumentDonor(runNumber, useLiteMode))
                if not item.instrumentPropertySource
                else (item.instrumentPropertySource, item.instrumentSource)
            )
            data = self.grocer.executeRecipe(
                filename=filename,
                workspace=workspaceName,
                loader="LoadNexusProcessed",
                instrumentPropertySource=instrumentPropertySource,
                instrumentSource=instrumentSource,
            )
            data["workspace"] = workspaceName

        return data

    def fetchGroceryList(self, groceryList: List[GroceryListItem]) -> List[WorkspaceName]:
        """
        :param groceryList: a list of GroceryListItems indicating the workspaces to create
        :type groceryList: List[GrocerListItem]
        :return: the names of the workspaces, in the same order as items in the grocery list
        :rtype: List[WorkspaceName]
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
                    res = self.fetchWorkspace(
                        self._createDiffcalOutputWorkspaceFilename(item),
                        self._createDiffcalOutputWorkspaceName(item),
                        loader="ReheatLeftovers",
                    )
                case "diffcal_diagnostic":
                    self.fetchWorkspace(
                        self._createDiffcalDiagnosticWorkspaceFilename(item),
                        self._createDiffcalOutputWorkspaceName(item),
                        loader="LoadNexusProcessed",
                    )
                case "diffcal_table":
                    indexor = self.dataService.calibrationIndexor(item.runNumber, item.useLiteMode)
                    if not isinstance(item.version, int):
                        item.version = indexor.latestApplicableVersion(item.runNumber)
                    record = indexor.readRecord(item.version)
                    if record is not Nonrecord:
                        item.runNumber = record.runNumber
                    # NOTE: fetchCalibrationWorkspaces will set the workspace name
                    # to that of the table workspace.  Because of possible confusion with
                    # the behavior of mask workspace, the workspace name is manually set here.
                    tableWorkspaceName = self._createDiffcalTableWorkspaceName(
                        str(item.runNumber), item.useLiteMode, item.version
                    )
                    res = self.fetchCalibrationWorkspaces(item)
                    res["workspace"] = tableWorkspaceName
                case "diffcal_mask":
                    # NOTE: fetchCalibrationWorkspaces will set the workspace name
                    # to that of the table workspace, not the mask.  This must be
                    # manually overwritten with correct workspace name.
                    maskWorkspaceName = self._createDiffcalMaskWorkspaceName(
                        item.runNumber, item.useLiteMode, item.version
                    )
                    res = self.fetchCalibrationWorkspaces(item)
                    res["workspace"] = maskWorkspaceName
                case "normalization":
                    indexor = self.dataService.normalizationIndexor(item.runNumber, item.useLiteMode)
                    if not isinstance(item.version, int):
                        logger.info(f"Version not detected for run {item.runNumber}, fetching from index.")
                        item.version = indexor.latestApplicableVersion(item.runNumber)
                        if not isinstance(item.version, int):
                            raise RuntimeError(
                                f"Could not find any Normalizations associated with run {item.runNumber}"
                            )
                        logger.info(f"Found version {item.version} for run {item.runNumber}")
                    record = indexor.readRecord(item.version)
                    if record is not Nonrecord:
                        item.runNumber = record.runNumber
                    logger.info(f"Fetching normalization workspace for run {item.runNumber}, version {item.version}")
                    res = self.fetchNormalizationWorkspaces(item)
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

        :param groceryDict: a dictionary of GroceryListItems, keyed by a property name
        :type groceryDict: Dict[str, GrocerListItem]
        :param kwargs: keyword arguments will be added to the created dictionary as argName: argValue.
            Use this to add additional workspaces to the dictionary for easier use in recipes.
        :type kwargs: Dict[string, WorkspaceName]
        :return: the workspace names of the fetched groceries, matched to their original keys
        :rtype: List[WorkspaceName]
        """
        groceryList = groceryDict.values()
        groceryNames = groceryDict.keys()
        groceries = self.fetchGroceryList(groceryList)
        data = dict(zip(groceryNames, groceries))
        data.update(kwargs)
        return data

    def convertToLiteMode(self, workspace: WorkspaceName):
        """
        Given a workspace, converts it (in place) to Lite mode using the lite data service.

        :param workspace: the workspace to be converted to lite mode (in place)
        :type workspace: WorkspaceName
        """

        from snapred.backend.service.LiteDataService import LiteDataService

        LiteDataService().reduceLiteData(workspace, workspace)

    ## CLEANUP METHODS

    # TODO: move `deleteWorkspace` methods to `DataExportService` via `LocalDataService`
    def deleteWorkspace(self, name: WorkspaceName):
        """
        Deletes a workspace from Mantid.
        Mostly for cleanup at the Service Layer.
        NOTE: this will not delete workspaces in CIS mode

        :param name: the name of the workspace in the ADS to be deleted.
        :type name: WorkspaceName
        """
        self.mantidSnapper.WashDishes(
            f"Washing dish {name}",
            Workspace=name,
        )
        self.mantidSnapper.executeQueue()

    # TODO: move `deleteWorkspace` methods to `DataExportService` via `LocalDataService`
    def deleteWorkspaceUnconditional(self, name: WorkspaceName):
        """
        Deletes a workspace from Mantid, regardless of CIS mode.
        Use when a workspace MUST be deleted for proper behavior.

        :param name: the name of the workspace in the ADS to be deleted
        :type name: WorkspaceName
        """
        if self.workspaceDoesExist(name):
            self.mantidSnapper.DeleteWorkspace(
                f"Deleting workspace {name}",
                Workspace=name,
            )
            self.mantidSnapper.executeQueue()
        else:
            pass

    def clearADS(self, exclude: List[WorkspaceName] = [], cache: bool = False):
        """
        Clears ADS of all workspaces except those in the exclude list and cache.

        :param exclude: a list of workspaces to retain in the ADS after clear
        :type exclude: List[WorkspaceName]
        :param cache: whether or not to clear cached workspaces (True = yes, clear the cache), optional (defaults to False)
        :type cache: bool
        """  # noqa E501
        workspacesToClear = mtd.getObjectNames()
        # filter exclude
        workspacesToClear = [w for w in workspacesToClear if w not in exclude]
        # filter caches
        if not cache:
            workspaceCache = self.getCachedWorkspaces()
            workspacesToClear = [w for w in workspacesToClear if w not in workspaceCache]
        # clear the workspaces
        for workspace in workspacesToClear:
            self.deleteWorkspaceUnconditional(workspace)

        self.rebuildCache()
