# ruff: noqa: F811
import json
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np
from mantid.dataobjects import MaskWorkspace
from pydantic import validate_call

from snapred.backend.dao.indexing.Versioning import VERSION_START, Version, VersionState
from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.LiveMetadata import LiveMetadata
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.util.PV_logs_util import mappingFromRun
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.meta.Config import Config
from snapred.meta.decorators.ConfigDefault import ConfigDefault, ConfigValue
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.InternalConstants import ReservedRunNumber
from snapred.meta.mantid.WorkspaceNameGenerator import (
    NameBuilder,
    WorkspaceName,
    WorkspaceType,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceNameGenerator as wng,
)

logger = snapredLogger.getLogger(__name__)


@Singleton
class GroceryService:
    """
    You need some data?

    Yeah, I can get that for you.

    Just send me a list.
    """

    diffcalTableFileExtension: str = ".h5"

    def __init__(self, dataService: LocalDataService = None):
        # 'LocalDataService' is a singleton:
        #   declare it here as an instance attribute, rather than a class attribute,
        #   to allow singleton reset during testing.
        self.dataService = self._defaultClass(dataService, LocalDataService)

        self.workspaceMetadataService = WorkspaceMetadataService()

        # _loadedRuns caches a count of the number of copies made from the neutron-data workspace
        #   corresponding to a given (runNumber, isLiteMode) key:
        #   This count is:
        #     None: if a workspace is not loaded;
        #        0: if it is loaded, but no copies have been made;
        #       >0: if any copies have been made, since loading
        self._loadedRuns: Dict[Tuple[str, bool], int] = {}

        # _liveDataKeys: cache keys into `_loadedRuns` for any live data that has been loaded
        self._liveDataKeys: List[Tuple[str, bool]] = []

        # Cache maps to workspace names, for various purposes
        self._loadedGroupings: Dict[Tuple[str, str, bool], str] = {}
        self._loadedInstruments: Dict[Tuple[str, bool], str] = {}

        self.normalizationCache: Set[WorkspaceName] = set()

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
    @ConfigDefault
    def getIPTS(self, runNumber: str, instrumentName: str = ConfigValue("instrument.name")) -> str:
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

    def _createNeutronFilePath(self, runNumber: str, useLiteMode: bool) -> Path:
        return self.dataService.createNeutronFilePath(runNumber, useLiteMode)

    @validate_call
    def _createGroupingFilename(self, runNumber: str, groupingScheme: str, useLiteMode: bool) -> str:
        if groupingScheme == "Lite":
            path = str(Config["instrument.lite.map.file"])
        else:
            groupingMap = self.dataService.readGroupingMap(runNumber)
            path = groupingMap.getMap(useLiteMode)[groupingScheme].definition
        return str(path)

    @validate_call
    def _createDiffCalOutputWorkspaceFilename(self, item: GroceryListItem) -> str:
        ext = Config["calibration.diffraction.output.extension"]
        return str(
            self._getCalibrationDataPath(item.runNumber, item.useLiteMode, item.diffCalVersion)
            / (self._createDiffCalOutputWorkspaceName(item) + ext)
        )

    @validate_call
    def _createDiffCalDiagnosticWorkspaceFilename(self, item: GroceryListItem) -> str:
        ext = Config["calibration.diffraction.diagnostic.extension"]
        return str(
            self._getCalibrationDataPath(item.runNumber, item.useLiteMode, item.diffCalVersion)
            / (self._createDiffCalOutputWorkspaceName(item) + ext)
        )

    def _createDiffCalTableFilepathFromWsName(
        self,
        runNumber: str,
        useLiteMode: bool,
        version: Optional[int],
        wsName: WorkspaceName,
        alternativeState: str = None,
    ) -> str:
        calibrationDataPath = self._getCalibrationDataPath(
            runNumber, useLiteMode, version, alternativeState=alternativeState
        )
        expectedWsName = self.createDiffCalTableWorkspaceName(runNumber, useLiteMode, version)
        if wsName != expectedWsName:
            record = self.dataService.calibrationIndexer(runNumber, useLiteMode).readRecord(version)
            raise ValueError(
                f"Workspace name {wsName} does not match the expected diffcal table workspace name for run {runNumber}",
                f"(i.e. {expectedWsName}), debug info: {record.model_dump_json(indent=4)}, path: {calibrationDataPath}",
            )

        return str(calibrationDataPath / (wsName + self.diffcalTableFileExtension))

    @validate_call
    def _createDiffCalTableFilepath(self, runNumber: str, useLiteMode: bool, version: Optional[int]) -> str:
        return str(
            Path(self._getCalibrationDataPath(runNumber, useLiteMode, version))
            / (self.createDiffCalTableWorkspaceName(runNumber, useLiteMode, version) + self.diffcalTableFileExtension)
        )

    @validate_call
    def _lookupNormalizationWorkspaceFilename(self, runNumber: str, useLiteMode: bool, version: Optional[int]) -> str:
        # NOTE: The normCal Record currently does NOT store workspace type -> workspace name mappings.
        #       It is just a list of workspace names. So we need to infer.
        return str(
            Path(self._getNormalizationDataPath(runNumber, useLiteMode, version))
            / (
                self._createNormalizationWorkspaceName(runNumber, useLiteMode, version, False)
                + Config["calibration.normalization.output.ws.extension"]
            )
        )

    @validate_call
    def _createReductionPixelMaskWorkspaceFilename(self, runNumber: str, useLiteMode: bool, timestamp: float) -> str:
        return str(
            Path(self._getReductionDataPath(runNumber, useLiteMode, timestamp))
            / (
                self._createReductionPixelMaskWorkspaceName(runNumber, useLiteMode, timestamp)
                + self.diffcalTableFileExtension
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
            return wng.liteDataMap().build()
        instr = "lite" if useLiteMode else "native"
        return f"__{Config['grouping.workspacename.' + instr]}_{groupingScheme}_{runNumber}"

    def _createDiffCalInputWorkspaceName(self, runNumber: str) -> WorkspaceName:
        return wng.diffCalInput().runNumber(runNumber).build()

    def _createDiffCalOutputWorkspaceName(self, item: GroceryListItem) -> WorkspaceName:
        return (
            wng.diffCalOutput()
            .unit(item.unit)
            .runNumber(item.runNumber)
            .version(item.diffCalVersion)
            .group(item.groupingScheme)
            .build()
        )

    @staticmethod
    def _findFirstWorkspaceOfType(items_, wsType: WorkspaceType) -> WorkspaceName | None:
        # Return the first workspace in the record's list for the correct workspace type.

        # `CalibrationRecord.workspaces: Dict[WorkspaceType, List[WorkspaceName]]`

        nameTuple = next(filter(lambda kv: kv[0] == wsType, items_), None)
        if nameTuple is not None:
            # return the first name in the list
            return nameTuple[1][0] if len(nameTuple[1]) > 0 else None
        return None

    def _lookupDiffCalWorkspaceNames(
        self, runNumber: str, useLiteMode: bool, version: Optional[int], alternativeState: Optional[str] = None
    ) -> Tuple[WorkspaceName, WorkspaceName | None]:
        # tableName, maskName = self._lookupDiffCalWorkspaceNames(<run number>, <use lite mode> [, ...])

        indexer = self.dataService.calibrationIndexer(runNumber, useLiteMode, alternativeState=alternativeState)
        if not isinstance(version, int):
            version = indexer.latestApplicableVersion(runNumber)

        record = indexer.readRecord(version)
        if record is None:
            raise RuntimeError(f"Could not find calibration record for run {runNumber} and version {version}")

        tableWorkspaceName = self._findFirstWorkspaceOfType(record.workspaces.items(), WorkspaceType.DIFFCAL_TABLE)
        if tableWorkspaceName is None:
            raise RuntimeError(f"Could not find any table workspaces in calibration record for run: '{runNumber}'")

        maskWorkspaceName = self._findFirstWorkspaceOfType(record.workspaces.items(), WorkspaceType.DIFFCAL_MASK)
        # A mask workspace is optional: maskWorkspaceName may be None.

        return tableWorkspaceName, maskWorkspaceName

    @validate_call
    def createDiffCalTableWorkspaceName(
        self,
        runNumber: str,
        useLiteMode: bool,  # noqa: ARG002
        version: Optional[Version],
    ) -> WorkspaceName:
        """
        NOTE: This method will IGNORE runNumber if the provided version is VersionState.DEFAULT
        """
        wsName = wng.diffCalTable().runNumber(runNumber).version(version).build()
        if version in [VersionState.DEFAULT, VERSION_START()]:
            wsName = wng.diffCalTable().runNumber("default").version(VersionState.DEFAULT).build()
        return wsName

    @validate_call
    def _createDiffCalMaskWorkspaceName(
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
        hidden: bool,
    ) -> WorkspaceName:
        return wng.rawVanadium().runNumber(runNumber).version(version).hidden(hidden).build()

    def _createReductionPixelMaskWorkspaceName(
        self,
        runNumber: str,
        useLiteMode: bool,  # noqa: ARG002
        timestamp: Optional[float],
    ) -> WorkspaceName:
        return wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()

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
        return self.mantidSnapper.mtd.unique_hidden_name()

    def workspaceDoesExist(self, name: WorkspaceName):
        """
        Check if the workspace exists in the ADS.

        :param name: the name of the workspace to check for existence
        :type name: WorkspaceName
        :return: True if the workspace exists, False if it does not exist
        :rtype: bool
        """
        return self.mantidSnapper.mtd.doesExist(name)

    def renameWorkspace(self, oldName: WorkspaceName, newName: WorkspaceName) -> WorkspaceName:
        """
        Renames a workspace in Mantid's ADS.

        :param oldName: the original name of the workspace in the ADS
        :type oldName: WorkspaceName
        :param newName: the name to replace the workspace name in the ADS
        :type newName: WorkspaceName
        """
        if oldName != newName:
            self.mantidSnapper.RenameWorkspace(
                f"Renaming '{oldName}' to '{newName}'",
                InputWorkspace=oldName,
                OutputWorkspace=newName,
            )
            self.mantidSnapper.executeQueue()
        return newName

    def renameWorkspaces(self, oldNames: List[WorkspaceName], newNames: List[WorkspaceName]):
        """
        Renames a list of workspaces in Mantid's ADS.

        :param oldNames: the original names of the workspaces in the ADS
        :type oldNames: List[WorkspaceName]
        :param newNames: the names to replace the workspace names in the ADS
        :type newNames: List[WorkspaceName]
        """
        self.mantidSnapper.RenameWorkspaces(
            "Renaming several workspaces",
            InputWorkspaces=oldNames,
            WorkspaceNames=newNames,
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
            return self.mantidSnapper.mtd[name]
        else:
            return None

    def getCloneOfWorkspace(self, name: WorkspaceName, copy: WorkspaceName) -> WorkspaceName:
        """
        Simple wrapper to clone a workspace, for the service layer.

        :param name: the name of the workspace to clone in the ADS
        :type name: WorkspaceName
        :param copy: the name of the resulting cloned workspace
        :type copy: WorkspaceName
        :return: the name of the cloned workspace
        :rtype: WorkspaceName
        """
        self.mantidSnapper.CloneWorkspace(
            f"Cloning {name} to {copy}",
            InputWorkspace=name,
            OutputWorkspace=copy,
        )
        self.mantidSnapper.executeQueue()
        return copy

    def writeWorkspaceMetadataAsTags(self, workspaceName: WorkspaceName, workspaceMetadata: WorkspaceMetadata):
        """
        Write workspace metadata to the workspace as tags.

        :param workspaceName: the name of the workspace to write the metadata to
        :type workspaceName: WorkspaceName
        :param workspaceMetadata: the metadata to write to the workspace
        :type workspaceMetadata: WorkspaceMetadata
        """
        metadata = workspaceMetadata.dict()
        for logname in metadata.keys():
            if metadata[logname] != UNSET:
                self.setWorkspaceTag(workspaceName, logname, metadata[logname])

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

    def checkPixelMask(self, pixelMask: WorkspaceName) -> bool:
        """
        Check if a pixel mask is valid.
        :param pixelMask: the name of the MaskWorkspace to check
        :type pixelMask: Workspacename
        :return: True if pixelMask is a non-trivial MaskWorkspace in the ADS.  False otherwise.
        :rtype: bool
        """
        # A non-trivial mask is a mask that has a non-zero number of masked pixels.

        if not self.mantidSnapper.mtd.doesExist(pixelMask):
            return False
        else:
            mask = self.mantidSnapper.mtd[pixelMask]
            if not isinstance(mask, MaskWorkspace):
                return False
            elif mask.getNumberMasked() == 0:
                return False
            else:
                return True

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
        This uses the run number (and lite mode) to locate the proper state, and from that the proper instrument
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
                if runNumber not in ReservedRunNumber.values():
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
        :type detectorState: DetectorState
        """

        # Add _ALL_ of the sample logs required to determine the instrument state:
        #
        #   * these logs include `det_arc..` and `det_lin..` parameters, which determine
        #   the instrument position, as well as the logs for <wavelength>, <frequency>
        #   and <guide state>.
        #
        #   * NOTE: after adding the logs, it is necessary to update the instrument to
        #   factor in these new parameters, or else calculations will be inconsistent.
        #   This is done with a call to `ws->populateInstrumentParameters()` from within mantid.
        #   This call only needs to happen with the last log

        logNames = [
            "det_arc1",
            "det_arc2",
            "BL3:Chop:Skf1:WavelengthUserReq",
            "BL3:Det:TH:BL:Frequency",
            "BL3:Mot:OpticsPos:Pos",
            "det_lin1",
            "det_lin2",
        ]
        logTypes = [
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
        ]
        logValues = [
            str(detectorState.arc[0]),
            str(detectorState.arc[1]),
            str(detectorState.wav),
            str(detectorState.freq),
            str(detectorState.guideStat),
            str(detectorState.lin[0]),
            str(detectorState.lin[1]),
        ]
        self.mantidSnapper.AddSampleLogMultiple(
            f"Updating instrument parameters for {wsName}",
            Workspace=wsName,
            LogNames=logNames[:-1],
            LogValues=logValues[:-1],
            LogTypes=logTypes[:-1],
            ParseType=False,
        )
        self.mantidSnapper.AddSampleLog(
            "...",
            Workspace=wsName,
            LogName=logNames[-1],
            logText=logValues[-1],
            logType=logTypes[-1],
            UpdateInstrumentParameters=True,
        )
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

    @validate_call
    def _getCalibrationDataPath(
        self, runNumber: str, useLiteMode: bool, version: Optional[int], alternativeState: Optional[str] = None
    ) -> str:
        """
        Get a path to the directory with the calibration data

        :param runNumber: a run number, whose state will be looked up
        :type runNumber: str
        :param version: the calibration version to use in the lookup
        :type version: int
        """
        return self.dataService.calibrationIndexer(
            runNumber, useLiteMode, alternativeState=alternativeState
        ).versionPath(version)

    @validate_call
    def _getNormalizationDataPath(self, runNumber: str, useLiteMode: bool, version: Optional[int]) -> str:
        """
        Get a path to the directory with the normalization data

        :param runNumber: a run number, whose state will be looked up
        :type runNumber: str
        :param version: the normalization version to use in the lookup
        :type version: int
        """
        return self.dataService.normalizationIndexer(runNumber, useLiteMode).versionPath(version)

    @validate_call
    def _getReductionDataPath(self, runNumber: str, useLiteMode: bool, timestamp: float) -> str:
        """
        Get a path to the directory with the reduction data

        :param runNumber: a run number, whose state will be looked up
        :type runNumber: str
        :param timestamp: the reduction timestamp to use in the lookup
        :type timestamp: float
        """
        return self.dataService._constructReductionDataPath(runNumber, useLiteMode, timestamp)

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

    def _fetchNeutronDataSingleUse(self, item: GroceryListItem, missingDataHandler: Callable) -> Dict[str, Any]:
        """
        Fetch a neutron data file, without copy-protection.
        If the workspace is truly only needed once, this saves time and memory.
        If the workspace needs to be loaded more than once, use the cached method.

        :param item: the grocery-list item
        :type item: GroceryListItem
        :return: a dictionary with the following keys

            - "result": true if everything ran correctly
            - "loader": the loader that was used by the algorithm; use it next time
            - "workspace": the name of the workspace created in the ADS

        :rtype: Dict[str, Any]
        """

        runNumber, useLiteMode, loader = item.runNumber, item.useLiteMode, item.loader

        data = {}
        key = self._key(runNumber, useLiteMode)
        rawWorkspaceName: WorkspaceName = self._createRawNeutronWorkspaceName(runNumber, useLiteMode)
        filepath: Path = self._createNeutronFilePath(runNumber, useLiteMode)

        # This checks to make sure existing cache is valid
        self._updateNeutronCacheFromADS(runNumber, useLiteMode)

        # 1. check cache
        # 2. check if file exists
        # e. cannot find data.
        workspaceName: str = None
        if self._loadedRuns.get(key) is not None:
            data = {"loader": "cached"}
            workspaceName = self._createNeutronWorkspaceName(runNumber, useLiteMode)
            self.getCloneOfWorkspace(rawWorkspaceName, workspaceName)
        elif filepath.exists() and item.liveDataArgs is None:
            workspaceName = self._createNeutronWorkspaceName(runNumber, useLiteMode)
            data = self.grocer.executeRecipe(str(filepath), workspaceName, loader)
        else:
            data = missingDataHandler()
            workspaceName = data["workspace"]

        data["workspace"] = workspaceName
        # NOTE: When would it ever be false?  If the workspace is not found, it should raise an error.
        data["result"] = True
        return data

    def _fetchLiveData(self, item: GroceryListItem):
        loader = "LoadLiveData"
        runNumber, useLiteMode, liveDataArgs = item.runNumber, item.useLiteMode, item.liveDataArgs
        workspaceName: WorkspaceName = self._createNeutronWorkspaceName(runNumber, useLiteMode)
        # Live-data fallback
        if self.dataService.hasLiveDataConnection():
            if liveDataArgs is None:
                # Only warn if live-data mode hasn't been explicitly requested.
                logger.warning(f"Input data for run '{runNumber}' was not found in IPTS, trying live-data fallback...")

            # When not specified in the `liveDataArgs` or when `liveDataArgs.duration == 0`,
            #   the default behavior will be to load the entire run.
            startTime = (
                (datetime.utcnow() - liveDataArgs.duration).isoformat()
                if liveDataArgs is not None and liveDataArgs.duration != 0
                else LiveMetadata.FROM_START_ISO8601
            )

            loaderArgs = {
                "Facility": Config["liveData.facility.name"],
                "Instrument": Config["liveData.instrument.name"],
                "AccumulationMethod": Config["liveData.accumulationMethod"],
                "PreserveEvents": True,
                "StartTime": startTime,
            }
            data = self.grocer.executeRecipe(workspace=workspaceName, loader=loader, loaderArgs=json.dumps(loaderArgs))
            data["fromLiveData"] = True
            if data["result"]:
                logs = mappingFromRun(self.mantidSnapper.mtd[workspaceName].getRun())
                liveRunNumber = str(logs["run_number"])

                if int(runNumber) != int(liveRunNumber):
                    # Live run number is unexpected => there has been a live-data run-state change.
                    self.deleteWorkspaceUnconditional(workspaceName)
                    data = {"result": False}
                    if liveDataArgs is not None:
                        raise LiveDataState.runStateTransition(liveRunNumber, runNumber)
                    raise RuntimeError(
                        f"Neutron data for run '{runNumber}' is not present on disk, " + "nor is it the live-data run"
                    )
                self._loadedRuns[self._key(runNumber, False)] = 0
                self._liveDataKeys.append(self._key(runNumber, False))
        else:
            raise ConnectionError("no live-data connection is available")
        return data

    def _fetchNeutronDataNative(self, item: GroceryListItem) -> Dict[str, Any]:
        _item = item.model_copy(deep=True)
        _item.useLiteMode = False

        def tryLiveData():
            try:
                return self._fetchLiveData(_item)
            except ConnectionError:
                raise RuntimeError(
                    (
                        f"Neutron data for run '{_item.runNumber}' is not present on disk,"
                        " and no live-data connection is available."
                    )
                )

        return self._fetchNeutronDataSingleUse(_item, tryLiveData)

    def _fetchNeutronDataLite(self, item: GroceryListItem, export=False) -> Dict[str, Any]:
        _item = item.model_copy(deep=True)
        _item.useLiteMode = True

        def tryNative():
            data = self._fetchNeutronDataNative(_item)
            data["fromNative"] = data["workspace"]
            nativeWorkspaceName = data["workspace"]
            liteWorkspaceName = self._createNeutronWorkspaceName(_item.runNumber, True)
            self.getCloneOfWorkspace(nativeWorkspaceName, liteWorkspaceName)
            self.convertToLiteMode(liteWorkspaceName, export=data.get("fromLiveData") is None and export)
            data["workspace"] = liteWorkspaceName
            return data

        return self._fetchNeutronDataSingleUse(_item, tryNative)

    def _fetchNeutronDataCached(self, item: GroceryListItem, loadMethod: Callable, **kwargs) -> Dict[str, Any]:
        # 1. get single use neutron data
        # 2. rename it to raw
        # 3. clone it with proper name scheme
        runNumber, useLiteMode = item.runNumber, item.useLiteMode

        # NOTE: This relies on _fetchNeutronDataSingleUse to handle the case where the data is not found/cached
        data = loadMethod(item, **kwargs)
        rawWorkspaceName = data["workspace"]
        key = self._key(runNumber, useLiteMode)
        if key not in self._loadedRuns:
            self._loadedRuns[key] = 0
            rawWorkspaceName = self.renameWorkspace(
                rawWorkspaceName, self._createRawNeutronWorkspaceName(runNumber, useLiteMode)
            )
        if "fromNative" in data:
            nativeKey = self._key(runNumber, False)
            if nativeKey not in self._loadedRuns:
                self._loadedRuns[nativeKey] = 0
                self.renameWorkspace(data["fromNative"], self._createRawNeutronWorkspaceName(runNumber, False))

        workspaceName = self._createCopyNeutronWorkspaceName(runNumber, useLiteMode, self._loadedRuns[key] + 1)
        data["result"] = self.getCloneOfWorkspace(rawWorkspaceName, workspaceName) is not None
        data["workspace"] = workspaceName
        self._loadedRuns[key] += 1
        return data

    def fetchNeutronDataSingleUse(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch a neutron data file, without copy-protection.
        If the workspace is truly only needed once, this saves time and memory.
        If the workspace needs to be loaded more than once, use the cached method.

        :param item: the grocery-list item
        :type item: GroceryListItem
        :return: a dictionary with the following keys

            - "result": true if everything ran correctly
            - "loader": the loader that was used by the algorithm; use it next time
            - "workspace": the name of the workspace created in the ADS

        :rtype: Dict[str, Any]
        """
        useLiteMode = item.useLiteMode
        result = None

        if useLiteMode:
            result = self._fetchNeutronDataLite(item, export=False)
        else:
            result = self._fetchNeutronDataNative(item)
        self._processNeutronDataCopy(item, result["workspace"])
        return result

    def fetchNeutronDataCached(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch a nexus data file using a cache system to prevent double-loading from disk

        :param item: the grocery-list item
        :type item: GroceryListItem
        :return: a dictionary with the following keys

            - "result": true if everything ran correctly
            - "loader": the loader that was used by the algorithm; use it next time
            - "workspace": the name of the workspace created in the ADS

        :rtype: Dict[str, Any]
        """
        useLiteMode = item.useLiteMode
        result = None

        if useLiteMode:
            result = self._fetchNeutronDataCached(item, self._fetchNeutronDataLite, export=True)
        else:
            result = self._fetchNeutronDataCached(item, self._fetchNeutronDataNative)
        self._processNeutronDataCopy(item, result["workspace"])
        return result

    def clearLiveDataCache(self):
        """
        Clear cache for and delete any live-data workspaces.
        """
        while self._liveDataKeys:
            key = self._liveDataKeys.pop()
            del self._loadedRuns[key]
            self.deleteWorkspaceUnconditional(self._createRawNeutronWorkspaceName(*key))

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
        stateId, _ = self.dataService.generateStateId(item.runNumber)
        key = self._key(item.groupingScheme, stateId, item.useLiteMode)
        workspaceName = self._createGroupingWorkspaceName(item.groupingScheme, item.runNumber, item.useLiteMode)
        workspaceName = self._loadedGroupings.get(key, workspaceName)

        self._updateGroupingCacheFromADS(key, workspaceName)

        if key in self._loadedGroupings:
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
        runNumber, version, useLiteMode, alternativeState = (
            item.runNumber,
            item.diffCalVersion,
            item.useLiteMode,
            item.alternativeState,
        )
        tableWorkspaceName, maskWorkspaceName = self._lookupDiffCalWorkspaceNames(runNumber, useLiteMode, version)

        # Table + mask are in the same hdf5 file: all of these clauses must deal with _both_!
        if self.workspaceDoesExist(tableWorkspaceName) and (
            maskWorkspaceName is None or self.workspaceDoesExist(maskWorkspaceName)
        ):
            data = {
                "result": True,
                "loader": "cached",
                "workspace": tableWorkspaceName,
            }
        else:
            # table + mask are in the same hdf5 file:
            filename = self._createDiffCalTableFilepathFromWsName(
                runNumber, useLiteMode, version, tableWorkspaceName, alternativeState=alternativeState
            )

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
                workspace=maskWorkspaceName if bool(maskWorkspaceName) else "",
                loader="LoadCalibrationWorkspaces",
                instrumentPropertySource=instrumentPropertySource,
                instrumentSource=instrumentSource,
                loaderArgs=json.dumps(
                    {
                        "CalibrationTable": tableWorkspaceName,
                        "MaskWorkspace": maskWorkspaceName if bool(maskWorkspaceName) else "",
                    }
                ),
            )
            data["workspace"] = tableWorkspaceName

        return data

    # this isnt really a fetch method, this generates data
    @validate_call
    def fetchDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool, version: int) -> WorkspaceName:
        tableWorkspaceName = self.createDiffCalTableWorkspaceName("default", useLiteMode, version)
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

    def fetchNormalizationWorkspace(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch normalization workspace

        :param item: a GroceryListItem corresponding to the normalization workspaces
        :type item: GroceryListItem
        :return: a dictionary with

            - "result", true if everything ran correctly
            - "loader", either "LoadNexusProcessed" or "cached"
            - "workspace", the name of the new workspace in the ADS;
                this defaults to the name of the normalization workspace,

        :rtype: Dict[str, Any]
        """

        runNumber, useLiteMode, version, hidden = item.runNumber, item.useLiteMode, item.normCalVersion, item.hidden
        workspaceName = self._createNormalizationWorkspaceName(runNumber, useLiteMode, version, hidden)

        if self.workspaceDoesExist(workspaceName):
            data = {
                "result": True,
                "loader": "cached",
                "workspace": workspaceName,
            }
        else:
            # clear normalization cache
            # NOTE: I dont believe mtd.getObjectNames() returns hidden workspaces.
            for ws in self.normalizationCache:
                if self.workspaceDoesExist(ws):
                    self.deleteWorkspaceUnconditional(ws)
            self.normalizationCache = set()

            # then load the new one
            filename = self._lookupNormalizationWorkspaceFilename(runNumber, useLiteMode, version)

            # Note: 'LoadNexusProcessed' neither requires nor makes use of an instrument donor.
            data = self.grocer.executeRecipe(
                filename=filename,
                workspace=workspaceName,
                loader="LoadNexusProcessed",
            )
            self._processNeutronDataCopy(item, workspaceName)
            self.normalizationCache.add(workspaceName)
        data["workspace"] = workspaceName
        return data

    def fetchReductionPixelMask(self, item: GroceryListItem) -> Dict[str, Any]:
        """
        Fetch a reduction pixel mask

        :param item: a GroceryListItem corresponding to the pixel mask workspace
        :type item: GroceryListItem
        :return: a dictionary with

            - "result", true if everything ran correctly
            - "loader", either "LoadCalibrationWorkspaces" or "cached"
            - "workspace", the name of the new workspace in the ADS;

        :rtype: Dict[str, Any]
        """
        maskWorkspaceName = self._createReductionPixelMaskWorkspaceName(
            item.runNumber, item.useLiteMode, item.timestamp
        )
        if self.workspaceDoesExist(maskWorkspaceName):
            data = {
                "result": True,
                "loader": "cached",
                "workspace": maskWorkspaceName,
            }
        else:
            filename = self._createReductionPixelMaskWorkspaceFilename(item.runNumber, item.useLiteMode, item.timestamp)

            # Unless overridden: use a cached workspace as the instrument donor.
            instrumentPropertySource, instrumentSource = (
                ("InstrumentDonor", self._fetchInstrumentDonor(item.runNumber, item.useLiteMode))
                if not item.instrumentPropertySource
                else (item.instrumentPropertySource, item.instrumentSource)
            )

            # For now, reduction pixel masks share the "LoadCalibrationWorkspaces" loader
            data = self.grocer.executeRecipe(
                filename=filename,
                workspace=maskWorkspaceName,
                loader="LoadCalibrationWorkspaces",
                instrumentPropertySource=instrumentPropertySource,
                instrumentSource=instrumentSource,
                loaderArgs=json.dumps({"MaskWorkspace": maskWorkspaceName}),
            )

        return data

    # @validate_call # For the moment, `@validate_call` is not supported with `WorkspaceName`.
    def fetchCompatiblePixelMask(self, maskWSName: WorkspaceName, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        # Fetch a blank mask workspace compatible with the specified <run number> and <lite mode>

        templateWSName = self._fetchInstrumentDonor(runNumber, useLiteMode)

        ### The following code _duplicates_ that found in        ###
        ###     `tests/util/helpers.py`::`createCompatibleMask`: ###

        # Number of non-monitor pixels
        pixelCount = self.mantidSnapper.mtd[templateWSName].getInstrument().getNumberDetectors(True)

        mask = self.mantidSnapper.CreateWorkspace(
            "Creating pixel mask workspace",
            OutputWorkspace=maskWSName,
            NSpec=pixelCount,
            DataX=list(np.zeros((pixelCount,))),
            DataY=list(np.zeros((pixelCount,))),
            ParentWorkspace=templateWSName,
        )
        self.mantidSnapper.executeQueue()
        mask = self.mantidSnapper.mtd[mask]

        # Rebuild the spectra map "by hand" to exclude detectors which are monitors.
        info = mask.detectorInfo()
        ids = info.detectorIDs()

        # Warning: <detector info>.indexOf(id_) != <workspace index of detectors excluding monitors>
        wi = 0
        for id_ in ids:
            if info.isMonitor(info.indexOf(int(id_))):
                continue
            s = mask.getSpectrum(wi)
            s.setSpectrumNo(wi)
            s.setDetectorID(int(id_))
            wi += 1

        # Convert workspace to a MaskWorkspace instance.
        self.mantidSnapper.ExtractMask("Extracting mask", OutputWorkspace=maskWSName, InputWorkspace=maskWSName)
        self.mantidSnapper.executeQueue()
        assert isinstance(self.mantidSnapper.mtd[maskWSName], MaskWorkspace)

        return maskWSName

    def _createMonitorWorkspaceName(self, runNumber: str) -> WorkspaceName:
        # a monitor cannot be lite
        return wng.monitor().runNumber(runNumber).build()

    def fetchMonitorWorkspace(self, item: GroceryListItem) -> WorkspaceName:
        runNumber, useLiteMode = item.runNumber, False
        workspaceName = self._createMonitorWorkspaceName(runNumber)

        # monitors take ~1 second to load, probably unnecessary to cache

        filepath: Path = self._createNeutronFilePath(runNumber, useLiteMode)
        self.grocer.executeRecipe(str(filepath), workspaceName, "LoadNexusMonitors")

        return workspaceName

    def fetchDiffCalForSample(self, item: GroceryListItem) -> Tuple[WorkspaceName, WorkspaceName | None]:
        diffcalItem = item.model_copy(deep=True)
        alternativeState = diffcalItem.alternativeState
        indexer = self.dataService.calibrationIndexer(
            diffcalItem.runNumber, diffcalItem.useLiteMode, alternativeState=alternativeState
        )

        record = indexer.readRecord(diffcalItem.diffCalVersion)
        if record is not None:
            diffcalItem.runNumber = record.runNumber

        # NOTE: fetchCalibrationWorkspaces will set the workspace name
        # to that of the table workspace.  Because of possible confusion with
        # the behavior of the mask workspace, the workspace name is overridden here.

        tableWorkspaceName, maskWorkspaceName = self._lookupDiffCalWorkspaceNames(
            diffcalItem.runNumber,
            diffcalItem.useLiteMode,
            diffcalItem.diffCalVersion,
            alternativeState=alternativeState,
        )

        self.fetchCalibrationWorkspaces(diffcalItem)
        return tableWorkspaceName, maskWorkspaceName

    def fetchNormCalForSample(self, item: GroceryListItem) -> WorkspaceName:
        indexer = self.dataService.normalizationIndexer(item.runNumber, item.useLiteMode)

        # TODO: it looks like the following clause should be "unreachable code".
        #   `version` is validated to `int` at `GroceryListItem`.
        if not isinstance(item.normCalVersion, int):
            logger.info(f"Normalization version not detected for run {item.runNumber}: fetching from index.")
            item.normCalVersion = indexer.latestApplicableVersion(item.runNumber)
            if not isinstance(item.normCalVersion, int):
                raise RuntimeError(f"Could not find any Normalizations associated with run {item.runNumber}")
            logger.info(f"Found normalization version {item.normCalVersion} for run {item.runNumber}")

        record = indexer.readRecord(item.normCalVersion)

        if record is None:
            raise RuntimeError(
                f"Could not find normalization record for run {item.runNumber} and version {item.normCalVersion}"
            )

        normCalItem = item.model_copy(deep=True)

        normCalItem.runNumber = record.runNumber
        logger.info(f"Fetching normalization workspace for run {normCalItem.runNumber}, version {item.normCalVersion}")

        return self.fetchNormalizationWorkspace(normCalItem)

    def fetchGroceryList(self, groceryList: Iterable[GroceryListItem]) -> List[WorkspaceName]:
        """
        :param groceryList: a list of GroceryListItems indicating the workspaces to create
        :type groceryList: List[GroceryListItem]
        :return: the names of the workspaces, in the same order as items in the grocery list
        :rtype: List[WorkspaceName]
        """
        groceries = []
        result = {}
        for item in groceryList:
            match item.workspaceType:
                # for neutron data stored in a nexus file
                case "neutron":
                    if item.keepItClean:
                        result = self.fetchNeutronDataCached(item)
                    else:
                        result = self.fetchNeutronDataSingleUse(item)
                # for grouping definitions
                case "grouping":
                    result = self.fetchGroupingDefinition(item)
                case "diffcal":
                    result = {"result": False, "workspace": self._createDiffCalInputWorkspaceName(item.runNumber)}
                    raise RuntimeError(
                        "not implemented: no path available to fetch diffcal "
                        + f"input table workspace: '{result['workspace']}'"
                    )
                # for diffraction-calibration workspaces
                case "diffcal_output":
                    result = self.fetchWorkspace(
                        self._createDiffCalOutputWorkspaceFilename(item),
                        self._createDiffCalOutputWorkspaceName(item),
                        loader="LoadNexus",
                    )
                case "diffcal_diagnostic":
                    result = self.fetchWorkspace(
                        self._createDiffCalDiagnosticWorkspaceFilename(item),
                        self._createDiffCalOutputWorkspaceName(item),
                        loader="LoadNexusProcessed",
                    )
                case "diffcal_table":
                    diffCalWorkspace, _ = self.fetchDiffCalForSample(item)

                    # TODO: Remove this `result` return pattern.
                    #       It doesnt seem to be used downstream in any meaningful capacity.
                    result = {
                        "result": True,
                        "workspace": diffCalWorkspace if diffCalWorkspace is not None else "",
                    }

                case "diffcal_mask":
                    _, maskWorkspace = self.fetchDiffCalForSample(item)

                    # TODO: Remove this `result` return pattern.
                    result = {
                        "result": True,
                        "workspace": maskWorkspace if maskWorkspace is not None else "",
                    }

                case "normalization":
                    result = self.fetchNormCalForSample(item)
                case "reduction_pixel_mask":
                    maskWorkspaceName = self._createReductionPixelMaskWorkspaceName(  # noqa: F841
                        item.runNumber, item.useLiteMode, item.timestamp
                    )
                    result = self.fetchReductionPixelMask(item)
                case _:
                    raise RuntimeError(f"unrecognized 'workspaceType': '{item.workspaceType}'")
            # check that the fetch operation succeeded and if so append the workspace
            if result["result"] is True:
                groceries.append(result["workspace"])
            else:
                # NOTE: When would it ever be false?  We should throw exceptions as early as possible.
                #       and rely on native error handling.
                raise RuntimeError(f"Error fetching item {item.model_dump_json(indent=2)}")

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

    def convertToLiteMode(self, workspace: WorkspaceName, export: bool = True):
        """
        Given a workspace, converts it (in place) to Lite mode using the lite data service.

        :param workspace: the workspace to be converted to lite mode (in place)
        :type workspace: WorkspaceName
        :param export: export the converted data to disk
        :type export: bool
        """

        from snapred.backend.service.LiteDataService import LiteDataService

        _, tolerance = LiteDataService().createLiteData(workspace, workspace, export=export)

        tag = UNSET
        if Config["constants.LiteDataCreationAlgo.toggleCompressionTolerance"]:
            tag = tolerance

        metadata = WorkspaceMetadata(liteDataCompressionTolerance=tag)

        self.writeWorkspaceMetadataAsTags(workspace, metadata)

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

    def clearADS(self, exclude: List[WorkspaceName] = [], clearCache: bool = False):
        """
        Clears ADS of all workspaces except those in the exclude list and cache.

        :param exclude: a list of workspaces to retain in the ADS after clear
        :type exclude: List[WorkspaceName]
        :param clearCache: whether or not to clear cached workspaces
        :type clearCache: bool
        """  # noqa E501
        workspacesToClear = set(self.mantidSnapper.mtd.getObjectNames())
        # filter exclude
        workspacesToClear = workspacesToClear.difference(exclude)
        # properly handle workspace groups -- also exclude deleting their constituents
        for ws in exclude:
            if self.workspaceDoesExist(ws) and self.mantidSnapper.mtd[ws].isGroup():
                workspacesToClear = workspacesToClear.difference(self.mantidSnapper.mtd[ws].getNames())
        # filter caches
        if not clearCache:
            workspacesToClear = workspacesToClear.difference(self.getCachedWorkspaces())
        # clear the workspaces
        for workspace in workspacesToClear:
            self.deleteWorkspaceUnconditional(workspace)

        if clearCache:
            self.rebuildCache()

    def getResidentWorkspaces(self, excludeCache: bool):
        """
        Get the list of ADS-resident workspaces:

        - optionally exclude the cached workspaces from this list.
        """
        workspaces = set(self.mantidSnapper.mtd.getObjectNames())
        if excludeCache:
            workspaces = workspaces.difference(self.getCachedWorkspaces())
        return list(workspaces)

    def _filterEvents(self, runNumber, workspaceName):
        instrumentState = self.dataService.generateInstrumentState(runNumber)
        self.mantidSnapper.CropWorkspace(
            "Cropping workspace",
            InputWorkspace=workspaceName,
            OutputWorkspace=workspaceName,
            XMin=instrumentState.particleBounds.tof.minimum,
            XMax=instrumentState.particleBounds.tof.maximum,
        )
        config = instrumentState.instrumentConfig
        width = config.width
        frequency = config.frequency
        self.mantidSnapper.RemovePromptPulse(
            "Removing prompt pulse",
            InputWorkspace=workspaceName,
            OutputWorkspace=workspaceName,
            Width=width,
            Frequency=frequency,
        )
        self.mantidSnapper.executeQueue()

    def _processNeutronDataCopy(self, item: GroceryListItem, workspaceName):
        runNumber = item.runNumber
        self._filterEvents(runNumber, workspaceName)

        if Config["mantid.workspace.normalizeByBeamMonitor"]:
            monitorNormID = Config["mantid.workspace.normMonitorID"]
            # 1. get monitor workspace
            # TODO: the width used for remove prompt pulse is different for monitors, same with particle bounds maybe?
            monitorWs = self.fetchMonitorWorkspace(item)
            # 2. apply the above croping and removing prompt pulse to the monitor workspace
            self._filterEvents(runNumber, monitorWs)
            # 3. get the number of events in the monitor workspace
            normalizationFactor = self.mantidSnapper.mtd[monitorWs].getSpectrum(monitorNormID).getNumberEvents()
            # 4. save this as normalization factor in the logs
            metadata = WorkspaceMetadata(
                normalizeByMonitorFactor=normalizationFactor, normalizeByMonitorID=monitorNormID
            )

            self.writeWorkspaceMetadataAsTags(workspaceName, metadata)

            self.deleteWorkspaceUnconditional(monitorWs)

        if item.diffCalVersion is not None:
            # then load a diffcal table and apply it.
            # NOTE: This can result in a different diffcal being applied to normalization vs sample
            #       This is the expected and desired behavior.

            diffcalWorkspace, _ = self.fetchDiffCalForSample(item)
            self.mantidSnapper.ApplyDiffCal(
                "Applying diffcal..",
                InstrumentWorkspace=workspaceName,
                CalibrationWorkspace=diffcalWorkspace,
            )
            self.mantidSnapper.executeQueue()
