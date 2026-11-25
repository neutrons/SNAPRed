# ruff: noqa: F811
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from mantid.dataobjects import MaskWorkspace
from pydantic import validate_call

from snapred.backend.dao.indexing.Versioning import Version
from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.source.CalibrationDataSources import CalibrationDataSources
from snapred.backend.data.source.CalibrationSources import diffCalTableExtension, diffCalTableName
from snapred.backend.data.source.DataResolver import CACHED_LOADER
from snapred.backend.data.source.GroupingSources import groupingWorkspaceName
from snapred.backend.data.source.InstrumentDonors import InstrumentDonors
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.NeutronDataRegistrations import NeutronDataRegistrations
from snapred.backend.data.source.NeutronDataSources import NeutronDataSources
from snapred.backend.data.source.ReductionDataSources import ReductionDataSources
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.meta.Config import Config
from snapred.meta.decorators.ConfigDefault import ConfigDefault, ConfigValue
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import (
    NameBuilder,
    WorkspaceName,
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

    @property
    def diffcalTableFileExtension(self) -> str:
        return diffCalTableExtension()

    def __init__(self, dataService: LocalDataService = None):
        # 'LocalDataService' is a singleton:
        #   declare it here as an instance attribute, rather than a class attribute,
        #   to allow singleton reset during testing.
        self.dataService = self._defaultClass(dataService, LocalDataService)

        self.workspaceMetadataService = WorkspaceMetadataService()

        self.grocer = FetchGroceriesRecipe()
        self.mantidSnapper = MantidSnapper(None, "Utensils")
        # The ADS operations, shared with the data sources.
        self.workspaces = WorkspaceOperations(self.mantidSnapper)

        # The neutron-data sources, and the neutron-data cache.
        self.neutronData = NeutronDataSources(self.dataService, self.grocer, self.workspaces)

        # The instrument donors, for data that is loaded without an instrument.
        self.instrumentDonors = InstrumentDonors(self.dataService, self.workspaces, self.neutronData.cache)

        # The sources of the diffraction-calibration table and mask, and of the normalization.
        self.calibrationData = CalibrationDataSources(
            self.dataService, self.grocer, self.workspaces, self.instrumentDonors
        )

        # The sources of the grouping workspaces and of the reduction pixel masks, and their caches.
        self.reductionData = ReductionDataSources(self.dataService, self.grocer, self.workspaces, self.instrumentDonors)

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
        self.neutronData.cache.rebuild()

    def rebuildGroupingCache(self):
        """
        Rebuild the grouping cache
        """
        self.reductionData.groupingCache.rebuild()

    def rebuildInstrumentCache(self):
        """
        Rebuild the instrument cache
        """
        self.instrumentDonors.rebuild()

    def getCachedWorkspaces(self):
        """
        :return: a list of all workspaces cached in GroceryService
        :rtype: List[WorkspaceName]
        """
        cachedWorkspaces = set()
        cachedWorkspaces.update(self.neutronData.cache.cachedWorkspaces())
        cachedWorkspaces.update(self.reductionData.groupingCache.cachedWorkspaces())
        cachedWorkspaces.update(self.instrumentDonors.cachedWorkspaces())

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
        self.neutronData.cache.updateFromADS(runNumber, useLiteMode)

    def _updateInstrumentCacheFromADS(self, runNumber: str, useLiteMode: bool, cacheKey: Tuple[str, bool]):
        """
        Ensure cache consistency for a single instrument-donor workspace.

        :param runNumber: a run number to a workspace being used as an instrument donor
        :type runNumber: str
        :param useLiteMode: whether to use lite or native resolution
        :type useLiteMode: bool
        :param cacheKey: the instrument-cache key, using the 'stateId' instead of the 'runNumber'
        :type cacheKey: Tuple[str, bool]
        """
        self.instrumentDonors.updateFromADS(runNumber, useLiteMode, cacheKey)

    ## FILENAME METHODS

    @ConfigDefault
    def getIPTS(self, runNumber: str, instrumentName: str = ConfigValue("instrument.name")) -> Path | None:
        """
        Find the approprate IPTS folder for a run number.

        :param runNumber: the data run number
        :type runNumber: str
        :param instrumentName: the name of the instrument, defaults to instrument defined in application.yml
        :type instrumentName: str
        :return: The IPTS directory
        :rtype: Path | None
        """
        return self.dataService.getIPTS(runNumber, instrumentName)

    def createNeutronFilePath(self, runNumber: str, useLiteMode: bool) -> Path | None:
        return self.dataService.createNeutronFilePath(runNumber, useLiteMode)

    @validate_call
    def _createDiffCalOutputWorkspaceFilename(self, item: GroceryListItem) -> str:
        ext = Config["calibration.diffraction.output.extension"]
        return str(
            self._getCalibrationDataPath(item.useLiteMode, item.diffCalVersion, item.state)
            / (self._createDiffCalOutputWorkspaceName(item) + ext)
        )

    @validate_call
    def _createDiffCalDiagnosticWorkspaceFilename(self, item: GroceryListItem) -> str:
        ext = Config["calibration.diffraction.diagnostic.extension"]
        return str(
            self._getCalibrationDataPath(item.useLiteMode, item.diffCalVersion, item.state)
            / (self._createDiffCalOutputWorkspaceName(item) + ext)
        )

    @validate_call
    def _createDiffCalTableFilepath(
        self, diffcalRunNumber: str, useLiteMode: bool, version: Optional[int], state: str
    ) -> str:
        return str(
            Path(self._getCalibrationDataPath(useLiteMode, version, state))
            / (
                self.createDiffCalTableWorkspaceName(diffcalRunNumber, useLiteMode, version)
                + self.diffcalTableFileExtension
            )
        )

    ## WORKSPACE NAME METHODS

    def _createNeutronWorkspaceNameBuilder(self, runNumber: str, useLiteMode: bool) -> NameBuilder:
        return wng.run().runNumber(runNumber).lite(wng.Lite.fromLiteMode(useLiteMode))

    def _createNeutronWorkspaceName(self, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        return self._createNeutronWorkspaceNameBuilder(runNumber, useLiteMode).build()

    def _createRawNeutronWorkspaceName(self, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        return NeutronDataCache.rawWorkspace(runNumber, useLiteMode)

    def _createCopyNeutronWorkspaceName(self, runNumber: str, useLiteMode: bool, numCopies: int) -> WorkspaceName:
        return (
            self._createNeutronWorkspaceNameBuilder(runNumber, useLiteMode)
            .auxiliary(wng.Auxiliary.cachedCopy(numCopies))
            .build()
        )

    def _createGroupingWorkspaceName(self, groupingScheme: str, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        return groupingWorkspaceName(groupingScheme, runNumber, useLiteMode)

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

    def _lookupDiffcalRunNumber(
        self,
        useLiteMode: bool,
        state: str,
        version: int,
    ):
        indexer = self.dataService.calibrationIndexer(useLiteMode, state=state)
        record = indexer.readRecord(version)

        return record.runNumber

    @validate_call
    def createDiffCalTableWorkspaceName(
        self,
        diffcalRunNumber: str,
        useLiteMode: bool,  # noqa: ARG002
        version: Optional[Version],
    ) -> WorkspaceName:
        """
        NOTE: This method will IGNORE runNumber if the provided version is VersionState.DEFAULT
        """
        return diffCalTableName(diffcalRunNumber, version)

    @validate_call
    def _createDiffCalMaskWorkspaceName(
        self,
        runNumber: str,
        useLiteMode: bool,  # noqa: ARG002
        version: Optional[int],
    ) -> WorkspaceName:
        return wng.diffCalMask().runNumber(runNumber).version(version).build()

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
        return self.workspaces.workspaceDoesExist(name)

    def renameWorkspace(self, oldName: WorkspaceName, newName: WorkspaceName) -> WorkspaceName:
        """
        Renames a workspace in Mantid's ADS.

        :param oldName: the original name of the workspace in the ADS
        :type oldName: WorkspaceName
        :param newName: the name to replace the workspace name in the ADS
        :type newName: WorkspaceName
        """
        return self.workspaces.renameWorkspace(oldName, newName)

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
        return self.workspaces.getWorkspaceForName(name)

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
        return self.workspaces.getCloneOfWorkspace(name, copy)

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

    def getSNAPRedWorkspaceMetadata(self, workspaceName: WorkspaceName) -> WorkspaceMetadata:
        if self.workspaceDoesExist(workspaceName):
            return self.workspaceMetadataService.readWorkspaceMetadata(workspaceName)
        else:
            raise RuntimeError(f"Workspace {workspaceName} does not exist")

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
        Grouping and mask workspaces require an instrument definition, and do not have their own instrument definition
        saved with them.  Therefore, for these workspace types, it is necessary to match them to
        the proper instrument definition *with the proper instrument state parameters*.
        This uses the run number (and lite mode) to locate the proper state, and from that the proper instrument
        definition with instrument params for that state.

        :param runNumber: a run number that was taken in the desired instrument state
        :type runNumber: str
        :param useLiteMode: whether to use lite or native resolution
        :type useLiteMode: bool
        :return: the name of a workspace in the ADS with a correctly updated instrument
        :rtype: WorkspaceName
        """
        return self.instrumentDonors.fetch(runNumber, useLiteMode)

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
        self.instrumentDonors.updateInstrumentParameters(wsName, detectorState)

    @validate_call
    def _getCalibrationDataPath(self, useLiteMode: bool, version: int, state: str) -> str:
        """
        Get a path to the directory with the calibration data

        :param runNumber: a run number, whose state will be looked up
        :type runNumber: str
        :param version: the calibration version to use in the lookup
        :type version: int
        """
        return self.dataService.calibrationIndexer(useLiteMode, state=state).versionPath(version)

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
            - "dataSource": the provenance of the data (see `snapred.backend.data.source`)

        :rtype: Dict[str, Any]
        """
        result = self.neutronData.fetchSingleUse(item)
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
            - "dataSource": the provenance of the data (see `snapred.backend.data.source`)

        :rtype: Dict[str, Any]
        """
        result = self.neutronData.fetchCached(item)
        self._processNeutronDataCopy(item, result["workspace"])
        return result

    def clearLiveDataCache(self):
        """
        Clear cache for and delete any live-data workspaces.
        """
        self.neutronData.cache.clearUncacheable()

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
        data = self.reductionData.fetchGrouping(item)
        return {"result": True, "loader": data.loader, "workspace": data.workspace}

    def _validateWorkspaceInstrument(self, item: GroceryListItem, workspaceName: str):
        targetPixelCount = (
            Config["instrument.lite.pixelResolution"]
            if item.useLiteMode
            else Config["instrument.native.pixelResolution"]
        )
        targetInstrumentName = (
            Config["instrument.lite.name"].lower() if item.useLiteMode else Config["instrument.native.name"].lower()
        )
        resolutionName = "lite" if item.useLiteMode else "native"
        ws = self.mantidSnapper.mtd[workspaceName]
        numHistos = ws.getNumberHistograms()
        instrumentName = ws.getInstrument().getName().lower()
        # remove file extensions that may or may not exist
        instrumentName = instrumentName.split(".")[0]
        # remove any path that may or may not exist
        instrumentName = instrumentName.split("/")[-1]
        if numHistos != targetPixelCount:
            raise RuntimeError(
                f"Workspace '{workspaceName}' has {numHistos} histograms"
                f"\nExpected {targetPixelCount} spectra for the {resolutionName} resolution."
                f"\ni.e. one histogram per pixel of the instrument."
                f"\nPlease contact your IS or CIS for assistance."
            )
        if targetInstrumentName != instrumentName:
            raise RuntimeError(
                f"Workspace '{workspaceName}' has an instrument with name '{instrumentName}'"
                f"\nExpected instrument to be {targetInstrumentName} for the {resolutionName} resolution."
                f"\nPlease contact your IS or CIS for assistance."
            )

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

        Only the last normalization that was loaded is kept.  A normalization that is loaded is processed
        as neutron data (see `_processNeutronDataCopy`): one that is reused from the ADS is not.

        :param item: a GroceryListItem corresponding to the normalization workspaces
        :type item: GroceryListItem
        :return: a dictionary with

            - "result", true if everything ran correctly
            - "loader", either "LoadNexusProcessed" or "cached"
            - "workspace", the name of the new workspace in the ADS;
                this defaults to the name of the normalization workspace,
            - "dataSource": the provenance of the data (see `snapred.backend.data.source`)

        :rtype: Dict[str, Any]
        """
        data = self.calibrationData.fetchNormalization(item)
        if data.loader != CACHED_LOADER:
            self._processNeutronDataCopy(item, data.workspace)
        return {"result": True, "loader": data.loader, "workspace": data.workspace, "dataSource": data.dataSource}

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
        data = self.reductionData.fetchReductionPixelMask(item)
        return {"result": True, "loader": data.loader, "workspace": data.workspace}

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

        # Warning: `filePath` will be `None` if no IPTS directory exists!
        #   This situation occurs during diagnostic runs in live-data mode.
        filePath = self.createNeutronFilePath(runNumber, useLiteMode)
        if not bool(filePath):
            raise RuntimeError(f"Cannot find IPTS directory for run '{runNumber}'")

        self.grocer.executeRecipe(str(filePath), workspaceName, "LoadNexusMonitors")

        return workspaceName

    def fetchNormCalForSample(self, item: GroceryListItem) -> Dict[str, Any]:
        logger.info(f"Fetching normalization workspace for run {item.runNumber}, version {item.normCalVersion}")
        return self.fetchNormalizationWorkspace(item)

    def fetchDiffCalTable(self, item: GroceryListItem) -> WorkspaceName:
        """
        Fetch the diffraction-calibration table: from `item.diffCalFilePath`, if that is set,
        otherwise from the calibration record of `item.diffCalVersion`.
        """
        return self.calibrationData.fetchDiffCalTable(item).workspace

    def fetchDiffCalMask(self, item: GroceryListItem) -> WorkspaceName | None:
        """
        Fetch the diffraction-calibration mask, as `fetchDiffCalTable`: `None` if the calibration has no mask.
        """
        data = self.calibrationData.fetchDiffCalMask(item)
        return data.workspace if data is not None else None

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
                    diffcalRunNumber = self._lookupDiffcalRunNumber(item.useLiteMode, item.state, item.diffCalVersion)
                    diffcalItem = item.model_copy(deep=True)
                    diffcalItem.runNumber = diffcalRunNumber
                    result = self.fetchWorkspace(
                        self._createDiffCalOutputWorkspaceFilename(diffcalItem),
                        self._createDiffCalOutputWorkspaceName(diffcalItem),
                        loader="LoadNexus",
                    )
                case "diffcal_diagnostic":
                    diffcalRunNumber = self._lookupDiffcalRunNumber(item.useLiteMode, item.state, item.diffCalVersion)
                    diffcalItem = item.model_copy(deep=True)
                    diffcalItem.runNumber = diffcalRunNumber
                    result = self.fetchWorkspace(
                        self._createDiffCalDiagnosticWorkspaceFilename(diffcalItem),
                        self._createDiffCalOutputWorkspaceName(diffcalItem),
                        loader="LoadNexusProcessed",
                    )
                case "diffcal_table":
                    # TODO: Remove this `result` return pattern.
                    #       It doesnt seem to be used downstream in any meaningful capacity.
                    result = {"result": True, "workspace": self.fetchDiffCalTable(item)}

                case "diffcal_mask":
                    maskWorkspace = self.fetchDiffCalMask(item)

                    # TODO: Remove this `result` return pattern.
                    result = {
                        "result": True,
                        "workspace": maskWorkspace if maskWorkspace is not None else "",
                    }

                case "normalization":
                    result = self.fetchNormCalForSample(item)
                case "reduction_pixel_mask":
                    result = self.fetchReductionPixelMask(item)
                case _:
                    raise RuntimeError(f"unrecognized 'workspaceType': '{item.workspaceType}'")

            if item.workspaceType in ["neutron", "normalization"] and result["loader"] in [
                "LoadEventNexus",
                "LoadNexusProcessed",
            ]:
                # The loader is not always set but *should* be at least for neutron/norm data.
                # Validate the instrument and pixel count.
                self._validateWorkspaceInstrument(item, result["workspace"])
            # check that the fetch operation succeeded and if so append the workspace
            if result["result"] is True:
                groceries.append(result["workspace"])
            else:
                # NOTE: When would it ever be false?  We should throw exceptions as early as possible.
                #       and rely on native error handling.
                raise RuntimeError(f"Error fetching item {item.model_dump_json(indent=2)}")

        return groceries

    def combinePixelMasks(self, outputMaskWsName: WorkspaceName, masks2Combine: List[WorkspaceName]):
        if not masks2Combine:
            raise ValueError("Internal Error: Lists of masks to combine is empty")

        if not self.mantidSnapper.mtd.doesExist(outputMaskWsName):
            raise ValueError(
                (
                    "Internal Error: outputMaskWs should exist before attempting to combine masks with it. "
                    "Consider using fetchCompatiblePixelMask to generate it."
                )
            )

        for maskWsName in masks2Combine:
            if maskWsName == outputMaskWsName:
                continue
            if not self.mantidSnapper.mtd.doesExist(maskWsName):
                raise ValueError(
                    f"Mask {maskWsName} of mask set {masks2Combine} does not exist, cannot combine into pixel mask."
                )
            self.mantidSnapper.BinaryOperateMasks(
                f"combine from pixel mask: '{maskWsName}'...",
                InputWorkspace1=outputMaskWsName,
                InputWorkspace2=maskWsName,
                OperationType="OR",
                OutputWorkspace=outputMaskWsName,
            )

        self.mantidSnapper.executeQueue()
        return outputMaskWsName

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
        self.neutronData.convertToLiteMode(workspace, export=export)

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
        self.workspaces.deleteWorkspaceUnconditional(name)

    def clearADS(self, exclude: List[WorkspaceName] = [], clearCache: bool = False):
        """
        Clears ADS of all workspaces except those in the exclude list and cache.

        :param exclude: a list of workspaces to retain in the ADS after clear
        :type exclude: List[WorkspaceName]
        :param clearCache: whether or not to clear cached workspaces
        :type clearCache: bool
        """  # noqa E501
        workspacesToClear = set(self.mantidSnapper.mtd.getObjectNames())
        # filter exclude, and the registered neutron data: SNAPRed never deletes a registered workspace
        exclude = [*exclude, *NeutronDataRegistrations().registeredWorkspaces()]
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
        # NOTE: We always want to generate the instrument state, to get the latest SNAPInstPRm parameters.
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

        if item.diffCalVersion is not None or item.diffCalFilePath is not None:
            # then load a diffcal table and apply it.
            # NOTE: This can result in a different diffcal being applied to normalization vs sample
            #       This is the expected and desired behavior.

            diffcalWorkspace = self.fetchDiffCalTable(item)
            self.mantidSnapper.ApplyDiffCal(
                "Applying diffcal..",
                InstrumentWorkspace=workspaceName,
                CalibrationWorkspace=diffcalWorkspace,
            )
            self.mantidSnapper.executeQueue()
