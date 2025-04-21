# ruff: noqa: F811 ARG005 ARG002
## Python standard imports
import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

##
## Test-related imports come last.
##
from unittest import mock

## Mantid imports
from mantid.simpleapi import (
    CreateEmptyTableWorkspace,
    CreateSampleWorkspace,
    ExtractMask,
    LoadDetectorsGroupingFile,
    LoadEmptyInstrument,
    mtd,
)
from pydantic import validate_call
from util.mock_util import mock_instance_methods
from util.WhateversInTheFridge import WhateversInTheFridge

## SNAPRed imports
from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.state import DetectorState
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.meta.Config import Config, Resource
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
@mock_instance_methods
class InstaEats(GroceryService):
    """
    No self-respecting chef would ever be caught dead ordering groceries from InstaEats...
    But this is just for a test, so it's probably okay.

    Use to mock out the GroceryService.
    """

    def __init__(self):
        super().__init__(dataService=WhateversInTheFridge())
        self.dataService = WhateversInTheFridge()
        assert isinstance(self.dataService, WhateversInTheFridge)
        self.groupingMap = self.dataService._readDefaultGroupingMap()
        self.testInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        self.instrumentDonorWorkspace = mtd.unique_name(prefix="_instrument_donor")
        self.grocer.executeRecipe = mock.Mock(
            side_effect=lambda filename="", workspace="", loader="", *args, **kwargs: (
                {"result": True, "loader": loader, "workspace": workspace}
            )
        )

    ## FILENAME METHODS

    def getIPTS(self, runNumber: str, instrumentName: str = None) -> str:
        return Resource.getPath("inputs/testInstrument/IPTS-456/")

    def _createNeutronFilename(self, runNumber: str, useLiteMode: bool) -> Path:
        instr = "nexus.lite" if useLiteMode else "nexus.native"
        pre = instr + ".prefix"
        ext = instr + ".extension"
        return Path(self.getIPTS(runNumber) + Config[pre] + str(runNumber) + Config[ext])

    @validate_call
    def _createGroupingFilename(self, runNumber: str, groupingScheme: str, useLiteMode: bool) -> str:
        if groupingScheme == "Lite":
            path = str(Config["instrument.lite.map.file"])
        else:
            path = self.groupingMap.getMap(useLiteMode)[groupingScheme].definition
        return str(path)

    def _lookupDiffCalWorkspaceNames(
        self, runNumber: str, useLiteMode: bool, version: int = 0
    ) -> Tuple[WorkspaceName, WorkspaceName]:
        return self.createDiffCalTableWorkspaceName(
            runNumber, useLiteMode, version
        ), self._createDiffCalMaskWorkspaceName(runNumber, useLiteMode, version)

    ## FETCH METHODS

    def _fetchInstrumentDonor(self, runNumber: str, useLiteMode: bool) -> WorkspaceName:
        LoadEmptyInstrument(Filename=self.testInstrumentFile, OutputWorkspace=self.instrumentDonorWorkspace)
        return self.instrumentDonorWorkspace

    def updateInstrumentParameters(self, wsName: WorkspaceName, detectorState: DetectorState):
        """
        This needs to be updated to take into account test instrument geometry.
        """
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

    def fetchWorkspace(self, filePath: str, name: WorkspaceName, loader: str = "") -> Dict[str, Any]:
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

        runNumber, useLiteMode, loader, liveDataArgs = item.runNumber, item.useLiteMode, item.loader, item.liveDataArgs

        # Live-data mode can be requested explicitly,
        #   but it also serves as the fallback mode.
        liveDataMode = loader == "LoadLiveData" or liveDataArgs is not None

        success = False
        convertToLiteMode = False

        self._updateNeutronCacheFromADS(runNumber, useLiteMode)

        key = self._key(runNumber, useLiteMode)
        rawWorkspaceName: WorkspaceName = self._createRawNeutronWorkspaceName(runNumber, useLiteMode)
        nativeRawWorkspaceName: WorkspaceName = self._createRawNeutronWorkspaceName(runNumber, False)

        # if the raw data has already been loaded, clone it
        if self._loadedRuns.get(key) is not None:
            data = {"loader": "cached"}
            success = True

        if not success and not liveDataMode:
            liteModeFilePath: Path = self._createNeutronFilename(runNumber, True)
            nativeModeFilePath: Path = self._createNeutronFilename(runNumber, False)
            cachedNativeMode = self._loadedRuns.get(self._key(runNumber, False)) is not None

            match (useLiteMode, cachedNativeMode, liteModeFilePath.exists(), nativeModeFilePath.exists()):
                # THERE ARE MANY SPECIAL CASES HERE, but basically the end result of a cached fetch will be
                #   a raw copy in the cache, and the return of a cloned copy.
                #   In addition, when converted from native mode
                #   there may be an additional native copy in the cache.

                case (True, _, True, _):
                    # lite mode and lite-mode exists on disk
                    data = self.grocer.executeRecipe(str(liteModeFilePath), rawWorkspaceName, loader)
                    self._loadedRuns[key] = 0
                    self._liveDataKeys.append(key)
                    success = True

                case (True, True, _, _):
                    # lite mode and native is cached
                    data = {"loader": "cached"}
                    convertToLiteMode = True
                    success = True

                case (True, _, _, True):
                    # lite mode and native exists on disk
                    goingNative = self._key(runNumber, False)
                    data = self.grocer.executeRecipe(str(nativeModeFilePath), nativeRawWorkspaceName, loader="")
                    self._loadedRuns[self._key(*goingNative)] = 0
                    self._liveDataKeys.append(self._key(*goingNative))
                    convertToLiteMode = True
                    success = True

                case (False, _, _, True):
                    # native mode and native exists on disk
                    data = self.grocer.executeRecipe(str(nativeModeFilePath), nativeRawWorkspaceName, loader)
                    self._loadedRuns[key] = 0
                    self._liveDataKeys.append(key)
                    success = True

                case _:
                    # live data fallback
                    pass

        if not success:
            # Live-data fallback
            liveDataMode = True

            if self.dataService.hasLiveDataConnection():
                # When not specified in the `liveDataArgs`,
                #   default behavior will be to load the entire run.
                startTime = (
                    (datetime.datetime.utcnow() - liveDataArgs.duration).isoformat() if liveDataArgs is not None else ""
                )

                loaderArgs = {
                    "Facility": Config["liveData.facility.name"],
                    "Instrument": Config["liveData.instrument.name"],
                    "AccumulationMethod": Config["liveData.accumulationMethod"],
                    "PreserveEvents": True,
                    "StartTime": startTime,
                }
                data = self.grocer.executeRecipe(
                    workspace=nativeRawWorkspaceName, loader="LoadLiveData", loaderArgs=json.dumps(loaderArgs)
                )
                if data["result"]:
                    # For `InstaEats` purposes: here we are never going to trigger this case:
                    """
                    liveRunNumber = self.mantidSnapper.mtd[workspace].getRun().runNumber()
                    """
                    liveRunNumber = runNumber

                    if int(runNumber) != int(liveRunNumber):
                        self.deleteWorkspaceUnconditional(nativeRawWorkspaceName)
                        data = {"result": False}
                        if liveDataArgs is not None:
                            # the live-data run isn't the expected one => a live-data state change has occurred
                            raise LiveDataState.runStateTransition(liveRunNumber, runNumber)
                        raise RuntimeError(
                            f"Neutron data for run '{runNumber}' is not present on disk, nor is it the live-data run"
                        )
                    self._loadedRuns[self._key(runNumber, False)] = 0
                    self._liveDataKeys.append(self._key(runNumber, False))
                    success = True
            else:
                raise RuntimeError(
                    f"Neutron data for run '{runNumber}' is not present on disk, "
                    + "and no live-data connection is available"
                )

        if success:
            if convertToLiteMode:
                # clone the native raw workspace
                # then reduce its resolution to make the lite raw workspace
                self.getCloneOfWorkspace(nativeRawWorkspaceName, rawWorkspaceName)
                self._loadedRuns[key] = 0
                self._liveDataKeys.append(key)
                self.convertToLiteMode(rawWorkspaceName, export=not liveDataMode)

            # create a copy of the raw data for use
            workspaceName = self._createCopyNeutronWorkspaceName(runNumber, useLiteMode, self._loadedRuns[key] + 1)
            data["result"] = True
            data["workspace"] = workspaceName
            self._loadedRuns[key] += 1

        return data

    def clearLiveDataCache(self):
        """
        Clear cache for and delete any live-data workspaces.
        """
        while self._liveDataKeys:
            key = self._liveDataKeys.pop()
            del self._loadedRuns[key]
            self.deleteWorkspaceUnconditional(self._createRawNeutronWorkspaceName(key))

    def fetchGroupingDefinition(self, item: GroceryListItem) -> Dict[str, Any]:
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
            instrumentSource = (
                self._fetchInstrumentDonor(item.runNumber, item.useLiteMode)
                if not item.instrumentSource
                else item.instrumentSource
            )
            LoadDetectorsGroupingFile(
                Inputworkspace=instrumentSource,
                OutputWorkspace=workspaceName,
                InputFile=filename,
            )
            data = {
                "result": True,
                "loader": groupingLoader,
                "workspace": workspaceName,
            }
            self._loadedGroupings[key] = data["workspace"]

        return data

    def fetchCompatiblePixelMask(self, maskWSName, runNumber, useLiteMode):
        CreateSampleWorkspace(
            OutputWorkspace=maskWSName,
            NumBanks=1,
            BankPixelWidth=1,
        )
        ExtractMask(
            InputWorkspace=maskWSName,
            OutputWorkspace=maskWSName,
        )

    def fetchCalibrationWorkspaces(self, item):
        runNumber, version, useLiteMode = item.runNumber, item.version, item.useLiteMode
        tableWorkspaceName, maskWorkspaceName = self._lookupDiffCalWorkspaceNames(runNumber, useLiteMode, version)
        self.fetchCompatiblePixelMask(maskWorkspaceName, runNumber, useLiteMode)
        CreateEmptyTableWorkspace(OutputWorkspace=tableWorkspaceName)
        return {
            "result": True,
            "loader": "LoadCalibrationWorkspaces",
            "workspace": tableWorkspaceName,
        }

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
                        res = self.fetchNeutronDataCached(item)
                    else:
                        res = self.fetchNeutronDataSingleUse(item)
                # for grouping definitions
                case "grouping":
                    res = self.fetchGroupingDefinition(item)
                case "diffcal":
                    res = {"result": False, "workspace": self._createDiffCalInputWorkspaceName(item.runNumber)}
                    raise RuntimeError(
                        "not implemented: no path available to fetch diffcal "
                        + f"input table workspace: '{res['workspace']}'"
                    )
                # for diffraction-calibration workspaces
                case "diffcal_output":
                    res = self.fetchWorkspace(
                        self._createDiffCalOutputWorkspaceFilename(item),
                        self._createDiffCalOutputWorkspaceName(item),
                        loader="LoadNexus",
                    )
                case "diffcal_diagnostic":
                    res = self.fetchWorkspace(
                        self._createDiffCalDiagnosticWorkspaceFilename(item),
                        self._createDiffCalOutputWorkspaceName(item),
                        loader="LoadNexusProcessed",
                    )
                case "diffcal_table":
                    res = self.fetchCalibrationWorkspaces(item)
                case "diffcal_mask":
                    maskWorkspaceName = self._createDiffCalMaskWorkspaceName(
                        item.runNumber, item.useLiteMode, item.version
                    )
                    res = self.fetchCalibrationWorkspaces(item)
                    res["workspace"] = maskWorkspaceName
                case "normalization":
                    res = self.fetchNormalizationWorkspace(item)
                case _:
                    raise RuntimeError(f"unrecognized 'workspaceType': '{item.workspaceType}'")
            # check that the fetch operation succeeded and if so append the workspace
            if res["result"] is True:
                groceries.append(res["workspace"])
            else:
                raise RuntimeError(f"Error fetching item {item.model_dump_json(indent=2)}")
        return groceries
