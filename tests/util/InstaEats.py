# ruff: noqa: F811 ARG005 ARG002
import os
from typing import Any, Dict, List
from unittest import mock

from mantid.simpleapi import LoadDetectorsGroupingFile, LoadEmptyInstrument, mtd
from pydantic import validate_call
from util.WhateversInTheFridge import WhateversInTheFridge

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.state import DetectorState
from snapred.backend.data.GroceryService import GroceryService
from snapred.meta.Config import Config, Resource
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class InstaEats(GroceryService):
    """
    No self-respecting chef would ever be caught dead ordering groceries from InstaEats...
    But this is just for a test, so it's probably okay.

    Use to mock out the GroceryService.
    """

    def __init__(self):
        super().__init__(dataService=WhateversInTheFridge)
        self.dataService = WhateversInTheFridge()
        self.groupingMap = self.dataService._readDefaultGroupingMap()
        self.testInstrumentFile = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        self.instrumentDonorWorkspace = mtd.unique_name(prefix="_instrument_donor")
        self.grocer.executeRecipe = mock.Mock(
            side_effect=lambda filename, workspace, loader, *args, **kwargs: (
                {"result": True, "loader": loader, "workspace": workspace}
            )
        )

    ## FILENAME METHODS

    def getIPTS(self, runNumber: str, instrumentName: str = None) -> str:
        return Resource.getPath("inputs/testInstrument/IPTS-456/")

    def _createNeutronFilename(self, runNumber: str, useLiteMode: bool) -> str:
        instr = "nexus.lite" if useLiteMode else "nexus.native"
        pre = instr + ".prefix"
        ext = instr + ".extension"
        return self.getIPTS(runNumber) + Config[pre] + str(runNumber) + Config[ext]

    @validate_call
    def _createGroupingFilename(self, runNumber: str, groupingScheme: str, useLiteMode: bool) -> str:
        if groupingScheme == "Lite":
            path = str(Config["instrument.lite.map.file"])
        else:
            path = self.groupingMap.getMap(useLiteMode)[groupingScheme].definition
        return str(path)

    def lookupDiffcalTableWorkspaceName(self, runNumber: str, useLiteMode: bool, version: int = 0) -> WorkspaceName:
        return self.createDiffcalTableWorkspaceName(runNumber, useLiteMode, version)

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

    def fetchNeutronDataCached(self, runNumber: str, useLiteMode: bool, loader: str = "") -> Dict[str, Any]:
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
            data = self.grocer.executeRecipe(nativeFilename, rawWorkspaceName, loader)
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
        data["result"] = True
        data["workspace"] = workspaceName
        self._loadedRuns[key] += 1

        return data

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
                        loader="LoadNexus",
                    )
                case "diffcal_diagnostic":
                    res = self.fetchWorkspace(
                        self._createDiffcalDiagnosticWorkspaceFilename(item),
                        self._createDiffcalOutputWorkspaceName(item),
                        loader="LoadNexusProcessed",
                    )
                case "diffcal_table":
                    res = self.fetchCalibrationWorkspaces(item)
                case "diffcal_mask":
                    maskWorkspaceName = self._createDiffcalMaskWorkspaceName(
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
                raise RuntimeError(f"Error fetching item {item.json(indent=2)}")
        return groceries
