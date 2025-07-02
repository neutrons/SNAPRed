import copy
import datetime
import glob
import json
import os
import re
import shutil
import socket
import tempfile
import time
from errno import ENOENT as NOT_FOUND
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import h5py
import numpy as np
from mantid.api import IEventWorkspace, Run
from mantid.dataobjects import MaskWorkspace
from mantid.kernel import ConfigService, PhysicalConstants
from pydantic import validate_call

from snapred.backend.dao import (
    GSASParameters,
    InstrumentConfig,
    ObjectSHA,
    ParticleBounds,
    RunConfig,
    RunMetadata,
    StateConfig,
    StateId,
)
from snapred.backend.dao.calibration import Calibration, CalibrationDefaultRecord, CalibrationRecord
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import Version, VersionState
from snapred.backend.dao.Limit import Limit, Pair
from snapred.backend.dao.normalization import Normalization, NormalizationRecord
from snapred.backend.dao.reduction import ReductionRecord
from snapred.backend.dao.request import (
    CreateCalibrationRecordRequest,
    CreateIndexEntryRequest,
    CreateNormalizationRecordRequest,
)
from snapred.backend.dao.state import (
    DetectorState,
    GroupingMap,
    InstrumentState,
)
from snapred.backend.dao.state.CalibrantSample import CalibrantSample
from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.decorators.ConfigDefault import ConfigDefault, ConfigValue
from snapred.meta.decorators.ExceptionHandler import ExceptionHandler
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.InternalConstants import ReservedRunNumber, ReservedStateId
from snapred.meta.mantid.WorkspaceNameGenerator import (
    ValueFormatter as wnvf,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceName,
    WorkspaceType,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceNameGenerator as wng,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceType as wngt,
)
from snapred.meta.redantic import parse_file_as, write_model_pretty

logger = snapredLogger.getLogger(__name__)

"""
    Looks up data on disk
    TBD the interface such that it is fairly generic
    but intersects that of the potential oncat data service interface
"""


def _createFileNotFoundError(msg, filename):
    return FileNotFoundError(NOT_FOUND, os.strerror(NOT_FOUND) + " " + msg, filename)


@Singleton
class LocalDataService:
    # conversion factor from microsecond/Angstrom to meters
    # (TODO: FIX THIS COMMENT! Obviously `m2cm` doesn't convert from 1.0 / Angstrom to 1.0 / meters.)
    CONVERSION_FACTOR = Config["constants.m2cm"] * PhysicalConstants.h / PhysicalConstants.NeutronMass

    def __init__(self) -> None:
        self.mantidSnapper = MantidSnapper(None, "Utensils")

    ##### MISCELLANEOUS METHODS #####

    @classproperty
    def verifyPaths(cls) -> bool:
        return Config["localdataservice.config.verifypaths"]

    def fileExists(self, path):
        return os.path.isfile(path)

    def readInstrumentConfig(self, runNumber: str) -> InstrumentConfig:
        instrumentConfig = self.readInstrumentParameters(runNumber)
        instrumentConfig.calibrationDirectory = Config["instrument.calibration.home"]
        if self.verifyPaths and not Path(instrumentConfig.calibrationDirectory).exists():
            raise _createFileNotFoundError("[calibration directory]", instrumentConfig.calibrationDirectory)

        return instrumentConfig

    def readStateConfig(self, runId: str, useLiteMode: bool) -> StateConfig:
        state, _ = self.generateStateId(runId)
        indexer = self.calibrationIndexer(useLiteMode, state)
        version = indexer.latestApplicableVersion(runId)
        diffCalibration = indexer.readParameters(version)

        # Read the grouping-schema map associated with this `StateConfig`.
        groupingMap = None
        if self._groupingMapPath(state).exists():
            groupingMap = self._readGroupingMap(state)
        else:
            # If no `GroupingMap` JSON file is present at the <state root>,
            #   it is assumed that this is the first time that this state configuration has been initialized.
            # WARNING: `_prepareStateRoot` is also called at `initializeState`: this allows
            #   some order independence of initialization if the back-end is run separately (e.g. in unit tests).
            self._prepareStateRoot(state)
            groupingMap = self._readGroupingMap(state)

        return StateConfig(
            calibration=diffCalibration,
            groupingMap=groupingMap,
            stateId=diffCalibration.instrumentState.id,
        )

    @staticmethod
    def checkFileandPermission(filePath: Path) -> Tuple[bool, bool]:
        if filePath is None:
            return False, False
        else:
            fileExists = Path(filePath).exists()
            writePermission = LocalDataService._hasWritePermissionstoPath(filePath)
            return fileExists, writePermission

    @staticmethod
    def _hasWritePermissionstoPath(filePath: Path) -> bool:
        if not filePath.is_dir():
            filePath = filePath.parent
        result = True
        preTempDir = tempfile.tempdir
        tempfile.tempdir = str(filePath)
        try:
            with tempfile.TemporaryFile() as fp:
                fp.write(b"Hello world!")
        except Exception:  # noqa: BLE001
            result = False
        tempfile.tempdir = preTempDir
        return result

    @staticmethod
    def checkWritePermissions(path: Path) -> bool:
        """Check if the user has permissions to write to, or to create, the specified path."""
        path_ = path
        while path_ and not path_.exists():
            path_ = path_.parent
        return LocalDataService._hasWritePermissionstoPath(path_) if (path_ and path_.exists()) else False

    @staticmethod
    def generateUserRootFolder():
        originalCalibrationHome = Path(Config["instrument.calibration.home"])
        Config.swapToUserYml()
        userCalibrationHome = Path(Config["instrument.calibration.home"])
        LocalDataService.copyCalibrationRootSkeleton(originalCalibrationHome, userCalibrationHome)

    @staticmethod
    def copyCalibrationRootSkeleton(originalCalibrationHome: Path, newCalibrationHome: Path):
        if not newCalibrationHome.exists():
            # need to copy: SNAPInstPrm, CalibrantSamples, Powder/PixelGroupingDefinitions, Powder/SNAPLite.xml,
            newCalibrationHome.mkdir(parents=True, exist_ok=True)
            itemsToCopy = [
                "SNAPInstPrm",
                "CalibrantSamples",
                "Powder/PixelGroupingDefinitions",
                "Powder/SNAPLite.xml",
            ]

            # Validate all items first
            for item in itemsToCopy:
                src = originalCalibrationHome / item
                if not (src.exists()):
                    raise FileNotFoundError(f"Failure to find {item} when creating user calibration home")
            # then copy them
            for item in itemsToCopy:
                src = originalCalibrationHome / item
                dst = newCalibrationHome / item
                if src.is_dir():
                    dst.mkdir(parents=True, exist_ok=True)

                    # ignoreing hidden directories
                    def ignore_hidden_paths(_, files):
                        return [f for f in files if f.startswith(".")]

                    shutil.copytree(src, dst, dirs_exist_ok=True, ignore=ignore_hidden_paths)
                else:
                    dst.write_bytes(src.read_bytes())
        else:
            logger.info(f"User calibration home already exists: {newCalibrationHome}.")

    @staticmethod
    def getUniqueTimestamp() -> float:
        """
        Generate a unique timestamp:

        * on some operating systems `time.time()` only has resolution to seconds;

        * this method checks its own most-recently returned value, and if necessary,
          increments it.

        * the complete `float` representation of the unix timestamp is retained,
          in order to allow arbitrary formatting.

        """
        _previousTimestamp = getattr(LocalDataService.getUniqueTimestamp, "_previousTimestamp", None)
        nextTimestamp = time.time()
        if _previousTimestamp is not None:
            # compare as `time.struct_time` to ensure uniqueness after formatting
            if nextTimestamp < _previousTimestamp or time.gmtime(nextTimestamp) == time.gmtime(_previousTimestamp):
                nextTimestamp = _previousTimestamp + 1.0
        LocalDataService.getUniqueTimestamp._previousTimestamp = nextTimestamp
        return nextTimestamp

    @lru_cache
    @ConfigDefault
    def getIPTS(self, runNumber: str, instrumentName: str = ConfigValue("instrument.name")) -> Path | None:
        # Fully cached version of `GetIPTS`:
        #   returns the IPTS-directory for the run or None if no IPTS directory exists.

        IPTS = self.mantidSnapper.CheckIPTS(
            "get IPTS directory", RunNumber=runNumber, Instrument=instrumentName, ClearCache=True
        )
        self.mantidSnapper.executeQueue()
        IPTS = str(IPTS)  # "collapse" the `Callback`
        return Path(IPTS) if bool(IPTS) else None

    def createNeutronFilePath(self, runNumber: str, useLiteMode: bool) -> Path | None:
        filePath = None
        IPTS = self.getIPTS(runNumber)
        if bool(IPTS):
            instr = "nexus.lite" if useLiteMode else "nexus.native"
            pre = instr + ".prefix"
            ext = instr + ".extension"
            filePath = IPTS / (Config[pre] + str(runNumber) + Config[ext])
        return filePath

    def stateExists(self, runId: str) -> bool:
        stateId, _ = self.generateStateId(runId)
        statePath = self.constructCalibrationStateRoot(stateId)
        # Shouldn't need to check lite as we init both at the same time
        return statePath.exists()

    def workspaceIsInstance(self, wsName: str, wsType: Any) -> bool:
        # Is the workspace an instance of the specified type.
        if not self.mantidSnapper.mtd.doesExist(wsName):
            return False
        return isinstance(self.mantidSnapper.mtd[wsName], wsType)

    def readRunConfig(self, runId: str) -> RunConfig:
        return self._readRunConfig(runId)

    def _readRunConfig(self, runId: str) -> RunConfig:
        IPTS = self.getIPTS(runId)
        if not bool(IPTS):
            raise RuntimeError(f"Cannot find IPTS directory for run '{runId}'")

        instrumentConfig = self.readInstrumentConfig(runId)
        return RunConfig(
            IPTS=str(IPTS),
            runNumber=runId,
            maskFileName="",
            maskFileDirectory=str(IPTS) + instrumentConfig.sharedDirectory,
            gsasFileDirectory=str(IPTS) + instrumentConfig.reducedDataDirectory,
            calibrationState=None,
        )

    def _constructPVFilePath(self, runId: str) -> Path | None:
        return self.createNeutronFilePath(runId, False)

    def _readPVFile(self, runId: str):
        filePath: Path = self._constructPVFilePath(runId)
        if bool(filePath) and filePath.exists():
            return h5py.File(filePath, "r")
        raise FileNotFoundError(f"No PVFile exists for run: '{runId}'")

    # NOTE `lru_cache` decorator needs to be on the outside
    @lru_cache
    @ExceptionHandler(StateValidationException)
    def generateStateId(self, runId: str) -> Tuple[str | None, DetectorState | None]:
        detectorState = None
        if runId in ReservedRunNumber.values():
            SHA = ObjectSHA(hex=ReservedStateId.forRun(runId))
        else:
            metadata = self.readRunMetadata(runId)
            detectorState = metadata.detectorState
            SHA = metadata.stateId
        return (SHA.hex if SHA is not None else ""), detectorState

    def findCompatibleStates(self, runId: str, useLiteMode: bool) -> List[str]:
        # 1. collect list of all existing states
        statesPath = Path(Config["instrument.calibration.powder.home"])
        stateFolders = [f for f in statesPath.iterdir() if f.is_dir()]
        targetMode = "lite" if useLiteMode else "native"
        # filter stateFolders to only ones that contain the targetMode folder
        # i.e. ignore PixelGroupingDefinition and other folders
        calibrationStateFolders = []
        for stateFolder in stateFolders:
            if (statesPath / stateFolder / targetMode).exists():
                calibrationStateFolders.append(stateFolder.name)

        stateCalibMap = []
        for stateId in calibrationStateFolders:
            indexer = self.calibrationIndexer(useLiteMode, stateId)
            defaultVersion = indexer.defaultVersion()
            # must have a real calibration and not just default.
            if defaultVersion != indexer.currentVersion() and indexer.latestApplicableVersion(runId) is not None:
                stateCalibMap.append((stateId, indexer.readParameters(defaultVersion)))

        # 2. pull instrumentState.detectorState from each
        currentDetectorState = self.readDetectorState(runId)
        referenceArc = currentDetectorState.arc
        referenceGuideStat = currentDetectorState.guideStat
        # 3. compare to current runId, if they match add to list
        compatibleStates = []
        for state, params in stateCalibMap:
            detectorState = params.instrumentState.detectorState
            arc = detectorState.arc
            guideStat = detectorState.guideStat
            # in the comparison we only care about the arc position and guidestat
            if arc == referenceArc and guideStat == referenceGuideStat:
                compatibleStates.append(state)

        return compatibleStates

    def copyCalibration(
        self,
        sourceStateID: str,
        targetStateID: str,
        targetIndexEntry: IndexEntry,
        sourceVersion: Version = VersionState.LATEST,
    ):
        useLiteMode = targetIndexEntry.useLiteMode
        sourcePath = self._constructCalibrationStatePath(sourceStateID, useLiteMode)
        sourceIndexer = Indexer(indexerType=IndexerType.CALIBRATION, directory=sourcePath)
        targetPath = self._constructCalibrationStatePath(targetStateID, useLiteMode)
        targetIndexer = Indexer(indexerType=IndexerType.CALIBRATION, directory=targetPath)

        if sourceVersion == VersionState.LATEST:
            sourceVersion = sourceIndexer.latestApplicableVersion(targetIndexEntry.runNumber)
        else:
            sourceVersion = sourceVersion

        # now we must extract and rename the calibration files, as well as re version them
        sourceVersionPath = sourceIndexer.versionPath(sourceVersion)
        sourceParameters = sourceIndexer.readParameters(sourceVersion)
        sourceRecord = sourceIndexer.readRecord(sourceVersion)
        sourceWorspaces = copy.deepcopy(sourceRecord.workspaces)
        targetVersion = targetIndexer.nextVersion()

        targetIndexEntry.version = targetVersion
        sourceRecord.version = targetVersion
        sourceRecord.indexEntry = targetIndexEntry
        sourceParameters.version = targetVersion
        sourceParameters.indexEntry = targetIndexEntry

        targetVersionPath = targetIndexer.versionPath(targetVersion)

        for workspaceType, workspaceNames in sourceWorspaces.items():
            sourceRecord.workspaces[workspaceType] = []
            for workspaceName in workspaceNames:
                # regex and replace version number to create new workspace name

                newWorkspaceName = re.sub(r"v\d+", wnvf.formatVersion(targetVersion), workspaceName)
                sourceRecord.workspaces[workspaceType].append(newWorkspaceName)

        targetIndexer.writeIndexedObject(sourceRecord)
        targetIndexer.writeIndexedObject(sourceParameters)

        for workspaceType, workspaceNames in sourceRecord.workspaces.items():
            for workspaceName, sourceWorkspaceName in zip(workspaceNames, sourceWorspaces[workspaceType]):
                if workspaceType == wngt.DIFFCAL_OUTPUT:
                    ext = Config["calibration.diffraction.output.extension"]
                elif workspaceType == wngt.DIFFCAL_DIAG:
                    ext = Config["calibration.diffraction.diagnostic.extension"]
                elif workspaceType == wngt.DIFFCAL_TABLE:
                    ext = ".h5"
                elif workspaceType == wngt.DIFFCAL_MASK:
                    continue
                else:
                    raise ValueError(f"Unknown workspace type: {workspaceType}")

                sourceWorkspacePath = sourceVersionPath / f"{sourceWorkspaceName}{ext}"
                targetWorkspacePath = targetVersionPath / f"{workspaceName}{ext}"
                targetWorkspacePath.write_bytes(sourceWorkspacePath.read_bytes())

    def _stateIdFromDetectorState(self, detectorState: DetectorState) -> ObjectSHA:
        return StateId.fromDetectorState(detectorState).SHA()

    def stateIdFromWorkspace(self, wsName: WorkspaceName) -> Tuple[str, DetectorState]:
        detectorState = self.detectorStateFromWorkspace(wsName)
        SHA = self._stateIdFromDetectorState(detectorState)
        return SHA.hex, detectorState

    def _findMatchingFileList(self, pattern, throws=True) -> List[str]:
        """
        Find all files matching a glob pattern.
        Optional: throws exception if nothing found.
        """
        fileList: List[str] = []
        for fname in glob.glob(str(pattern), recursive=True):
            if os.path.isfile(fname):
                fileList.append(fname)
        if len(fileList) == 0 and throws:
            raise ValueError(f"No files could be found with pattern: {pattern}")
        return fileList

    ##### PATH METHODS #####

    def _appendTimestamp(self, root: Path, timestamp: float) -> Path:
        # Append a timestamp directory to a data path
        return root / wnvf.pathTimestamp(timestamp)

    def constructCalibrationStateRoot(self, stateId) -> Path:
        return Path(Config["instrument.calibration.powder.home"], str(stateId))

    def _getLiteModeString(self, useLiteMode: bool) -> str:
        return "lite" if useLiteMode else "native"

    def _constructCalibrationStatePath(self, stateId, useLiteMode) -> Path:
        mode = self._getLiteModeString(useLiteMode)
        return self.constructCalibrationStateRoot(stateId) / mode / "diffraction"

    def _constructNormalizationStatePath(self, stateId, useLiteMode) -> Path:
        mode = self._getLiteModeString(useLiteMode)
        return self.constructCalibrationStateRoot(stateId) / mode / "normalization"

    def _hasWritePermissionsCalibrationStateRoot(self) -> bool:
        return self.checkWritePermissions(Path(Config["instrument.calibration.powder.home"]))

    # reduction paths #

    @validate_call
    def _constructReductionStateRoot(self, runNumber: str) -> Path:
        pathFmt = Config["instrument.reduction.home"]
        if "{IPTS}" in pathFmt:
            IPTS = self.getIPTS(runNumber)
            if not bool(IPTS):
                raise RuntimeError(f"Cannot find IPTS directory for run '{runNumber}'")

            # substitute the last component of the IPTS-directory for the '{IPTS}' tag
            reductionHome = Path(pathFmt.format(IPTS=IPTS.name))
        else:
            reductionHome = Path(pathFmt)

        # Only generate the stateId, if we've successfully gotten this far.
        # (In live-data case, there may be no 'IPTS' directory at all.)
        stateId, _ = self.generateStateId(runNumber)
        return reductionHome / stateId

    @validate_call
    def _constructReductionDataRoot(self, runNumber: str, useLiteMode: bool) -> Path:
        mode = "lite" if useLiteMode else "native"
        return self._constructReductionStateRoot(runNumber) / mode / runNumber

    @validate_call
    def _constructReductionDataPath(self, runNumber: str, useLiteMode: bool, timestamp: float) -> Path:
        return self._appendTimestamp(self._constructReductionDataRoot(runNumber, useLiteMode), timestamp)

    @validate_call
    def _constructReductionRecordFilePath(self, runNumber: str, useLiteMode: bool, timestamp: float) -> Path:
        recordPath = self._constructReductionDataPath(runNumber, useLiteMode, timestamp) / "ReductionRecord.json"
        return recordPath

    @validate_call
    def _constructReductionDataFilePath(self, runNumber: str, useLiteMode: bool, timestamp: float) -> Path:
        fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
        fileName += Config["reduction.output.extension"]
        filePath = self._constructReductionDataPath(runNumber, useLiteMode, timestamp) / fileName
        return filePath

    @validate_call
    def _reducedRuns(self, runNumber: str, useLiteMode: bool) -> List[str]:
        # A list of already reduced runs sharing the same state as the specified run
        # NOTE: fix this, double check with WORKSPACE name generator for formatting numbers, check consistency
        runNumberFormat = re.compile(r"\d{5,}$")
        mode = "lite" if useLiteMode else "native"
        runs = []
        try:
            stateRoot = self._constructReductionStateRoot(runNumber) / mode
            if stateRoot.exists():
                with os.scandir(stateRoot) as entries:
                    for entry in entries:
                        if entry.is_dir():
                            if runNumberFormat.match(entry.name):
                                runs.append(entry.name)
        except RuntimeError as e:
            # In live-data mode, an IPTS-directory may not exist.
            if "Cannot find IPTS directory" not in str(e):
                raise
        return runs

    @validate_call
    def _reducedTimestamps(self, runNumber: str, useLiteMode: bool) -> List[int]:
        # A list of timestamps from existing reduced data for the specified run and grouping.

        # Implementation notes:
        # * in python >=3.11, the iso-format parsing can be replaced by
        #   `<datetime class>.fromisoformat(entry.name).timestamp()`

        timestampPathTag = re.compile(Config["mantid.workspace.nameTemplate.formatter.timestamp.path_regx"])
        reductionDataRoot = self._constructReductionDataRoot(runNumber, useLiteMode)
        tss = []
        if reductionDataRoot.exists():
            with os.scandir(reductionDataRoot) as entries:
                for entry in entries:
                    if entry.is_dir():
                        part = entry.name
                        match_ = timestampPathTag.match(part)
                        if match_:
                            tss.append(
                                datetime.datetime(
                                    year=int(match_.group(1)),
                                    month=int(match_.group(2)),
                                    day=int(match_.group(3)),
                                    hour=int(match_.group(4)),
                                    minute=int(match_.group(5)),
                                    second=int(match_.group(6)),
                                ).timestamp()
                            )
        return tss

    ##### INDEX / VERSION METHODS #####
    @validate_call
    def readCalibrationIndex(self, useLiteMode: bool, state: str):
        return self.calibrationIndexer(useLiteMode, state).getIndex()

    @validate_call
    def readNormalizationIndex(self, useLiteMode: bool, state: str):
        return self.normalizationIndexer(useLiteMode, state).getIndex()

    def _statePathForWorkflow(self, stateId: str, useLiteMode: bool, indexerType: IndexerType):
        match indexerType:
            case IndexerType.CALIBRATION:
                path = self._constructCalibrationStatePath(stateId, useLiteMode)
            case IndexerType.NORMALIZATION:
                path = self._constructNormalizationStatePath(stateId, useLiteMode)
            case _:
                raise NotImplementedError(f"Indexer of type {indexerType} is not supported by the LocalDataService")
        return path

    @lru_cache
    @validate_call
    def calibrationIndexer(self, useLiteMode: bool, state: str):
        path = self._constructCalibrationStatePath(state, useLiteMode)
        return Indexer(indexerType=IndexerType.CALIBRATION, directory=path)

    @lru_cache
    @validate_call
    def normalizationIndexer(self, useLiteMode: bool, state: str):
        path = self._constructNormalizationStatePath(state, useLiteMode)
        return Indexer(indexerType=IndexerType.NORMALIZATION, directory=path)

    def instrumentParameterIndexer(self):
        return Indexer(
            indexerType=IndexerType.INSTRUMENT_PARAMETER, directory=Path(Config["instrument.parameters.home"])
        )

    def writeCalibrationIndexEntry(self, entry: IndexEntry):
        """
        The entry must have correct version.
        """
        state, _ = self.generateStateId(entry.runNumber)
        self.calibrationIndexer(entry.useLiteMode, state).addIndexEntry(entry)

    def writeNormalizationIndexEntry(self, entry: IndexEntry):
        """
        The entry must have correct version.
        """
        state, _ = self.generateStateId(entry.runNumber)
        self.normalizationIndexer(entry.useLiteMode, state).addIndexEntry(entry)

    ##### Instrument Parameter Methods #####

    # 1. write new instrumen param file 2. read appropriate instrument param file
    def writeInstrumentParameters(self, instrumentParameters: InstrumentConfig, appliesTo: str, author: str):
        """
        Writes the instrument parameters to disk.
        """
        indexer = self.instrumentParameterIndexer()
        newEntry = indexer.createIndexEntry(
            runNumber=ReservedRunNumber.NATIVE,
            useLiteMode=False,
            # The above two fields are irrelevant for instrument parameters, but required by the IndexEntry model.
            version=VersionState.NEXT,
            appliesTo=appliesTo,
            author=author,
        )
        instrumentParameters.indexEntry = newEntry
        indexer.writeIndexedObject(instrumentParameters)

    def readInstrumentParameters(self, runNumber: str):
        indexer = self.instrumentParameterIndexer()
        version = indexer.latestApplicableVersion(runNumber)
        if version is None:
            raise FileNotFoundError(f"No instrument parameters found for run {runNumber}")
        return indexer.readIndexedObject(InstrumentConfig, version)

    ##### NORMALIZATION METHODS #####

    def createNormalizationIndexEntry(self, request: CreateIndexEntryRequest) -> IndexEntry:
        state, _ = self.generateStateId(request.runNumber)
        indexer = self.normalizationIndexer(request.useLiteMode, state)
        entryParams = request.model_dump()
        entryParams["version"] = entryParams.get("version", VersionState.NEXT)
        if entryParams["version"] is None:
            entryParams["version"] = VersionState.NEXT
        return indexer.createIndexEntry(**entryParams)

    def createNormalizationRecord(self, request: CreateNormalizationRecordRequest) -> NormalizationRecord:
        state, _ = self.generateStateId(request.runNumber)
        indexer = self.normalizationIndexer(request.useLiteMode, state)
        return indexer.createRecord(**request.model_dump())

    def normalizationExists(self, runId: str, useLiteMode: bool, state: str) -> bool:
        version = self.normalizationIndexer(useLiteMode, state).latestApplicableVersion(runId)
        return version is not None

    @validate_call
    def readNormalizationRecord(
        self, runId: str, useLiteMode: bool, state: str, version: Version = VersionState.LATEST
    ):
        """
        Will return a normalization record for the given version.
        If no version given, will choose the latest applicable version from the index.
        """
        indexer = self.normalizationIndexer(useLiteMode, state)
        if version is VersionState.LATEST:
            version = indexer.latestApplicableVersion(runId)
        record = None
        if version is not None:
            logger.info(f"loading normalization version: {version} for runId: {runId}")
            record = indexer.readRecord(version)

        return record

    def writeNormalizationRecord(self, record: NormalizationRecord):
        """
        Persists a `NormalizationRecord` to either a new version folder, or overwrites a specific version.
        Record must be set with correct version.
        -- side effect: creates any directories needed for save
        """
        state, _ = self.generateStateId(record.runNumber)
        indexer = self.normalizationIndexer(record.useLiteMode, state)
        record.calculationParameters.indexEntry = record.indexEntry.model_copy()
        # write the record to file
        indexer.writeRecord(record)
        # separately write the normalization state
        indexer.writeParameters(record.calculationParameters)

        logger.info(f"wrote NormalizationRecord: version: {record.version}")

    def writeNormalizationWorkspaces(self, record: NormalizationRecord):
        """
        Writes the workspaces associated with a `NormalizationRecord` to disk:
        Record must be set with correct version and workspace names must be finalized.
        -- assumes that `writeNormalizationRecord` has already been called, and that the version folder exists
        """
        instrumentState = self.generateInstrumentState(record.runNumber)
        indexer = self.normalizationIndexer(record.useLiteMode, instrumentState.id.hex)
        normalizationDataPath: Path = indexer.versionPath(record.version)
        if not normalizationDataPath.exists():
            normalizationDataPath.mkdir(parents=True, exist_ok=True)

        for workspace in record.workspaceNames:
            ws = self.mantidSnapper.mtd[workspace]
            if Config["nexus.dataFormat.event"] and isinstance(ws, IEventWorkspace):
                # Remove binning from any event workspaces in order to speed-up reload:
                #    (XMin, XMax, Delta) == (NaN, NaN, <outer bound>) will create a single bin
                #    [<actual minimum>, <actual maximum>]
                TOFOuterBound = instrumentState.particleBounds.tof.maximum * 10.0
                numberOfSpectra = ws.getNumberHistograms()
                self.mantidSnapper.RebinRagged(
                    "strip binning info, before saving normalization workspace",
                    OutputWorkspace=workspace,
                    InputWorkspace=workspace,
                    XMin=list((np.nan,) * numberOfSpectra),
                    XMax=list((np.nan,) * numberOfSpectra),
                    Delta=list((TOFOuterBound,) * numberOfSpectra),
                    PreserveEvents=True,
                )
                self.mantidSnapper.executeQueue()

            filename = Path(workspace + ".nxs")
            self.writeWorkspace(normalizationDataPath, filename, workspace)

    ##### CALIBRATION METHODS #####

    def createCalibrationIndexEntry(self, request: CreateIndexEntryRequest) -> IndexEntry:
        state, _ = self.generateStateId(request.runNumber)
        indexer = self.calibrationIndexer(request.useLiteMode, state)
        return indexer.createIndexEntry(**request.model_dump())

    def createCalibrationRecord(self, request: CreateCalibrationRecordRequest) -> CalibrationRecord:
        state, _ = self.generateStateId(request.runNumber)
        indexer = self.calibrationIndexer(request.useLiteMode, state)
        return indexer.createRecord(**request.model_dump())

    def calibrationExists(self, runId: str, useLiteMode: bool, state: str) -> bool:
        version = self.calibrationIndexer(useLiteMode, state).latestApplicableVersion(runId)
        return version is not None

    @validate_call
    def readCalibrationRecord(
        self,
        runId: str,
        useLiteMode: bool,
        state: str,
        version: Version = VersionState.LATEST,
    ):
        """
        Will return a calibration record for the given version.
        If no version given, will choose the latest applicable version from the index.
        """
        indexer = self.calibrationIndexer(useLiteMode, state)
        if version is VersionState.LATEST:
            version = indexer.latestApplicableVersion(runId)
        record = None
        if version is not None:
            logger.info(f"loading calibration version: {version} for runId: {runId}")
            record = indexer.readRecord(version)

        return record

    def writeCalibrationRecord(self, record: CalibrationRecord):
        """
        Persists a `CalibrationRecord` to either a new version folder, or overwrite a specific version.
        Record must be set with correct version.
        -- side effect: creates any directories needed for save
        """
        stateId, _ = self.generateStateId(record.runNumber)
        indexer = self.calibrationIndexer(record.useLiteMode, stateId)
        record.calculationParameters.indexEntry = record.indexEntry.model_copy()
        # write record to file
        indexer.writeRecord(record)
        # separately write the calibration state
        indexer.writeParameters(record.calculationParameters)

        logger.info(f"Wrote CalibrationRecord: version: {record.version}")

    def writeCalibrationWorkspaces(self, record: CalibrationRecord):
        """
        Writes the workspaces associated with a `CalibrationRecord` to disk:
        Record must be set with correct version and workspace names must be finalized.
        -- assumes that `writeCalibrationRecord` has already been called, and that the version folder exists
        """
        state, _ = self.generateStateId(record.runNumber)
        indexer = self.calibrationIndexer(record.useLiteMode, state)
        calibrationDataPath = indexer.versionPath(record.version)
        if not calibrationDataPath.exists():
            calibrationDataPath.mkdir(parents=True, exist_ok=True)

        # write the output d-spacing calibrated data
        wsNames = record.workspaces.get(wngt.DIFFCAL_OUTPUT, [])
        ext = Config["calibration.diffraction.output.extension"]
        for wsName in wsNames:
            filename = Path(wsName + ext)
            self.writeWorkspace(calibrationDataPath, filename, wsName)

        # write the diagnostic output
        wsNames = record.workspaces.get(wngt.DIFFCAL_DIAG, [])
        ext = Config["calibration.diffraction.diagnostic.extension"]
        for wsName in wsNames:
            filename = Path(wsName + ext)
            self.writeWorkspace(calibrationDataPath, filename, wsName)

        # write the diffcal table and mask
        tableWSNames = record.workspaces.get(wngt.DIFFCAL_TABLE, [])
        maskWSNames = record.workspaces.get(wngt.DIFFCAL_MASK, [])
        ext = ".h5"
        for tableWSName, maskWSName in zip(tableWSNames, maskWSNames):
            diffCalFilename = Path(tableWSName + ext)
            self.writeDiffCalWorkspaces(
                calibrationDataPath,
                diffCalFilename,
                tableWorkspaceName=tableWSName,
                maskWorkspaceName=maskWSName,
            )

    ##### REDUCTION METHODS #####

    @validate_call
    def readReductionRecord(self, runNumber: str, useLiteMode: bool, timestamp: float) -> ReductionRecord:
        """
        Return a reduction record for the specified timestamp.
        """
        filePath: Path = self._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        if not filePath.exists():
            raise RuntimeError(f"expected reduction record file at '{filePath}' does not exist")
        with open(filePath, "r") as f:
            record = ReductionRecord.model_validate_json(f.read())
        return record

    def writeReductionRecord(self, record: ReductionRecord) -> ReductionRecord:
        """
        Persists a `ReductionRecord` to either a new timestamp folder, or overwrites a specific timestamp.
        * timestamp must be set and output-workspaces list finalized.
        * must be called before any call to `writeReductionData`.
        * -- side effect: creates the output directories when required.
        """

        runNumber, useLiteMode, timestamp = record.runNumber, record.useLiteMode, record.timestamp

        filePath: Path = self._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        if filePath.exists():
            logger.warning(f"overwriting existing reduction record at '{filePath}'")

        if not filePath.parent.exists():
            filePath.parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(record, filePath)
        logger.info(f"wrote reduction record to file: {filePath}")

    def writeReductionData(self, record: ReductionRecord):
        """
        Persists the reduction data associated with a `ReductionRecord`
        -- `writeReductionRecord` must have been called prior to this method.
        """

        # Implementation notes:
        #
        # 1) For SNAPRed's current reduction-workflow output implementation:
        #
        #      * In case an effective instrument has been substituted,
        #        `SaveNexusESS` _must_ be used, `SaveNexus` by itself won't work;
        #
        #      * ONLY a simplified instrument geometry can be saved,
        #        for example, as produced by `EditInstrumentGeometry`:
        #          this geometry includes no monitors, only a single non-nested detector bank, and no parameter map.
        #
        #      * `LoadNexus` should work with all of this _automatically_.
        #
        #    Hopefully this will eventually be fixed, but right now this is a limitation of Mantid's
        #    instrument-I/O implementation (for non XML-based instruments).
        #
        # 2) For SNAPRed internal use:
        #        if `reduction.output.useEffectiveInstrument` is set to false in "application.yml",
        #    output workspaces will be saved without converting their instruments to the reduced form.
        #    Both of these alternatives are retained to allow some flexibility in what specifically
        #    is saved with the reduction data.
        #

        runNumber, useLiteMode, timestamp = record.runNumber, record.useLiteMode, record.timestamp

        filePath = self._constructReductionDataFilePath(runNumber, useLiteMode, timestamp)
        if filePath.exists():
            logger.warning(f"overwriting existing reduction data at '{filePath}'")

        if not filePath.parent.exists():
            # WARNING: `writeReductionRecord` must be called before `writeReductionData`.
            raise RuntimeError(f"reduction version directories {filePath.parent} do not exist")

        useEffectiveInstrument = Config["reduction.output.useEffectiveInstrument"]

        for ws in record.workspaceNames:
            # Append workspaces to hdf5 file, in order of the `workspaces` list

            if ws.tokens("workspaceType") == wngt.REDUCTION_PIXEL_MASK:
                # The mask workspace always uses the non-reduced instrument.
                self.mantidSnapper.SaveNexus(
                    f"Append workspace '{ws}' to reduction output",
                    InputWorkspace=ws,
                    Filename=str(filePath),
                    Append=True,
                )
                self.mantidSnapper.executeQueue()
                # Write an additional copy of the combined pixel mask as a separate `SaveDiffCal`-format file
                maskFilename = ws + ".h5"
                self.writePixelMask(filePath.parent, Path(maskFilename), ws)
            else:
                if useEffectiveInstrument:
                    self.mantidSnapper.SaveNexusESS(
                        f"Append workspace '{ws}' to reduction output",
                        InputWorkspace=ws,
                        Filename=str(filePath),
                        Append=True,
                    )
                else:
                    self.mantidSnapper.SaveNexus(
                        f"Append workspace '{ws}' to reduction output",
                        InputWorkspace=ws,
                        Filename=str(filePath),
                        Append=True,
                    )
                self.mantidSnapper.executeQueue()

        # Append the "metadata" group, containing the `ReductionRecord` metadata
        with h5py.File(filePath, "a") as h5:
            n5m.insertMetadataGroup(h5, record.dict(), "/metadata")

        logger.info(f"wrote reduction data to file '{filePath}'")

    @validate_call
    def readReductionData(self, runNumber: str, useLiteMode: bool, timestamp: float) -> ReductionRecord:
        """
        This method is complementary to `writeReductionData`:
        -- it is provided primarily for diagnostic purposes, and is not yet connected to any workflow
        """
        filePath = self._constructReductionDataFilePath(runNumber, useLiteMode, timestamp)
        if not filePath.exists():
            raise RuntimeError(f"[readReductionData]: file '{filePath}' does not exist")

        # read the metadata first, in order to use the workspaceNames list
        record = None
        with h5py.File(filePath, "r") as h5:
            record = ReductionRecord.model_validate(n5m.extractMetadataGroup(h5, "/metadata"))
        for ws in record.workspaceNames:
            if self.mantidSnapper.mtd.doesExist(ws):
                raise RuntimeError(f"[readReductionData]: workspace '{ws}' already exists in the ADS")

        # Read the workspaces, one by one;
        #   * as an alternative, these could be loaded into a group workspace with a single call to `readWorkspace`.
        pixelMaskKeyword = Config["mantid.workspace.nameTemplate.template.reduction.pixelMask"].split(",")[0]
        for n, ws in enumerate(record.workspaceNames):
            self.readWorkspace(filePath.parent, Path(filePath.name), ws, entryNumber=n + 1)
            # ensure that any mask workspace is actually a `MaskWorkspace` instance
            if pixelMaskKeyword in ws:
                self.mantidSnapper.ExtractMask(
                    f"converting '{ws}' to MaskWorkspace instance", OutputWorkspace=ws, InputWorkspace=ws
                )
        self.mantidSnapper.executeQueue()
        logger.info(f"loaded reduction data from '{filePath}'")
        return record

    ##### CALIBRANT SAMPLE METHODS #####

    def readSampleFilePaths(self):
        sampleFolder = Config["instrument.calibration.sample.home"]
        extensions = Config["instrument.calibration.sample.extensions"]
        # collect list of all json in folder
        sampleFiles = set()
        for extension in extensions:
            sampleFiles.update(self._findMatchingFileList(f"{sampleFolder}/*.{extension}", throws=False))
        if len(sampleFiles) < 1:
            raise RuntimeError(f"No samples found in {sampleFolder} for extensions {extensions}")
        sampleFiles = list(sampleFiles)
        sampleFiles.sort()
        return sampleFiles

    def writeCalibrantSample(self, sample: CalibrantSample):
        samplePath: str = Config["samples.home"]
        fileName: str = sample.name + "_" + sample.unique_id
        filePath = os.path.join(samplePath, fileName) + ".json"
        if os.path.exists(filePath):
            raise ValueError(f"the file '{filePath}' already exists")
        write_model_pretty(sample, filePath)

    def readCalibrantSample(self, filePath: str):
        if not os.path.exists(filePath):
            raise ValueError(f"The file '{filePath}' does not exist")
        with open(filePath, "r") as file:
            sampleJson = json.load(file)
            if "mass-density" in sampleJson and "packingFraction" in sampleJson:
                logger.warn(  # noqa: F821
                    "Can't specify both mass-density and packing fraction for single-element materials"
                )  # noqa: F821
            del sampleJson["material"]["packingFraction"]
            sample = CalibrantSample.model_validate_json(json.dumps(sampleJson))
            return sample

    def readCifFilePath(self, sampleId: str):
        samplePath: str = Config["samples.home"]
        fileName: str = sampleId + ".json"
        filePath = os.path.join(samplePath, fileName)
        if not os.path.exists(filePath):
            raise ValueError(f"the file '{filePath}' does not exist")
        with open(filePath, "r") as f:
            calibrantSampleDict = json.load(f)
        filePath = Path(calibrantSampleDict["crystallography"]["cifFile"])
        # Allow relative paths:
        if not filePath.is_absolute():
            filePath = Path(Config["samples.home"]).joinpath(filePath)
        return str(filePath)

    ##### READ / WRITE STATE METHODS #####

    @validate_call
    def readCalibrationState(
        self,
        runId: str,
        useLiteMode: bool,
        state: str,
        version: Optional[Version] = VersionState.LATEST,
    ):
        if not self.calibrationExists(runId, useLiteMode, state=state):
            if self._hasWritePermissionsCalibrationStateRoot():
                raise RecoverableException.stateUninitialized(runId, useLiteMode)
            else:
                raise RuntimeError(
                    "No calibration exists, and you lack permissions to create one."  # fmt: skip
                    " Please contact your IS or CIS."  # fmt: skip
                )

        indexer = self.calibrationIndexer(useLiteMode, state)
        # NOTE if we prefer latest version in index, uncomment below
        parameters = None
        if version is VersionState.LATEST:
            version = indexer.latestApplicableVersion(runId)
        if version is not None:
            parameters = indexer.readParameters(version)

        return parameters

    @validate_call
    def readNormalizationState(
        self, runId: str, useLiteMode: bool, state: str = None, version: Version = VersionState.LATEST
    ):
        if state is None:
            state, _ = self.generateStateId(runId)
        indexer = self.normalizationIndexer(useLiteMode, state)
        if version is VersionState.LATEST:
            version = indexer.latestApplicableVersion(runId)

        if version is None:
            raise RuntimeError(f"No normalization exists for run {runId}, state {state}, lite {useLiteMode}")

        return indexer.readParameters(version)

    def writeCalibrationState(self, calibration: Calibration):
        """
        Calibration state must have version set.
        """
        state, _ = self.generateStateId(calibration.seedRun)
        indexer = self.calibrationIndexer(calibration.useLiteMode, state)
        indexer.writeParameters(calibration)

    def writeNormalizationState(self, normalization: Normalization):
        """
        Normalization state must have version set.
        """
        state, _ = self.generateStateId(normalization.seedRun)
        indexer = self.normalizationIndexer(normalization.useLiteMode, state)
        indexer.writeParameters(normalization)

    def readDetectorState(self, runNumber: str) -> DetectorState | None:
        # Assemble a detector state from either the PVLogs, or the current live-data run.

        # Note that `readRunMetadata` uses `lru_cache`: it is redundant to additionally apply it to this
        #   method.
        return self.readRunMetadata(runNumber).detectorState

    def detectorStateFromWorkspace(self, wsName: WorkspaceName) -> DetectorState:
        # Assemble a detector state from the Workspace logs (i.e. `Mantid.api.Run`).
        return RunMetadata.fromRun(self.mantidSnapper.mtd[wsName].getRun()).detectorState

    @validate_call
    def _writeDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool):
        from snapred.backend.data.GroceryService import GroceryService

        state, _ = self.generateStateId(runNumber)
        indexer = self.calibrationIndexer(useLiteMode, state)
        version = indexer.defaultVersion()
        grocer = GroceryService()
        filename = Path(grocer.createDiffCalTableWorkspaceName("default", useLiteMode, version) + ".h5")
        outWS = grocer.fetchDefaultDiffCalTable(runNumber, useLiteMode, version)

        calibrationDataPath = indexer.versionPath(version)
        self.writeDiffCalWorkspaces(calibrationDataPath, filename, tableWorkspaceName=outWS)

        # TODO: all of this should have its own workflow, in which case, it could act like all other workflows
        #   and delete its workspaces after completion.
        grocer.deleteWorkspaceUnconditional(outWS)

    def generateInstrumentState(self, runId: str):
        # Read the detector state from the PV data file,
        #   and generate the stateID SHA.
        stateId, detectorState = self.generateStateId(runId)

        # read data from the common calibration state parameters stored at root of calibration directory
        instrumentConfig = self.readInstrumentConfig(runId)

        # pull static values from resources
        defaultGroupSliceValue = Config["calibration.parameters.default.groupSliceValue"]
        fwhmMultipliers = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
        peakTailCoefficient = Config["calibration.parameters.default.peakTailCoefficient"]
        gsasParameters = GSASParameters(
            alpha=Config["calibration.parameters.default.alpha"], beta=Config["calibration.parameters.default.beta"]
        )

        # Calculate the derived values
        lambdaLimit = Limit(
            minimum=detectorState.wav - (instrumentConfig.bandwidth / 2) + instrumentConfig.lowWavelengthCrop,
            maximum=detectorState.wav + (instrumentConfig.bandwidth / 2),
        )
        L = instrumentConfig.L1 + instrumentConfig.L2
        tofLimit = Limit(
            minimum=lambdaLimit.minimum * L / self.CONVERSION_FACTOR,
            maximum=lambdaLimit.maximum * L / self.CONVERSION_FACTOR,
        )
        particleBounds = ParticleBounds(wavelength=lambdaLimit, tof=tofLimit)

        return InstrumentState(
            id=stateId,
            instrumentConfig=instrumentConfig,
            detectorState=detectorState,
            gsasParameters=gsasParameters,
            particleBounds=particleBounds,
            defaultGroupingSliceValue=defaultGroupSliceValue,
            fwhmMultipliers=fwhmMultipliers,
            peakTailCoefficient=peakTailCoefficient,
        )

    @validate_call
    @ExceptionHandler(StateValidationException)
    # NOTE if you are debugging and got here, coment out the ExceptionHandler and try again
    def initializeState(self, runId: str, useLiteMode: bool, name: str = None):
        from snapred.backend.data.GroceryService import GroceryService

        grocer = GroceryService()
        instrumentState = self.generateInstrumentState(runId)
        state = instrumentState.id.hex

        calibrationReturnValue = None

        # Make sure that the state root directory has been initialized:
        stateRootPath: Path = self.constructCalibrationStateRoot(state)
        if not stateRootPath.exists():
            # WARNING: `_prepareStateRoot` is also called at `readStateConfig`; this allows
            #   some order independence of initialization if the back-end is run separately (e.g. in unit tests).
            self._prepareStateRoot(state)

        # now save default versions of files in both lite and native resolution directories
        for liteMode in [True, False]:
            indexer = self.calibrationIndexer(liteMode, state)
            version = indexer.defaultVersion()

            entry = indexer.createIndexEntry(
                runNumber=runId,
                useLiteMode=liteMode,
                version=version,
                appliesTo=">=0",
                author="SNAPRed Internal",
                comments="The default configuration when loading StateConfig if none other is found",
            )

            calibration = indexer.createParameters(
                instrumentState=instrumentState,
                name=name,
                seedRun=runId,
                useLiteMode=liteMode,
                creationDate=datetime.datetime.now(),
                version=version,
                indexEntry=entry,
            )

            # NOTE: this creates a bare record without any other CalibrationRecord data
            defaultDiffCalTableName = grocer.createDiffCalTableWorkspaceName("default", liteMode, version)
            workspaces: Dict[WorkspaceType, List[WorkspaceName]] = {
                wngt.DIFFCAL_TABLE: [defaultDiffCalTableName],
            }

            record = CalibrationDefaultRecord(
                runNumber=runId,
                useLiteMode=liteMode,
                version=version,
                calculationParameters=calibration,
                workspaces=workspaces,
                indexEntry=entry,
            )
            # write the calibration state
            indexer.writeRecord(record)
            indexer.writeParameters(record.calculationParameters)
            # write the default diffcal table
            self._writeDefaultDiffCalTable(runId, liteMode)

            if useLiteMode == liteMode:
                calibrationReturnValue = calibration

        return calibrationReturnValue

    def _prepareStateRoot(self, stateId: str):
        """
        Create the state root directory, and populate it with any necessary metadata files.
        """
        stateRootPath: Path = self.constructCalibrationStateRoot(stateId)
        if not stateRootPath.exists():
            stateRootPath.mkdir(parents=True, exist_ok=True)

        # If no `GroupingMap` JSON file is present at the <state root>,
        #   it is assumed that this is the first time that this state configuration has been initialized.
        # Any `StateConfig`'s `GroupingMap` always starts as a copy of the default `GroupingMap`.
        groupingMap = self._readDefaultGroupingMap()
        groupingMap.coerceStateId(stateId)
        # This is the _ONLY_ place that the grouping-schema map is written
        #   to its separate JSON file at <state root>.
        self._writeGroupingMap(stateId, groupingMap)

    def checkCalibrationFileExists(self, runId: str):
        # TODO: run number format validation does not belong here!

        # first perform some basic validation of the run ID
        # - it must be a string of only digits
        # - it must be greater than some minimal run number
        if not runId.isdigit() or int(runId) < Config["instrument.startingRunNumber"]:
            return False
        # then make sure the run number has a valid IPTS
        try:
            # The existence of a calibration state root does not necessarily have anything to do with
            #   whether or not the run has an existing IPTS directory.

            stateID, _ = self.generateStateId(runId)
            calibrationStatePath: Path = self.constructCalibrationStateRoot(stateID)
            return calibrationStatePath.exists()
        except (FileNotFoundError, RuntimeError):
            return False

    ##### GROUPING MAP METHODS #####

    def readGroupingMap(self, runNumber: str):
        # if the state exists then look up its grouping map
        if self.checkCalibrationFileExists(runNumber):
            stateId, _ = self.generateStateId(runNumber)
            return self._readGroupingMap(stateId)
        # otherwise return the default map
        else:
            return self._readDefaultGroupingMap()

    def _readGroupingMap(self, stateId: str) -> GroupingMap:
        path: Path = self._groupingMapPath(stateId)
        if not path.exists():
            raise FileNotFoundError(f'required grouping-schema map for state "{stateId}" at "{path}" does not exist')
        return parse_file_as(GroupingMap, path)

    def readDefaultGroupingMap(self):
        return self._readDefaultGroupingMap()

    def _readDefaultGroupingMap(self) -> GroupingMap:
        path: Path = self._defaultGroupingMapPath()
        if not path.exists():
            raise FileNotFoundError(f'required default grouping-schema map "{path}" does not exist')
        return parse_file_as(GroupingMap, path)

    def _writeGroupingMap(self, stateId: str, groupingMap: GroupingMap):
        # Write a GroupingMap to a file in JSON format, but only if it has been modified.
        groupingMapPath: Path = self._groupingMapPath(stateId)
        if not groupingMapPath.parent.exists():
            raise FileNotFoundError(f'state-root directory "{groupingMapPath.parent}" does not exist')

        # Only write once and do not allow overwrite.
        if groupingMap.isDirty and not groupingMapPath.exists():
            # For consistency: write out `_isDirty` as False
            groupingMap.setDirty(False)
            write_model_pretty(groupingMap, groupingMapPath)

    def _defaultGroupingMapPath(self) -> Path:
        return GroupingMap.calibrationGroupingHome() / "defaultGroupingMap.json"

    def _groupingMapPath(self, stateId) -> Path:
        return self.constructCalibrationStateRoot(stateId) / "groupingMap.json"

    ## PIXEL-MASK SUPPORT METHODS

    def isCompatibleMask(self, wsName: WorkspaceName, runNumber: str, useLiteMode: bool) -> bool:
        """
        Test if a MaskWorkspace is compatible with a specified run number and lite-mode flag:
        * a compatible mask is a MaskWorkspace;
        * a compatible mask has the same number of spectra as non-monitor pixels in the instrument.
        * a compatible mask has the same instrument state as the run number;
        """
        if not isinstance(self.mantidSnapper.mtd[wsName], MaskWorkspace):
            return False
        targetPixelCount = (
            Config["instrument.lite.pixelResolution"] if useLiteMode else Config["instrument.native.pixelResolution"]
        )
        if self.mantidSnapper.mtd[wsName].getNumberHistograms() != targetPixelCount:
            return False
        expectedStateId, _ = self.generateStateId(runNumber)
        actualStateId, _ = self.stateIdFromWorkspace(wsName)
        if actualStateId != expectedStateId:
            return False
        return True

    @validate_call
    def getCompatibleReductionMasks(self, runNumber: str, useLiteMode: bool) -> List[WorkspaceName]:
        # Assemble a list of masks, both resident and otherwise, that are compatible with the current reduction
        masks: Set[WorkspaceName] = set()
        excludedCount = 0

        # First: add all masks from previous reductions in the same state
        for reducedRun in self._reducedRuns(runNumber, useLiteMode):
            for ts in self._reducedTimestamps(reducedRun, useLiteMode):
                maskName = wng.reductionPixelMask().runNumber(reducedRun).timestamp(ts).build()
                maskFilePath = self._constructReductionDataPath(reducedRun, useLiteMode, ts) / (maskName + ".h5")

                # Implementation notes:
                # * No compatibility check is required for reduction masks on the filesystem:
                #     they are guaranteed to be compatible;

                if maskName not in masks and maskFilePath.exists():
                    # Ensure that any _resident_ mask is compatible:
                    if self.mantidSnapper.mtd.doesExist(maskName) and not self.isCompatibleMask(
                        maskName, runNumber, useLiteMode
                    ):
                        # There is a possible name collision
                        # between reduction pixel masks from different lite-mode settings.
                        #   This clause bypasses that collision in the most straightforward way:
                        #     such a mask will be excluded, even if there may be a compatible mask
                        #     of the same name on the filesystem.
                        excludedCount += 1
                        continue
                    masks.add(maskName)

        # Next: add compatible user-created masks that are already resident in the ADS
        mantidMaskName = re.compile(r"MaskWorkspace(_([0-9]+))?")
        wsNames = self.mantidSnapper.mtd.getObjectNames()
        for ws in wsNames:
            match_ = mantidMaskName.match(ws)
            if match_:
                if not self.isCompatibleMask(ws, runNumber, useLiteMode):
                    excludedCount += 1
                    continue

                # Convert to a `WorkspaceName`
                maskName = (
                    wng.reductionUserPixelMask()
                    .numberTag(int(match_.group(2)) if match_.group(2) is not None else 1)
                    .build()
                )
                masks.add(maskName)

        if excludedCount > 0:
            logger.warning(
                f"Excluded {excludedCount} incompatible pixel masks "
                + f"from a total of {excludedCount + len(masks)} masks:\n"
                + "  please make sure that both the instrument state, and the lite-mode setting are the same."
            )

        return list(masks)

    ## WRITING AND READING WORKSPACES TO / FROM DISK

    def writeWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName, append=False):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        if not str(filename).endswith(".nxs.h5") and not str(filename).endswith(".nxs"):
            raise RuntimeError(
                f"[writeWorkspace]: specify filename including '.nxs' or '.nxs.h5' extension, not {filename}"
            )
        self.mantidSnapper.SaveNexus(
            "Save a workspace using Nexus format",
            InputWorkspace=workspaceName,
            Filename=str(path / filename),
            Append=append,
        )
        self.mantidSnapper.executeQueue()

    def readWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName, entryNumber: int = 0):
        """
        Read a MatrixWorkspace (derived) workspace from disk in nexus format.
        """
        if not str(filename).endswith(".nxs.h5") and not str(filename).endswith(".nxs"):
            raise RuntimeError(
                f"[readWorkspace]: specify filename including '.nxs' or '.nxs.h5' extension, not {filename}"
            )
        self.mantidSnapper.LoadNexus(
            "Load a workspace using Nexus format",
            OutputWorkspace=workspaceName,
            Filename=str(path / filename),
            EntryNumber=entryNumber,
        )
        self.mantidSnapper.executeQueue()

        # Work-around for known Mantid defect with respect to instrument parameter-map write precision.
        # (Without this step, the precision of the read back detector rotations is only 1.0e-5, instead of 1.0e-15.)
        self.mantidSnapper.mtd[workspaceName].populateInstrumentParameters()

    def writeGroupingWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a grouping workspace to disk in Mantid 'SaveDiffCal' hdf-5 format.
        """
        self.writeDiffCalWorkspaces(path, filename, groupingWorkspaceName=workspaceName)

    def writeDiffCalWorkspaces(
        self,
        path: Path,
        filename: Path,
        tableWorkspaceName: WorkspaceName = "",
        maskWorkspaceName: WorkspaceName = "",
        groupingWorkspaceName: WorkspaceName = "",
    ):
        """
        Writes any or all of the calibration table, mask and grouping workspaces to disk:
        -- up to three workspaces may be written to one 'SaveDiffCal' hdf-5 format file.
        """
        if filename.suffix != ".h5":
            raise RuntimeError(f"[writeDiffCalWorkspaces]: specify filename including '.h5' extension, not {filename}")
        self.mantidSnapper.SaveDiffCal(
            "Save a diffcal table or grouping file",
            CalibrationWorkspace=tableWorkspaceName,
            MaskWorkspace=maskWorkspaceName,
            GroupingWorkspace=groupingWorkspaceName,
            Filename=str(path / filename),
        )
        self.mantidSnapper.executeQueue()

    def writePixelMask(
        self,
        path: Path,
        filename: Path,
        maskWorkspaceName: WorkspaceName,
    ):
        """
        Write a MaskWorkspace to disk in 'SaveDiffCal' hdf-5 format
        """
        # At present, this method is just a wrapper for 'writeDiffCalWorkspaces':
        #   its existence allows for the separation of pixel-mask I/O from diffraction-calibration workspace I/O.
        self.writeDiffCalWorkspaces(path, filename, maskWorkspaceName=maskWorkspaceName)

    ## RunMetadata SUPPORT METHODS

    @lru_cache
    def readRunMetadata(self, runNumber: str) -> RunMetadata:
        success = False
        metadata = None
        try:
            metadata = RunMetadata.fromNeXusLogs(self._readPVFile(runNumber))
            if metadata.runNumber != runNumber:
                raise RuntimeError(
                    f"Expected run-number '{runNumber}' from the filename does not match "
                    + f"the actual run-number '{metadata.runNumber}' from the input-data logs.\n"
                    + "  This should never happen!  Please check your input-data file."
                )
            success = True
        except FileNotFoundError as e:
            if "No PVFile exists" not in str(e) or not self.hasLiveDataConnection():
                raise  # the existing exception is sufficient

        if not success:
            metadata = self.readLiveMetadata()

            if metadata.runNumber == runNumber:
                success = True
            else:
                raise RuntimeError(
                    f"No PVFile exists for run: {runNumber}, "
                    + (
                        f"and it isn't the live run: {metadata.runNumber}."
                        if metadata.hasActiveRun()
                        else "and no live run is active."
                    )
                )
        return metadata

    ## LIVE-DATA SUPPORT METHODS

    @lru_cache
    def hasLiveDataConnection(self) -> bool:
        """For 'live data' methods: test if there is a listener connection to the instrument."""

        # NOTE: adding `lru_cache` to this method bypasses a possible race condition in
        #   `ConfigService.getFacility(...)`.  (And yes, that should be a `const` method.  :( )

        # In addition to 'analysis.sns.gov', other nodes on the subnet should be OK as well.
        #   So this check should also return True on those nodes.
        # If this method returns True, then the `SNSLiveEventDataListener` should be able to function.

        status = False
        if Config["liveData.enabled"]:
            # Normalize to an actual "URL" and then strip off the protocol (not actually "http") and port:
            #   `liveDataAddress` returns a string similar to "bl3-daq1.sns.gov:31415".

            facility, instrument = Config["liveData.facility.name"], Config["liveData.instrument.name"]
            hostname = urlparse(
                "http://" + ConfigService.getFacility(facility).instrument(instrument).liveDataAddress()
            ).hostname
            status = True
            try:
                socket.gethostbyaddr(hostname)
            except Exception as e:  # noqa: BLE001
                # specifically:
                #   we're expecting a `socket.gaierror`, but any exception will indicate that there's no connection
                logger.debug(f"`hasLiveDataConnection` returns `False`: exception: {e}")
                status = False

        return status

    def _liveMetadataFromRun(self, run: Run) -> RunMetadata:
        """Construct a 'RunMetadata' instance from a 'mantid.api.Run' instance."""

        metadata = None
        try:
            metadata = RunMetadata.fromRun(run, liveData=True)
        except (KeyError, RuntimeError, ValueError) as e:
            raise RuntimeError(f"Unable to extract RunMetadata from Run:\n  {e}") from e
        return metadata

    def _readLiveData(self, ws: WorkspaceName, duration: int, *, accumulationMethod="Replace", preserveEvents=False):
        # Initialize `startTime` to indicate that we want `duration` seconds of data prior to the current time.
        startTime = (
            RunMetadata.FROM_NOW_ISO8601
            if duration == 0
            else (datetime.datetime.utcnow() + datetime.timedelta(seconds=-duration)).isoformat()
        )

        # TODO: this call is partially duplicated at `FetchGroceriesAlgorithm`.
        #   However, this separate method is required in order to specify a "fast load" for metadata purposes.
        self.mantidSnapper.LoadLiveData(
            "load live-data chunk",
            OutputWorkspace=ws,
            Instrument=Config["liveData.instrument.name"],
            AccumulationMethod=accumulationMethod,
            StartTime=startTime,
            PreserveEvents=preserveEvents,
        )
        self.mantidSnapper.executeQueue()

        return ws

    def readLiveMetadata(self) -> RunMetadata:
        # This method serves both as the direct `RunMetadata` access point for the non-fallback live-data case,
        #   and also for the fallback case.

        ws = self.mantidSnapper.mtd.unique_hidden_name()

        # Retrieve the smallest possible data increment, in order to read the logs:
        ws = self._readLiveData(ws, duration=0)
        metadata = self._liveMetadataFromRun(self.mantidSnapper.mtd[ws].getRun())

        self.mantidSnapper.DeleteWorkspace("delete temporary workspace", Workspace=ws)
        self.mantidSnapper.executeQueue()
        return metadata

    def readLiveData(self, ws: WorkspaceName, duration: int) -> WorkspaceName:
        # A duration of zero => read all of the available data.
        return self._readLiveData(ws, duration)
