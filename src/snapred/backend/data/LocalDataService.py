from collections.abc import Mapping
from contextlib import contextmanager
import datetime
import glob
import h5py
import json
import numpy as np
import os
import re
import socket
import stat
import time
from errno import ENOENT as NOT_FOUND
from functools import lru_cache
from pathlib import Path
from pydantic import validate_call, ValidationError
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Set, Tuple

from mantid.dataobjects import MaskWorkspace
from mantid.kernel import ConfigService, PhysicalConstants
from mantid.api import Run
from mantid.simpleapi import GetIPTS, mtd

from snapred.backend.dao import (
    GSASParameters,
    InstrumentConfig,
    LiveMetadata,
    ObjectSHA,
    ParticleBounds,
    RunConfig,
    StateConfig,
    StateId,
)
from snapred.backend.dao.calibration import Calibration, CalibrationDefaultRecord, CalibrationRecord
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT
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
from snapred.backend.data.util.mapping_util import (
    mappingFromRun,
    mappingFromNeXusLogs
)
from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
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
        self._verifyPaths = Config["localdataservice.config.verifypaths"]
        self._instrumentConfig = self._readInstrumentConfig()
        self.mantidSnapper = MantidSnapper(None, "Utensils")

    ##### MISCELLANEOUS METHODS #####

    def getInstrumentConfig(self):
        return self._instrumentConfig
        
    def fileExists(self, path):
        return os.path.isfile(path)

    def _determineInstrConfigPaths(self) -> None:
        """This method locates the instrument configuration path and
        sets the instance variable ``_instrumentConfigPath``."""
        # verify parent directory exists
        self._instrumentHomePath = Path(Config["instrument.home"])
        if self._verifyPaths and not self._instrumentHomePath.exists():
            raise _createFileNotFoundError(Config["instrument.home"], self._instrumentHomePath)

        # look for the config file and verify it exists
        self._instrumentConfigPath = Config["instrument.config"]
        if self._verifyPaths and not Path(self._instrumentConfigPath).exists():
            raise _createFileNotFoundError("Missing Instrument Config", Config["instrument.config"])

    def _readInstrumentConfig(self) -> InstrumentConfig:
        self._determineInstrConfigPaths()

        instrumentParameterMap = self._readInstrumentParameters()
        try:
            instrumentParameterMap["bandwidth"] = instrumentParameterMap.pop("neutronBandwidth")
            instrumentParameterMap["maxBandwidth"] = instrumentParameterMap.pop("extendedNeutronBandwidth")
            instrumentParameterMap["delTOverT"] = instrumentParameterMap.pop("delToT")
            instrumentParameterMap["delLOverL"] = instrumentParameterMap.pop("delLoL")
            instrumentParameterMap["version"] = str(instrumentParameterMap["version"])
            instrumentConfig = InstrumentConfig(**instrumentParameterMap)
        except KeyError as e:
            raise KeyError(f"{e}: while reading instrument configuration '{self._instrumentConfigPath}'") from e
        if self._instrumentHomePath:
            instrumentConfig.calibrationDirectory = Path(Config["instrument.calibration.home"])
            if self._verifyPaths and not instrumentConfig.calibrationDirectory.exists():
                raise _createFileNotFoundError("[calibration directory]", instrumentConfig.calibrationDirectory)

        return instrumentConfig

    def _readInstrumentParameters(self) -> Dict[str, Any]:
        instrumentParameterMap: Dict[str, Any] = {}
        try:
            with open(self._instrumentConfigPath, "r") as json_file:
                instrumentParameterMap = json.load(json_file)
            return instrumentParameterMap
        except FileNotFoundError as e:
            raise _createFileNotFoundError("Instrument configuration file", self._instrumentConfigPath) from e

    def readStateConfig(self, runId: str, useLiteMode: bool) -> StateConfig:
        diffCalibration = self.calibrationIndexer(runId, useLiteMode).readParameters()
        stateId = str(diffCalibration.instrumentState.id)

        # Read the grouping-schema map associated with this `StateConfig`.
        groupingMap = None
        if self._groupingMapPath(stateId).exists():
            groupingMap = self._readGroupingMap(stateId)
        else:
            # If no `GroupingMap` JSON file is present at the <state root>,
            #   it is assumed that this is the first time that this state configuration has been initialized.
            # WARNING: `_prepareStateRoot` is also called at `initializeState`: this allows
            #   some order independence of initialization if the back-end is run separately (e.g. in unit tests).
            self._prepareStateRoot(stateId)
            groupingMap = self._readGroupingMap(stateId)

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
        # WARNING: `os.access` does not work correctly on the `/SNS` shared filesystem.

        # formerly:
        #   `return os.access(filePath, os.W_OK) if filePath.exists() else False`

        # alternative implementation (LINUX specific):
        hasPermissions = False
        if filePath.exists():
            stat_ = os.stat(filePath)

            # Get the path's permissions mode bits.
            mode = stat.S_IMODE(stat_.st_mode)

            # Get the path owner's user-id, and group-id.
            fuid = stat_.st_uid
            fgid = stat_.st_gid

            # Get the current user's user-id and group-ids
            uid = os.getuid()
            gids = os.getgroups()

            # Check for any overlap with the write permission's mode bits:
            #   checking the user-bits, if the current user is the owner;
            #   then checking the group-bits, if the user belongs to the file-owner's group;
            #   and finally checking the other-bits.
            hasPermissions = (
                (uid == fuid) and bool(mode & 0o200) or (fgid in gids) and bool(mode & 0o020) or bool(mode & 0o002)
            )
        return hasPermissions

    @staticmethod
    def checkWritePermissions(path: Path) -> bool:
        """Check if the user has permissions to write to, or to create, the specified path."""
        path_ = path
        while path_ and not path_.exists():
            path_ = path_.parent
        return LocalDataService._hasWritePermissionstoPath(path_) if (path_ and path_.exists()) else False

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
    def getIPTS(self, runNumber: str, instrumentName: str = Config["instrument.name"]) -> str:
        IPTS = GetIPTS(RunNumber=runNumber, Instrument=instrumentName)
        
        # WARNING: 
        #   When successful, `GetIPTS` returns the _likely_ user-data directory for this run number.
        # It does _not_ actually check whether any input-data file for this run number exists.

        return str(IPTS)

    def stateExists(self, runId: str) -> bool:
        stateId, _ = self.generateStateId(runId)
        statePath = self.constructCalibrationStateRoot(stateId)
        # Shouldn't need to check lite as we init both at the same time
        return statePath.exists()

    def workspaceIsInstance(self, wsName: str, wsType: Any) -> bool:
        # Is the workspace an instance of the specified type.
        if not mtd.doesExist(wsName):
            return False
        return isinstance(mtd[wsName], wsType)

    def readRunConfig(self, runId: str) -> RunConfig:
        return self._readRunConfig(runId)

    def _readRunConfig(self, runId: str) -> RunConfig:
        # lookup path for IPTS number
        iptsPath = self.getIPTS(runId)

        return RunConfig(
            IPTS=iptsPath,
            runNumber=runId,
            maskFileName="",
            maskFileDirectory=iptsPath + self._instrumentConfig.sharedDirectory,
            gsasFileDirectory=iptsPath + self._instrumentConfig.reducedDataDirectory,
            calibrationState=None,
        )  # TODO: where to find case? "before" "after"

    def _constructPVFilePath(self, runId: str) -> Path:
        runConfig = self._readRunConfig(runId)
        return Path(
            runConfig.IPTS,
            self._instrumentConfig.nexusDirectory,
            f"SNAP_{str(runConfig.runNumber)}{self._instrumentConfig.nexusFileExtension}",
        )

    def _readPVFile(self, runId: str):
        fileName: Path = self._constructPVFilePath(runId)

        if fileName.exists():
            h5 = h5py.File(fileName, "r")
        else:
            raise FileNotFoundError(f"PVFile '{fileName}' does not exist")
        return h5

    # NOTE `lru_cache` decorator needs to be on the outside
    @lru_cache
    @ExceptionHandler(StateValidationException)
    def generateStateId(self, runId: str) -> Tuple[str, DetectorState]:
        if runId in ReservedRunNumber.values():
            SHA = ObjectSHA(hex=ReservedStateId.forRun(runId))
        else:
            detectorState = self.readDetectorState(runId)
            SHA = self._stateIdFromDetectorState(detectorState)
        return SHA.hex, detectorState

    def _stateIdFromDetectorState(self, detectorState: DetectorState) -> ObjectSHA:
        stateID = StateId(
            vdet_arc1=detectorState.arc[0],
            vdet_arc2=detectorState.arc[1],
            WavelengthUserReq=detectorState.wav,
            Frequency=detectorState.freq,
            Pos=detectorState.guideStat,
            # TODO: these should probably be added:
            #   if they change with the runId, there will be a potential hash collision.
            # det_lin1=detectorState.lin[0],
            # det_lin2=detectorState.lin[1],
        )
        return ObjectSHA.fromObject(stateID)

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
        stateId, _ = self.generateStateId(runNumber)
        pathFmt = Config["instrument.reduction.home"]
        if "{IPTS}" in pathFmt:
            IPTS = Path(self.getIPTS(runNumber))
            # substitute the last component of the IPTS-directory for the '{IPTS}' tag
            reductionHome = Path(pathFmt.format(IPTS=IPTS.name))
        else:
            reductionHome = Path(pathFmt)
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
        stateModeRoot = self._constructReductionStateRoot(runNumber) / mode
        runs = []
        if stateModeRoot.exists():
            with os.scandir(stateModeRoot) as entries:
                for entry in entries:
                    if entry.is_dir():
                        if runNumberFormat.match(entry.name):
                            runs.append(entry.name)
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

    def readCalibrationIndex(self, runId: str, useLiteMode: bool):
        return self.calibrationIndexer(runId, useLiteMode).getIndex()

    def readNormalizationIndex(self, runId: str, useLiteMode: bool):
        return self.normalizationIndexer(runId, useLiteMode).getIndex()

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
    def _indexer(self, stateId: str, useLiteMode: bool, indexerType: IndexerType):
        path = self._statePathForWorkflow(stateId, useLiteMode, indexerType)
        return Indexer(indexerType=indexerType, directory=path)

    def indexer(self, runNumber: str, useLiteMode: bool, indexerType: IndexerType):
        stateId, _ = self.generateStateId(runNumber)
        return self._indexer(stateId, useLiteMode, indexerType)

    def calibrationIndexer(self, runId: str, useLiteMode: bool):
        return self.indexer(runId, useLiteMode, IndexerType.CALIBRATION)

    def normalizationIndexer(self, runId: str, useLiteMode: bool):
        return self.indexer(runId, useLiteMode, IndexerType.NORMALIZATION)

    def writeCalibrationIndexEntry(self, entry: IndexEntry):
        """
        The entry must have correct version.
        """
        self.calibrationIndexer(entry.runNumber, entry.useLiteMode).addIndexEntry(entry)

    def writeNormalizationIndexEntry(self, entry: IndexEntry):
        """
        The entry must have correct version.
        """
        self.normalizationIndexer(entry.runNumber, entry.useLiteMode).addIndexEntry(entry)

    ##### NORMALIZATION METHODS #####

    def createNormalizationIndexEntry(self, request: CreateIndexEntryRequest) -> IndexEntry:
        indexer = self.normalizationIndexer(request.runNumber, request.useLiteMode)
        return indexer.createIndexEntry(**request.model_dump())

    def createNormalizationRecord(self, request: CreateNormalizationRecordRequest) -> NormalizationRecord:
        indexer = self.normalizationIndexer(request.runNumber, request.useLiteMode)
        return indexer.createRecord(**request.model_dump())

    def normalizationExists(self, runId: str, useLiteMode: bool) -> bool:
        version = self.normalizationIndexer(runId, useLiteMode).currentVersion()
        return version is not None

    @validate_call
    def readNormalizationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        """
        Will return a normalization record for the given version.
        If no version given, will choose the latest applicable version from the index.
        """
        indexer = self.normalizationIndexer(runId, useLiteMode)
        if version is None:
            version = indexer.latestApplicableVersion(runId)
        return indexer.readRecord(version)

    def writeNormalizationRecord(self, record: NormalizationRecord):
        """
        Persists a `NormalizationRecord` to either a new version folder, or overwrites a specific version.
        Record must be set with correct version.
        -- side effect: creates any directories needed for save
        """

        indexer = self.normalizationIndexer(record.runNumber, record.useLiteMode)
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
        indexer = self.normalizationIndexer(record.runNumber, record.useLiteMode)
        normalizationDataPath: Path = indexer.versionPath(record.version)
        if not normalizationDataPath.exists():
            normalizationDataPath.mkdir(parents=True, exist_ok=True)
        for workspace in record.workspaceNames:
            filename = Path(workspace + ".nxs")
            self.writeWorkspace(normalizationDataPath, filename, workspace)

    ##### CALIBRATION METHODS #####

    def createCalibrationIndexEntry(self, request: CreateIndexEntryRequest) -> IndexEntry:
        indexer = self.calibrationIndexer(request.runNumber, request.useLiteMode)
        return indexer.createIndexEntry(**request.model_dump())

    def createCalibrationRecord(self, request: CreateCalibrationRecordRequest) -> CalibrationRecord:
        indexer = self.calibrationIndexer(request.runNumber, request.useLiteMode)
        return indexer.createRecord(**request.model_dump())

    def calibrationExists(self, runId: str, useLiteMode: bool) -> bool:
        version = self.calibrationIndexer(runId, useLiteMode).currentVersion()
        return version is not None

    @validate_call
    def readCalibrationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        """
        Will return a calibration record for the given version.
        If no version given, will choose the latest applicable version from the index.
        """
        indexer = self.calibrationIndexer(runId, useLiteMode)
        if version is None:
            # NOTE Indexer.readRecord defaults to currentVersion
            version = indexer.latestApplicableVersion(runId)
        return indexer.readRecord(version)

    def writeCalibrationRecord(self, record: CalibrationRecord):
        """
        Persists a `CalibrationRecord` to either a new version folder, or overwrite a specific version.
        Record must be set with correct version.
        -- side effect: creates any directories needed for save
        """

        indexer = self.calibrationIndexer(record.runNumber, record.useLiteMode)
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
        indexer = self.calibrationIndexer(record.runNumber, record.useLiteMode)
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
            if mtd.doesExist(ws):
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
    def readCalibrationState(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        if not self.calibrationExists(runId, useLiteMode):
            if self._hasWritePermissionsCalibrationStateRoot():
                raise RecoverableException.stateUninitialized(runId, useLiteMode)
            else:
                raise RuntimeError(
                    "No calibration exists, and you lack permissions to create one."  # fmt: skip
                    " Please contact your IS or CIS."  # fmt: skip
                )

        indexer = self.calibrationIndexer(runId, useLiteMode)
        # NOTE if we prefer latest version in index, uncomment below
        # if version is None:
        #     version = indexer.latestApplicableVersion(runId)
        return indexer.readParameters(version)

    @validate_call
    def readNormalizationState(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        indexer = self.normalizationIndexer(runId, useLiteMode)
        # NOTE if we prefer latest version in index, uncomment below
        # if version is None:
        #     version = indexer.latestApplicableVersion(runId)
        return indexer.readParameters(version)

    def writeCalibrationState(self, calibration: Calibration):
        """
        Calibration state must have version set.
        """
        indexer = self.calibrationIndexer(calibration.seedRun, calibration.useLiteMode)
        indexer.writeParameters(calibration)

    def writeNormalizationState(self, normalization: Normalization):
        """
        Normalization state must have version set.
        """
        indexer = self.normalizationIndexer(normalization.seedRun, normalization.useLiteMode)
        indexer.writeParameters(normalization)

    def _detectorStateFromMapping(self, logs: Mapping) -> DetectorState:
        detectorState = None
        wav_key_1 = "BL3:Chop:Gbl:WavelengthReq"
        wav_key_2 = "BL3:Chop:Skf1:WavelengthUserReq"
        try:
            try:
                detectorState = DetectorState(
                    arc=[logs["det_arc1"][0], logs["det_arc2"][0]],
                    wav=logs["BL3:Chop:Gbl:WavelengthReq"][0] if wav_key_1 in logs\
                        else logs[wav_key_2][0],
                    freq=logs["BL3:Det:TH:BL:Frequency"][0],
                    guideStat=logs["BL3:Mot:OpticsPos:Pos"][0],
                    lin=[logs["det_lin1"][0], logs["det_lin2"][0]]
                )
            except (KeyError, TypeError) as e:
                raise RuntimeError("Some required logs are not present. Cannot assemble a DetectorState") from e            
        except ValidationError as e:
            raise RuntimeError("Logs have an unexpected format.  Cannot assemble a DetectorState.") from e
        return detectorState
        
    def readDetectorState(self, runNumber: str) -> DetectorState:
        detectorState = None
        try:
            detectorState = self._detectorStateFromMapping(mappingFromNeXusLogs(self._readPVFile(runNumber)))
        except FileNotFoundError as e:
            if not self.hasLiveDataConnection():
                raise # the existing exception is sufficient
            metadata = self.readLiveMetadata()
            if metadata.runNumber == runNumber:
                detectorState = metadata.detectorState
            else:
                raise RuntimeError(f"No PVFile exists for run {runNumber}, and it isn't a live run.")
        return detectorState
                
    def detectorStateFromWorkspace(self, wsName: WorkspaceName) -> DetectorState:
        return self._detectorStateFromMapping(mappingFromRun(mtd[wsName].getRun()))
        
    @validate_call
    def _writeDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool):
        from snapred.backend.data.GroceryService import GroceryService

        indexer = self.calibrationIndexer(runNumber, useLiteMode)
        version = indexer.defaultVersion()
        grocer = GroceryService()
        filename = Path(grocer.createDiffcalTableWorkspaceName("default", useLiteMode, version) + ".h5")
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
        
        # Pull static values from resources
        defaultGroupSliceValue = Config["calibration.parameters.default.groupSliceValue"]
        fwhmMultipliers = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
        peakTailCoefficient = Config["calibration.parameters.default.peakTailCoefficient"]
        gsasParameters = GSASParameters(
            alpha=Config["calibration.parameters.default.alpha"], beta=Config["calibration.parameters.default.beta"]
        )
        
        # Calculate the derived values
        lambdaLimit = Limit(
            minimum=detectorState.wav - (self._instrumentConfig.bandwidth / 2) + self._instrumentConfig.lowWavelengthCrop,
            maximum=detectorState.wav + (self._instrumentConfig.bandwidth / 2),
        )
        L = self._instrumentConfig.L1 + self._instrumentConfig.L2
        tofLimit = Limit(
            minimum=lambdaLimit.minimum * L / self.CONVERSION_FACTOR,
            maximum=lambdaLimit.maximum * L / self.CONVERSION_FACTOR,
        )
        particleBounds = ParticleBounds(wavelength=lambdaLimit, tof=tofLimit)

        return InstrumentState(
            id=stateId,
            instrumentConfig=self._instrumentConfig,
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
        stateId = instrumentState.id

        calibrationReturnValue = None

        # Make sure that the state root directory has been initialized:
        stateRootPath: Path = self.constructCalibrationStateRoot(stateId)
        if not stateRootPath.exists():
            # WARNING: `_prepareStateRoot` is also called at `readStateConfig`; this allows
            #   some order independence of initialization if the back-end is run separately (e.g. in unit tests).
            self._prepareStateRoot(stateId)

        # now save default versions of files in both lite and native resolution directories
        version = VERSION_DEFAULT
        for liteMode in [True, False]:
            indexer = self.calibrationIndexer(runId, liteMode)
            calibration = indexer.createParameters(
                instrumentState=instrumentState,
                name=name,
                seedRun=runId,
                useLiteMode=liteMode,
                creationDate=datetime.datetime.now(),
                version=version,
            )

            # NOTE: this creates a bare record without any other CalibrationRecord data
            defaultDiffCalTableName = grocer.createDiffcalTableWorkspaceName("default", liteMode, version)
            workspaces: Dict[WorkspaceType, List[WorkspaceName]] = {
                wngt.DIFFCAL_TABLE: [defaultDiffCalTableName],
            }
            record = CalibrationDefaultRecord(
                runNumber=runId,
                useLiteMode=liteMode,
                version=version,
                calculationParameters=calibration,
                workspaces=workspaces,
            )
            entry = indexer.createIndexEntry(
                runNumber=runId,
                useLiteMode=liteMode,
                version=version,
                appliesTo=">=0",
                author="SNAPRed Internal",
                comments="The default configuration when loading StateConfig if none other is found",
            )
            # write the calibration state
            indexer.writeRecord(record)
            indexer.writeParameters(record.calculationParameters)
            indexer.addIndexEntry(entry)
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
        if not isinstance(mtd[wsName], MaskWorkspace):
            return False
        targetPixelCount = (
            Config["instrument.lite.pixelResolution"] if useLiteMode else Config["instrument.native.pixelResolution"]
        )
        if mtd[wsName].getNumberHistograms() != targetPixelCount:
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
        for run in self._reducedRuns(runNumber, useLiteMode):
            for ts in self._reducedTimestamps(runNumber, useLiteMode):
                maskName = wng.reductionPixelMask().runNumber(runNumber).timestamp(ts).build()
                maskFilePath = self._constructReductionDataPath(runNumber, useLiteMode, ts) / (maskName + ".h5")

                # Implementation notes:
                # * No compatibility check is required for reduction masks on the filesystem:
                #     they are guaranteed to be compatible;

                if maskName not in masks and maskFilePath.exists():
                    # Ensure that any _resident_ mask is compatible:
                    if mtd.doesExist(maskName) and not self.isCompatibleMask(maskName, runNumber, useLiteMode):
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
        wsNames = mtd.getObjectNames()
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

    ## LIVE-DATA SUPPORT METHODS

    @contextmanager
    def _useFacility(self, facility: str):
        _facilitySave: str = ConfigService.getFacility()
        ConfigService.setFacility(facility)
        yield facility
        
        # exit
        ConfigService.setFacility(_facilitySave)
        
    def hasLiveDataConnection(self, facility: str = Config["liveData.facility.name"], instrument: str = Config["liveData.instrument.name"]) -> bool:
        """For 'live data' methods: test if there is a listener connection to the instrument."""
        
        # In addition to 'analysis.sns.gov', other nodes on the subnet should be OK as well.
        #   So this check should also return True on those nodes.
        # If this method returns True, then the `SNSLiveEventDataListener` should be able to function.

        # Normalize to an actual "URL" and then strip off the protocol (not actually "http") and port:
        #   `liveDataAddress` returns a string similar to "bl3-daq1.sns.gov:31415".

        hostname = urlparse("http://" + ConfigService.getFacility(facility).instrument(instrument).liveDataAddress()).hostname
        status = True
        try:
            socket.gethostbyaddr(hostname)
        except Exception: 
            # specifically: expecting a `socket.gaierror`, but any exception will indicate that there's no connection
            status = False
        return status
        """
        # *** DEBUG *** mock
        return True
        """
        
    def _liveMetadataFromRun(self, run: Run) -> LiveMetadata:
        """Construct a 'LiveMetadata' instance from a 'mantid.api.Run' instance."""
        
        logs = mappingFromRun(run)
        metadata = None
        try:
            run_number: str = str(logs['run_number'])
            
            # See comment at `snapred.backend.data.util.mapping_util.mappingFromRun` about this conversion.
            start_time: datetime.datetime = np.datetime64(logs['start_time'], "us").astype(datetime.datetime)
            end_time: datetime.datetime = np.datetime64(logs['end_time'], "us").astype(datetime.datetime)

            # Many required log values will not be present if a run is inactive.
            detector_state=self._detectorStateFromMapping(logs) if run_number != str(LiveMetadata.INACTIVE_RUN) else None

            proton_charge = logs['proton_charge']
            
            metadata = LiveMetadata(
                runNumber=run_number,
                startTime=start_time,
                endTime=end_time,
                detectorState=detector_state,
                protonCharge=proton_charge
            )
        except (KeyError, RuntimeError, ValidationError) as e:
            raise RuntimeError("unable to extract LiveMetadata from Run") from e
        return metadata

    def _readLiveData(self, ws: WorkspaceName, duration: int, facility: str, instrument: str):
        # 'StartTime=""' => read all of the available data
        
        startTime = (datetime.datetime.utcnow() + datetime.timedelta(seconds=-duration)).isoformat()\
            if duration != 0 else ""
        
        # TODO: duplicated at `FetchGroceriesAlgorithm`.  Probably that should be called here.    
        with self._useFacility(facility):
            self.mantidSnapper.LoadLiveData(
                "load live-data chunk",
                OutputWorkspace=ws,
                Instrument=instrument,
                AccumulationMethod=Config["liveData.accumulationMethod"],
                StartTime=startTime
            )
            self.mantidSnapper.executeQueue()
        
        return ws

    def readLiveMetadata(self, facility: str = Config["liveData.facility.name"], instrument: str = Config["liveData.instrument.name"]) -> LiveMetadata:
        ws = self.mantidSnapper.mtd.unique_hidden_name()
        
        # Retrieve the smallest possible data increment, in order to read the logs:
        ws = self._readLiveData(ws, duration=1, facility=facility, instrument=instrument)
        metadata = self._liveMetadataFromRun(mtd[ws].getRun())
        
        self.mantidsnapper.DeleteWorkspace(ws)
        self.mantidsnapper.executeQueue()
        return metadata
        """                  
        # *** DEBUG *** : mock
        duration = datetime.timedelta(minutes=1)
        now = datetime.datetime.utcnow()
        return LiveMetadata.model_construct(
            runNumber="46680",
            startTime=now - duration,
            endTime=now,
            # WARNING: probably not DetectorState for "46680"
            detectorState=DetectorState(
                arc=(-65.3, 104.95),
                wav=2.1,
                freq=60.0,
                guideStat=1,
                lin=(0.045, 0.043)
            ),
            protonCharge=0.0
        )
        """
        
    def readLiveData(
        self,
        ws: WorkspaceName,
        duration: int,
        facility: str = Config["liveData.facility.name"],
        instrument: str = Config["liveData.instrument.name"]
    ) -> WorkspaceName:
        # A duration of zero => read all of the available data.
        return self._readLiveData(ws, duration, facility, instrument)
