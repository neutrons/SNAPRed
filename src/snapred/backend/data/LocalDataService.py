import datetime
import glob
import json
import os
from errno import ENOENT as NOT_FOUND
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import h5py
from mantid.kernel import PhysicalConstants
from mantid.simpleapi import GetIPTS, mtd
from pydantic import validate_call

from snapred.backend.dao import (
    GSASParameters,
    InstrumentConfig,
    ObjectSHA,
    ParticleBounds,
    RunConfig,
    StateConfig,
    StateId,
)
from snapred.backend.dao.calibration import Calibration, CalibrationRecord
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
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
from snapred.backend.dao.state.CalibrantSample import CalibrantSamples
from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.decorators.ExceptionHandler import ExceptionHandler
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
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
    instrumentConfig: "InstrumentConfig"
    verifyPaths: bool = True

    # conversion factor from microsecond/Angstrom to meters
    CONVERSION_FACTOR = Config["constants.m2cm"] * PhysicalConstants.h / PhysicalConstants.NeutronMass

    def __init__(self) -> None:
        self.verifyPaths = Config["localdataservice.config.verifypaths"]
        self.instrumentConfig = self.readInstrumentConfig()
        self.mantidSnapper = MantidSnapper(None, "Utensils")

    ##### MISCELLANEOUS METHODS #####

    def fileExists(self, path):
        return os.path.isfile(path)

    def _determineInstrConfigPaths(self) -> None:
        """This method locates the instrument configuration path and
        sets the instance variable ``instrumentConfigPath``."""
        # verify parent directory exists
        self.dataPath = Path(Config["instrument.home"])
        if self.verifyPaths and not self.dataPath.exists():
            raise _createFileNotFoundError(Config["instrument.home"], self.dataPath)

        # look for the config file and verify it exists
        self.instrumentConfigPath = Config["instrument.config"]
        if self.verifyPaths and not Path(self.instrumentConfigPath).exists():
            raise _createFileNotFoundError("Missing Instrument Config", Config["instrument.config"])

    def readInstrumentConfig(self) -> InstrumentConfig:
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
            raise KeyError(f"{e}: while reading instrument configuration '{self.instrumentConfigPath}'") from e
        if self.dataPath:
            instrumentConfig.calibrationDirectory = Path(Config["instrument.calibration.home"])
            if self.verifyPaths and not instrumentConfig.calibrationDirectory.exists():
                raise _createFileNotFoundError("[calibration directory]", instrumentConfig.calibrationDirectory)

        return instrumentConfig

    def _readInstrumentParameters(self) -> Dict[str, Any]:
        instrumentParameterMap: Dict[str, Any] = {}
        try:
            with open(self.instrumentConfigPath, "r") as json_file:
                instrumentParameterMap = json.load(json_file)
            return instrumentParameterMap
        except FileNotFoundError as e:
            raise _createFileNotFoundError("Instrument configuration file", self.instrumentConfigPath) from e

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

    @lru_cache
    def getIPTS(self, runNumber: str, instrumentName: str = Config["instrument.name"]) -> str:
        ipts = GetIPTS(runNumber, instrumentName)
        return str(ipts)

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
            maskFileDirectory=iptsPath + self.instrumentConfig.sharedDirectory,
            gsasFileDirectory=iptsPath + self.instrumentConfig.reducedDataDirectory,
            calibrationState=None,
        )  # TODO: where to find case? "before" "after"

    def _constructPVFilePath(self, runId: str) -> Path:
        runConfig = self._readRunConfig(runId)
        return Path(
            runConfig.IPTS,
            self.instrumentConfig.nexusDirectory,
            f"SNAP_{str(runConfig.runNumber)}{self.instrumentConfig.nexusFileExtension}",
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
    def _generateStateId(self, runId: str) -> Tuple[str, str]:
        detectorState = self.readDetectorState(runId)
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
        SHA = ObjectSHA.fromObject(stateID)

        return SHA.hex, SHA.decodedKey

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

    def _constructCalibrationStateRoot(self, stateId) -> Path:
        return Path(Config["instrument.calibration.powder.home"], str(stateId))

    def _constructCalibrationStatePath(self, stateId, useLiteMode) -> Path:
        mode = "lite" if useLiteMode else "native"
        return self._constructCalibrationStateRoot(stateId) / mode / "diffraction"

    def _constructNormalizationStatePath(self, stateId, useLiteMode) -> Path:
        mode = "lite" if useLiteMode else "native"
        return self._constructCalibrationStateRoot(stateId) / mode / "normalization"

    # reduction paths #

    @validate_call
    def _constructReductionStateRoot(self, runNumber: str) -> Path:
        stateId, _ = self._generateStateId(runNumber)
        IPTS = Path(self.getIPTS(runNumber))
        # substitute the last component of the IPTS-directory for the '{IPTS}' tag
        reductionHome = Path(Config["instrument.reduction.home"].format(IPTS=IPTS.name))
        return reductionHome / stateId

    @validate_call
    def _constructReductionDataRoot(self, runNumber: str, useLiteMode: bool) -> Path:
        reductionStateRoot = self._constructReductionStateRoot(runNumber)
        mode = "lite" if useLiteMode else "native"
        return reductionStateRoot / mode / runNumber

    @validate_call
    def _constructReductionDataPath(self, runNumber: str, useLiteMode: bool, version: int) -> Path:
        return self._constructReductionDataRoot(runNumber, useLiteMode) / wnvf.fileVersion(version)

    @validate_call
    def _constructReductionRecordFilePath(self, runNumber: str, useLiteMode: bool, version: int) -> Path:
        recordPath = self._constructReductionDataPath(runNumber, useLiteMode, version) / "ReductionRecord.json"
        return recordPath

    @validate_call
    def _constructReductionDataFilePath(self, runNumber: str, useLiteMode: bool, version: int) -> Path:
        stateId, _ = self._generateStateId(runNumber)
        fileName = wng.reductionOutputGroup().stateId(stateId).version(version).build()
        fileName += Config["nexus.file.extension"]
        filePath = self._constructReductionDataPath(runNumber, useLiteMode, version) / fileName
        return filePath

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
        stateId, _ = self._generateStateId(runNumber)
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

    # TODO delete this and replace with something else.
    def _getLatestReductionVersionNumber(self, runNumber: str, useLiteMode: bool) -> int:
        dataRoot = self._constructReductionDataRoot(runNumber, useLiteMode)
        versions = []
        for dire in dataRoot.glob("v_*"):
            versions.append(int(str(dire).split("_")[-1]))
        return max(versions)

    ##### NORMALIZATION METHODS #####

    def createNormalizationIndexEntry(self, request: CreateIndexEntryRequest) -> IndexEntry:
        indexer = self.normalizationIndexer(request.runNumber, request.useLiteMode)
        return indexer.createIndexEntry(**request.model_dump())

    def createNormalizationRecord(self, request: CreateNormalizationRecordRequest) -> NormalizationRecord:
        indexer = self.normalizationIndexer(request.runNumber, request.useLiteMode)
        return indexer.createRecord(**request.model_dump())

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
        -- side effect: creates needed directories for save
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
        Record must be set with correct version and workspace names finalized.
        -- assumes that `writeNormalizationRecord` has already been called, and that the version folder exists
        """
        indexer = self.normalizationIndexer(record.runNumber, record.useLiteMode)
        normalizationDataPath: Path = indexer.versionPath(record.version)
        if not normalizationDataPath.exists():
            normalizationDataPath.mkdir(parents=True, exist_ok=True)
        for workspace in record.workspaceNames:
            ws = mtd[workspace]
            if ws.isRaggedWorkspace():
                filename = Path(workspace + ".nxs.h5")
                self.writeRaggedWorkspace(normalizationDataPath, filename, workspace)
            else:
                filename = Path(workspace + ".nxs")
                self.writeWorkspace(normalizationDataPath, filename, workspace)

    ##### CALIBRATION METHODS #####

    def createCalibrationIndexEntry(self, request: CreateIndexEntryRequest) -> IndexEntry:
        indexer = self.calibrationIndexer(request.runNumber, request.useLiteMode)
        return indexer.createIndexEntry(**request.model_dump())

    def createCalibrationRecord(self, request: CreateCalibrationRecordRequest) -> CalibrationRecord:
        indexer = self.calibrationIndexer(request.runNumber, request.useLiteMode)
        return indexer.createRecord(**request.model_dump())

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
        Record must be set with correct version and workspace names finalized.
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
            self.writeRaggedWorkspace(calibrationDataPath, filename, wsName)

        # write the diagnostic output
        wsNames = record.workspaces.get(wngt.DIFFCAL_DIAG, [])
        ext = Config["calibration.diffraction.diagnostic.extension"]
        for wsName in wsNames:
            filename = Path(wsName + ext)
            self.writeWorkspace(calibrationDataPath, filename, wsName)

        # write the diffcal table and attached mask
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
    def readReductionRecord(self, runNumber: str, useLiteMode: bool, version: Optional[int] = None) -> ReductionRecord:
        """
        Will return a reduction record for the given version.
        If no version given, will choose the most recent version.
        """
        if version is None:
            version = self._getLatestReductionVersionNumber(runNumber, useLiteMode)
        record = None
        if version is not None:
            filePath: Path = self._constructReductionRecordFilePath(runNumber, useLiteMode, version)
            record = parse_file_as(ReductionRecord, filePath)
        return record

    def writeReductionRecord(self, record: ReductionRecord):
        """
        Persists a `ReductionRecord` to either a new version folder, or overwrites a specific version.
        * side effect: creates the version directory if none exists;
        """
        # For the moment, a single run number is assumed:
        runNumber = record.runNumbers[0]
        version = record.version

        filePath: Path = self._constructReductionRecordFilePath(runNumber, record.useLiteMode, version)
        if not filePath.parent.exists():
            filePath.parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(record, filePath)
        logger.info(f"wrote ReductionRecord: version: {version}")

    def writeReductionData(self, record: ReductionRecord):
        """
        Persists the reduction data associated with a `ReductionRecord`
        * side effect: creates the version directory if none exists
        """

        # For the moment, a single run number is assumed:
        runNumber = record.runNumbers[0]
        version = record.version

        dataFilePath = self._constructReductionDataFilePath(runNumber, record.useLiteMode, version)
        if not dataFilePath.parent.exists():
            # write reduction record must be called first
            self.writeReductionRecord(record)

        for ws in record.workspaceNames:
            # Append workspaces to hdf5 file, in order of the `workspaces` list
            ws_ = mtd[ws]
            if ws_.isRaggedWorkspace():
                self.writeRaggedWorkspace(dataFilePath.parent, Path(dataFilePath.name), ws)
            else:
                self.writeWorkspace(dataFilePath.parent, Path(dataFilePath.name), ws, append=True)

        # Append the "metadata" group, containing the `ReductionRecord` metadata
        with h5py.File(dataFilePath, "a") as h5:
            n5m.insertMetadataGroup(h5, record.dict(), "/metadata")

        logger.info(f"wrote reduction data to {dataFilePath}: version: {version}")

    @validate_call
    def readReductionData(self, runNumber: str, useLiteMode: bool, version: int) -> ReductionRecord:
        """
        This method is complementary to `writeReductionData`:
        * it is provided primarily for diagnostic purposes, and is not yet connected to any workflow
        * note that the "version" argument is mandatory.
        """
        dataFilePath = self._constructReductionDataFilePath(runNumber, useLiteMode, version)
        if not dataFilePath.exists():
            raise RuntimeError(f"[readReductionData]: file {dataFilePath} does not exist")

        # read the metadata first, in order to use the workspaceNames list
        record = None
        with h5py.File(dataFilePath, "r") as h5:
            record = ReductionRecord.model_validate(n5m.extractMetadataGroup(h5, "/metadata"))
        for ws in record.workspaceNames:
            if mtd.doesExist(ws):
                raise RuntimeError(f"[readReductionData]: workspace {ws} already exists in the ADS")

        # Read the workspaces, one by one;
        #   * as an alternative, these could be loaded into a group workspace with a single call to `readWorkspace`.
        for n, ws in enumerate(record.workspaceNames):
            self.readWorkspace(dataFilePath.parent, Path(dataFilePath.name), ws, entryNumber=n + 1)

        logger.info(f"loaded reduction data from {dataFilePath}: version: {version}")
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

    def writeCalibrantSample(self, sample: CalibrantSamples):
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
            for atom in sampleJson["crystallography"]["atoms"]:
                atom["symbol"] = atom.pop("atom_type")
                atom["coordinates"] = atom.pop("atom_coordinates")
                atom["siteOccupationFactor"] = atom.pop("site_occupation_factor")
            sample = CalibrantSamples.model_validate_json(json.dumps(sampleJson))
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
    @ExceptionHandler(RecoverableException, "'NoneType' object has no attribute 'instrumentState'")
    def readCalibrationState(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
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

    def readDetectorState(self, runId: str) -> DetectorState:
        detectorState = None
        pvFile = self._readPVFile(runId)
        try:
            detectorState = DetectorState(
                arc=[pvFile.get("entry/DASlogs/det_arc1/value")[0], pvFile.get("entry/DASlogs/det_arc2/value")[0]],
                wav=pvFile.get("entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value")[0],
                freq=pvFile.get("entry/DASlogs/BL3:Det:TH:BL:Frequency/value")[0],
                guideStat=pvFile.get("entry/DASlogs/BL3:Mot:OpticsPos:Pos/value")[0],
                lin=[pvFile.get("entry/DASlogs/det_lin1/value")[0], pvFile.get("entry/DASlogs/det_lin2/value")[0]],
            )
        except:  # noqa: E722
            raise ValueError(f"Could not find all required logs in file '{self._constructPVFilePath(runId)}'")
        return detectorState

    @validate_call
    def _writeDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool):
        from snapred.backend.data.GroceryService import GroceryService

        indexer = self.calibrationIndexer(runNumber, useLiteMode)
        version = indexer.defaultVersion()
        grocer = GroceryService()
        filename = Path(grocer._createDiffcalTableWorkspaceName("default", useLiteMode, version) + ".h5")
        outWS = grocer.fetchDefaultDiffCalTable(runNumber, useLiteMode, version)

        calibrationDataPath = indexer.versionPath(version)
        self.writeDiffCalWorkspaces(calibrationDataPath, filename, outWS)

    @validate_call
    @ExceptionHandler(StateValidationException)
    # NOTE if you are debugigng and got here, coment out the ExceptionHandler and try again
    def initializeState(self, runId: str, useLiteMode: bool, name: str = None):
        stateId, _ = self._generateStateId(runId)

        # Read the detector state from the pv data file
        detectorState = self.readDetectorState(runId)

        # then read data from the common calibration state parameters stored at root of calibration directory
        instrumentConfig = self.readInstrumentConfig()
        # then pull static values specified by Malcolm from resources
        defaultGroupSliceValue = Config["calibration.parameters.default.groupSliceValue"]
        fwhmMultipliers = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
        peakTailCoefficient = Config["calibration.parameters.default.peakTailCoefficient"]
        gsasParameters = GSASParameters(
            alpha=Config["calibration.parameters.default.alpha"], beta=Config["calibration.parameters.default.beta"]
        )
        # then calculate the derived values
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

        instrumentState = InstrumentState(
            id=stateId,
            instrumentConfig=instrumentConfig,
            detectorState=detectorState,
            gsasParameters=gsasParameters,
            particleBounds=particleBounds,
            defaultGroupingSliceValue=defaultGroupSliceValue,
            fwhmMultipliers=fwhmMultipliers,
            peakTailCoefficient=peakTailCoefficient,
        )

        calibrationReturnValue = None

        # Make sure that the state root directory has been initialized:
        stateRootPath: Path = self._constructCalibrationStateRoot(stateId)
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
            # NOTE a bare record without other CalibrationRecord data
            record = Record(
                runNumber=runId,
                useLiteMode=liteMode,
                version=version,
                calculationParameters=calibration,
            )
            entry = indexer.createIndexEntry(
                runNumber=runId,
                useLiteMode=liteMode,
                version=version,
                appliesTo=">=0",
                author="SNAPRed Internal",
                comments="The default condition for loading StateConfigs if none other found",
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
        stateRootPath: Path = self._constructCalibrationStateRoot(stateId)
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
        # first perform some basic validation of the run ID
        # - it must be a string of only digits
        # - it must be greater than some minimal run number
        if not runId.isdigit() or int(runId) < Config["instrument.startingRunNumber"]:
            return False
        # then make sure the run number has a valid IPTS
        try:
            self.getIPTS(runId)
        # if no IPTS found, return false
        except RuntimeError:
            return False
        # if found, try to construct the path and test if the path exists
        else:
            stateID, _ = self._generateStateId(runId)
            calibrationStatePath: Path = self._constructCalibrationStateRoot(stateID)
            return calibrationStatePath.exists()

    ##### GROUPING MAP METHODS #####

    def _readGroupingMap(self, stateId: str) -> GroupingMap:
        path: Path = self._groupingMapPath(stateId)
        if not path.exists():
            raise FileNotFoundError(f'required grouping-schema map for state "{stateId}" at "{path}" does not exist')
        with open(path, "r") as f:
            groupingMap = GroupingMap.model_validate_json(f.read())
        return groupingMap

    def readGroupingMap(self, runNumber: str):
        # if the state exists then lookup its grouping map
        if self.checkCalibrationFileExists(runNumber):
            stateId, _ = self._generateStateId(runNumber)
            return self._readGroupingMap(stateId)
        # otherwise return the default map
        else:
            return self._readDefaultGroupingMap()

    def _readDefaultGroupingMap(self) -> GroupingMap:
        path: Path = self._defaultGroupingMapPath()
        if not path.exists():
            raise FileNotFoundError(f'required default grouping-schema map "{path}" does not exist')
        with open(path, "r") as f:
            groupingMap = GroupingMap.model_validate_json(f.read())
        return groupingMap

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
        return self._constructCalibrationStateRoot(stateId) / "groupingMap.json"

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

    def writeRaggedWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a ragged workspace to disk in a .tar format.
        """
        self.mantidSnapper.WrapLeftovers(
            "Store the ragged workspace",
            InputWorkspace=workspaceName,
            Filename=str(path / filename),
        )
        self.mantidSnapper.executeQueue()

    def readRaggedWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Read a ragged workspace from disk in a .tar format.
        """
        self.mantidSnapper.ReheatLeftovers(
            "Load a ragged workspace",
            Filename=str(path / filename),
            OutputWorkspace=workspaceName,
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
            raise RuntimeError(
                f"[writeCalibrationWorkspaces]: specify filename including '.h5' extension, not {filename}"
            )
        self.mantidSnapper.SaveDiffCal(
            "Save a diffcal table or grouping file",
            CalibrationWorkspace=tableWorkspaceName,
            MaskWorkspace=maskWorkspaceName,
            GroupingWorkspace=groupingWorkspaceName,
            Filename=str(path / filename),
        )
        self.mantidSnapper.executeQueue()
