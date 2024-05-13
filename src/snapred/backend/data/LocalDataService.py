import datetime
import glob
import json
import os
from copy import deepcopy
from errno import ENOENT as NOT_FOUND
from pathlib import Path
from typing import Any, Dict, List, Tuple

import h5py
from mantid.kernel import PhysicalConstants
from mantid.simpleapi import GetIPTS, mtd
from pydantic import parse_file_as

from snapred.backend.dao import (
    GSASParameters,
    InstrumentConfig,
    ObjectSHA,
    ParticleBounds,
    RunConfig,
    StateConfig,
    StateId,
)
from snapred.backend.dao.calibration import Calibration, CalibrationIndexEntry, CalibrationRecord
from snapred.backend.dao.Limit import Limit, Pair
from snapred.backend.dao.normalization import Normalization, NormalizationIndexEntry, NormalizationRecord
from snapred.backend.dao.state import (
    DetectorState,
    GroupingMap,
    InstrumentState,
)
from snapred.backend.dao.state.CalibrantSample import CalibrantSamples
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
from snapred.meta.redantic import (
    write_model_list_pretty,
    write_model_pretty,
)

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
    reductionParameterCache: Dict[str, Any] = {}
    iptsCache: Dict[Tuple[str, str], Any] = {}
    stateIdCache: Dict[str, ObjectSHA] = {}
    instrumentConfig: "InstrumentConfig"
    verifyPaths: bool = True

    # conversion factor from microsecond/Angstrom to meters
    CONVERSION_FACTOR = Config["constants.m2cm"] * PhysicalConstants.h / PhysicalConstants.NeutronMass

    def __init__(self) -> None:
        self.verifyPaths = Config["localdataservice.config.verifypaths"]
        self.instrumentConfig = self.readInstrumentConfig()
        self.mantidSnapper = MantidSnapper(None, "Utensils")

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
        previousDiffCalRecord: CalibrationRecord = self.readCalibrationRecord(runId, useLiteMode=useLiteMode)
        if previousDiffCalRecord is None:
            diffCalibration: Calibration = self.readCalibrationState(runId, useLiteMode)
        else:
            diffCalibration: Calibration = previousDiffCalRecord.calibrationFittingIngredients

        stateId = diffCalibration.instrumentState.id

        # Read the grouping-schema map associated with this `StateConfig`.
        groupingMap = None
        if self._groupingMapPath(str(stateId)).exists():
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

    def getIPTS(self, runNumber: str, instrumentName: str = Config["instrument.name"]) -> str:
        key = (runNumber, instrumentName)
        if key not in self.iptsCache:
            self.iptsCache[key] = GetIPTS(RunNumber=int(runNumber), Instrument=instrumentName)
        return str(self.iptsCache[key])

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

    def _constructPVFilePath(self, runId: str):
        runConfig = self._readRunConfig(runId)
        return (
            runConfig.IPTS
            + self.instrumentConfig.nexusDirectory
            + "/SNAP_"
            + str(runConfig.runNumber)
            + self.instrumentConfig.nexusFileExtension
        )

    def _readPVFile(self, runId: str):
        fName: str = self._constructPVFilePath(runId)

        if os.path.exists(fName):
            f = h5py.File(fName, "r")
        else:
            raise FileNotFoundError(f"PVFile '{fName}' does not exist")
        return f

    @ExceptionHandler(StateValidationException)
    def _generateStateId(self, runId: str) -> Tuple[str, str]:
        if runId in self.stateIdCache:
            SHA = self.stateIdCache[runId]
            return SHA.hex, SHA.decodedKey

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
        self.stateIdCache[runId] = SHA

        return SHA.hex, SHA.decodedKey

    def _findMatchingFileList(self, pattern, throws=True) -> List[str]:
        """
        Find all files matching a glob pattern.
        Optional: throws exception if nothing found.
        """
        fileList: List[str] = []
        for fname in glob.glob(pattern, recursive=True):
            if os.path.isfile(fname):
                fileList.append(fname)
        if len(fileList) == 0 and throws:
            raise ValueError(f"No files could be found with pattern: {pattern}")

        return fileList

    def _findMatchingDirList(self, pattern, throws=True) -> List[str]:
        """
        Find all directories matching a glob pattern.
        Optional: throws exception if nothing found.
        """
        fileList: List[str] = []
        for fname in glob.glob(pattern, recursive=True):
            if os.path.isdir(fname):
                fileList.append(fname)
        if len(fileList) == 0 and throws:
            raise ValueError(f"No directories could be found with pattern: {pattern}")

        return fileList

    def _constructCalibrationStateRoot(self, stateId):
        # TODO: Propagate pathlib through codebase
        return f"{Config['instrument.calibration.powder.home']}/{str(stateId)}/"

    def _constructCalibrationStatePath(self, stateId, useLiteMode):
        # TODO: Propagate pathlib through codebase
        if useLiteMode:
            mode = "lite"
        else:
            mode = "native"
        return f"{self._constructCalibrationStateRoot(stateId)}/{str(mode)}/diffraction/"

    def _constructNormalizationCalibrationStatePath(self, stateId, useLiteMode):
        # TODO: Propagate pathlib through codebase
        if useLiteMode:
            mode = "lite"
        else:
            mode = "native"
        return f"{self._constructCalibrationStateRoot(stateId)}/{str(mode)}/normalization/"

    def readCalibrationIndex(self, runId: str, useLiteMode: bool):
        # Need to run this because of its side effect, TODO: Remove side effect
        stateId, _ = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationStatePath(stateId, useLiteMode)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        calibrationIndex: List[CalibrationIndexEntry] = []
        if os.path.exists(indexPath):
            calibrationIndex = parse_file_as(List[CalibrationIndexEntry], indexPath)
        return calibrationIndex

    def readNormalizationIndex(self, runId: str, useLiteMode: bool):
        # Need to run this because of its side effect, TODO: Remove side effect
        stateId, _ = self._generateStateId(runId)
        normalizationPath: str = self._constructNormalizationCalibrationStatePath(stateId, useLiteMode)
        indexPath: str = normalizationPath + "NormalizationIndex.json"
        normalizationIndex: List[NormalizationIndexEntry] = []
        if os.path.exists(indexPath):
            normalizationIndex = parse_file_as(List[NormalizationIndexEntry], indexPath)
        return normalizationIndex

    def _parseAppliesTo(self, appliesTo: str):
        return CalibrationIndexEntry.parseAppliesTo(appliesTo)

    def _compareRunNumbers(self, runNumber1: str, runNumber2: str, symbol: str):
        expressions = {
            ">=": lambda x, y: x >= y,
            "<=": lambda x, y: x <= y,
            "<": lambda x, y: x < y,
            ">": lambda x, y: x > y,
            "": lambda x, y: x == y,
        }
        return expressions[symbol](int(runNumber1), int(runNumber2))

    def _isApplicableEntry(self, calibrationIndexEntry, runId):
        """
        Checks to see if an entry in the calibration index applies to a given run id via numerical comparison.
        """

        symbol, runNumber = self._parseAppliesTo(calibrationIndexEntry.appliesTo)
        return self._compareRunNumbers(runId, runNumber, symbol)

    def _getVersionFromCalibrationIndex(self, runId: str, useLiteMode: bool) -> str:
        """
        Loads calibration index and inspects all entries to attain latest calibration version that applies to the run id
        """
        # lookup calibration index
        calibrationIndex = self.readCalibrationIndex(runId, useLiteMode)
        # From the index find the latest calibration
        latestCalibration = None
        version = None
        if calibrationIndex:
            # sort by timestamp
            calibrationIndex.sort(key=lambda x: x.timestamp)
            # filter for latest applicable
            relevantEntries = list(filter(lambda x: self._isApplicableEntry(x, runId), calibrationIndex))
            if len(relevantEntries) < 1:
                raise ValueError(f"No applicable calibration index entries found for runId {runId}")
            latestCalibration = relevantEntries[-1]
            version = latestCalibration.version
        return version

    def _getVersionFromNormalizationIndex(self, runId: str, useLiteMode: bool) -> str:
        """
        Loads normalization index and inspects all entries to attain
        latest normalization version that applies to the run id
        """
        # lookup normalization index
        normalizationIndex = self.readNormalizationIndex(runId, useLiteMode)
        # From the index find the latest normalization
        latestNormalization = None
        version = None
        if normalizationIndex:
            # sort by timestamp
            normalizationIndex.sort(key=lambda x: x.timestamp)
            # filter for latest applicable
            relevantEntries = list(filter(lambda x: self._isApplicableEntry(x, runId), normalizationIndex))
            if len(relevantEntries) < 1:
                raise ValueError(f"No applicable calibration index entries found for runId {runId}")
            latestNormalization = relevantEntries[-1]
            version = latestNormalization.version
        return version

    def _constructCalibrationDataPath(self, runId: str, version: str, useLiteMode: bool):
        """
        Generates the path for an instrument state's versioned calibration files.
        """
        stateId, _ = self._generateStateId(runId)
        statePath = self._constructCalibrationStatePath(stateId, useLiteMode)
        calibrationVersionPath: str = statePath + "v_{}/".format(
            wnvf.formatVersion(version=version, use_v_prefix=False)
        )
        return calibrationVersionPath

    def _constructNormalizationCalibrationDataPath(self, runId: str, version: str, useLiteMode: bool):
        """
        Generates the path for an instrument state's versioned normalization calibration files.
        """
        stateId, _ = self._generateStateId(runId)
        statePath = self._constructNormalizationCalibrationStatePath(stateId, useLiteMode)
        normalizationVersionPath: str = statePath + "v_{}/".format(
            wnvf.formatVersion(version=version, use_v_prefix=False)
        )
        return normalizationVersionPath

    def writeCalibrationIndexEntry(self, entry: CalibrationIndexEntry, useLiteMode: bool):
        stateId, _ = self._generateStateId(entry.runNumber)
        calibrationPath: str = self._constructCalibrationStatePath(stateId, useLiteMode)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        # append to index and write to file
        calibrationIndex = self.readCalibrationIndex(entry.runNumber, useLiteMode)
        calibrationIndex.append(entry)
        write_model_list_pretty(calibrationIndex, indexPath)

    def writeNormalizationIndexEntry(self, entry: NormalizationIndexEntry, useLiteMode: bool):
        stateId, _ = self._generateStateId(entry.runNumber)
        normalizationPath: str = self._constructNormalizationCalibrationStatePath(stateId, useLiteMode)
        indexPath: str = normalizationPath + "NormalizationIndex.json"
        # append to index and write to file
        normalizationIndex = self.readNormalizationIndex(entry.runNumber, useLiteMode)
        normalizationIndex.append(entry)
        write_model_list_pretty(normalizationIndex, indexPath)

    def getCalibrationRecordPath(self, runId: str, version: str, useLiteMode: bool):
        recordPath: str = f"{self._constructCalibrationDataPath(runId, version, useLiteMode)}CalibrationRecord.json"
        return recordPath

    def getNormalizationRecordPath(self, runId: str, version: str, useLiteMode: bool):
        recordPath: str = (
            f"{self._constructNormalizationCalibrationDataPath(runId, version, useLiteMode)}NormalizationRecord.json"
        )
        return recordPath

    def _extractFileVersion(self, file: str) -> int:
        return int(file.split("/v_")[-1].split("/")[0])

    def _getFileOfVersion(self, fileRegex: str, version):
        foundFiles = self._findMatchingFileList(fileRegex, throws=False)
        returnFile = None
        for file in foundFiles:
            fileVersion = self._extractFileVersion(file)
            if fileVersion == int(version):
                returnFile = file
                break
        return returnFile

    def _getLatestFile(self, fileRegex: str):
        foundFiles = self._findMatchingFileList(fileRegex, throws=False)
        latestVersion = 0
        latestFile = None
        for file in foundFiles:
            version = self._extractFileVersion(file)
            if version > latestVersion:
                latestVersion = version
                latestFile = file
        return latestFile

    def _getLatestCalibrationVersionNumber(self, stateId: str, useLiteMode: bool) -> int:
        """
        Ignoring the calibration index, get the version number of the latest set of calibration files.
        """
        calibrationStatePath = self._constructCalibrationStatePath(stateId, useLiteMode)
        calibrationVersionPath = f"{calibrationStatePath}v_*/"
        latestVersion = 0
        versionDirs = self._findMatchingDirList(calibrationVersionPath, throws=False)
        for versionDir in versionDirs:
            version = int(versionDir.split("/")[-2].split("_")[-1])
            if version > latestVersion:
                latestVersion = version
        return latestVersion

    def _getLatestNormalizationCalibrationVersionNumber(self, stateId: str, useLiteMode: bool) -> int:
        """
        Ignoring the normalization index, get the version number of the latest set of normalization files.
        """
        normalizationStatePath = self._constructNormalizationCalibrationStatePath(stateId, useLiteMode)
        normalizationVersionPath = f"{normalizationStatePath}v_*/"
        latestVersion = 0
        versionDirs = self._findMatchingDirList(normalizationVersionPath, throws=False)
        for versionDir in versionDirs:
            version = int(versionDir.split("/")[-2].split("_")[-1])
            if version > latestVersion:
                latestVersion = version
        return latestVersion

    def readNormalizationRecord(self, runId: str, version: str = None, useLiteMode: bool = False):
        latestFile = ""
        recordPath: str = self.getNormalizationRecordPath(runId, version if version else "*", useLiteMode)
        if version:
            latestFile = self._getFileOfVersion(recordPath, version)
        else:
            latestFile = self._getLatestFile(recordPath)
        record: NormalizationRecord = None  # noqa: F821
        if latestFile:
            logger.info(f"reading NormalizationRecord from {latestFile}")
            record = parse_file_as(NormalizationRecord, latestFile)  # noqa: F821

        return record

    def writeNormalizationRecord(self, record: NormalizationRecord, version: int = None) -> NormalizationRecord:  # noqa: F821
        """
        Persists a `NormalizationRecord` to either a new version folder, or overwrite a specific version.
        -- side effect: updates version numbers of incoming `NormalizationRecord` and its nested `Normalization`.
        """
        runNumber = record.runNumber
        stateId, _ = self._generateStateId(runNumber)
        previousVersion = self._getLatestNormalizationCalibrationVersionNumber(stateId, record.useLiteMode)
        if not version:
            version = record.version
        if not version:
            version = previousVersion + 1
        recordPath: str = self.getNormalizationRecordPath(runNumber, version, record.useLiteMode)
        record.version = version

        # There seems no need to write the _nested_ Normalization,
        # because it's written to a separate file during 'writeNormalizationState'.
        # However, if it is going to be _nested_, this marks it with the correct version.
        # (For example, use pydantic Field(exclude=True) to _stop_ nesting it.)
        record.calibration.version = version

        normalizationPath = self._constructNormalizationCalibrationDataPath(runNumber, version, record.useLiteMode)
        # check if directory exists for runId
        if not os.path.exists(normalizationPath):
            os.makedirs(normalizationPath)
        # append to record and write to file
        write_model_pretty(record, recordPath)
        logger.info(f"wrote NormalizationRecord: version: {version}")
        return record

    def writeNormalizationWorkspaces(self, record: NormalizationRecord) -> NormalizationRecord:
        """
        Writes the workspaces associated with a `NormalizationRecord` to disk:
        -- assumes that `writeNormalizationRecord` has already been called, and that the version folder exists
        """
        normalizationDataPath = Path(
            self._constructNormalizationCalibrationDataPath(record.runNumber, record.version, record.useLiteMode)
        )
        for workspace in record.workspaceNames:
            filename = workspace + "_" + wnvf.formatVersion(record.version)
            ws = mtd[workspace]
            if ws.isRaggedWorkspace():
                filename = Path(filename + ".tar")
                self.writeRaggedWorkspace(normalizationDataPath, filename, workspace)
            else:
                filename = Path(filename + ".nxs")
                self.writeWorkspace(normalizationDataPath, filename, workspace)
        return record

    def readCalibrationRecord(self, runId: str, version: str = None, useLiteMode: bool = False):
        recordFile: str = None
        if version:
            recordPath: str = self.getCalibrationRecordPath(runId, version, useLiteMode)
            recordFile = self._getFileOfVersion(recordPath, int(version))
        else:
            recordPath: str = self.getCalibrationRecordPath(runId, "*", useLiteMode)
            recordFile = self._getLatestFile(recordPath)
        record: CalibrationRecord = None
        if recordFile:
            logger.info(f"reading CalibrationRecord from {recordFile}")
            record = parse_file_as(CalibrationRecord, recordFile)
        return record

    def writeCalibrationRecord(self, record: CalibrationRecord, version: str = None):
        """
        Persists a `CalibrationRecord` to either a new version folder, or overwrite a specific version.
        -- side effect: updates version numbers of incoming `CalibrationRecord` and its nested `Calibration`.
        """
        runNumber = record.runNumber
        stateId, _ = self._generateStateId(runNumber)
        previousVersion: int = self._getLatestCalibrationVersionNumber(stateId, record.useLiteMode)
        if not version:
            version = record.version
        if not version:
            version = previousVersion + 1
        recordPath: str = self.getCalibrationRecordPath(runNumber, str(version), record.useLiteMode)
        record.version = version

        # As above at 'writeNormalizationRecord':
        # There seems no need to write the _nested_ Calibration,
        # because it's written to a separate file during 'writeCalibrationState'.
        # However, if it is going to be _nested_, this marks it with the correct version.
        # (For example, use pydantic Field(exclude=True) to _stop_ nesting it.)
        record.calibrationFittingIngredients.version = version

        calibrationPath = self._constructCalibrationDataPath(runNumber, str(version), record.useLiteMode)
        # check if directory exists for runId
        if not os.path.exists(calibrationPath):
            os.makedirs(calibrationPath)

        # Update the to-be saved record's "workspaces" information
        #   to correspond to the filenames that will actually be saved to disk.
        savedWorkspaces = {}
        workspaces = record.workspaces.copy()
        wss = []
        for wsName in workspaces.pop(wngt.DIFFCAL_OUTPUT, []):
            # Rebuild the workspace name to strip any "iteration" number:
            #   * WARNING: this workaround does not work correctly if there are multiple workspaces of each "unit" type.
            if wng.Units.TOF.lower() in wsName:
                ws = (
                    wng.diffCalOutput()
                    .unit(wng.Units.TOF)
                    .runNumber(record.runNumber)
                    .version(record.version)
                    .group(record.focusGroupCalibrationMetrics.focusGroupName)
                    .build()
                )
            elif wng.Units.DSP.lower() in wsName:
                ws = (
                    wng.diffCalOutput()
                    .unit(wng.Units.DSP)
                    .runNumber(record.runNumber)
                    .version(record.version)
                    .group(record.focusGroupCalibrationMetrics.focusGroupName)
                    .build()
                )
            else:
                raise RuntimeError(
                    f"cannot save a workspace-type: {wngt.DIFFCAL_OUTPUT} without a units token in its name {wsName}"
                )
            wss.append(ws)
        savedWorkspaces[wngt.DIFFCAL_OUTPUT] = wss
        wss = []
        for wsName in workspaces.pop(wngt.DIFFCAL_TABLE, []):
            # Rebuild the workspace name to strip any "iteration" number:
            #   * WARNING: this workaround does not work correctly if there are multiple table workspaces.
            ws = wng.diffCalTable().runNumber(record.runNumber).version(record.version).build()
            wss.append(ws)
        savedWorkspaces[wngt.DIFFCAL_TABLE] = wss
        wss = []
        for wsName in workspaces.pop(wngt.DIFFCAL_MASK, []):
            # Rebuild the workspace name to strip any "iteration" number:
            #   * WARNING: this workaround does not work correctly if there are multiple mask workspaces.
            ws = wng.diffCalMask().runNumber(record.runNumber).version(record.version).build()
            wss.append(ws)
        savedWorkspaces[wngt.DIFFCAL_MASK] = wss
        savedRecord = deepcopy(record)
        savedRecord.workspaces = savedWorkspaces
        # write record to file
        write_model_pretty(savedRecord, recordPath)

        self.writeCalibrationState(runNumber, record.calibrationFittingIngredients, str(version), record.useLiteMode)

        logger.info(f"Wrote CalibrationRecord: version: {version}")
        return record

    def writeCalibrationWorkspaces(self, record: CalibrationRecord):
        """
        Writes the workspaces associated with a `CalibrationRecord` to disk:
        -- assumes that `writeCalibrationRecord` has already been called, and that the version folder exists
        """
        calibrationDataPath = Path(
            self._constructCalibrationDataPath(record.runNumber, str(record.version), record.useLiteMode)
        )

        # Assumes all workspaces are of WNG-type:
        workspaces = record.workspaces.copy()
        for wsName in workspaces.pop(wngt.DIFFCAL_OUTPUT, []):
            # Rebuild the filename to strip any "iteration" number:
            #   * WARNING: this workaround does not work correctly if there are multiple workspaces of each "unit" type.
            ext = Config["calibration.diffraction.output.extension"]
            if wng.Units.TOF.lower() in wsName:
                filename = Path(
                    wng.diffCalOutput()
                    .unit(wng.Units.TOF)
                    .runNumber(record.runNumber)
                    .version(record.version)
                    .group(record.focusGroupCalibrationMetrics.focusGroupName)
                    .build()
                    + ext
                )
            elif wng.Units.DSP.lower() in wsName:
                filename = Path(
                    wng.diffCalOutput()
                    .unit(wng.Units.DSP)
                    .runNumber(record.runNumber)
                    .version(record.version)
                    .group(record.focusGroupCalibrationMetrics.focusGroupName)
                    .build()
                    + ext
                )
            else:
                raise RuntimeError(
                    f"cannot save a workspace-type: {wngt.DIFFCAL_OUTPUT} without a units token in its name {wsName}"
                )
            self.writeRaggedWorkspace(calibrationDataPath, filename, wsName)
        for tableWSName, maskWSName in zip(
            workspaces.pop(wngt.DIFFCAL_TABLE, []),
            workspaces.pop(wngt.DIFFCAL_MASK, []),
        ):
            # Rebuild the filename to strip any "iteration" number:
            #   * WARNING: this workaround does not work correctly if there are multiple table workspaces.
            diffCalFilename = Path(
                wng.diffCalTable().runNumber(record.runNumber).version(record.version).build() + ".h5"
            )
            self.writeDiffCalWorkspaces(
                calibrationDataPath,
                diffCalFilename,
                tableWorkspaceName=tableWSName,
                maskWorkspaceName=maskWSName,
            )
        if workspaces:
            raise RuntimeError(f"not implemented: unable to save unexpected workspace types: {workspaces}")
        return record

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
                warnings.warn(  # noqa: F821
                    "Can't specify both mass-density and packing fraction for single-element materials"
                )  # noqa: F821
            del sampleJson["material"]["packingFraction"]
            for atom in sampleJson["crystallography"]["atoms"]:
                atom["symbol"] = atom.pop("atom_type")
                atom["coordinates"] = atom.pop("atom_coordinates")
                atom["siteOccupationFactor"] = atom.pop("site_occupation_factor")
            sample = CalibrantSamples.parse_raw(json.dumps(sampleJson))
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

    def _getCurrentCalibrationRecord(self, runId: str, useLiteMode: bool):
        version = self._getVersionFromCalibrationIndex(runId, useLiteMode)
        return self.readCalibrationRecord(runId, version, useLiteMode)

    def _getCurrentNormalizationRecord(self, runId: str, useLiteMode: bool):
        version = self._getVersionFromNormalizationIndex(runId, useLiteMode)
        return self.readNormalizationRecord(runId, version, useLiteMode)

    def _constructCalibrationParametersFilePath(self, runId: str, version: str, useLiteMode: bool):
        statePath: str = f"{self._constructCalibrationDataPath(runId, version, useLiteMode)}CalibrationParameters.json"
        return statePath

    def _constructNormalizationParametersFilePath(self, runId: str, version: str, useLiteMode: bool):
        statePath: str = f"{self._constructNormalizationCalibrationDataPath(runId, version, useLiteMode)}NormalizationParameters.json"  # noqa: E501
        return statePath

    @ExceptionHandler(RecoverableException, "'NoneType' object has no attribute 'instrumentState'")
    def readCalibrationState(self, runId: str, useLiteMode: bool, version: str = None):
        # get stateId and check to see if such a folder exists, if not create it and initialize it
        stateId, _ = self._generateStateId(runId)
        calibrationStatePath = self._constructCalibrationParametersFilePath(runId, "*", useLiteMode)

        latestFile = ""
        if version is not None:
            latestFile = self._getFileOfVersion(calibrationStatePath, version)
        else:
            # TODO: This should refer to the calibration index
            latestFile = self._getLatestFile(calibrationStatePath)

        calibrationState = None
        if latestFile:
            calibrationState = parse_file_as(Calibration, latestFile)

        if calibrationState is None:
            raise ValueError("calibrationState is None")

        return calibrationState

    def readNormalizationState(self, runId: str, version: str = None, useLiteMode: bool = False):
        stateId, _ = self._generateStateId(runId)
        normalizationStatePathGlob = self._constructNormalizationParametersFilePath(runId, "*", useLiteMode)

        latestFile = ""
        if version:
            latestFile = self._getFileOfVersion(normalizationStatePathGlob, version)
        else:
            # TODO: This should refer to the calibration index
            latestFile = self._getLatestFile(normalizationStatePathGlob)

        normalizationState = None
        if latestFile:
            normalizationState = parse_file_as(Normalization, latestFile)  # noqa: F821

        return normalizationState

    def writeCalibrationState(
        self, runId: str, calibration: Calibration, version: str = None, useLiteMode: bool = False
    ):
        """
        Writes a `Calibration` to either a new version folder, or overwrites a specific version.
        -- side effect: updates version number of incoming `Calibration`.
        """
        stateId, _ = self._generateStateId(runId)
        previousVersion: int = self._getLatestCalibrationVersionNumber(stateId, useLiteMode)
        if not version:
            version = previousVersion + 1

        # Check for the existence of a calibration parameters file
        calibrationParametersFilePath = self._constructCalibrationParametersFilePath(runId, str(version), useLiteMode)
        if os.path.exists(calibrationParametersFilePath):
            logger.warning(f"overwriting calibration parameters at {calibrationParametersFilePath}")

        calibration.version = int(version)
        calibrationDataPath = self._constructCalibrationDataPath(runId, str(version), useLiteMode)
        if not os.path.exists(calibrationDataPath):
            os.makedirs(calibrationDataPath)
        # write the calibration state.
        write_model_pretty(calibration, calibrationParametersFilePath)

    def writeNormalizationState(
        self, runId: str, normalization: Normalization, version: str = None, useLiteMode: bool = False
    ):  # noqa: F821
        """
        Writes a `Normalization` to either a new version folder, or overwrites a specific version.
        -- side effect: updates version number of incoming `Normalization`.
        """
        stateId, _ = self._generateStateId(runId)
        previousVersion: int = self._getLatestNormalizationCalibrationVersionNumber(stateId, useLiteMode)
        if not version:
            version = previousVersion + 1
        # check for the existence of a normalization parameters file
        normalizationParametersFilePath = self._constructNormalizationParametersFilePath(
            runId, str(version), useLiteMode
        )
        if os.path.exists(normalizationParametersFilePath):
            logger.warning(f"overwriting normalization parameters at {normalizationParametersFilePath}")
        normalization.version = version
        normalizationDataPath = self._constructNormalizationCalibrationDataPath(runId, str(version), useLiteMode)
        if not os.path.exists(normalizationDataPath):
            os.makedirs(normalizationDataPath)
        write_model_pretty(normalization, normalizationParametersFilePath)

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

    def _writeDefaultDiffCalTable(self, runNumber: str, useLiteMode: bool):
        from snapred.backend.data.GroceryService import GroceryService

        version = 1
        grocer = GroceryService()
        filename = Path(grocer._createDiffcalTableWorkspaceName("default", version) + ".h5")
        outWS = grocer.fetchDefaultDiffCalTable(runNumber, useLiteMode, version)

        calibrationDataPath = self._constructCalibrationDataPath(runNumber, str(version), useLiteMode)

        self.writeDiffCalWorkspaces(calibrationDataPath, filename, outWS)
        calibrationIndexEntry = CalibrationIndexEntry(
            runNumber=runNumber,
            version=version,
            comments="Inferred from the instrument geometry",
            author="Generated automatically by SNAPRed",
        )
        self.writeCalibrationIndexEntry(calibrationIndexEntry, useLiteMode)

    @ExceptionHandler(StateValidationException)
    def initializeState(self, runId: str, name: str = None, useLiteMode: bool = False):
        stateId, _ = self._generateStateId(runId)

        # Read the detector state from the pv data file
        detectorState = self.readDetectorState(runId)

        # then read data from the common calibration state parameters stored at root of calibration directory
        instrumentConfig = self.readInstrumentConfig()
        # then pull static values specified by Malcolm from resources
        defaultGroupSliceValue = Config["calibration.parameters.default.groupSliceValue"]
        fwhmMultipliers = Pair.parse_obj(Config["calibration.parameters.default.FWHMMultiplier"])
        peakTailCoefficient = Config["calibration.parameters.default.peakTailCoefficient"]
        gsasParameters = GSASParameters(
            alpha=Config["calibration.parameters.default.alpha"], beta=Config["calibration.parameters.default.beta"]
        )
        # then calculate the derived values
        lambdaLimit = Limit(
            minimum=detectorState.wav - (instrumentConfig.bandwidth / 2),
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

        # finally add seedRun, creation date, and a human readable name
        calibration = Calibration(
            instrumentState=instrumentState,
            name=name,
            seedRun=runId,
            creationDate=datetime.datetime.now(),
            version=0,
        )

        # Make sure that the state root directory has been initialized:
        stateRootPath = self._constructCalibrationStateRoot(stateId)
        if not os.path.exists(stateRootPath):
            # WARNING: `_prepareStateRoot` is also called at `readStateConfig`; this allows
            #   some order independence of initialization if the back-end is run separately (e.g. in unit tests).
            self._prepareStateRoot(stateId)

        self.writeCalibrationState(runId, calibration, useLiteMode=useLiteMode)
        self._writeDefaultDiffCalTable(runId, useLiteMode)

        return calibration

    def _prepareStateRoot(self, stateId: str):
        """
        Create the state root directory, and populate it with any necessary metadata files.
        """
        stateRootPath = self._constructCalibrationStateRoot(stateId)
        if not os.path.exists(stateRootPath):
            os.makedirs(stateRootPath)

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

        # first make sure the run number has a valid IPTS
        try:
            self.getIPTS(runId)
        # if no IPTS found, return false
        except RuntimeError:
            return False
        # if found, try to construct the path and test if the path exists
        else:
            stateID, _ = self._generateStateId(runId)
            calibrationStatePath: str = self._constructCalibrationStateRoot(stateID)
            if os.path.exists(calibrationStatePath):
                return True
            else:
                return False

    def readSamplePaths(self):
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

    def _readGroupingMap(self, stateId: str) -> GroupingMap:
        path = self._groupingMapPath(stateId)
        if not path.exists():
            raise FileNotFoundError(f'required grouping-schema map for state "{stateId}" at "{path}" does not exist')
        return parse_file_as(GroupingMap, path)

    def readGroupingMap(self, runNumber: str):
        # if the state exists then lookup its grouping map
        if self.checkCalibrationFileExists(runNumber):
            stateId, _ = self._generateStateId(runNumber)
            return self._readGroupingMap(stateId)
        # otherwise return the default map
        else:
            return self._readDefaultGroupingMap()

    def _readDefaultGroupingMap(self) -> GroupingMap:
        path = self._defaultGroupingMapPath()
        if not path.exists():
            raise FileNotFoundError(f'required default grouping-schema map "{path}" does not exist')
        return parse_file_as(GroupingMap, path)

    def _writeGroupingMap(self, stateId: str, groupingMap: GroupingMap):
        # Write a GroupingMap to a file in JSON format, but only if it has been modified.
        groupingMapPath = self._groupingMapPath(stateId)
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
        return Path(self._constructCalibrationStateRoot(stateId)) / "groupingMap.json"

    ## WRITING WORKSPACES TO DISK

    def writeWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        if filename.suffix != ".nxs":
            raise RuntimeError(f"[writeWorkspace]: specify filename including '.nxs' extension, not {filename}")
        self.mantidSnapper.SaveNexus(
            "Save a workspace with Nexus",
            InputWorkspace=workspaceName,
            Filename=str(path / filename),
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
