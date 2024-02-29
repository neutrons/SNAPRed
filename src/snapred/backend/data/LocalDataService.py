import datetime
import glob
import json
import os
from errno import ENOENT as NOT_FOUND
from pathlib import Path
from typing import Any, Dict, List, Tuple

import h5py
from mantid.api import AlgorithmManager, ITableWorkspace
from mantid.dataobjects import MaskWorkspace
from mantid.kernel import PhysicalConstants
from mantid.simpleapi import GetIPTS, mtd
from pydantic import parse_file_as

from snapred.backend.dao import (
    GSASParameters,
    InstrumentConfig,
    Limit,
    ObjectSHA,
    ParticleBounds,
    RunConfig,
    StateConfig,
    StateId,
)
from snapred.backend.dao.calibration import Calibration, CalibrationIndexEntry, CalibrationRecord
from snapred.backend.dao.normalization import Normalization, NormalizationIndexEntry, NormalizationRecord
from snapred.backend.dao.state import (
    DetectorState,
    DiffractionCalibrant,
    FocusGroup,
    GroupingMap,
    InstrumentState,
    NormalizationCalibrant,
)
from snapred.backend.dao.state.CalibrantSample import CalibrantSamples
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.SaveGroupingDefinition import SaveGroupingDefinition
from snapred.meta.Config import Config, Resource
from snapred.meta.decorators.ExceptionHandler import ExceptionHandler
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as WNG
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
    iptsCache: Dict[str, Any] = {}
    stateIdCache: Dict[str, ObjectSHA] = {}
    instrumentConfig: "InstrumentConfig"
    verifyPaths: bool = True

    # conversion factor from microsecond/Angstrom to meters
    CONVERSION_FACTOR = Config["constants.m2cm"] * PhysicalConstants.h / PhysicalConstants.NeutronMass

    def __init__(self) -> None:
        self.verifyPaths = Config["localdataservice.config.verifypaths"]
        self.instrumentConfig = self.readInstrumentConfig()

    def _determineInstrConfigPaths(self) -> None:
        """This method locates the instrument configuration path and
        sets the instance variable ``instrumentConfigPath``."""
        # verify parent directory exists
        self.dataPath = Path(Config["instrument.home"])
        if self.verifyPaths and not self.dataPath.exists():
            raise _createFileNotFoundError("Config['instrument.home']", self.dataPath)

        # look for the config file and verify it exists
        self.instrumentConfigPath = Config["instrument.config"]

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

    def readStateConfig(self, runId: str) -> StateConfig:
        previousDiffCalRecord: CalibrationRecord = self.readCalibrationRecord(runId)
        if previousDiffCalRecord is None:
            diffCalibration: Calibration = self.readCalibrationState(runId)
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
        fileList: List[str] = []
        for fname in glob.glob(pattern, recursive=True):
            if os.path.isfile(fname):
                fileList.append(fname)
        if len(fileList) == 0 and throws:
            raise ValueError(f"No files could be found with pattern: {pattern}")

        return fileList

    def _findMatchingDirList(self, pattern, throws=True) -> List[str]:
        """
        Similar to the above method `_findMatchingFileList` except for directories!
        Throw if nothing found.(Or dont!)
        """
        fileList: List[str] = []
        for fname in glob.glob(pattern, recursive=True):
            if os.path.isdir(fname):
                fileList.append(fname)
        if len(fileList) == 0 and throws:
            raise ValueError(f"No directories could be found with pattern: {pattern}")

        return fileList

    def _constructCalibrationStateRootPath(self, stateId):
        # TODO: Propagate pathlib through codebase
        return f"{self.instrumentConfig.calibrationDirectory}/Powder/{stateId}/"

    def _constructNormalizationCalibrationStatePath(self, stateId):
        # TODO: Propagate pathlib through codebase
        # return f"{self.instrumentConfig.calibrationDirectory / 'Powder' / stateId / 'normalization'}/"
        return f"{self.instrumentConfig.calibrationDirectory}/Powder/{stateId}/normalization/"

    def readCalibrationIndex(self, runId: str):
        # Need to run this because of its side effect, TODO: Remove side effect
        stateId, _ = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationStateRootPath(stateId)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        calibrationIndex: List[CalibrationIndexEntry] = []
        if os.path.exists(indexPath):
            calibrationIndex = parse_file_as(List[CalibrationIndexEntry], indexPath)
        return calibrationIndex

    def readNormalizationIndex(self, runId: str):
        # Need to run this because of its side effect, TODO: Remove side effect
        stateId, _ = self._generateStateId(runId)
        normalizationPath: str = self._constructNormalizationCalibrationStatePath(stateId)
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

    def _getVersionFromCalibrationIndex(self, runId: str):
        """
        Loads calibration index and inspects all entries to attain latest calibration version that applies to the run id
        """
        # lookup calibration index
        calibrationIndex = self.readCalibrationIndex(runId)
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

    def _getVersionFromNormalizationIndex(self, runId: str):
        """
        Loads normalization index and inspects all entries to attain
        latest normalization version that applies to the run id
        """
        # lookup normalization index
        normalizationIndex = self.readNormalizationIndex(runId)
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

    def _constructCalibrationDataPath(self, runId: str, version: str):
        """
        Generates the path for an instrument state's versioned calibration files.
        """
        stateId, _ = self._generateStateId(runId)
        statePath = self._constructCalibrationStateRootPath(stateId)
        cablibrationVersionPath: str = statePath + "v_{}/".format(version)
        return cablibrationVersionPath

    def _constructNormalizationCalibrationDataPath(self, runId: str, version: str):
        """
        Generates the path for an instrument state's versioned normalization calibration files.
        """
        stateId, _ = self._generateStateId(runId)
        statePath = self._constructNormalizationCalibrationStatePath(stateId)
        cablibrationVersionPath: str = statePath + "v_{}/".format(version)
        return cablibrationVersionPath

    def _getCalibrationDataPath(self, runId: str):
        """
        Given a run id, get the latest and greatest calibration file set's path for said run.
        """
        version = self._getVersionFromCalibrationIndex(runId)
        if version is None:
            raise ValueError(f"No calibration data found for runId {runId}")
        return self._constructCalibrationDataPath(runId, version)

    def writeCalibrationIndexEntry(self, entry: CalibrationIndexEntry):
        stateId, _ = self._generateStateId(entry.runNumber)
        calibrationPath: str = self._constructCalibrationStateRootPath(stateId)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        # append to index and write to file
        calibrationIndex = self.readCalibrationIndex(entry.runNumber)
        calibrationIndex.append(entry)
        write_model_list_pretty(calibrationIndex, indexPath)

    def writeNormalizationIndexEntry(self, entry: NormalizationIndexEntry):
        stateId, _ = self._generateStateId(entry.runNumber)
        normalizationPath: str = self._constructNormalizationCalibrationStatePath(stateId)
        indexPath: str = normalizationPath + "NormalizationIndex.json"
        # append to index and write to file
        normalizationIndex = self.readNormalizationIndex(entry.runNumber)
        normalizationIndex.append(entry)
        write_model_list_pretty(normalizationIndex, indexPath)

    def getCalibrationRecordPath(self, runId: str, version: str):
        recordPath: str = f"{self._constructCalibrationDataPath(runId, version)}CalibrationRecord.json"
        return recordPath

    def getNormalizationRecordPath(self, runId: str, version: str):
        recordPath: str = f"{self._constructNormalizationCalibrationDataPath(runId, version)}NormalizationRecord.json"
        return recordPath

    def _extractFileVersion(self, file: str):
        return int(file.split("/v_")[-1].split("/")[0])

    def _getFileOfVersion(self, fileRegex: str, version):
        foundFiles = self._findMatchingFileList(fileRegex, throws=False)
        returnFile = None
        for file in foundFiles:
            fileVersion = self._extractFileVersion(file)
            if fileVersion == version:
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

    def _getLatestCalibrationVersion(self, stateId: str):
        """
        Ignoring the calibration index, whats the last set of calibration files to be generated.
        """
        calibrationStatePath = self._constructCalibrationStateRootPath(stateId)
        calibrationVersionPath = f"{calibrationStatePath}v_*/"
        latestVersion = 0
        versionDirs = self._findMatchingDirList(calibrationVersionPath, throws=False)
        for versionDir in versionDirs:
            version = int(versionDir.split("/")[-2].split("_")[-1])
            if version > latestVersion:
                latestVersion = version
        return latestVersion

    def _getLatestNormalizationCalibrationVersion(self, stateId: str):
        """
        Ignoring the calibration index, whats the last set of calibration files to be generated.
        """
        calibrationStatePath = self._constructNormalizationCalibrationStatePath(stateId)
        calibrationVersionPath = f"{calibrationStatePath}v_*/"
        latestVersion = 0
        versionDirs = self._findMatchingDirList(calibrationVersionPath, throws=False)
        for versionDir in versionDirs:
            version = int(versionDir.split("/")[-2].split("_")[-1])
            if version > latestVersion:
                latestVersion = version
        return latestVersion

    def readNormalizationRecord(self, runId: str, version: str = None):
        recordPath: str = self.getNormalizationRecordPath(runId, "*")
        latestFile = ""
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
        previousVersion = self._getLatestNormalizationCalibrationVersion(stateId)
        if not version:
            version = previousVersion + 1
        recordPath: str = self.getNormalizationRecordPath(runNumber, version)
        record.version = version

        # There seems no need to write the _nested_ Normalization,
        # because it's written to a separate file during 'writeNormalizationState'.
        # However, if it is going to be _nested_, this marks it with the correct version.
        # (For example, use pydantic Field(exclude=True) to _stop_ nesting it.)
        record.calibration.version = version

        normalizationPath = self._constructNormalizationCalibrationDataPath(runNumber, version)
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
        normalizationDataPath = Path(self._constructNormalizationCalibrationDataPath(record.runNumber, record.version))
        for workspace in record.workspaceNames:
            filename = Path(workspace + ".nxs")
            self.writeWorkspace(normalizationDataPath, filename, workspace)
        return record

    def readCalibrationRecord(self, runId: str, version: str = None):
        recordFile: str = None
        if version:
            recordPath: str = self.getCalibrationRecordPath(runId, version)
            recordFile = self._getFileOfVersion(recordPath, int(version))
        else:
            recordPath: str = self.getCalibrationRecordPath(runId, "*")
            recordFile = self._getLatestFile(recordPath)
        record: CalibrationRecord = None
        if recordFile:
            logger.info(f"reading CalibrationRecord from {recordFile}")
            record = parse_file_as(CalibrationRecord, recordFile)
        return record

    def writeCalibrationRecord(self, record: CalibrationRecord, version: int = None):
        """
        Persists a `CalibrationRecord` to either a new version folder, or overwrite a specific version.
        -- side effect: updates version numbers of incoming `CalibrationRecord` and its nested `Calibration`.
        """
        runNumber = record.runNumber
        stateId, _ = self._generateStateId(runNumber)
        previousVersion = self._getLatestCalibrationVersion(stateId)
        if not version:
            version = previousVersion + 1
        recordPath: str = self.getCalibrationRecordPath(runNumber, version)
        record.version = version

        # As above at 'writeNormalizationRecord':
        # There seems no need to write the _nested_ Calibration,
        # because it's written to a separate file during 'writeCalibrationState'.
        # However, if it is going to be _nested_, this marks it with the correct version.
        # (For example, use pydantic Field(exclude=True) to _stop_ nesting it.)
        record.calibrationFittingIngredients.version = version

        calibrationPath = self._constructCalibrationDataPath(runNumber, version)
        # check if directory exists for runId
        if not os.path.exists(calibrationPath):
            os.makedirs(calibrationPath)
        # append to record and write to file
        write_model_pretty(record, recordPath)

        self.writeCalibrationState(runNumber, record.calibrationFittingIngredients, version)
        logger.info(f"Wrote CalibrationRecord: version: {version}")
        return record

    def writeCalibrationWorkspaces(self, record: CalibrationRecord):
        """
        Writes the workspaces associated with a `CalibrationRecord` to disk:
        -- assumes that `writeCalibrationRecord` has already been called, and that the version folder exists
        """
        calibrationDataPath = Path(self._constructCalibrationDataPath(record.runNumber, record.version))
        calibrationTable = None
        maskWorkspace = None
        for workspace in record.workspaceNames:
            if self.workspaceIsInstance(workspace, ITableWorkspace):
                calibrationTable = workspace
                continue
            if self.workspaceIsInstance(workspace, MaskWorkspace):
                maskWorkspace = workspace
                continue
            workspaceFilename = Path(workspace + ".nxs")
            self.writeWorkspace(calibrationDataPath, workspaceFilename, workspace)
        if not calibrationTable:
            logger.warning("the diffraction-calibration table is missing from the workspace list")
        if not maskWorkspace:
            logger.warning("the diffraction-calibration mask is missing from the workspace list")
        if calibrationTable or maskWorkspace:
            diffCalFilename = Path(
                WNG.diffCalTable().runNumber(record.runNumber).version(record.version).build() + ".h5"
            )
            self.writeDiffCalWorkspaces(
                calibrationDataPath,
                diffCalFilename,
                tableWorkspaceName=calibrationTable,
                maskWorkspaceName=maskWorkspace,
            )
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
        return calibrantSampleDict["crystallography"]["cifFile"]

    def _getCurrentCalibrationRecord(self, runId: str):
        version = self._getVersionFromCalibrationIndex(runId)
        return self.readCalibrationRecord(runId, version)

    def _getCurrentNormalizationRecord(self, runId: str):
        version = self._getVersionFromNormalizationIndex(runId)
        return self.readNormalizationRecord(runId, version)

    def _constructCalibrationParametersFilePath(self, runId: str, version: str):
        statePath: str = f"{self._constructCalibrationDataPath(runId, version)}CalibrationParameters.json"
        return statePath

    def getNormalizationStatePath(self, runId: str, version: str):
        # TODO make its own path?
        statePath: str = (
            f"{self._constructNormalizationCalibrationDataPath(runId, version)}NormalizationParameters.json"
        )
        return statePath

    def readCalibrationState(self, runId: str, version: str = None):
        # get stateId and check to see if such a folder exists, if not create it and initialize it
        stateId, _ = self._generateStateId(runId)
        calibrationStatePath = self._constructCalibrationParametersFilePath(runId, "*")

        latestFile = ""
        if version:
            latestFile = self._getFileOfVersion(calibrationStatePath, version)
        else:
            # TODO: This should refer to the calibration index
            latestFile = self._getLatestFile(calibrationStatePath)

        calibrationState = None
        if latestFile:
            calibrationState = parse_file_as(Calibration, latestFile)

        return calibrationState

    def readNormalizationState(self, runId: str, version: str = None):
        stateId, _ = self._generateStateId(runId)
        normalizationStatePath = self.getNormalizationStatePath(runId, "*")

        latestFile = ""
        if version:
            latestFile = self._getFileOfVersion(normalizationStatePath, version)
        else:
            # TODO: This should refer to the calibration index
            latestFile = self._getLatestFile(normalizationStatePath)

        normalizationState = None
        if latestFile:
            normalizationState = parse_file_as(Normalization, latestFile)  # noqa: F821

        return normalizationState

    def writeCalibrationState(self, runId: str, calibration: Calibration, version: int = None):
        """
        Writes a `Calibration` to either a new version folder, or overwrites a specific version.
        -- side effect: updates version number of incoming `Calibration`.
        """
        stateId, _ = self._generateStateId(runId)
        previousVersion = self._getLatestCalibrationVersion(stateId)
        if not version:
            version = previousVersion + 1

        # Check for the existence of a calibration parameters file
        calibrationParametersFilePath = self._constructCalibrationParametersFilePath(runId, version)
        if os.path.exists(calibrationParametersFilePath):
            logger.warning(f"overwriting calibration parameters at {calibrationParametersFilePath}")

        calibration.version = version
        calibrationDataPath = self._constructCalibrationDataPath(runId, version)
        if not os.path.exists(calibrationDataPath):
            os.makedirs(calibrationDataPath)
        # write the calibration state.
        write_model_pretty(calibration, calibrationParametersFilePath)

    def writeNormalizationState(self, runId: str, normalization: Normalization, version: int = None):  # noqa: F821
        """
        Writes a `Normalization` to either a new version folder, or overwrites a specific version.
        -- side effect: updates version number of incoming `Normalization`.
        """
        stateId, _ = self._generateStateId(runId)
        normalizationPath: str = self._constructNormalizationCalibrationStatePath(stateId)
        previousVersion = self._getLatestCalibrationVersion(stateId)
        if not version:
            version = previousVersion + 1
        # check for the existenece of a calibration parameters file
        normalizationParametersPath = self.getNormalizationStatePath(runId, version)
        normalization.version = version
        normalizationPath = self._constructNormalizationCalibrationDataPath(runId, version)
        if not os.path.exists(normalizationPath):
            os.makedirs(normalizationPath)
        write_model_pretty(normalization, normalizationParametersPath)

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

    @ExceptionHandler(StateValidationException)
    def initializeState(self, runId: str, name: str = None):
        stateId, _ = self._generateStateId(runId)

        # Read the detector state from the pv data file
        detectorState = self.readDetectorState(runId)

        # then read data from the common calibration state parameters stored at root of calibration directory
        instrumentConfig = self.readInstrumentConfig()
        # then pull static values specified by Malcolm from resources
        defaultGroupSliceValue = Config["calibration.parameters.default.groupSliceValue"]
        fwhmMultiplier = Limit(
            minimum=Config["calibration.parameters.default.FWHMMultiplier"][0],
            maximum=Config["calibration.parameters.default.FWHMMultiplier"][1],
        )
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
            fwhmMultiplierLimit=fwhmMultiplier,
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
        stateRootPath = self._constructCalibrationStateRootPath(stateId)
        if not os.path.exists(stateRootPath):
            # WARNING: `_prepareStateRoot` is also called at `readStateConfig`; this allows
            #   some order independence of initialization if the back-end is run separately (e.g. in unit tests).
            self._prepareStateRoot(stateId)

        self.writeCalibrationState(runId, calibration)

        return calibration

    def _prepareStateRoot(self, stateId: str):
        """
        Create the state root directory, and populate it with any necessary metadata files.
        """
        stateRootPath = self._constructCalibrationStateRootPath(stateId)
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
        stateID, _ = self._generateStateId(runId)
        calibrationStatePath: str = self._constructCalibrationStateRootPath(stateID)

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
            write_model_pretty(groupingMap, groupingMapPath)
            groupingMap.setDirty(False)

    def _defaultGroupingMapPath(self) -> Path:
        return GroupingMap.calibrationGroupingHome() / "defaultGroupingMap.json"

    def _groupingMapPath(self, stateId) -> Path:
        return Path(self._constructCalibrationStateRootPath(stateId)) / "groupingMap.json"

    def readGroupingFiles(self):
        groupingFolder = Config["instrument.calibration.powder.grouping.home"]
        extensions = Config["instrument.calibration.powder.grouping.extensions"]
        # collect list of all files in folder that are applicable extensions
        groupingFiles = []
        for extension in extensions:
            groupingFiles.extend(self._findMatchingFileList(f"{groupingFolder}/*.{extension}", throws=False))
        if len(groupingFiles) < 1:
            raise RuntimeError(f"No grouping files found in {groupingFolder} for extensions {extensions}")
        groupingFiles.sort()
        return groupingFiles

    def readFocusGroups(self):
        groupingFiles = self.readGroupingFiles()
        focusGroups = {}
        for file in groupingFiles:
            focusGroups[file] = FocusGroup(
                name=self.groupingSchemaFromPath(file),
                definition=file,
            )
        return focusGroups

    def groupingSchemaFromPath(self, path: str) -> str:
        return path.split("/")[-1].split("_")[-1].split(".")[0]

    ## WRITING WORKSPACES TO DISK

    def writeWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        if filename.suffix != ".nxs":
            raise RuntimeError(f"[writeWorkspace]: specify filename including '.nxs' extension, not {filename}")
        saveAlgo = AlgorithmManager.create("SaveNexus")
        saveAlgo.setProperty("InputWorkspace", workspaceName)
        saveAlgo.setProperty("Filename", str(path / filename))
        saveAlgo.execute()

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
        saveAlgo = AlgorithmManager.create("SaveDiffCal")
        saveAlgo.setPropertyValue("CalibrationWorkspace", tableWorkspaceName)
        saveAlgo.setPropertyValue("MaskWorkspace", maskWorkspaceName)
        saveAlgo.setPropertyValue("GroupingWorkspace", groupingWorkspaceName)
        saveAlgo.setPropertyValue("Filename", str(path / filename))
        saveAlgo.execute()
