import datetime
import glob
import json
import os
from errno import ENOENT as NOT_FOUND
from pathlib import Path
from typing import Any, Dict, List, Tuple

import h5py
from mantid.kernel import PhysicalConstants
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
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.meta.Config import Config, Resource
from snapred.meta.decorators.ExceptionHandler import ExceptionHandler
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.redantic import (
    write_model_list_pretty,
    write_model_pretty,
)

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
    instrumentConfig: "InstrumentConfig"  # Optional[InstrumentConfig]
    verifyPaths: bool = True
    groceryService: GroceryService = GroceryService()
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
        reductionParameters = self._readReductionParameters(runId)
        previousDiffCalRecord: CalibrationRecord = self.readCalibrationRecord(runId)
        if previousDiffCalRecord is None:
            diffCalibration: Calibration = self.readCalibrationState(runId)
        else:
            diffCalibration: Calibration = previousDiffCalRecord.calibrationFittingIngredients

        return StateConfig(
            calibration=diffCalibration,
            focusGroups=self._readFocusGroups(runId),
            rawVanadiumCorrectionFileName=reductionParameters["rawVCorrFileName"],
            vanadiumFilePath=str(
                self.instrumentConfig.calibrationDirectory
                / "Powder"
                / diffCalibration.instrumentState.id.hex
                / reductionParameters["rawVCorrFileName"]
            ),
            stateId=diffCalibration.instrumentState.id,
        )  # TODO: fill with real value

    def _readFocusGroups(self, runId: str) -> List[FocusGroup]:  # noqa: ARG002
        reductionParameters = self._readReductionParameters(runId)
        # TODO: fix hardcode reductionParameters["focGroupLst"]
        # dont have time to figure out why its reading the wrong data
        focusGroupNames = ["Column", "Bank", "All"]
        focusGroups = []
        for i, name in enumerate(focusGroupNames):
            focusGroups.append(
                FocusGroup(
                    name=name,
                    definition=str(
                        self.instrumentConfig.calibrationDirectory
                        / "Powder"
                        / self.instrumentConfig.pixelGroupingDirectory
                        / reductionParameters["focGroupDefinition"][i]
                    ),
                )
            )
        return focusGroups

    def readRunConfig(self, runId: str) -> RunConfig:
        return self._readRunConfig(runId)

    def _readRunConfig(self, runId: str) -> RunConfig:
        # lookup IPST number
        iptsPath = self.groceryService.getIPTS(runId)

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
            raise FileNotFoundError("File {} does not exist".format(fName))
        return f

    @ExceptionHandler(StateValidationException)
    def _generateStateId(self, runId: str) -> Tuple[str, str]:
        if runId in self.stateIdCache:
            SHA = self.stateIdCache[runId]
            return SHA.hex, SHA.decodedKey

        f = self._readPVFile(runId)

        try:
            det_arc1 = f.get("entry/DASlogs/det_arc1/value")[0]
            det_arc2 = f.get("entry/DASlogs/det_arc2/value")[0]
            wav = f.get("entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value")[0]
            freq = f.get("entry/DASlogs/BL3:Det:TH:BL:Frequency/value")[0]
            GuideIn = f.get("entry/DASlogs/BL3:Mot:OpticsPos:Pos/value")[0]
        except:  # noqa: E722
            raise ValueError("Could not find all required logs in file {}".format(self._constructPVFilePath(runId)))

        stateID = StateId(
            vdet_arc1=det_arc1,
            vdet_arc2=det_arc2,
            WavelengthUserReq=wav,
            Frequency=freq,
            Pos=GuideIn,
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

    def _constructCalibrationStatePath(self, stateId):
        # TODO: Propagate pathlib through codebase
        return f"{self.instrumentConfig.calibrationDirectory / 'Powder' / stateId}/"

    def _readReductionParameters(self, runId: str) -> Dict[Any, Any]:
        # lookup IPTS number
        run: int = int(runId)
        stateId, _ = self._generateStateId(runId)

        calibrationPath: str = self._constructCalibrationStatePath(stateId)
        calibSearchPattern: str = f"{calibrationPath}{self.instrumentConfig.calibrationFilePrefix}*{self.instrumentConfig.calibrationFileExtension}"  # noqa: E501

        foundFiles = self._findMatchingFileList(calibSearchPattern)

        calibFileList = []

        # TODO: Allow non lite files
        for file in foundFiles:
            if "lite" in file:
                calibFileList.append(file)

        calibRunList = []
        # TODO: Why are we overwriting dictIn every iteration?
        for string in calibFileList:
            runStr = string[
                string.find(self.instrumentConfig.calibrationFilePrefix)
                + len(self.instrumentConfig.calibrationFilePrefix) :
            ].split(".")[0]
            if not runStr.isnumeric():
                continue
            calibRunList.append(int(runStr))

            relRuns = [x - run != 0 for x in calibRunList]

            pos = [i for i, val in enumerate(relRuns) if val >= 0]
            [i for i, val in enumerate(relRuns) if val <= 0]

            # TODO: Account for errors
            closestAfter = min([calibRunList[i] for i in pos])
            calIndx = calibRunList.index(closestAfter)

            with open(calibFileList[calIndx], "r") as json_file:
                dictIn = json.load(json_file)

            # useful to also path location of calibration directory
            fullCalPath = calibFileList[calIndx]
            fSlash = [pos for pos, char in enumerate(fullCalPath) if char == "/"]
            dictIn["calPath"] = fullCalPath[0 : fSlash[-1] + 1]

        # Now push data into DAO object
        self.reductionParameterCache[runId] = dictIn
        dictIn["stateId"] = stateId
        return dictIn

    def readCalibrationIndex(self, runId: str):
        # Need to run this because of its side effect, TODO: Remove side effect
        stateId, _ = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationStatePath(stateId)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        calibrationIndex: List[CalibrationIndexEntry] = []
        if os.path.exists(indexPath):
            calibrationIndex = parse_file_as(List[CalibrationIndexEntry], indexPath)
        return calibrationIndex

    def readNormalizationIndex(self, runId: str):
        # Need to run this because of its side effect, TODO: Remove side effect
        stateId, _ = self._generateStateId(runId)
        normalizationPath: str = self._constructCalibrationStatePath(stateId)
        indexPath: str = normalizationPath + "NormalizationIndex.json"
        normalizationIndex: List[NormalizationIndexEntry] = []
        if os.path.exists(indexPath):
            normalizationIndex = parse_file_as(List[NormalizationIndexEntry], indexPath)
        return normalizationIndex

    def _isApplicableEntry(self, calibrationIndexEntry, runId):
        """
        Checks to see if an entry in the calibration index applies to a given run id via numerical comparison.
        """
        if calibrationIndexEntry.appliesTo == runId:
            return True
        if calibrationIndexEntry.appliesTo.startswith(">"):
            # get latest entry that applies to a runId greater than this runId
            if int(runId) > int(calibrationIndexEntry.appliesTo[1:]):
                return True
        if calibrationIndexEntry.appliesTo.startswith("<"):
            # get latest entry that applies to a runId less than this runId
            if int(runId) < int(calibrationIndexEntry.appliesTo[1:]):
                return True
        return False

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
        statePath = self._constructCalibrationStatePath(stateId)
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
        calibrationPath: str = self._constructCalibrationStatePath(stateId)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        # append to index and write to file
        calibrationIndex = self.readCalibrationIndex(entry.runNumber)
        calibrationIndex.append(entry)
        write_model_list_pretty(calibrationIndex, indexPath)

    def writeNormalizationIndexEntry(self, entry: NormalizationIndexEntry):
        stateId, _ = self._generateStateId(entry.runNumber)
        normalizationPath: str = self._constructCalibrationStatePath(stateId)
        indexPath: str = normalizationPath + "NormalizationIndex.json"
        # append to index and write to file
        normalizationIndex = self.readNormalizationIndex(entry.runNumber)
        normalizationIndex.append(entry)
        write_model_list_pretty(normalizationIndex, indexPath)

    def getCalibrationRecordPath(self, runId: str, version: str):
        recordPath: str = f"{self._constructCalibrationDataPath(runId, version)}CalibrationRecord.json"
        return recordPath

    def getNormalizationRecordPath(self, runId: str, version: str):
        recordPath: str = f"{self._constructCalibrationDataPath(runId, version)}NormalizationRecord.json"
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
        calibrationStatePath = self._constructCalibrationStatePath(stateId)
        calibrationVersionPath = f"{calibrationStatePath}v_*/"
        latestVersion = 0
        versionDirs = self._findMatchingDirList(calibrationVersionPath, throws=False)
        for versionDir in versionDirs:
            version = int(versionDir.split("/")[-2].split("_")[-1])
            if version > latestVersion:
                latestVersion = version
        return latestVersion

    def readNormalizationRecord(self, runId: str, version: str = None):
        stateId, _ = self._generateStateId(runId)
        self._readReductionParameters(runId)
        recordPath: str = self.getNormalizationRecordPath(runId, "*")
        latestFile = ""
        if version:
            latestFile = self._getFileOfVersion(recordPath, version)
        else:
            latestFile = self._getLatestFile(recordPath)
        record: NormalizationRecord = None  # noqa: F821
        if latestFile:
            print(f"reading NormalizationRecord from {latestFile}")
            record = parse_file_as(NormalizationRecord, latestFile)  # noqa: F821

            # Read the state's grouping-schema map from its separate JSON file.
            groupingMap = self._readGroupingMap(stateId)
            # Attach the grouping-schema map to the normalization record's state:
            record.normalization.instrumentState.attachGroupingMap(groupingMap)
        return record

    def writeNormalizationRecord(self, record: NormalizationRecord, version: int = None):  # noqa: F821
        """
        Persists a `NormalizationRecord` to either a new version folder, or overwrite a specific version.
        -- side effect: updates version numbers of incoming `NormalizationRecord` and its nested `Normalization`.
        """
        runNumber = record.runNumber
        stateId, _ = self._generateStateId(runNumber)
        previousVersion = self._getLatestCalibrationVersion(stateId)
        if not version:
            version = previousVersion + 1
        recordPath: str = self.getNormalizationRecordPath(runNumber, version)
        record.version = version

        # There seems no need to write the _nested_ Normalization, because it's written to a separate file during 'writeNormalizationState'.
        # However, if it is going to be _nested_, this marks it with the correct version.
        # (For example, use pydantic Field(exclude=True) to _stop_ nesting it.)
        record.normalization.version = version

        normalizationPath = self._constructCalibrationDataPath(runNumber, version)
        # check if directory exists for runId
        if not os.path.exists(normalizationPath):
            os.makedirs(normalizationPath)
        # append to record and write to file
        write_model_pretty(record, recordPath)

        self.writeNormalizationState(runNumber, record.normalization, version)
        for workspace in record.workspaceNames:
            self.groceryService.writeWorkspace(normalizationPath, workspace)
        print(f"wrote NormalizationRecord: version: {version}")
        return record

    def readCalibrationRecord(self, runId: str, version: str = None):
        stateId, _ = self._generateStateId(runId)
        recordPath: str = self.getCalibrationRecordPath(runId, "*")
        # find the latest version
        latestFile = ""
        if version:
            latestFile = self._getFileOfVersion(recordPath, version)
        else:
            latestFile = self._getLatestFile(recordPath)
        # read the file
        record: CalibrationRecord = None
        if latestFile:
            print(f"reading CalibrationRecord from {latestFile}")
            record = parse_file_as(CalibrationRecord, latestFile)

            # Read the state's grouping-schema map from its separate JSON file.
            groupingMap = self._readGroupingMap(stateId)
            # Attach the grouping-schema map to the calibration record's state:
            record.calibrationFittingIngredients.instrumentState.attachGroupingMap(groupingMap)

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
        # There seems no need to write the _nested_ Calibration, because it's written to a separate file during 'writeCalibrationState'.
        # However, if it is going to be _nested_, this marks it with the correct version.
        # (For example, use pydantic Field(exclude=True) to _stop_ nesting it.)
        record.calibrationFittingIngredients.version = version

        calibrationPath = self._constructCalibrationDataPath(runNumber, version)
        # check if directory exists for runId
        if not os.path.exists(calibrationPath):
            os.makedirs(calibrationPath)
        # append to record and write to file # *** DEBUG *** .version==2, .<calibration>.version==1
        write_model_pretty(record, recordPath)

        self.writeCalibrationState(
            runNumber, record.calibrationFittingIngredients, version
        )  # *** DEBUG *** <calibration>.version==2
        for workspace in record.workspaceNames:
            self.groceryService.writeWorkspace(calibrationPath, workspace)
        print(f"Wrote CalibrationRecord: version: {version}")
        return record

    def writeCalibrationReductionResult(self, runId: str, workspaceName: WorkspaceName, dryrun: bool = False):
        # use mantid to write workspace to file
        stateId, _ = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationStatePath(stateId)
        filenameFormat = f"{calibrationPath}{runId}/{workspaceName}" + "_v{}.nxs"
        # find total number of files
        foundFiles = self._findMatchingFileList(filenameFormat.format("*"), throws=False)
        version = len(foundFiles) + 1

        filename = filenameFormat.format(version)
        if not dryrun:
            self.groceryService.writeWorkspace(filename, workspaceName)
        return filename

    def writeCalibrantSample(self, sample: CalibrantSamples):
        samplePath: str = Config["samples.home"]
        fileName: str = sample.name + "_" + sample.unique_id
        # TODO: Test code should not pollute production code, why is this here?
        if fileName == "test_id123":
            filePath = os.path.join(Resource._resourcesPath + fileName) + ".json"
        else:
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

    def getCalibrationStatePath(self, runId: str, version: str):
        statePath: str = f"{self._constructCalibrationDataPath(runId, version)}CalibrationParameters.json"
        return statePath

    def getNormalizationStatePath(self, runId: str, version: str):
        # TODO make its own path?
        statePath: str = f"{self._constructCalibrationDataPath(runId, version)}NormalizationParameters.json"
        return statePath

    def readCalibrationState(self, runId: str, version: str = None):
        # get stateId and check to see if such a folder exists, if not create it and initialize it
        stateId, _ = self._generateStateId(runId)
        calibrationStatePath = self.getCalibrationStatePath(runId, "*")

        latestFile = ""
        if version:
            latestFile = self._getFileOfVersion(calibrationStatePath, version)
        else:
            # TODO: This should refer to the calibration index
            latestFile = self._getLatestFile(calibrationStatePath)

        calibrationState = None
        if latestFile:
            calibrationState = parse_file_as(Calibration, latestFile)

            # Read the state's grouping-schema map from its separate JSON file.
            groupingMap = self._readGroupingMap(stateId)
            # Attach the grouping-schema map to the normalization state:
            calibrationState.instrumentState.attachGroupingMap(groupingMap)

        return calibrationState

    def readNormalizationState(self, runId: str, version: str = None):
        stateId, _ = self._generateStateId(runId)
        normalizationStatePath = self.getCalibrationStatePath(runId, "*")

        latestFile = ""
        if version:
            latestFile = self._getFileOfVersion(normalizationStatePath, version)
        else:
            # TODO: This should refer to the calibration index
            latestFile = self._getLatestFile(normalizationStatePath)

        normalizationState = None
        if latestFile:
            normalizationState = parse_file_as(Normalization, latestFile)  # noqa: F821

            # Read the state's grouping-schema map from its separate JSON file.
            groupingMap = self._readGroupingMap(stateId)
            # Attach the grouping-schema map to the normalization state:
            normalizationState.instrumentState.attachGroupingMap(groupingMap)

        return normalizationState

    def writeCalibrationState(self, runId: str, calibration: Calibration, version: int = None):
        """
        Writes a `Calibration` to either a new version folder, or overwrites a specific version.
        -- side effect: updates version number of incoming `Calibration`.
        """
        stateId, _ = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationStatePath(stateId)
        previousVersion = self._getLatestCalibrationVersion(stateId)
        if not version:
            version = previousVersion + 1
        # check for the existenece of a calibration parameters file
        calibrationParametersPath = self.getCalibrationStatePath(runId, version)
        calibration.version = version
        calibrationPath = self._constructCalibrationDataPath(runId, version)
        if not os.path.exists(calibrationPath):
            os.makedirs(calibrationPath)
        # write the calibration state.
        write_model_pretty(calibration, calibrationParametersPath)

    def writeNormalizationState(self, runId: str, normalization: Normalization, version: int = None):  # noqa: F821
        """
        Writes a `Normalization` to either a new version folder, or overwrites a specific version.
        -- side effect: updates version number of incoming `Normalization`.
        """
        stateId, _ = self._generateStateId(runId)
        normalizationPath: str = self._constructCalibrationStatePath(stateId)
        previousVersion = self._getLatestCalibrationVersion(stateId)
        if not version:
            version = previousVersion + 1
        # check for the existenece of a calibration parameters file
        normalizationParametersPath = self.getNormalizationStatePath(runId, version)
        normalization.version = version
        normalizationPath = self._constructCalibrationDataPath(runId, version)
        if not os.path.exists(normalizationPath):
            os.makedirs(normalizationPath)
        write_model_pretty(normalization, normalizationParametersPath)

    @ExceptionHandler(StateValidationException)
    def initializeState(self, runId: str, name: str = None):
        stateId, _ = self._generateStateId(runId)

        # pull pv data similar to how we generate stateId
        pvFile = self._readPVFile(runId)
        detectorState = DetectorState(
            arc=[pvFile.get("entry/DASlogs/det_arc1/value")[0], pvFile.get("entry/DASlogs/det_arc2/value")[0]],
            wav=pvFile.get("entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value")[0],
            freq=pvFile.get("entry/DASlogs/BL3:Det:TH:BL:Frequency/value")[0],
            guideStat=pvFile.get("entry/DASlogs/BL3:Mot:OpticsPos:Pos/value")[0],
            lin=[pvFile.get("entry/DASlogs/det_lin1/value")[0], pvFile.get("entry/DASlogs/det_lin2/value")[0]],
        )
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

        # Read the grouping-schema map from its separate JSON file, and attach it to the state:
        #   * the 'stateId' of the grouping map is adjusted to match that of the state.
        groupingMap = self._readDefaultGroupingMap()
        instrumentState.attachGroupingMap(groupingMap, coerceStateId=True)

        # finally add seedRun, creation date, and a human readable name
        calibration = Calibration(
            instrumentState=instrumentState,
            name=name,
            seedRun=runId,
            creationDate=datetime.datetime.now(),
            version=0,
        )

        self.writeCalibrationState(runId, calibration)

        # This is the _ONLY_ place that the grouping-schema map is written to its separate JSON file.
        self._writeGroupingMap(stateId, instrumentState.groupingMap)

        return calibration

    def checkCalibrationFileExists(self, runId: str):
        stateID, _ = self._generateStateId(runId)
        calibrationStatePath: str = self._constructCalibrationStatePath(stateID)

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
        return list(sampleFiles)

    def _readGroupingMap(self, stateId: str) -> GroupingMap:
        path = self._groupingMapPath(stateId)
        if not path.exists():
            raise FileNotFoundError(f'required grouping-schema map for state "{stateId}" at "{path}" does not exist')
        # IMPORTANT: <grouping map>.stateId is updated from its default value at <instrument state>: root_validator.  _Not_ here.
        return parse_file_as(GroupingMap, path)

    def _readDefaultGroupingMap(self) -> GroupingMap:
        path = self._defaultGroupingMapPath()
        if not path.exists():
            raise FileNotFoundError(f'required default grouping-schema map "{path}" does not exist')
        # IMPORTANT: <grouping map>.stateId is updated from its default value at <instrument state>: root_validator.  _Not_ here.
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
        return Path(self._constructCalibrationStatePath(stateId)) / "groupingMap.json"

    def readGroupingFiles(self):
        groupingFolder = Config["instrument.calibration.powder.grouping.home"]
        extensions = Config["instrument.calibration.powder.grouping.extensions"]
        # collect list of all files in folder that are applicable extensions
        groupingFiles = []
        for extension in extensions:
            groupingFiles.extend(self._findMatchingFileList(f"{groupingFolder}/*.{extension}", throws=False))
        if len(groupingFiles) < 1:
            raise RuntimeError(f"No grouping files found in {groupingFolder} for extensions {extensions}")
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
