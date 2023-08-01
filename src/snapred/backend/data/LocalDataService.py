import datetime
import glob
import hashlib
import json
import os
from errno import ENOENT as NOT_FOUND
from pathlib import Path
from typing import Any, Dict, List, Tuple

import h5py
from mantid.api import AlgorithmManager, mtd
from pydantic import parse_file_as

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.state.DiffractionCalibrant import DiffractionCalibrant
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.NormalizationCalibrant import NormalizationCalibrant
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.dao.StateId import StateId
from snapred.meta.Config import Config, Resource
from snapred.meta.decorators.Singleton import Singleton

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
    stateIdCache: Dict[str, str] = {}
    instrumentConfig: "InstrumentConfig"  # Optional[InstrumentConfig]
    verifyPaths: bool = True

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
        self.instrumentConfigPath = self.dataPath / Config["instrument.config"]

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
            instrumentConfig.calibrationDirectory = self.dataPath / "shared/Calibration/"
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

        return StateConfig(
            diffractionCalibrant=self._readDiffractionCalibrant(runId),
            emptyInstrumentRunNumber=reductionParameters["VBRun"][0],
            normalizationCalibrant=self._readNormalizationCalibrant(runId),
            geometryCalibrationFileName=None,  # TODO: missing, reductionParameters['GeomCalFileName'],
            calibrationAuthor=reductionParameters.get("calibBy"),
            calibrationDate=reductionParameters.get("calibDate"),
            focusGroups=self._readFocusGroups(runId),
            isLiteMode=True,  # TODO: Support non lite mode
            rawVanadiumCorrectionFileName=reductionParameters["rawVCorrFileName"],
            vanadiumFilePath=str(
                self.instrumentConfig.calibrationDirectory
                / "Powder"
                / reductionParameters["stateId"]
                / reductionParameters["rawVCorrFileName"]
            ),
            calibrationMaskFileName=reductionParameters.get("CalibrationMaskFilename"),
            stateId=reductionParameters["stateId"],
            tofBin=reductionParameters["tofBin"],
            tofMax=reductionParameters["tofMax"],
            tofMin=reductionParameters["tofMin"],
            version=reductionParameters["version"],
            wallclockTof=reductionParameters["wallClockTol"],
            temporalProximity=None,
        )  # TODO: fill with real value

    def _readDiffractionCalibrant(self, runId: str) -> DiffractionCalibrant:
        reductionParameters = self._readReductionParameters(runId)

        return DiffractionCalibrant(
            filename=reductionParameters["calFileName"],
            runNumber=reductionParameters["CRun"][0],
            name=reductionParameters.get("CalibrantName"),
            diffCalPath=str(
                self.instrumentConfig.calibrationDirectory
                / "Powder"
                / reductionParameters["stateId"]
                / reductionParameters["calFileName"]
            ),
            latticeParameters=None,  # TODO: missing, reductionParameters['CalibrantLatticeParameters'],
            reference=None,
        )  # TODO: missing, reductionParameters['CalibrantReference'])

    def _readNormalizationCalibrant(self, runId: str) -> NormalizationCalibrant:
        reductionParameters = self._readReductionParameters(runId)
        return NormalizationCalibrant(
            numAnnuli=reductionParameters["NAnnul"],
            numSlices=None,  # TODO: missing, reductionParameters['Nslice'],
            attenuationCrossSection=reductionParameters["VAttenuationXSection"],
            attenuationHeight=reductionParameters["VHeight"],
            geometry=None,  # TODO: missing, reductionParameters['VGeometry'],
            FWHM=reductionParameters["VFWHM"],
            mask=reductionParameters["VMsk"],
            material=None,  # TODO: missing,
            peaks=reductionParameters["VPeaks"].split(","),
            radius=reductionParameters["VRad"],
            sampleNumberDensity=reductionParameters["VSampleNumberDensity"],
            scatteringCrossSection=reductionParameters["VScatteringXSection"],
            smoothPoints=reductionParameters["VSmoothPoints"],
            calibrationState=None,
        )  # TODO: missing, reductionParameters['VCalibState'])

    def _readFocusGroups(self, runId: str) -> List[FocusGroup]:
        reductionParameters = self._readReductionParameters(runId)
        # TODO: fix hardcode reductionParameters["focGroupLst"]
        # dont have time to figure out why its reading the wrong data
        focusGroupNames = ["Column", "Bank", "All"]
        focusGroups = []
        for i, name in enumerate(focusGroupNames):
            focusGroups.append(
                FocusGroup(
                    name=name,
                    nHst=reductionParameters["focGroupNHst"][i],
                    FWHM=reductionParameters["VFWHM"][i],
                    dBin=reductionParameters["focGroupDBin"][i],
                    dMax=reductionParameters["focGroupDMax"][i],
                    dMin=reductionParameters["focGroupDMin"][i],
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

    def _findIPTS(self, runId: str) -> str:
        path: str
        # lookup IPST number
        if runId in self.iptsCache:
            path = self.iptsCache[runId]
        else:
            algorithm = AlgorithmManager.create("GetIPTS")
            algorithm.setProperty("RunNumber", runId)
            algorithm.setProperty("Instrument", "SNAP")
            algorithm.execute()
            path = algorithm.getProperty("Directory").value

            self.iptsCache[runId] = path

        return path

    def _readRunConfig(self, runId: str) -> RunConfig:
        # lookup IPST number
        iptsPath = self._findIPTS(runId)

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

    def _generateStateId(self, runId: str) -> Tuple[Any, Any]:
        if runId in self.stateIdCache:
            return self.stateIdCache[runId]

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
        hasher = hashlib.shake_256()

        decodedKey = json.dumps(stateID.__dict__).encode("utf-8")

        hasher.update(decodedKey)

        hashedKey = hasher.digest(8).hex()
        self.stateIdCache[runId] = hashedKey, decodedKey

        return hashedKey, decodedKey

    def _findMatchingFileList(self, pattern, throws=True) -> List[str]:
        fileList: List[str] = []
        for fname in glob.glob(pattern, recursive=True):
            if os.path.isfile(fname):
                fileList.append(fname)
        if len(fileList) == 0 and throws:
            raise ValueError("No files could be found with pattern: {}".format(pattern))

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
            raise ValueError("No directories could be found with pattern: {}".format(pattern))

        return fileList

    def _constructCalibrationStatePath(self, stateId):
        # TODO: Propogate pathlib through codebase
        return str(self.instrumentConfig.calibrationDirectory) + "/Powder/" + stateId + "/"

    def _readReductionParameters(self, runId: str) -> Dict[Any, Any]:
        # lookup IPST number
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
                raise ValueError("No applicable calibration index entries found for runId {}".format(runId))
            latestCalibration = relevantEntries[-1]
            version = latestCalibration.version
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
            raise ValueError("No calibration data found for runId {}".format(runId))
        return self._constructCalibrationDataPath(runId, version)

    def writeCalibrationIndexEntry(self, entry: CalibrationIndexEntry):
        stateId, _ = self._generateStateId(entry.runNumber)
        calibrationPath: str = self._constructCalibrationStatePath(stateId)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        # append to index and write to file
        calibrationIndex = self.readCalibrationIndex(entry.runNumber)
        calibrationIndex.append(entry)
        with open(indexPath, "w") as indexFile:
            indexFile.write(json.dumps([entry.dict() for entry in calibrationIndex]))

    def getCalibrationRecordPath(self, runId: str, version: str):
        recordPath: str = self._constructCalibrationDataPath(runId, version) + "CalibrationRecord.json"
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
        calibrationVersionPath = calibrationStatePath + "v_*/"
        latestVersion = 0
        versionDirs = self._findMatchingDirList(calibrationVersionPath, throws=False)
        for versionDir in versionDirs:
            version = int(versionDir.split("/")[-2].split("_")[-1])
            if version > latestVersion:
                latestVersion = version
        return latestVersion

    def readCalibrationRecord(self, runId: str, version: str = None):
        # Need to run this because of its side effect, TODO: Remove side effect
        self._readReductionParameters(runId)
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
            record = parse_file_as(CalibrationRecord, latestFile)
        return record

    def writeCalibrationRecord(self, record: CalibrationRecord, version: int = None):
        """
        Persists a `CalibrationRecord` to either a new version folder, or overwrite a specific version.
        """
        runNumber = record.reductionIngredients.runConfig.runNumber
        stateId, _ = self._generateStateId(record.reductionIngredients.runConfig.runNumber)
        previousVersion = self._getLatestCalibrationVersion(stateId)
        if not version:
            version = previousVersion + 1
        recordPath: str = self.getCalibrationRecordPath(record.reductionIngredients.runConfig.runNumber, version)
        record.version = version
        calibrationPath = self._constructCalibrationDataPath(record.reductionIngredients.runConfig.runNumber, version)
        # check if directory exists for runId
        if not os.path.exists(calibrationPath):
            os.makedirs(calibrationPath)
        # append to record and write to file
        with open(recordPath, "w") as recordFile:
            recordFile.write(record.json())

        self.writeCalibrationState(runNumber, record.calibrationFittingIngredients, version)
        for workspace in record.workspaceNames:
            self.writeWorkspace(calibrationPath, workspace)
        return record

    def writeWorkspace(self, path: str, workspaceName: str):
        """
        Writes a Mantid Workspace to disk.
        """
        saveAlgo = AlgorithmManager.create("SaveNexus")
        saveAlgo.setProperty("InputWorkspace", workspaceName)
        saveAlgo.setProperty("Filename", path + workspaceName)
        saveAlgo.execute()

    def writeCalibrationReductionResult(self, runId: str, workspaceName: str, dryrun: bool = False):
        # use mantid to write workspace to file
        stateId, _ = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationStatePath(stateId)
        filenameFormat = calibrationPath + "{}/".format(runId) + workspaceName + "_v{}.nxs"
        # find total number of files
        foundFiles = self._findMatchingFileList(filenameFormat.format("*"), throws=False)
        version = len(foundFiles) + 1

        filename = filenameFormat.format(version)
        if not dryrun:
            saveAlgo = AlgorithmManager.create("SaveNexus")
            saveAlgo.setProperty("InputWorkspace", workspaceName)
            saveAlgo.setProperty("Filename", filename)
            saveAlgo.execute()
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
        with open(filePath, "w") as sampleFile:
            sampleFile.write(json.dumps(sample.dict()))

    def _getCurrentCalibrationRecord(self, runId: str):
        version = self._getVersionFromCalibrationIndex(runId)
        return self.readCalibrationRecord(runId, version)

    def getCalibrationStatePath(self, runId: str, version: str):
        statePath: str = self._constructCalibrationDataPath(runId, version) + "CalibrationParameters.json"
        return statePath

    def readCalibrationState(self, runId: str, version: str = None):
        # get stateId and check to see if such a folder exists, if not create an initialize it
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

        return calibrationState

    def writeCalibrationState(self, runId: str, calibration: Calibration, version: int = None):
        """
        Writes a `Calibration` to either a new version folder, or overwrite a specific version.
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
        # write the file and return the calibration state
        with open(calibrationParametersPath, "w") as calibrationParametersFile:
            calibrationParametersFile.write(calibration.json())

    def initializeState(self, runId: str, name: str = None):
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
        # then pull static values specified by malcolm from resources
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
        tofLimit = Limit(minimum=lambdaLimit.minimum * L / 3.9561e-3, maximum=lambdaLimit.maximum * L / 3.9561e-3)
        particleBounds = ParticleBounds(wavelength=lambdaLimit, tof=tofLimit)
        # finally add seedRun, creation date, and a human readable name
        instrumentState = InstrumentState(
            instrumentConfig=instrumentConfig,
            detectorState=detectorState,
            gsasParameters=gsasParameters,
            particleBounds=particleBounds,
            defaultGroupingSliceValue=defaultGroupSliceValue,
            fwhmMultiplierLimit=fwhmMultiplier,
            peakTailCoefficient=peakTailCoefficient,
        )

        calibration = Calibration(
            instrumentState=instrumentState,
            name=name,
            seedRun=runId,
            creationDate=datetime.datetime.now(),
            version=0,
        )

        self.writeCalibrationState(runId, calibration)
        return calibration

    def getWorkspaceForName(self, name):
        """
        Returns a workspace from Mantid if it exists.
        Abstraction for the Service layer to interact with mantid data.
        Usually we only deal in references as its quicker,
        but sometimes its already in memory due to some previous step.
        """
        try:
            return mtd[name]
        except ValueError:
            return None

    def deleteWorkspace(self, workspaceName: str):
        """
        Deletes a workspace from Mantid.
        Mostly for cleanup at the Service Layer.
        """
        if self.getWorkspaceForName(workspaceName) is not None:
            deleteWorkspaceAlgo = AlgorithmManager.create("DeleteWorkspace")
            deleteWorkspaceAlgo.setProperty("Workspace", workspaceName)
            deleteWorkspaceAlgo.execute()
