import datetime
import glob
import hashlib
import json
import os
from typing import Any, Dict, List, Tuple

import h5py
from mantid.api import AlgorithmManager
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


@Singleton
class LocalDataService:
    reductionParameterCache: Dict[str, Any] = {}
    iptsCache: Dict[str, Any] = {}
    stateIdCache: Dict[str, str] = {}
    dataPath = Config["instrument.home"]
    instrumentConfigPath: str = dataPath + Config["instrument.config"]
    instrumentConfig: InstrumentConfig  # Optional[InstrumentConfig]

    def __init__(self) -> None:
        self.instrumentConfig = self.readInstrumentConfig()

    def readInstrumentConfig(self) -> InstrumentConfig:
        instrumentParameterMap = self._readInstrumentParameters()

        instrumentConfig = InstrumentConfig(**instrumentParameterMap)
        if self.dataPath:
            instrumentConfig.calibrationDirectory = self.dataPath + "shared/Calibration/"

        return instrumentConfig

    def _readInstrumentParameters(self) -> Dict[str, Any]:
        instrumentParameterMap: Dict[str, Any] = {}
        with open(self.instrumentConfigPath, "r") as json_file:
            instrumentParameterMap = json.load(json_file)
        return instrumentParameterMap

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
            vanadiumFilePath=self.instrumentConfig.calibrationDirectory
            + "Powder/"
            + reductionParameters["stateId"]
            + "/"
            + reductionParameters["rawVCorrFileName"],
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
            diffCalPath=self.instrumentConfig.calibrationDirectory
            + "Powder/"
            + reductionParameters["stateId"]
            + "/"
            + reductionParameters["calFileName"],
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
        focusGroupNames = ["Column", "Bank", "All", "Mid"]
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
                    definition=self.instrumentConfig.calibrationDirectory
                    + "Powder/"
                    + self.instrumentConfig.pixelGroupingDirectory
                    + reductionParameters["focGroupDefinition"][i],
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

    def _constructCalibrationPath(self, stateId):
        return self.instrumentConfig.calibrationDirectory + "Powder/" + stateId + "/"

    def _readReductionParameters(self, runId: str) -> Dict[Any, Any]:
        # lookup IPST number
        run: int = int(runId)
        stateId = self._generateStateId(runId)

        calibrationPath: str = self._constructCalibrationPath(stateId)
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
        stateId = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationPath(stateId)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        calibrationIndex: List[CalibrationIndexEntry] = []
        if os.path.exists(indexPath):
            calibrationIndex = parse_file_as(List[CalibrationIndexEntry], indexPath)
        return calibrationIndex

    def writeCalibrationIndexEntry(self, entry: CalibrationIndexEntry):
        stateId = self._generateStateId(entry.runNumber)
        calibrationPath: str = self._constructCalibrationPath(stateId)
        indexPath: str = calibrationPath + "CalibrationIndex.json"
        # append to index and write to file
        calibrationIndex = self.readCalibrationIndex(entry.runNumber)
        calibrationIndex.append(entry)
        with open(indexPath, "w") as indexFile:
            indexFile.write(json.dumps([entry.dict() for entry in calibrationIndex]))

    def getCalibrationRecordPath(self, runId: str, version: str):
        stateId = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationPath(stateId)
        recordPath: str = calibrationPath + "{}/CalibrationRecord_v{}.json".format(runId, version)
        return recordPath

    def _extractFileVersion(self, file: str):
        return int(file.split("_v")[-1].split(".json")[0])

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

    def readCalibrationRecord(self, runId: str, version: str = None):
        # Need to run this because of its side effect, TODO: Remove side effect
        self._readReductionParameters(runId)
        recordPath: str = self.getCalibrationRecordPath(runId, "*")
        # lookup record by regex
        self._findMatchingFileList(recordPath, throws=False)
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

    def writeCalibrationRecord(self, record: CalibrationRecord):
        stateId = self._generateStateId(record.parameters.runConfig.runNumber)
        calibrationPath: str = self._constructCalibrationPath(stateId)
        version = 1
        previousCalibration = self.readCalibrationRecord(record.parameters.runConfig.runNumber)
        if previousCalibration:
            version = previousCalibration.version + 1
        recordPath: str = self.getCalibrationRecordPath(record.parameters.runConfig.runNumber, version)
        record.version = version
        # check if directory exists for runId
        if not os.path.exists(calibrationPath + record.parameters.runConfig.runNumber):
            os.makedirs(calibrationPath + record.parameters.runConfig.runNumber)
        # append to record and write to file
        with open(recordPath, "w") as recordFile:
            recordFile.write(json.dumps(record.dict()))
        return record

    def writeCalibrationReductionResult(self, runId: str, workspaceName: str):
        # use mantid to write workspace to file
        stateId = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationPath(stateId)
        filenameFormat = calibrationPath + "{}/".format(runId) + workspaceName + "_v{}.nxs"
        # find total number of files
        foundFiles = self._findMatchingFileList(filenameFormat.format("*"), throws=False)
        version = len(foundFiles) + 1

        saveAlgo = AlgorithmManager.create("SaveNexus")
        saveAlgo.setProperty("InputWorkspace", workspaceName)
        saveAlgo.setProperty("Filename", filenameFormat.format(version))
        saveAlgo.execute()

    def writeCalibrantSample(self, sample: CalibrantSamples):
        samplePath: str = Config["samples.home"]
        fileName: str = sample.name + "_" + sample.unique_id
        if fileName == "test_id123":
            filePath = os.path.join(Resource._resourcesPath + fileName) + ".json"
        else:
            filePath = os.path.join(samplePath, fileName) + ".json"
        if os.path.exists(filePath):
            raise ValueError(f"the file '{filePath}' already exists")
        with open(filePath, "w") as sampleFile:
            sampleFile.write(json.dumps(sample.dict()))
            
    def _isApplicableEntry(self, calibrationIndexEntry, runId):
        if calibrationIndexEntry.appliesTo == runId:
            return True
        if calibrationIndexEntry.appliesTo.startswith(">"):
            # get latest entry that applies to a runId greater than this runId
            if int(calibrationIndexEntry.appliesTo[1:]) > int(runId):
                return True
        if calibrationIndexEntry.appliesTo.startswith("<"):
            # get latest entry that applies to a runId less than this runId
            if int(calibrationIndexEntry.appliesTo[1:]) < int(runId):
                return True

    def _getVersionFromCalibrationIndex(self, runId: str):
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

    def _getCurrentCalibrationRecord(self, runId: str):
        version = self._getVersionFromCalibrationIndex(runId)
        return self.readCalibrationRecord(runId, version)

    def readCalibrationState(self, runId: str):
        # get stateId and check to see if such a folder exists, if not create an initialize it
        stateId = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationPath(stateId)
        calibrationState = None
        if os.path.exists(calibrationPath):
            # check for the existenece of a calibration parameters file
            calibrationParametersPath = calibrationPath + "CalibrationParameters.json"
            if os.path.exists(calibrationParametersPath):
                # read the file and return the calibration state
                calibrationState = parse_file_as(Calibration, calibrationParametersPath)
        else:
            os.makedirs(calibrationPath)

        return calibrationState

    def writeCalibrationState(self, runId: str, calibration: Calibration):
        # get stateId and check to see if such a folder exists, if not create an initialize it
        stateId = self._generateStateId(runId)
        calibrationPath: str = self._constructCalibrationPath(stateId)
        # check for the existenece of a calibration parameters file
        calibrationParametersPath = calibrationPath + "CalibrationParameters.json"
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
        gsasParameters = GSASParameters(
            alpha=Config["calibration.parameters.default.alpha"], beta=Config["calibration.parameters.default.beta"]
        )
        # then calculate the derived values
        lambdaLimit = Limit(
            minimum=detectorState.wavelength - (instrumentConfig.bandwidth / 2),
            maximum=detectorState.wavelength + (instrumentConfig.bandwidth / 2),
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
        )

        calibration = Calibration(
            instrumentState=instrumentState,
            name=name,
            seedRun=runId,
            creationDate=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        self.writeCalibrationState(runId, calibration)
        return calibration
