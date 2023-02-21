from snapred.meta.Singleton import Singleton
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateId import StateId
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.dao.state.DiffractionCalibrant import DiffractionCalibrant
from snapred.backend.dao.state.NormalizationCalibrant import NormalizationCalibrant
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config

from mantid.api import AlgorithmManager

import hashlib
import json
import glob
import os
import h5py

"""
    Looks up data on disk
    TBD the interface such that it is fairly generic
    but intersects that of the potential oncat data service interface
"""
@Singleton
class LocalDataService:
    reductionParameterCache = {}
    instrument = Config['instrument.name'] # refered to as inst in the prototype
    stateId = None
    nexusFileExt = Config['nexus.file.extension']
    nexusFilePre = Config['nexus.file.prefix']
    stateLoc=Config['instrument.home'] + 'shared/Calibration/' #location of all statefolders
    calibFileExt = Config['calibration.file.extension']
    calibFilePre = Config['calibration.file.prefix']
    # relative paths from main IPTS folder. Full paths generated from these for specific runs
    sharedDirLoc='shared/'
    nexusDirLoc=Config['nexus.home'] #relative to ipts directory 
    reducedDirLoc='shared/test/reduced/' #temporary location

    def __init__(self):
        pass

    def readInstrumentConfig(self, runId):
        reductionParameters = self._readReductionParameters(runId)

        return InstrumentConfig(name=self.instrument,
        nexusFileExtension=self.nexusFileExt,
        nexusFilePrefix=self.nexusFilePre,
        calibrationFileExtension=self.calibFileExt,
        calibrationFilePrefix=self.calibFilePre,
        calibrationDirectory=self.stateLoc,
        sharedDirectory=self.sharedDirLoc,
        nexusDirectory=self.nexusDirLoc,
        reducedDataDirectory=self.reducedDirLoc)

    def readStateConfig(self, runId):
        reductionParameters = self._readReductionParameters(runId)

        return StateConfig(diffractionCalibrant=self._readDiffractionCalibrant(runId),
        emptyInstrumentRunNumber=reductionParameters['VBRun'][0],
        normalizationCalibrant=self._readNormalizationCalibrant(runId),
        geometryCalibrationFileName=None, #TODO: missing, reductionParameters['GeomCalFileName'],
        calibrationAuthor=reductionParameters.get('calibBy'),
        calibrationDate=reductionParameters.get('calibDate'),
        focusGroups=self._readFocusGroups(runId),
        isLiteMode=True, #TODO: Support non lite mode
        rawVanadiumCorrectionFileName=reductionParameters['rawVCorrFileName'],
        calibrationMaskFileName=reductionParameters.get('CalibrationMaskFilename'),
        stateId=self.stateId,
        tofBin=reductionParameters['tofBin'],
        tofMax=reductionParameters['tofMax'],
        tofMin=reductionParameters['tofMin'],
        version=reductionParameters['version'],
        wallclockTof=reductionParameters['wallClockTol'],
        temporalProximity=None) #TODO: fill with real value

    def _readDiffractionCalibrant(self, runId):
        reductionParameters = self._readReductionParameters(runId)
        return DiffractionCalibrant(
    name=reductionParameters.get('CalibrantName'),
    latticeParameters=None, #TODO: missing, reductionParameters['CalibrantLatticeParameters'],
    reference=None) #TODO: missing, reductionParameters['CalibrantReference'])

    def _readNormalizationCalibrant(self, runId):
        reductionParameters = self._readReductionParameters(runId)
        return NormalizationCalibrant(
    numAnnuli=reductionParameters['NAnnul'],
    numSlices=None, #TODO: missing, reductionParameters['Nslice'],
    attenuationCrossSection=reductionParameters['VAttenuationXSection'],
    attenuationHeight=reductionParameters['VHeight'],
    geometry=None, #TODO: missing, reductionParameters['VGeometry'],
    FWHM=reductionParameters['VFWHM'],
    mask=reductionParameters['VMsk'],
    material=None, #TODO: missing, 
    peaks=reductionParameters['VPeaks'].split(','),
    radius=reductionParameters['VRad'],
    sampleNumberDensity=reductionParameters['VSampleNumberDensity'],
    scatteringCrossSection=reductionParameters['VScatteringXSection'],
    smoothPoints=reductionParameters['VSmoothPoints'],
    calibrationState=None) #TODO: missing, reductionParameters['VCalibState'])

    def _readFocusGroups(self, runId):
        reductionParameters = self._readReductionParameters(runId)
        focusGroupNames = reductionParameters['focGroupLst']
        focusGroups = []
        for i, name in enumerate(focusGroupNames):
            focusGroups.append(FocusGroup(name=name,
            nHst=reductionParameters['focGroupNHst'][i],
            FWHM=reductionParameters['VFWHM'][i],
            dBin=reductionParameters['focGroupDBin'][i],
            dMax=reductionParameters['focGroupDMax'][i],
            dMin=reductionParameters['focGroupDMin'][i],
            definition=None #TODO: missing, reductionParameters['focGroupDefinition'][i]
            ))
        return focusGroups

    def readRunConfig(self, runId):
        return self._readRunConfig(runId)


    def _findIPTS(self, runId):
        # lookup IPST number
        algorithm = AlgorithmManager.create("GetIPTS")
        algorithm.setProperty("RunNumber", runId)
        algorithm.setProperty("Instrument", "SNAP")
        algorithm.execute()
        path = algorithm.getProperty('Directory').value
        return path

    def _readRunConfig(self, runId):
        # lookup IPST number
        iptsPath = self._findIPTS(runId)
        return RunConfig(  IPTS=iptsPath,
                                runNumber=runId,
                                maskFileName='',
                                maskFileDirectory=iptsPath + self.sharedDirLoc,
                                gsasFileDirectory=iptsPath + self.reducedDirLoc,
                                calibrationState=None) #TODO: where to find case? "before" "after"

    def _generateStateId(self, runConfig):
        fName = runConfig.IPTS + self.nexusDirLoc + '/SNAP_' + str(runConfig.runNumber) + self.nexusFileExt

        if os.path.exists(fName):
            f = h5py.File(fName, 'r')
        else:
            raise FileNotFoundError('File {} does not exist'.format(fName))

        try:
            det_arc1 = f.get('entry/DASlogs/det_arc1/value')[0]
            det_arc2 = f.get('entry/DASlogs/det_arc2/value')[0]
            wav = f.get('entry/DASlogs/BL3:Chop:Skf1:WavelengthUserReq/value')[0]
            freq = f.get('entry/DASlogs/BL3:Det:TH:BL:Frequency/value')[0]
            GuideIn = f.get('entry/DASlogs/BL3:Mot:OpticsPos:Pos/value')[0]
        except:
            raise ValueError('Could not find all required logs in file {}'.format(fName))

        stateID = StateId(vdet_arc1=det_arc1, vdet_arc2=det_arc2, WavelengthUserReq=wav, Frequency =freq, Pos=GuideIn)
        hasher = hashlib.shake_256()

        decodedKey = json.dumps(stateID.__dict__).encode('utf-8')

        hasher.update(decodedKey)

        hashedKey = hasher.digest(8).hex()

        return hashedKey, decodedKey

    def _findMatchingFileList(self, pattern):
        fileList = []
        for fname in glob.glob(pattern, recursive=True):
            if os.path.isfile(fname):
                fileList.append(fname)
        if len(fileList) == 0:
            raise ValueError('No files could be found with pattern: {}'.format(pattern))
        
        return fileList 


    def _readReductionParameters(self, runId):
        # lookup IPST number
        runConfig = self._readRunConfig(runId)
        run = int(runId)
        stateId, _ = self._generateStateId(runConfig)
        self.stateId = stateId

        calibrationPath = self.stateLoc + stateId + '/powder/'
        calibSearchPattern=f'{calibrationPath}{self.calibFilePre}*{self.calibFileExt}'

        foundFiles = self._findMatchingFileList(calibSearchPattern)

        calibFileList = []

        # TODO: Allow non lite files
        for file in foundFiles:
            if 'lite' in file:
                calibFileList.append(file)

        calibRunList = []
        # TODO: Why are we overwriting dictIn every iteration?
        for str in calibFileList:
            runStr = str[str.find(self.calibFilePre)+len(self.calibFilePre):].split('.')[0]
            calibRunList.append(int(runStr))

            relRuns = [ x-run != 0 for x in calibRunList ] 

            pos  = [i for i,val in enumerate(relRuns)if val >= 0] 
            neg  = [i for i,val in enumerate(relRuns)if val <= 0] 

            # TODO: Account for errors
            closestAfter = min([calibRunList[i] for i in pos])
            calIndx = calibRunList.index(closestAfter)


            with open(calibFileList[calIndx], "r") as json_file:
                dictIn = json.load(json_file)

            #useful to also path location of calibration directory
            fullCalPath = calibFileList[calIndx]
            fSlash = [pos for pos, char in enumerate(fullCalPath) if char == '/']
            dictIn['calPath']=fullCalPath[0:fSlash[-1]+1]


        # Now push data into DAO object
        self.reductionParameterCache[runId] = dictIn
        return dictIn
