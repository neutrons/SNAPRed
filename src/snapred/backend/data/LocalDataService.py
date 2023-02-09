from snapred.meta.Singleton import Singleton
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateId import StateId
from snapred.backend.dao.InstrumentConfig import InstrumentConfig

from mantid.api import AlgorithmManager

import hashlib
import json
import glob
import os

"""
    Looks up data on disk
    TBD the interface such that it is fairly generic
    but intersects that of the potential oncat data service interface
"""
@Singleton
class LocalDataService:
    reductionParameterCache = {}
    # TODO: Read these values from config file
    instrument = 'SNAP' # refered to as inst in the prototype
    nexusFileExt='.nxs.h5'
    nexusFilePre='SNAP_'
    stateLoc='/SNS/SNAP/shared/Calibration/' #location of all statefolders
    calibFilePre='SNAPcalibLog'
    calibFileExt='json'
    # relative paths from main IPTS folder. Full paths generated from these for specific runs
    sharedDirLoc='shared/'
    nexusDirLoc='nexus/' #relative to ipts directory 
    reducedDirLoc='shared/test/reduced/' #temporary location

    def __init__(self):
        pass

    def readInstrumentConfig(self, runId):
        reductionParameters = self._readReductionParameters(runId)
        return InstrumentConfig()

    def readStateConfig(self, runId):
        reductionParameters = self._readReductionParameters(runId)
        return StateConfig()

    def readRunConfig(self, runId):
        return self._readRunConfig(runId)


    def _findIPTS(self, runId):
        # lookup IPST number
        algorithm = AlgorithmManager.create("GetIPTS")
        algorithm.setProperty("RunNumber", runId)
        algorithm.setProperty("Instrument", "SNAP")
        path = algoritm.execute()
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
        fName = runConfig.IPTS + self.nexusDirLoc + '/SNAP_' + str(runNum) + self.nexusFileExt

        if exists(fName):
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
        # TODO: raise exception if no files found
        return fileList 


    def _readReductionParameters(self, runId):
        # lookup IPST number
        runConfig = self._readRunConfig(runId)
        stateId, _ = self._generateStateId(runId)

        calibrationPath = self.stateLoc + stateId + '/powder/'
        calibSearchPattern=f'{calibrationPath}{self.calibFilePre}*.{self.calibFileExt}'

        foundFiles = self._findMatchingFileList(calibSearchPattern)

        calibFileList = []

        # TODO: Allow non lite files
        for file in foundFiles:
            if 'lite' in file:
                calibFileList.append(file)

        calibRunList = []
        for str in calibFileList:
            runStr = str[str.find(self.calibFilePre)+len(self.calibFilePre):].split('.')[0]
            calibRunList.append(int(runStr))

            relRuns = [ x-run for x in calibRunList ] 

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


        # Now push data into DAO objects
        import pds; pds.set_trace()
        reductionParameterCache[runId] = dictIn
        return dictIn
