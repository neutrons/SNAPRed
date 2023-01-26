from snapred.meta.Singleton import Singleton

from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.dao.state.DiffractionCalibrant import DiffractionCalibrant
from snapred.backend.dao.state.NormalizationCalibrant import NormalizationCalibrant
from snapred.backend.dao.state.FocusGroup import FocusGroup

from snapred.backend.data.LocalDataService import LocalDataService

from mantid.api import AnalysisDataService as ADS

@Singleton
class DataFactoryService:

    lookupService = None

    def __init__(self, lookupService=LocalDataService()):
        self.lookupService = lookupService

    def getReductionState(self, runId):
        # lookup and package data
        # reductionState.geometricConfig = self._getGeometricConfig
        return ReductionState(instrumentConfig=self.getInstrumentConfig(runId), stateConfig=self.getStateConfig(runId))

    def getInstrumentConfig(self, runId):
        # throw unimplemented exception
        raise NotImplementedError("getInstrumentConfig() is not implemented")
        return InstrumentConfig()
    
    def getStateConfig(self, runId):
        raise NotImplementedError("getStateConfig() is not implemented")
        return StateConfig()

    def loadNexusFile(self, reductionState, deepcopy=True):
        # cacheService.get(filepath)
        # else lookupService.loadFile(filepath);cacheService.put(filepath, data)
        # if deepcopy: clone workspace
        raise NotImplementedError("_loadNexusFile() is not implemented")
    
    def loadCalibrationFile(self, reductionState, deepcopy=True):
        raise NotImplementedError("_loadCalibrationFile() is not implemented")

    def loadCalibrationGeometryFile(self, reductionState, deepcopy=True):
        raise NotImplementedError("_loadCalibrationGeometryFile() is not implemented")

    def loadRawVanadiumCorrectionFile(self, reductionState, deepcopy=True):
        raise NotImplementedError("_loadRawVanadiumCorrectionFile() is not implemented")
    
    def loadCalibrationMaskFile(self, reductionState, deepcopy=True):
        raise NotImplementedError("_loadCalibrationMaskFile() is not implemented")

    def _getDiffractionCalibrant(self, runId):
        raise NotImplementedError("_getDiffractionCalibrant() is not implemented")
        return DiffractionCalibrant()
    
    def _getNormalizationCalibrant(self, runId):
        raise NotImplementedError("_getNormalizationCalibrant() is not implemented")
        return NormalizationCalibrant()
    
    def _getFocusGroups(self, runId):
        raise NotImplementedError("_getFocusGroups() is not implemented")
        return [FocusGroup()]

    def _constructStateId(self, runId):
        raise NotImplementedError("_constructStateId() is not implemented")
        return "stateId"

    def _getGetometricConfig(self, runId):
        raise NotImplementedError("_getGetometricConfig() is not implemented")
        # call additional data service, specify shallow copy
 