from snapred.meta.Singleton import Singleton

from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
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
    # TODO: rules for busting cache
    cache = {}

    def __init__(self, lookupService=LocalDataService()):
        self.lookupService = lookupService

    def getReductionIngredients(self, runId):
        return ReductionIngredients(reductionState=self.getReductionState(runId), runConfig=self.getRunConfig(runId))

    def getReductionState(self, runId):
        if runId in self.cache:
            return self.cache[runId]
        else:
            # lookup and package data
            reductionState = ReductionState(instrumentConfig=self.getInstrumentConfig(runId), stateConfig=self.getStateConfig(runId))
            self.cache[runId] = reductionState
            return reductionState

    def getRunConfig(self, runId):
        return self.lookupService.readRunConfig(runId)

    def getInstrumentConfig(self, runId):
        return self.lookupService.readInstrumentConfig()
    
    def getStateConfig(self, runId):
        return self.lookupService.readStateConfig(runId)

    def loadNexusFile(self, reductionState, deepcopy=True):
        # cacheService.get(filepath)
        # else lookupService.loadFile(filepath);cacheService.put(filepath, data)
        # if deepcopy: clone workspace
        raise NotImplementedError("_loadNexusFile() is not implemented")

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
 