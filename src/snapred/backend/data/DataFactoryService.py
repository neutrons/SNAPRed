from snapred.meta.Singleton import Singleton
from snapred.backend.dao.ReductionState import ReductionState

@Singleton
class DataFactoryService:
    def __init__(self):
        pass

    def getReductionState(self, runId):
        # lookup and package data
        # reductionState.geometricConfig = self._getGeometricConfig
        return ReductionState("Test")

    def _getGetometricConfig(self):
        # call additional data service, specify shallow copy
        pass

    # ...