from typing import List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class StateIdLookupService(Service):
    _name = "stateId"
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory please!
    def __init__(self):
        self.registerPath("", self.getStateIds)
        return

    def name(self):
        return self._name

    @FromString
    def getStateIds(self, runs: List[RunConfig]):
        data = {}
        stateIds = []
        for run in runs:
            stateIds.append(self.dataFactoryService.constructStateId(run.runNumber))
        data["StateIds"] = stateIds
        return data
