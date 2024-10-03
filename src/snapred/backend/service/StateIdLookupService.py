from typing import List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class StateIdLookupService(Service):
    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        # 'DataFactoryService' is a singleton:
        #  declaring it as an instance attribute, instead of a class attribute,
        #  allows singleton reset during testing.
        self.dataFactoryService = DataFactoryService()
        self.registerPath("", self.getStateIds)
        return

    @staticmethod
    def name():
        return "stateId"

    @FromString
    def getStateIds(self, runs: List[RunConfig]):
        data = {}
        stateIds = []
        for run in runs:
            stateIds.append(self.dataFactoryService.constructStateId(run.runNumber))
        data["StateIds"] = stateIds
        return data
