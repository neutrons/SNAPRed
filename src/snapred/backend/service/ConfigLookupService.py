from typing import List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton

# from snapred.backend.recipe.ReductionRecipe


@Singleton
class ConfigLookupService(Service):
    _name = "config"
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory
    def __init__(self):
        self.registerPath("", self.getConfigs)
        return

    def name(self):
        return self._name

    @FromString
    def getConfigs(self, runs: List[RunConfig]):
        data = {}
        for run in runs:
            data[run.runId] = self.dataFactoryService.getReductionState(run.runId)
        return data
