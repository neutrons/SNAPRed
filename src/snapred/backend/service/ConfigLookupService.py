from typing import List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton

# from snapred.backend.recipe.ReductionRecipe


@Singleton
class ConfigLookupService(Service):
    dataFactoryService: "DataFactoryService"

    # register the service in ServiceFactory
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.registerPath("", self.getConfigs)
        self.registerPath("samplePaths", self.getSampleFilePaths)
        self.registerPath("groupingMap", self.getGroupingMap)

        return

    @staticmethod
    def name():
        return "config"

    def getGroupingMap(self, runId: str):
        return self.dataFactoryService.getGroupingMap(runId)

    def getSampleFilePaths(self):
        return self.dataFactoryService.getSampleFilePaths()

    @FromString
    def getConfigs(self, runs: List[RunConfig]):
        data = {}
        for run in runs:
            data[run.runNumber] = self.dataFactoryService.getReductionState(run.runNumber, run.useLiteMode)
        return data
