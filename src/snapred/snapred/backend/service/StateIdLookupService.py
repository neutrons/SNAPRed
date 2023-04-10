from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.meta.Singleton import Singleton


@Singleton
class StateIdLookupService:
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory please!
    def __init__(self):
        return

    def orchestrateRecipe(self, reductionRequest):
        data = {}
        stateIds = []
        for run in reductionRequest.runs:
            stateIds.append(self.dataFactoryService.constructStateId(run.runNumber))
        data["StateIds"] = stateIds
        return data
