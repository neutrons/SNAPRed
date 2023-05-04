from typing import Any, Dict

from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.meta.Singleton import Singleton


@Singleton
class StateIdLookupService:
    name = "stateId"
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory please!
    def __init__(self):
        return

    def orchestrateRecipe(self, request: SNAPRequest) -> Dict[Any, Any]:
        data = {}
        stateIds = []
        for run in request.runs:
            stateIds.append(self.dataFactoryService.constructStateId(run.runNumber))
        data["StateIds"] = stateIds
        return data
