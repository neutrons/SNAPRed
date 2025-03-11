

from os import path
from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.ReductionService import ReductionService

import time

runId="59039"
useLiteMode=True

dataService = DataFactoryService()
compatibleStates = dataService.getCompatibleStates(runId, useLiteMode)
print(compatibleStates)
assert "myAltState" in compatibleStates


reductionService = ReductionService()
groups = reductionService.loadAllGroupings(runId, useLiteMode)
# find column group
columnGroup = next((group for group in groups["focusGroups"] if "column" in group.name.lower()), None)

requestPayload = ReductionRequest(
    runNumber=runId,
    useLiteMode=useLiteMode,
    timestamp=time.time(),
    focusGroups=[columnGroup],
    alternativeState="myAltState"
)

request = SNAPRequest(path="reduction", payload=requestPayload.model_dump_json())

interface = InterfaceController()

interface.executeRequest(request)



