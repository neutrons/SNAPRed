

from os import path
from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.Hook import Hook
from snapred.backend.dao.request.ReductionExportRequest import ReductionExportRequest
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.service.ReductionService import ReductionService

import time

runId="59039"
useLiteMode=True


reductionService = ReductionService()
groups = reductionService.loadAllGroupings(runId, useLiteMode)
# find column group
columnGroup = next((group for group in groups["focusGroups"] if "column" in group.name.lower()), None)

# NOTE: Be careful how you define hook methods.  
#       They should not be class/bound methods. (i.e. no self or cls parameters)
#       @staticmethods are encouraged, module level functions are fine.
class HookCollection:
    @staticmethod
    def cheeseHook(context, cheeseMasks: list[str]):
        # raise RuntimeError(f"Hook Executed!: {cheeseMasks}")
        for mask in cheeseMasks:
            #extract units from ws name (table workspaces don't have logs)
            maskUnits = mask.split('_')[-1]
            #ensure units of workspace match
            context.mantidSnapper.ConvertUnits(
                f"Converting units to match Bin Mask with units of {maskUnits}",
                InputWorkspace=context.outputWs,
                Target = maskUnits,
                OutputWorkspace=context.outputWs
            )
            #mask bins
            context.mantidSnapper.MaskBinsFromTable(
                "Masking bins...",
                InputWorkspace=context.outputWs,
                MaskingInformation=mask,
                OutputWorkspace=context.outputWs
            )
        context.mantidSnapper.executeQueue()
    
hook = Hook(func=HookCollection.cheeseHook,cheeseMasks=[])

hooks = {
    "PostPreprocessReductionRecipe" : [hook, hook],
}

requestPayload = ReductionRequest(
    runNumber=runId,
    useLiteMode=useLiteMode,
    timestamp=time.time(),
    focusGroups=[columnGroup],
    hooks=hooks, # so that they are persisted to disk.
)


request = SNAPRequest(path="reduction", payload=requestPayload, hooks=hooks)

interface = InterfaceController()

record = interface.executeRequest(request).data.record

exportRequest = ReductionExportRequest(record=record)
request = SNAPRequest(path="reduction/save", payload=exportRequest)
interface.executeRequest(request)