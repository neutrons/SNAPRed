

from os import path
from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.service.ReductionService import ReductionService

import time

runId="59039"
useLiteMode=True


reductionService = ReductionService()
groups = reductionService.loadAllGroupings(runId, useLiteMode)
# find column group
columnGroup = next((group for group in groups["focusGroups"] if "column" in group.name.lower()), None)

requestPayload = ReductionRequest(
    runNumber=runId,
    useLiteMode=useLiteMode,
    timestamp=time.time(),
    focusGroups=[columnGroup],
)

cheeseMasks = []

# NOTE: Be careful how you define hook methods.  They should not be class methods.
def cheeseHook(self):
    # raise RuntimeError(f"Hook Executed!: {cheeseMasks}")
    for mask in cheeseMasks:
        #extract units from ws name (table workspaces don't have logs)
        maskUnits = mask.split('_')[-1]
        #ensure units of workspace match
        self.mantidSnapper.ConvertUnits(
            f"Converting units to match Bin Mask with units of {maskUnits}",
            InputWorkspace=self.outputWs,
            Target = maskUnits,
            OutputWorkspace=self.outputWs
        )
        #mask bins
        self.mantidSnapper.MaskBinsFromTable(
            "Masking bins...",
            InputWorkspace=self.outputWs,
            MaskingInformation=mask,
            OutputWorkspace=self.outputWs
        )
    self.mantidSnapper.executeQueue()
    
hooks = {
    "PostPreprocessReductionRecipe" : cheeseHook
}



request = SNAPRequest(path="reduction", payload=requestPayload.model_dump_json(), hooks=hooks)

interface = InterfaceController()

interface.executeRequest(request)