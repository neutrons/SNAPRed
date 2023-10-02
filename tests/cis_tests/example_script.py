## This is an example script, showing how some simple ingredients can be initialized using SNAPRed as a library

import sys

sys.path.append("/home/4rx/SNAPRed/src")

from mantid.simpleapi import *
from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest

# this will run a request to initialize a calibration state
initializeStateRequest = InitializeStateRequest(runId="58882", humanReadableName="DAC setting 2.4 Ang")
print(initializeStateRequest)
request = SNAPRequest(path="calibration/initializeState", payload=initializeStateRequest.json())
print(request)
interfaceController = InterfaceController()
res = interfaceController.executeRequest(request)

# the quantity res is a SNAPResponse object
# it has three members:
#   code: ResponseCode
#   message: string
#   data: dictionary
# the data dictionary correspond to the object requested
# in this case, it corresponds to a calibration object
print(f"code: {res.code}\nmessage: {res.message}")
print(f"data: {res.data}")

from snapred.backend.dao.calibration.Calibration import Calibration

calibration = Calibration.parse_obj(res.data)
print("success initialize calibration state")
assert False
