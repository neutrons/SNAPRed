from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.request.UpdateCycleRequest import UpdateCycleRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.state.Cycle import Cycle

# Define the cycle object
cycle = Cycle(
    cycleID="2022-A",
    startDate="2022-01-04",
    stopDate="2022-03-13",
    firstRun=52862,
)

# Build the request to update the InstrumentConfig with the cycle
updateCycleRequest = UpdateCycleRequest(
    cycle=cycle,
    author="author: snap test script",
)

request = SNAPRequest(
    path="config/updateCycle",
    payload=updateCycleRequest.model_dump_json(),
)

# Execute the request through the InterfaceController
interfaceController = InterfaceController()
response = interfaceController.executeRequest(request)

print(f"Response code: {response.code}")
print(f"Response message: {response.message}")
updated_cycle = getattr(response.data, "cycle", None)
if updated_cycle != cycle:
    raise RuntimeError(
        f"Cycle update failed or returned unexpected data: "
        f"code={response.code}, message={response.message}, data={response.data}"
    )

print(f"Updated InstrumentConfig cycle: {updated_cycle}")
