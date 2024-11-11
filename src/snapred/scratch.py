from pydantic import BaseModel

from snapred.backend.dao.state.RunNumber import RunNumber

runStr = "52345"
runNumber = RunNumber(runNumber=runStr)

print(f"Run String {runStr} is converted to RunNumber object {runNumber}")
print("They should be interchangeable")
assert runStr == runNumber


class SimpleModel(BaseModel):
    runNumber: RunNumber


instFromStr = SimpleModel(runNumber=runStr)
instFromRunNumber = SimpleModel(runNumber=runNumber)

assert instFromStr == instFromRunNumber

# should fail
SimpleModel(runNumber="1")
