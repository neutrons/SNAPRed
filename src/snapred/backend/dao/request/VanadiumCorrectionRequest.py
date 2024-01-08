
from pydantic import BaseModel

class VanadiumCorrectionRequest(BaseModel):
    samplePath: str
    runNumber: str
    groupingPath: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    inputWorkspace: str
    backgroundWorkspace: str
    outputWorkspace: str