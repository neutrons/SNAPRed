from matplotlib import use
from pydantic import BaseModel


class FocusSpectraRequest(BaseModel):
    inputWorkspace: str
    groupingWorkspace: str
    runNumber: str
    groupingPath: str
    useLiteMode: bool = True
    outputWorkspace: str
