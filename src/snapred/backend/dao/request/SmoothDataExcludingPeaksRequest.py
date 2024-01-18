from pydantic import BaseModel

from snapred.meta.Config import Config


class SmoothDataExcludingPeaksRequest(BaseModel):
    inputWorkspace: str
    outputWorkspace: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    groupingPath: str
    samplePath: str
    runNumber: str
    smoothingParameter: float
    dMin: float
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
