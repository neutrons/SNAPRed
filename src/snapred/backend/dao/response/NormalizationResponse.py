from pydantic import BaseModel


class NormalizationResponse(BaseModel):
    correctedVanadium: str
    outputWorkspace: str
    smoothedOutput: str
