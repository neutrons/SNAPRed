from pydantic import BaseModel


class CompatibleMasksRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
