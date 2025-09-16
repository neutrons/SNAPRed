from pydantic import BaseModel


class NeutronFileSizeRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
