from pydantic import BaseModel


class HasStateRequest(BaseModel):
    runId: str
    useLiteMode: bool
