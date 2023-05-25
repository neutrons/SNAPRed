from pydantic import BaseModel


class InitializeStateRequest(BaseModel):
    runId: str
    humanReadableName: str
