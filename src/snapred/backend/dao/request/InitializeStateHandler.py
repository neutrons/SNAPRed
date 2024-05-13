from pydantic import BaseModel


class InitializeStateHandler(BaseModel):
    runId: str
    useLiteMode: bool
