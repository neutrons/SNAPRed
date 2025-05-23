from pydantic import BaseModel


class RunMetadataRequest(BaseModel):
    runId: str
