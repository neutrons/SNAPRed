from pydantic import BaseModel


class RunFeedbackRequest(BaseModel):
    runId: str
