from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord


class ReductionResponse(BaseModel):
    record: ReductionRecord

    model_config = ConfigDict(
        extra="forbid",
    )
