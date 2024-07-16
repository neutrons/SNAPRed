from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord


class ReductionExportRequest(BaseModel):
    """

    Sent from reduction workflow to reduction service, requesting
    the reduction service save the data for the reduction.

    """

    reductionRecord: ReductionRecord
    version: Optional[int] = None
