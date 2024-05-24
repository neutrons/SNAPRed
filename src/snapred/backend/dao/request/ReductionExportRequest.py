from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord


class ReductionExportRequest(BaseModel):
    """

    Sent from reduction workflow to reduction service, requesting
    the reduction service save the data for the reduction.

    """

    reductionRecord: ReductionRecord

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
