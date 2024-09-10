from typing import Optional

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class ReductionResponse(BaseModel):
    record: ReductionRecord
    unfocusedData: Optional[WorkspaceName] = None

    model_config = ConfigDict(
        extra="forbid",
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
