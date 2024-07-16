from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class PreprocessReductionIngredients(BaseModel):
    maskList: Optional[List[WorkspaceName]] = None

    model_config = ConfigDict(
        extra="forbid",
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
