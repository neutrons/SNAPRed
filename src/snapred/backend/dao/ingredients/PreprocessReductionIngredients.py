from typing import List, Optional

from pydantic import BaseModel

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class PreprocessReductionIngredients(BaseModel):
    maskList: Optional[List[WorkspaceName]]
