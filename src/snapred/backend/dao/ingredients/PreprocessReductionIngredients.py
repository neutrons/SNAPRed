from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class PreprocessReductionIngredients(BaseModel):
    maskList: Optional[List[WorkspaceName]]
