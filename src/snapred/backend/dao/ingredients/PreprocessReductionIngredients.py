from typing import List

from pydantic import BaseModel

from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class PreprocessReductionIngredients(BaseModel):
    maskList: List[WorkspaceName]
