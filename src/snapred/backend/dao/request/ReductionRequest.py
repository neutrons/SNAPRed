from typing import List, Literal, Optional, Union

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup


class ReductionRequest(BaseModel):
    runNumber: Union[str, List[str]]
    useLiteMode: bool
    focusGroup: Union[Optional[FocusGroup], List[FocusGroup]]
    userSelectedMaskPath: Optional[str]
    version: Union[int, Literal["*"]] = "*"
