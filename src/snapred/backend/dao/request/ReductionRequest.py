from typing import List, Optional, Union

from pydantic import BaseModel, Extra

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning


class ReductionRequest(BaseModel, extra=Extra.forbid):
    runNumber: Union[str, List[str]]
    useLiteMode: bool
    focusGroup: Union[Optional[FocusGroup], List[FocusGroup]]
    userSelectedMaskPath: Optional[str]
    version: Optional[int]

    # TODO: Move to SNAPRequest
    continueFlags: Optional[ContinueWarning.Type] = None
