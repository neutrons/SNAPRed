from typing import List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict

from snapred.backend.error.ContinueWarning import ContinueWarning


class ReductionRequest(BaseModel):
    runNumber: Union[str, List[str]]
    useLiteMode: bool
    userSelectedMaskPath: Optional[str] = None
    version: Union[int, Literal["*"]] = "*"

    # TODO: Move to SNAPRequest
    continueFlags: Optional[ContinueWarning.Type] = None

    model_config = ConfigDict(extra="forbid")
