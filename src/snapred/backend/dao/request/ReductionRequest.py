from typing import List, NamedTuple, Optional, Union

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning

Versions = NamedTuple("Versions", [("calibration", Optional[int]), ("normalization", Optional[int])])


class ReductionRequest(BaseModel):
    runNumber: Union[str, List[str]]
    useLiteMode: bool
    timestamp: Optional[float] = None
    focusGroups: List[FocusGroup] = []

    # Calibration and normalization versions:
    #   `None` => <use latest version>
    versions: Versions = Versions(None, None)

    # TODO: Move to SNAPRequest
    continueFlags: Optional[ContinueWarning.Type] = None

    model_config = ConfigDict(extra="forbid")
