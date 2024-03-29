from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class VanadiumCorrectionRequest(BaseModel):
    runNumber: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    focusGroup: FocusGroup

    calibrantSamplePath: str

    inputWorkspace: str
    backgroundWorkspace: str
    outputWorkspace: str

    crystalDMin: float = Config["constants.CrystallographicInfo.dMin"]
    crystalDMax: float = Config["constants.CrystallographicInfo.dMax"]
