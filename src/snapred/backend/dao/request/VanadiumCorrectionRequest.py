from pydantic import BaseModel, Field

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class VanadiumCorrectionRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    focusGroup: FocusGroup

    calibrantSamplePath: str

    inputWorkspace: str
    backgroundWorkspace: str
    outputWorkspace: str

    crystalDMin: float = Field(default_factory=lambda: Config["constants.CrystallographicInfo.crystalDMin"])
    crystalDMax: float = Field(default_factory=lambda: Config["constants.CrystallographicInfo.crystalDMax"])
