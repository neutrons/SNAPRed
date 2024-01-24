from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup


class VanadiumCorrectionRequest(BaseModel):
    runNumber: str
    useLiteMode: bool = True  # TODO turn this on inside the view and workflow
    focusGroup: FocusGroup

    calibrantSamplePath: str

    inputWorkspace: str
    backgroundWorkspace: str
    outputWorkspace: str
