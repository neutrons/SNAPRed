from enum import Flag, auto

from pydantic import BaseModel


class ContinueWarning(Exception):
    """
    The user has chosen to do something inadvisable, but not invalid.
    Warn the user, but allow the user to continue.
    """

    class Type(Flag):
        MISSING_DIFFRACTION_CALIBRATION = auto()
        MISSING_NORMALIZATION = auto()
        LOW_PEAK_COUNT = auto()

    class Model(BaseModel):
        message: str
        flags: "ContinueWarning.Type"

    def __init__(self, message: str, flags: "Type"):
        ContinueWarning.Model.model_rebuild(force=True)
        self.model = ContinueWarning.Model(message=message, flags=flags)
        super().__init__(message)
