from enum import Flag, auto

from pydantic import BaseModel


class ContinueWarning(Exception):
    """
    The user has chosen to do something inadvisable, but not invalid.
    Warn the user, but allow the user to continue.
    """

    class Type(Flag):
        UNSET = 0
        MISSING_DIFFRACTION_CALIBRATION = auto()
        ALTERNATE_DIFFRACTION_CALIBRATION = auto()
        DEFAULT_DIFFRACTION_CALIBRATION = auto()
        MISSING_NORMALIZATION = auto()
        LOW_PEAK_COUNT = auto()
        NO_WRITE_PERMISSIONS = auto()
        CONTINUE_WITHOUT_NORMALIZATION = auto()

    class Model(BaseModel):
        message: str
        flags: "ContinueWarning.Type"

    @property
    def message(self):
        return self.model.message

    @property
    def flags(self):
        return self.model.flags

    def __init__(self, message: str, flags: "Type" = 0):
        ContinueWarning.Model.model_rebuild(force=True)  # replaces: `update_forward_refs` method
        self.model = ContinueWarning.Model(message=message, flags=flags)
        super().__init__(message)

    def parse_raw(raw):
        raw = ContinueWarning.Model.model_validate_json(raw)
        return ContinueWarning(raw.message, raw.flags)
