from typing import Optional

from pydantic import BaseModel, validator

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry


class NormalizationIndexEntry(BaseModel):
    """

    This class represents a Normalization Index Entry object with various attributes and a custom validator.
    The purpose of this class is to model a normalization index entry with attributes like runNumber,
    backgroundRunNumber, version, appliesTo, comments, author, and timestamp. It also includes a custom
    validator method called appliesToFormatChecker to validate the format of the appliesTo attribute if it
    is present.

    """

    runNumber: str
    useLiteMode: bool
    backgroundRunNumber: str
    version: Optional[int]
    appliesTo: Optional[str]
    comments: Optional[str]
    author: Optional[str]
    timestamp: Optional[int]

    @validator("appliesTo", allow_reuse=True)
    def appliesToFormatChecker(cls, v):
        """

        Validator ensures 'appliesTo' adheres to the specified format if present, enhancing the
        integrity of data referencing.

        """
        return CalibrationIndexEntry.appliesToFormatChecker(v)
