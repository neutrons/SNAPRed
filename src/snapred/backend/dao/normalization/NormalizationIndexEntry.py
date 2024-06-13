from typing import Optional

from pydantic import BaseModel, field_validator

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
    version: Optional[int] = None
    appliesTo: Optional[str] = None
    comments: Optional[str] = None
    author: Optional[str] = None
    timestamp: Optional[int] = None

    @field_validator("appliesTo")
    @classmethod
    def appliesToFormatChecker(cls, v):
        """

        Validator ensures 'appliesTo' adheres to the specified format if present, enhancing the
        integrity of data referencing.

        """
        return CalibrationIndexEntry.appliesToFormatChecker(v)
