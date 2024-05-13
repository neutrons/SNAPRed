from typing import Optional

from pydantic import BaseModel, validator


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
    version: Optional[str]
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
        testValue = v
        if testValue is not None:
            if testValue.startswith(">") or testValue.startswith("<"):
                testValue = testValue[1:]
                try:
                    int(testValue)
                except ValueError:
                    raise ValueError("appliesTo must be in the format of 'runNumber', '>runNumber', or '<runNumber'.")

        return v
