from typing import Optional

from pydantic import BaseModel, validator


class NormalizationIndexEntry(BaseModel):
    """Class to hold Normalization Index Entry data."""

    runNumber: str
    backgroundRunNumber: str
    version: Optional[str]
    appliesTo: Optional[str]
    comments: str
    author: str
    timestamp: Optional[int]

    @validator("appliesTo", allow_reuse=True)
    def appliesToFormatChecker(cls, v):
        """
        This validator ensures that if appliesTo is present,
        it is in the format of 'runNumber', '>runNumber', or '<runNumber'.
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
