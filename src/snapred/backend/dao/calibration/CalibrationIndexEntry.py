from typing import Optional

from pydantic import BaseModel, validator


class CalibrationIndexEntry(BaseModel):
    """Class to hold Calibration Index Entry data."""

    runNumber: str
    version: Optional[str]
    appliesTo: Optional[str]
    comments: str
    author: str
    timestamp: Optional[int]

    def parseAppliesTo(appliesTo: str):
        symbols = [">=", "<=", "<", ">"]
        # find first
        symbol = next((s for s in symbols if s in appliesTo), "")
        # parse runnumber
        runNumber = appliesTo if symbol == "" else appliesTo.split(symbol)[-1]
        return symbol, runNumber

    @validator("appliesTo", allow_reuse=True)
    def appliesToFormatChecker(cls, v):
        """
        This validator ensures that if appliesTo is present,
        it is in the format of 'runNumber', or '{symbol}runNumber' where symbol is one of '>', '<', '>=', '<='.
        """
        testValue = v
        if testValue is not None:
            symbol, _ = cls.parseAppliesTo(v)
            if symbol != "":
                testValue = testValue.split(symbol)[-1]
            try:
                int(testValue)
            except ValueError:
                raise ValueError(
                    "appliesTo must be in the format of 'runNumber',"
                    "or '\{symbol\}runNumber' where symbol is one of '>', '<', '>=', '<='.."
                )

        return v
