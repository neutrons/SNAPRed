from typing import Optional

from pydantic import BaseModel, field_validator


class CalibrationIndexEntry(BaseModel):
    """

    The CalibrationIndexEntry class is a Pydantic model designed to encapsulate the details of
    calibration index entries, including essential information like runNumber, version, and
    comments, along with the author and a timestamp. It features a specialized method, parseAppliesTo,
    to interpret the appliesTo field, which indicates the applicability of the calibration entry,
    supporting comparisons with symbols such as '>', '<', '>=', and '<='. Additionally, a validator,
    appliesToFormatChecker, ensures the appliesTo field conforms to expected formats, either a simple
    'runNumber' or a comparison format, enhancing data integrity by enforcing consistent entry formats.

    """

    runNumber: str
    useLiteMode: bool
    version: Optional[int] = None
    appliesTo: Optional[str] = None
    comments: str
    author: str
    timestamp: Optional[float] = None

    def parseAppliesTo(appliesTo: str):
        symbols = [">=", "<=", "<", ">"]
        # find first
        symbol = next((s for s in symbols if s in appliesTo), "")
        # parse runnumber
        runNumber = appliesTo if symbol == "" else appliesTo.split(symbol)[-1]
        return symbol, runNumber

    @field_validator("appliesTo")
    @classmethod
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
                    "or '{symbol}runNumber' where symbol is one of '>', '<', '>=', '<='.."
                )

        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def timestamp_validator(cls, v):
        """
        Read both new and legacy timestamp format.
        """
        if v is not None:
            if isinstance(v, float):
                v = int(round(v * 1000.0))
        return v
