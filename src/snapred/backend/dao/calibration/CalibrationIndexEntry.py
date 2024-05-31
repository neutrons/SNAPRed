from typing import Optional

from pydantic import BaseModel, validator


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
    version: Optional[int]
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
