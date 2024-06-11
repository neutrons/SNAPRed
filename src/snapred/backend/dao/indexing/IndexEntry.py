from typing import Literal, Optional, Union

from pydantic import BaseModel, Extra, validator
from snapred.meta.Config import Config

UNINITIALIZED = Config["version.error"]
Version = Union[int, Literal[UNINITIALIZED]]


class IndexEntry(BaseModel, extra=Extra.ignore):
    """

    This is the basic, bare-bones entry for workflow indices.
    It records a run number, the resolution (native/lite), and a version.
    The appliesTo field will indicate which runs the record applies to,
    so that only applicable calibrations/normalizations can be loaded.

    This is meant to coordinate with the Indexor service object.

    The special Nonentry object should be used in instances where an entry is expected,
    but none exists.  Use in place of None.

    """

    runNumber: str
    useLiteMode: bool
    version: Version = UNINITIALIZED
    appliesTo: Optional[str]
    comments: Optional[str]
    author: Optional[str]
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


Nonentry = IndexEntry(
    # NOTE use the Nonentry in instances where an entry is expected but none exists.
    # Use this in preference to None.
    runNumber="none",
    useLiteMode=False,
    version=UNINITIALIZED,
    appliesTo="<0",
    comments="this is a non-entry - do not use",
    author="SNAPRed Internal",
    timestamp=0,
)
