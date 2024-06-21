from typing import Optional

from pydantic import field_validator
from snapred.backend.dao.indexing.Versioning import VersionedObject


class IndexEntry(VersionedObject, extra="ignore"):
    """

    This is the basic, bare-bones entry for workflow indices.
    It records a run number, the resolution (native/lite), and a version.
    The appliesTo field will indicate which runs the record applies to,
    so that only applicable calibrations/normalizations can be loaded.

    This is meant to coordinate with the Indexor service object.

    """

    # inherits from VersionedObject:
    # - version: int

    runNumber: str
    useLiteMode: bool
    appliesTo: Optional[str] = None
    comments: Optional[str] = None
    author: Optional[str] = None
    timestamp: Optional[int] = None

    def parseAppliesTo(appliesTo: str):
        symbols = [">=", "<=", "<", ">"]
        # find first
        symbol = next((s for s in symbols if s in appliesTo), "")
        # parse runnumber
        runNumber = appliesTo if symbol == "" else appliesTo.split(symbol)[-1]
        return symbol, runNumber

    @field_validator("appliesTo", mode="before")
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
