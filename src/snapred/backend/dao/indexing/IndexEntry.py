import time
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class IndexEntry(BaseModel, extra="ignore"):
    """

    This is the basic, bare-bones entry for workflow indices.
    It records a run number, the resolution (native/lite), and a version.
    The appliesTo field will indicate which runs the record applies to,
    so that only applicable calibrations/normalizations can be loaded.

    This is meant to coordinate with the Indexer service object.

    """

    runNumber: str
    useLiteMode: bool
    appliesTo: Optional[str] = None
    comments: Optional[str] = None
    author: Optional[str] = None
    timestamp: float = Field(default_factory=lambda: time.time())

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
                    "or '{{symbol}}runNumber' where symbol is one of '>', '<', '>=', '<='.."
                )

        return v

    @model_validator(mode="before")
    @classmethod
    def validate_timestamp(cls, v: Any):
        if isinstance(v, dict):
            if "timestamp" in v:
                timestamp = v["timestamp"]
                # support reading the _legacy_ timestamp integer encoding
                if isinstance(timestamp, int):
                    v["timestamp"] = float(timestamp) / 1000.0
        return v
