import time
from typing import Any, Optional

import numpy as np
from pydantic import Field, field_validator, model_validator

from snapred.backend.dao.indexing.VersionedObject import VersionedObject


class IndexEntry(VersionedObject, extra="ignore"):
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

    @classmethod
    def parseConditional(cls, conditional: str):
        symbols = [">=", "<=", "<", ">"]
        # find first
        symbol = next((s for s in symbols if s in conditional), "")
        # parse runnumber
        runNumber = conditional if symbol == "" else conditional.split(symbol)[-1]
        return symbol, runNumber

    @classmethod
    def parseAppliesTo(cls, appliesTo: str):
        conditionals = appliesTo.split(",")
        return [cls.parseConditional(c.strip()) for c in conditionals]

    @field_validator("appliesTo", mode="before")
    def appliesToFormatChecker(cls, v):
        """
        This validator ensures that if appliesTo is present,
        it is in the format of 'runNumber', or '{symbol}runNumber' where symbol is one of '>', '<', '>=', '<='.
        """
        testValue = v
        if testValue is not None:
            conditionals = cls.parseAppliesTo(v)
            for _, runNumber in conditionals:
                try:
                    # if runnumber isnt just an int, there were extra unrecognized characters
                    int(runNumber)
                except ValueError:
                    raise ValueError(
                        "appliesTo must be in the format of 'runNumber',"
                        "or '{{symbol}}runNumber, ...' where symbol is one of '>', '<', '>=', '<='.."
                    )

        return v

    @field_validator("useLiteMode", mode="before")
    def convert_numpy_bool(cls, value):
        if isinstance(value, np.bool_):
            return bool(value)
        return value

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
