from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Extra, Field, field_validator
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import UNINITIALIZED, IndexEntry, Nonentry, Version


class Record(BaseModel, extra=Extra.allow):
    """

    This is the basic, bare-bones record of a workflow completion.
    It contains only the run number, the resolution (native/lite), and a version.

    This is meant to coordinate with the Indexor service object.

    The class method `indexFromRecord` will create a compatible index entry from a record.

    The special Nonrecord object should be used in instances where a record is expected but none exists.
    Use this in place of None.

    """

    runNumber: str
    useLiteMode: bool
    version: Version = UNINITIALIZED
    calculationParameters: Optional[CalculationParameters] = Field(None, exclude=True)

    @classmethod
    def indexEntryFromRecord(cls, record) -> IndexEntry:
        entry = Nonentry
        if record is not Nonrecord:
            entry = IndexEntry(
                runNumber=record.runNumber,
                useLiteMode=record.useLiteMode,
                version=record.version,
                appliesTo=f">={record.runNumber}",
                author="SNAPRed Internal",
                comments="This index entry was created from a record",
                timestamp=0,
            )
        return entry

    @field_validator("runNumber", mode="before")
    @classmethod
    def validate_runNumber(cls, v: Any) -> Any:
        if isinstance(v, int):
            v = str(v)
        return v

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )


Nonrecord = Record(
    # NOTE use the Nonrecord when a record is expected, but none present.
    # Use this in preference to None.
    runNumber="none",
    useLiteMode=False,
    version=UNINITIALIZED,
)
