from typing import Any

from pydantic import ConfigDict, field_validator
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VersionedObject


class Record(VersionedObject, extra="allow"):
    """

    This is the basic, bare-bones record of a workflow completion.
    It contains only the run number, the resolution (native/lite), and a version.

    This is meant to coordinate with the Indexor service object.

    The class method `indexFromRecord` will create a compatible index entry from a record.

    """

    # inherits from VersionedObject
    # - version: int

    runNumber: str
    useLiteMode: bool
    # NOTE calculationParameters is a VERSIONED object.
    # the version on the calculation parameters MUST match the version on this record.
    # a future validator should enforce this condition
    calculationParameters: CalculationParameters

    @classmethod
    def indexEntryFromRecord(cls, record) -> IndexEntry:
        entry = None
        if record is not None:
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

    @field_validator("calculationParameters", mode="before")
    @classmethod
    def validate_calculationParameters(cls, v: Any) -> Any:
        if v is None:
            raise ValueError("calculationParameters cannot be set to None")
        return v

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
