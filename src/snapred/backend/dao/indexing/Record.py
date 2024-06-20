from typing import Any, Optional

from pydantic import ConfigDict, field_validator
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry, Nonentry
from snapred.backend.dao.indexing.Versioning import VersionedObject


class Record(VersionedObject, extra="allow"):
    """

    This is the basic, bare-bones record of a workflow completion.
    It contains only the run number, the resolution (native/lite), and a version.

    This is meant to coordinate with the Indexor service object.

    The class method `indexFromRecord` will create a compatible index entry from a record.

    The special Nonrecord object should be used in instances where a record is expected but none exists.
    Use this in place of None.

    """

    # inherits from VersionedObject
    # - version: int

    runNumber: str
    useLiteMode: bool
    # NOTE calculationParameters is NOT optional, and is enforced by a validator
    # the "None" case is restricted to a single case for the Nonrecord
    calculationParameters: Optional[CalculationParameters]

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

    @field_validator("calculationParameters", mode="before")
    @classmethod
    def validate_calculationParameters(cls, v: Any) -> Any:
        if v is None:
            raise ValueError("calculationParameters cannot be set to None")
        return v

    def model_dump_json(self, **kwargs):
        print(f"VERSION AT DUMP {self.version}: RECORD")
        return super().model_dump_json(**kwargs)

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )


Nonrecord = Record.model_construct(
    # NOTE use the Nonrecord when a record is expected, but none present.
    # Use this in preference to None.
    runNumber="none",
    useLiteMode=False,
    version=None,
    calculationParameters=None,
)
