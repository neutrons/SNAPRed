from pydantic import BaseModel, Extra
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

    @classmethod
    def indexEntryFromRecord(record) -> IndexEntry:
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


Nonrecord = Record(
    runNumber="none",
    useLiteMode=False,
    version=UNINITIALIZED,
)
