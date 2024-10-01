from typing import Any, TypeVar

from pydantic import ConfigDict, field_validator
from snapred.backend.dao.indexing.Versioning import VersionedObject

T = TypeVar("T")


class Record(VersionedObject[T], extra="allow"):
    """

    This is the basic, bare-bones record of a workflow completion.
    It contains only the run number, the resolution (native/lite), and a version.

    This is meant to coordinate with the Indexer service object.

    The class method `indexFromRecord` will create a compatible index entry from a record.

    """

    # inherits from VersionedObject
    # - version: int

    runNumber: str
    useLiteMode: bool

    # NOTE: Dropping the versioned CalculationParameters field
    #       Why did we need those to be versioned?

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
