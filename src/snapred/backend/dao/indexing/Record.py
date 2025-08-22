from typing import Any, Dict, List

import numpy
from pydantic import ConfigDict, Field, field_serializer, field_validator

from snapred.backend.dao.Hook import Hook
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexedObject import IndexedObject
from snapred.meta.Config import Config


class Record(IndexedObject, extra="allow"):
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

    # NOTE calculationParameters is a VERSIONED object.
    #   the version on the calculation parameters MUST match the version on this record.
    # TODO (complicated): add a validator to enforce this.
    calculationParameters: CalculationParameters

    hooks: Dict[str, List[Hook]] | None = None

    snapredVersion: str = Field(default_factory=lambda: Config.snapredVersion())
    snapwrapVersion: str | None = Field(default_factory=lambda: Config.snapwrapVersion())

    @field_validator("runNumber", mode="before")
    @classmethod
    def validate_runNumber(cls, v: Any) -> Any:
        if isinstance(v, int):
            v = str(v)
        return v

    @field_validator("useLiteMode", mode="before")
    @classmethod
    def validate_useLiteMode(cls, v):
        # this allows the use of 'strict' mode
        if isinstance(v, numpy.bool_):
            # Conversion from HDF5 metadata.
            v = bool(v)
        return v

    @field_serializer("snapredVersion")
    @classmethod
    def serialize_snapredVersion(cls, v: str) -> str:
        if v is None or v == "" or v == "unknown":
            raise ValueError(
                f"snapredVersion is required for Record serialization. current value: {v}",
            )
        return v

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True
    )
