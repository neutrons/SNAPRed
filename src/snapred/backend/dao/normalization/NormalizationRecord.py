from typing import Any

from pydantic import field_validator

from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import VersionedObject
from snapred.backend.dao.normalization.NormalizationMetadata import NormalizationMetadata


# TODO: Also make Record a wrapper for data, as to not complicate the inheritance chain
#       by composing functinality via inheritance instead of composition.
class NormalizationRecord(Record, extra="ignore"):
    """

    This class is crucial for tracking the specifics of each normalization step, facilitating
    reproducibility and data management within scientific workflows. It serves as a comprehensive
    record of the parameters and context of normalization operations, contributing to the integrity
    and utility of the resulting data.

    """

    # inherits from Record
    # - runNumber
    # - useLiteMode
    # - version
    data: NormalizationMetadata

    # must also parse integers as background run numbers
    @field_validator("data", mode="before")
    @classmethod
    def validate_backgroundRunNumber(cls, v: Any) -> Any:
        return cls.validate_runNumber(v.backgroundRunNumber)

    @field_validator("data", mode="before")
    @classmethod
    def version_is_integer(cls, v: Any) -> Any:
        return VersionedObject.parseVersion(v.calibrationVersionUsed)
