from pydantic import ConfigDict

from snapred.backend.dao.normalization import NormalizationRecord
from snapred.backend.dao.request.CreateIndexEntryRequest import CreateIndexEntryRequest


class CreateNormalizationRecordRequest(NormalizationRecord, extra="forbid"):
    """
    The needed data to create a NormalizationRecord.
    """

    indexEntry: CreateIndexEntryRequest
    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )

    @classmethod
    def parseVersion(cls, version, *, exclude_none: bool = False, exclude_default: bool = False) -> int | None:  # noqa: ARG003
        return version
