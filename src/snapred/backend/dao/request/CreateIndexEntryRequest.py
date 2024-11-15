from pydantic import BaseModel

from snapred.backend.dao.indexing.Versioning import Version, VersionState


class CreateIndexEntryRequest(BaseModel):
    """
    The data needed to create an IndexEntry.
    """

    runNumber: str
    useLiteMode: bool
    version: Version = VersionState.NEXT
    comments: str
    author: str
    appliesTo: str
