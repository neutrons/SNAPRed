from typing import Optional

from pydantic import BaseModel


class CreateIndexEntryRequest(BaseModel):
    """
    The data needed to create an IndexEntry.
    """

    runNumber: str
    useLiteMode: bool
    version: Optional[int] = None
    comments: str
    author: str
    appliesTo: str
