from pydantic import BaseModel


class IndexEntryData(BaseModel):
    """
    The data needed to create an IndexEntry.
    """

    runNumber: str
    useLiteMode: bool
    comments: str
    author: str
    appliesTo: str
