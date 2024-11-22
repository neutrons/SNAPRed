from typing import List

from pydantic import BaseModel


class MatchRunsRequest(BaseModel):
    runNumbers: List[str]
    useLiteMode: bool
