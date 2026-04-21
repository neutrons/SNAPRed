from pydantic import BaseModel

from snapred.backend.dao.state.Cycle import Cycle


class UpdateCycleRequest(BaseModel):
    runNumber: str
    cycle: Cycle
    appliesTo: str
    author: str
