from pydantic import BaseModel

from snapred.backend.dao.state.Cycle import Cycle


class UpdateCycleRequest(BaseModel):
    cycle: Cycle
    author: str
