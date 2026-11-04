from pydantic import BaseModel


class Cycle(BaseModel):
    cycleID: str
    startDate: str
    stopDate: str
    firstRun: int
