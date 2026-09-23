from typing import ClassVar, Optional

from pydantic import BaseModel, Field

from snapred.backend.dao.indexing.RunRange import RunRange


class Cycle(BaseModel):
    # SNS facility cycle designation, e.g. "2024-A": a four-digit year, a hyphen,
    #   and an uppercase letter identifying the run cycle within that year.
    CYCLE_ID_PATTERN: ClassVar[str] = r"^\d{4}-[A-Z]$"

    # Sentinel cycle ID used when cycle info is absent/invalid; reduction continues as "diagnostic".
    #   Deliberately does not match `CYCLE_ID_PATTERN`, so it can never collide with a real cycle.
    NO_CYCLE: ClassVar[str] = "unknown"

    cycleID: str = Field(pattern=CYCLE_ID_PATTERN)
    startDate: str
    stopDate: str
    firstRun: int

    # The cycle's final run, once it is known. A cycle that is still running has no last run
    #   yet, and `None` leaves its range unbounded above until one is recorded.
    lastRun: Optional[int] = None

    @property
    def runRange(self) -> RunRange:
        """The runs belonging to this cycle."""
        return RunRange(firstRun=self.firstRun, lastRun=self.lastRun)

    @classmethod
    def noCycle(cls) -> "Cycle":
        # Sentinel `Cycle` returned when cycle info is absent/invalid; reduction continues as
        #   "diagnostic". Built via `model_construct` to bypass `CYCLE_ID_PATTERN`, so its
        #   `cycleID` (`NO_CYCLE`) can never collide with a real cycle.
        return cls.model_construct(cycleID=cls.NO_CYCLE, startDate="", stopDate="", firstRun=0, lastRun=None)
