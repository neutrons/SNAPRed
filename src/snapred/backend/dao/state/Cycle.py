from typing import ClassVar

from pydantic import BaseModel, Field


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
