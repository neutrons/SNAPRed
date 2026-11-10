from typing import List, Optional

from pydantic import BaseModel, model_validator

from snapred.backend.dao.indexing.IndexEntry import IndexEntry


class RunRange(BaseModel):
    """
    An inclusive range of run numbers.

    A `lastRun` of None means the range has no upper bound.

    An `appliesTo` string holds the same information. `fromAppliesTo` and `toAppliesTo`
    convert between the two forms.
    """

    firstRun: int
    lastRun: Optional[int] = None

    @model_validator(mode="after")
    def validateOrder(self) -> "RunRange":
        if self.lastRun is not None and self.lastRun < self.firstRun:
            raise ValueError(f"a run range cannot end before it starts: {self.firstRun} to {self.lastRun}")
        return self

    @classmethod
    def fromAppliesTo(cls, appliesTo: Optional[str]) -> "RunRange":
        """
        The runs an `appliesTo` expression selects.

        Each comparison in the expression either raises the lower bound or lowers the upper
        bound. An entry with no expression applies to every run.
        """
        if appliesTo is None:
            return cls(firstRun=0, lastRun=None)

        firstRun, lastRun = 0, None
        for symbol, runNumber in IndexEntry.parseAppliesTo(appliesTo):
            run = int(runNumber)
            if symbol == ">=":
                firstRun = max(firstRun, run)
            elif symbol == ">":
                firstRun = max(firstRun, run + 1)
            elif symbol == "<=":
                lastRun = run if lastRun is None else min(lastRun, run)
            elif symbol == "<":
                lastRun = run - 1 if lastRun is None else min(lastRun, run - 1)
            else:
                # a bare run number is an equality: the range is that single run
                firstRun = max(firstRun, run)
                lastRun = run if lastRun is None else min(lastRun, run)
        return cls(firstRun=firstRun, lastRun=lastRun)

    def toAppliesTo(self) -> str:
        """The `appliesTo` expression selecting exactly this range."""
        if self.lastRun is None:
            return f">={self.firstRun}"
        return f">={self.firstRun},<={self.lastRun}"

    @property
    def runAfter(self) -> Optional[int]:
        """The first run past the end of this range, or None if it is unbounded above."""
        return None if self.lastRun is None else self.lastRun + 1

    def contains(self, runNumber: int) -> bool:
        return runNumber >= self.firstRun and (self.lastRun is None or runNumber <= self.lastRun)

    def overlaps(self, other: "RunRange") -> bool:
        return self.intersection(other) is not None

    def difference(self, other: "RunRange") -> List["RunRange"]:
        """
        The runs in this range but not in `other`.

        Removing runs from the middle leaves two pieces, so this returns a list.
        """
        overlap = self.intersection(other)
        if overlap is None:
            return [self]
        pieces = []
        if overlap.firstRun > self.firstRun:
            pieces.append(RunRange(firstRun=self.firstRun, lastRun=overlap.firstRun - 1))
        if overlap.lastRun is not None and (self.lastRun is None or overlap.lastRun < self.lastRun):
            pieces.append(RunRange(firstRun=overlap.lastRun + 1, lastRun=self.lastRun))
        return pieces

    def intersection(self, other: "RunRange") -> Optional["RunRange"]:
        """The runs in both ranges, or None if they do not meet."""
        firstRun = max(self.firstRun, other.firstRun)
        if self.lastRun is None:
            lastRun = other.lastRun
        elif other.lastRun is None:
            lastRun = self.lastRun
        else:
            lastRun = min(self.lastRun, other.lastRun)

        if lastRun is not None and lastRun < firstRun:
            return None
        return RunRange(firstRun=firstRun, lastRun=lastRun)
