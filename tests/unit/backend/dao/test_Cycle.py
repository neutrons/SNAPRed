import re

import pytest
from pydantic import ValidationError

from snapred.backend.dao.state.Cycle import Cycle


def _cycle(cycleID: str) -> Cycle:
    return Cycle(cycleID=cycleID, startDate="2026-05-01", stopDate="2026-08-01", firstRun=100)


class TestCycleID:
    @pytest.mark.parametrize("cycleID", ["2024-A", "2026-B", "2026-B.1", "2026-B.9"])
    def test_accepted(self, cycleID):
        assert _cycle(cycleID).cycleID == cycleID

    @pytest.mark.parametrize(
        "cycleID",
        [
            "2026-b",  # lowercase letter
            "26-B",  # two-digit year
            "2026B",  # no hyphen
            "2026-B.",  # split marker without a number
            "2026-B.0",  # the unsplit cycle is "2026-B", not "2026-B.0"
            "2026-B.10",  # two digits would sort before "2026-B.2"
            "2026-B-1",  # wrong separator
            "2026-B.1 ",  # trailing space
        ],
    )
    def test_refused(self, cycleID):
        with pytest.raises(ValidationError):
            _cycle(cycleID)

    def test_splitsSortBetweenTheirCycleAndTheNext(self):
        # callers order cycles by ID, so a split must sort after its cycle and before the next one
        ids = ["2026-C", "2026-B.2", "2026-B", "2027-A", "2026-B.1"]
        assert sorted(ids) == ["2026-B", "2026-B.1", "2026-B.2", "2026-C", "2027-A"]
        assert all(re.match(Cycle.CYCLE_ID_PATTERN, c) for c in ids)

    def test_noCycleSentinelStillNeverMatches(self):
        assert not re.match(Cycle.CYCLE_ID_PATTERN, Cycle.NO_CYCLE)
        assert Cycle.noCycle().cycleID == Cycle.NO_CYCLE
