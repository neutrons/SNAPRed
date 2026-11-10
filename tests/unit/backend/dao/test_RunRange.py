import pytest

from snapred.backend.dao.indexing.RunRange import RunRange


class TestRunRangeFromAppliesTo:
    def test_boundedBothSides(self):
        assert RunRange.fromAppliesTo(">=100,<=199") == RunRange(firstRun=100, lastRun=199)

    def test_boundedBelowOnly(self):
        assert RunRange.fromAppliesTo(">=100") == RunRange(firstRun=100, lastRun=None)

    def test_exclusiveBoundsNameTheNeighbouringRun(self):
        # ">100" starts at 101 and "<200" ends at 199
        assert RunRange.fromAppliesTo(">100,<200") == RunRange(firstRun=101, lastRun=199)

    def test_bareRunNumberIsASingleRun(self):
        assert RunRange.fromAppliesTo("100") == RunRange(firstRun=100, lastRun=100)

    def test_noExpressionAppliesEverywhere(self):
        assert RunRange.fromAppliesTo(None) == RunRange(firstRun=0, lastRun=None)


class TestRunRangeToAppliesTo:
    @pytest.mark.parametrize("appliesTo", [">=100,<=199", ">=100"])
    def test_roundTrip(self, appliesTo):
        assert RunRange.fromAppliesTo(appliesTo).toAppliesTo() == appliesTo

    def test_exclusiveBoundsNormalise(self):
        # the range is what matters, so exclusive bounds come back as the inclusive ones
        assert RunRange.fromAppliesTo(">100,<200").toAppliesTo() == ">=101,<=199"


class TestRunRangeOperations:
    def test_runAfter(self):
        assert RunRange(firstRun=100, lastRun=199).runAfter == 200
        assert RunRange(firstRun=100).runAfter is None

    @pytest.mark.parametrize(("runNumber", "expected"), [(99, False), (100, True), (199, True), (200, False)])
    def test_contains(self, runNumber, expected):
        assert RunRange(firstRun=100, lastRun=199).contains(runNumber) is expected

    def test_unboundedRangeContainsAnyLaterRun(self):
        assert RunRange(firstRun=100).contains(10**6)

    def test_intersectionOfOverlappingRanges(self):
        assert RunRange(firstRun=100, lastRun=250).intersection(RunRange(firstRun=200, lastRun=300)) == RunRange(
            firstRun=200, lastRun=250
        )

    def test_intersectionWithUnboundedRange(self):
        assert RunRange(firstRun=100, lastRun=250).intersection(RunRange(firstRun=200)) == RunRange(
            firstRun=200, lastRun=250
        )

    def test_disjointRangesDoNotMeet(self):
        assert RunRange(firstRun=100, lastRun=199).intersection(RunRange(firstRun=200, lastRun=300)) is None
        assert not RunRange(firstRun=100, lastRun=199).overlaps(RunRange(firstRun=200, lastRun=300))

    def test_adjacentRangesDoNotOverlap(self):
        # a cycle ending at 199 and the next starting at 200 must not be treated as meeting
        assert not RunRange(firstRun=100, lastRun=199).overlaps(RunRange(firstRun=200))

    def test_rangeCannotEndBeforeItStarts(self):
        with pytest.raises(ValueError, match="cannot end before it starts"):
            RunRange(firstRun=200, lastRun=100)
