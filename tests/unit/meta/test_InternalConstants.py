import unittest

from snapred.meta.InternalConstants import ReservedRunNumber, ReservedStateId


class TestCallback(unittest.TestCase):
    def test_state_from_run(self):
        reservedRunNumbers = ReservedRunNumber.values()
        reservedStateIds = ReservedStateId.values()
        for x, y in zip(reservedRunNumbers, reservedStateIds):
            assert y == ReservedStateId.forRun(x)
