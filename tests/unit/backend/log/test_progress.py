import unittest
from unittest.mock import MagicMock
from mantid.api import Progress
from snapred.backend.log.logger import snapredLogger

class TestSnapRedLogger(unittest.TestCase):
    def setUp(self):
        self.logger = snapredLogger.getLogger(__name__)
        snapredLogger.resetProgress(0, 1.0, 10)

    def test_resetProgress(self):

        self.assertEqual(snapredLogger._progressCounter, 0)

        original_progress = snapredLogger._progressReporter
        snapredLogger._progressReporter = MagicMock(spec=Progress)
        
        snapredLogger.reportProgress("Initial step")
        snapredLogger.reportProgress("Second step")
        
        expected_calls = [unittest.mock.call(0, "Initial step"), unittest.mock.call(1, "Second step")]
        snapredLogger._progressReporter.report.assert_has_calls(expected_calls, any_order=False)
        
        snapredLogger.resetProgress(0, 1.0, 20)
        self.assertEqual(snapredLogger._progressCounter, 0)

        snapredLogger._progressReporter = original_progress