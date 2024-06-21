import unittest

import pytest
from snapred.backend.service.MetadataLookupService import MetadataLookupService


class TestMetadataLookupService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.instance = MetadataLookupService()
        super().setUpClass()

    def setUp(self):
        return super().setUp()

    def test_verifyMultipleRuns(self):
        # Test valid number of runs
        runs = self.instance.verifyMultipleRuns("46342:46352")
        assert len(runs) == 10

        # Test exceeding max number of runs
        runs = self.instance.verifyMultipleRuns("46342:46355")
        assert len(runs) == 0

        # Test bad input
        with pytest.raises(Exception):  # noqa: PT011
            runs = self.instance.verifyMultipleRuns("46342, word, -1")
