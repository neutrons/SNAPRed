import importlib
import unittest

import pytest
from snapred.backend.service.MetadataLookupService import MetadataLookupService
from snapred.meta.Config import Config

ServiceModule = importlib.import_module(MetadataLookupService.__module__)


class TestMetadataLookupService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.instance = MetadataLookupService()
        super().setUpClass()

    def test_verifyMultipleRuns(self):
        # Test valid number of runs
        runs = self.instance.verifyMultipleRuns("46342:46351")
        assert len(runs) == 10

    def test_too_many_runs(self):
        # Test exceeding max number of runs
        maxRuns = Config["instrument.maxNumberOfRuns"]
        with pytest.raises(ValueError, match=f"Maximum value of {maxRuns} run numbers exceeded*"):
            self.instance.verifyMultipleRuns("46342:46355")

    def test_remove_invalid_runs(self):
        # Test removing invalid runs from list
        runs = self.instance.verifyMultipleRuns("1,2,46343")
        assert len(runs) == 1

    def test_invalid_input(self):
        # Test invalid input
        with pytest.raises(Exception):  # noqa: PT011
            self.instance.verifyMultipleRuns("46342, word, -1")
