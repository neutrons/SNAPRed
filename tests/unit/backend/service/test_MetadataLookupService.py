import importlib
import logging
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
        runs = self.instance.verifyMultipleRuns("46342:46352")
        assert len(runs) == 10

        # Test exceeding max number of runs
        maxRuns = Config["instrument.maxNumberOfRuns"]
        with self.assertLogs(logger=ServiceModule.logger, level=logging.WARNING) as cm:
            runs = self.instance.verifyMultipleRuns("46342:46355")
        assert f"Maximum value of {maxRuns} run numbers exceeded" in cm.output[0]

        # Test removing invalid runs from list
        runs = self.instance.verifyMultipleRuns("1,2,46343")
        assert len(runs) == 1

        # Test invalid input
        with pytest.raises(Exception):  # noqa: PT011
            runs = self.instance.verifyMultipleRuns("46342, word, -1")
