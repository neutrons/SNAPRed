import importlib
import logging
import unittest

from mantid.simpleapi import (
    CreateTableWorkspace,
    DeleteWorkspace,
    mtd,
)

# the algorithm to test
from snapred.backend.recipe.algorithm.VerifyChiSquared import (
    VerifyChiSquared as Algo,  # noqa: E402
)
from snapred.meta.pointer import access_pointer, create_pointer

VC2Module = importlib.import_module(Algo.__module__)


class TestVerifyChiSquared(unittest.TestCase):
    def setUp(self):
        self.goodChi2 = 0.0
        self.badChi2 = 100.0
        self.indices = [1, 2, 3, 4]
        self.centres = [0.5, 1.0, 1.5, 2.0]
        goodDict = {
            "chi2": [self.goodChi2] * len(self.indices),
            "wsindex": self.indices,
            "centre": self.centres,
        }
        badDict = {
            "chi2": [self.badChi2] * len(self.indices),
            "wsindex": self.indices,
            "centre": self.centres,
        }
        mixedDict = {
            "chi2": [self.goodChi2, self.badChi2, self.badChi2, self.goodChi2],
            "wsindex": self.indices,
            "centre": self.centres,
        }
        self.all_good = CreateTableWorkspace(Data=create_pointer(goodDict), OutputWorkspace=mtd.unique_name())
        self.all_bad = CreateTableWorkspace(Data=create_pointer(badDict), OutputWorkspace=mtd.unique_name())
        self.mixed = CreateTableWorkspace(Data=create_pointer(mixedDict), OutputWorkspace=mtd.unique_name())

    def tearDown(self) -> None:
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        return super().tearDownClass()

    def test_validate_chi2(self):
        invalid = {"wsindex": [], "centre": []}
        ws = mtd.unique_name()
        tab = CreateTableWorkspace(Data=create_pointer(invalid), OutputWorkspace=ws)
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", tab)
        err = algo.validateInputs()
        assert "InputWorkspace" in err
        assert "chi2" in err["InputWorkspace"]
        assert "wsindex" not in err["InputWorkspace"]
        assert "centre" not in err["InputWorkspace"]

    def test_validate_wsindex(self):
        invalid = {"chi2": [], "centre": []}
        tab = CreateTableWorkspace(OutputWorkspace=mtd.unique_name(), Data=create_pointer(invalid))
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", tab)
        err = algo.validateInputs()
        assert "InputWorkspace" in err
        assert "chi2" not in err["InputWorkspace"]
        assert "wsindex" in err["InputWorkspace"]
        assert "centre" not in err["InputWorkspace"]

    def test_validate_centre(self):
        invalid = {"wsindex": [], "chi2": []}
        tab = CreateTableWorkspace(OutputWorkspace=mtd.unique_name(), Data=create_pointer(invalid))
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", tab)
        err = algo.validateInputs()
        assert "InputWorkspace" in err
        assert "chi2" not in err["InputWorkspace"]
        assert "wsindex" not in err["InputWorkspace"]
        assert "centre" in err["InputWorkspace"]

    def test_validate_all(self):
        invalid = {"notmuch": []}
        tab = CreateTableWorkspace(OutputWorkspace=mtd.unique_name(), Data=create_pointer(invalid))
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", tab)
        err = algo.validateInputs()
        assert "InputWorkspace" in err
        assert "chi2" in err["InputWorkspace"]
        assert "wsindex" in err["InputWorkspace"]
        assert "centre" in err["InputWorkspace"]

    def test_success_all_good(self):
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.all_good)
        algo.setProperty("MaximumChiSquared", self.badChi2)
        algo.execute()
        good = access_pointer(algo.getProperty("GoodPeaks").value)
        bad = access_pointer(algo.getProperty("BadPeaks").value)

        assert bad == []
        assert len(good) == len(self.indices)
        assert good == [
            {"Spectrum": x, "Peak Location": y, "Chi2": self.goodChi2} for x, y in zip(self.indices, self.centres)
        ]

    def test_success_all_bad(self):
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.all_bad)
        algo.setProperty("MaximumChiSquared", self.badChi2)
        algo.execute()
        good = access_pointer(algo.getProperty("GoodPeaks").value)
        bad = access_pointer(algo.getProperty("BadPeaks").value)

        assert good == []
        assert len(bad) == len(self.indices)
        assert bad == [
            {"Spectrum": x, "Peak Location": y, "Chi2": self.badChi2} for x, y in zip(self.indices, self.centres)
        ]

    def test_success_mixed(self):
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.mixed)
        algo.setProperty("MaximumChiSquared", self.badChi2)
        algo.execute()
        good = access_pointer(algo.getProperty("GoodPeaks").value)
        bad = access_pointer(algo.getProperty("BadPeaks").value)

        assert len(good) == 2
        assert len(bad) == 2

    def test_log_results_good(self):
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.all_good)
        algo.setProperty("MaximumChiSquared", self.badChi2)
        algo.setProperty("LogResults", True)
        with self.assertLogs(logger=VC2Module.logger, level=logging.INFO) as cm:
            algo.execute()
        assert "Sufficient number of well-fitted peaks" in cm.output[0]
        assert f"(chi2 < {self.badChi2})" in cm.output[0]
        assert f"{len(self.indices)}" in cm.output[0]

    def test_log_results_bad(self):
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.all_bad)
        algo.setProperty("MaximumChiSquared", self.badChi2)
        algo.setProperty("LogResults", True)
        with self.assertLogs(logger=VC2Module.logger, level=logging.INFO) as cm:
            algo.execute()
        assert "Insufficient number of well-fitted peaks" in cm.output[0]
        assert f"(chi2 < {self.badChi2})" in cm.output[0]
