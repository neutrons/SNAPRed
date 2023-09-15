import unittest

import pytest

# mantid imports
from mantid.simpleapi import CreateSampleWorkspace, mtd
from snapred.backend.recipe.algorithm.WashDishes import WashDishes

# the algorithm to test
from snapred.meta.Config import Config


class TestWashDishes(unittest.TestCase):
    def test_wash_one_dish(self):
        wsname = "_test_wash_one_dish"
        CreateSampleWorkspace(
            OutputWorkspace=wsname,
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=10,
            Xmax=100,
            BinWidth=1,
            XUnit="TOF",
            NumBanks=14,
            BankPixelWidth=2,
            Random=True,
        )
        assert wsname in mtd
        algo = WashDishes()
        algo.initialize()
        algo._CISmode = False
        algo.setProperty("Workspace", wsname)
        algo.execute()
        assert wsname not in mtd

    def test_wash_some_dishes(self):
        wsnames = []
        for i in range(10):
            wsnames.append(f"_test_wash_dish_{i}")

        for wsname in wsnames:
            CreateSampleWorkspace(
                OutputWorkspace=wsname,
                Function="User Defined",
                UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
                Xmin=10,
                Xmax=100,
                BinWidth=1,
                XUnit="TOF",
                NumBanks=14,
                BankPixelWidth=2,
                Random=True,
            )
            assert wsname in mtd
        algo = WashDishes()
        algo.initialize()
        algo._CISmode = False
        algo.setProperty("WorkspaceList", wsnames)
        algo.execute()
        for wsname in wsnames:
            assert wsname not in mtd

    def test_wash_dish_in_cis_mode(self):
        wsname = "_test_wash_one_dish"
        CreateSampleWorkspace(
            OutputWorkspace=wsname,
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=10,
            Xmax=100,
            BinWidth=1,
            XUnit="TOF",
            NumBanks=14,
            BankPixelWidth=2,
            Random=True,
        )
        assert wsname in mtd
        algo = WashDishes()
        algo.initialize()
        algo._CISmode = True
        algo.setProperty("Workspace", wsname)
        algo.execute()
        assert wsname in mtd

    def test_wash_some_dishes_in_cis_mode(self):
        wsnames = []
        for i in range(10):
            wsnames.append(f"_test_wash_dish_{i}")

        for wsname in wsnames:
            CreateSampleWorkspace(
                OutputWorkspace=wsname,
                Function="User Defined",
                UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
                Xmin=10,
                Xmax=100,
                BinWidth=1,
                XUnit="TOF",
                NumBanks=14,
                BankPixelWidth=2,
                Random=True,
            )
            assert wsname in mtd
        algo = WashDishes()
        algo.initialize()
        algo._CISmode = True
        algo.setProperty("WorkspaceList", wsnames)
        algo.execute()
        for wsname in wsnames:
            assert wsname in mtd

    def test_wash_dish_using_yaml(self):
        wsname = "_test_wash_one_dish"
        cismode = Config["cis_mode"]
        CreateSampleWorkspace(
            OutputWorkspace=wsname,
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=10,
            Xmax=100,
            BinWidth=1,
            XUnit="TOF",
            NumBanks=14,
            BankPixelWidth=2,
            Random=True,
        )
        assert wsname in mtd
        algo = WashDishes()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        algo.execute()
        if cismode:
            assert wsname in mtd
        elif not cismode:
            assert wsname not in mtd


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
