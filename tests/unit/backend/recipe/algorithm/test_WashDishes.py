import unittest

import pytest

# mantid imports
from mantid.simpleapi import CreateWorkspace, mtd

# the algorithm to test
from snapred.backend.recipe.algorithm.WashDishes import WashDishes
from snapred.meta.Config import Config


class TestWashDishes(unittest.TestCase):
    def test_wash_one_dish(self):
        wsname = "_test_wash_one_dish"
        CreateWorkspace(OutputWorkspace=wsname, DataX=1, DataY=1)
        assert wsname in mtd
        algo = WashDishes()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        # verify workspace not deleted when in CIS mode
        algo._CISmode = True
        algo.execute()
        assert wsname in mtd
        # verify workspace deleted when not in CIS mode
        algo._CISmode = False
        algo.execute()
        assert wsname not in mtd

    def test_wash_some_dishes(self):
        wsnames = []
        for i in range(10):
            wsnames.append(f"_test_wash_dish_{i}")

        for wsname in wsnames:
            CreateWorkspace(OutputWorkspace=wsname, DataX=1, DataY=1)
            assert wsname in mtd
        algo = WashDishes()
        algo.initialize()
        algo.setProperty("WorkspaceList", wsnames)
        # verify no workapces deleted when in CIS mode
        algo._CISmode = True
        algo.execute()
        for wsname in wsnames:
            assert wsname in mtd
        # verify all workspaces deleted when not in CIS mode
        algo._CISmode = False
        algo.execute()
        for wsname in wsnames:
            assert wsname not in mtd

    def test_wash_dish_using_yaml(self):
        # verify that the algorithm will behave as expected with the config key
        wsname = "_test_wash_one_dish"
        cismode = Config["cis_mode"]
        CreateWorkspace(OutputWorkspace=wsname, DataX=1, DataY=1)
        assert wsname in mtd
        algo = WashDishes()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        algo.execute()
        if cismode:
            assert wsname in mtd
        elif not cismode:
            assert wsname not in mtd
