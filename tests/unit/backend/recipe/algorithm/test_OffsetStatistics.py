import unittest
from random import random
from typing import Dict

import numpy as np
from mantid.simpleapi import (
    CreateWorkspace,
    mtd,
)
from pydantic import TypeAdapter
from snapred.backend.recipe.algorithm.OffsetStatistics import OffsetStatistics as Algo


class TestGOffsetStatistics(unittest.TestCase):
    def tearDown(self) -> None:
        mtd.clear()
        assert len(mtd.getObjectNames()) == 0
        return super().tearDown()

    def test_init(self):
        algo = Algo()
        algo.initialize()

    def test_random(self):
        algo = Algo()
        algo.initialize()

        xVal = []
        yVal = []
        for i in range(100):
            xVal.append(i)
            yVal.append(random() * 2 - 1)
        wksp = mtd.unique_name(5, "offset_stats_")
        CreateWorkspace(
            OutputWorkspace=wksp,
            DataX=xVal,
            DataY=yVal,
        )

        algo.setProperty("OffsetsWorkspace", wksp)
        assert algo.execute()
        data = TypeAdapter(Dict[str, float]).validate_json(algo.getPropertyValue("Data"))
        assert data["medianOffset"] >= 0
        assert data["meanOffset"] >= 0
        assert data["medianOffset"] == abs(np.median(yVal))
        assert data["meanOffset"] == abs(np.mean(yVal))
        assert data["minOffset"] >= -1
        assert data["maxOffset"] <= 1
