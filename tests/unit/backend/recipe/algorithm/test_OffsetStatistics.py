import unittest
from random import random

import numpy as np
from mantid.simpleapi import (
    CreateWorkspace,
    OffsetStatistics,
    mtd,
)
from snapred.meta.redantic import access_pointer


class TestOffsetStatistics(unittest.TestCase):
    def tearDown(self) -> None:
        mtd.clear()
        assert len(mtd.getObjectNames()) == 0
        return super().tearDown()

    def test_random(self):
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

        data = OffsetStatistics(wksp)
        data = access_pointer(data)
        assert data["medianOffset"] >= 0
        assert data["meanOffset"] >= 0
        assert data["medianOffset"] == abs(np.median(yVal))
        assert data["meanOffset"] == abs(np.mean(yVal))
        assert data["minOffset"] >= -1
        assert data["maxOffset"] <= 1
