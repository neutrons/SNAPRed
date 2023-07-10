import json
import unittest.mock as mock

import pytest

# with mock.patch.dict(
#     "sys.modules",
#     {
#         "snapred.backend.log": mock.Mock(),
#         "snapred.backend.log.logger": mock.Mock(),
#     },
# ):

# from mantid.simpleapi import DeleteWorkspace, mtd

# from snapred.backend.dao.calibration.Calibration import Calibration
# from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
#     PixelGroupingParametersCalculationAlgorithm,
# )
# from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import (
#     CrystallographicInfo,
# )
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import (
    SmoothDataExcludingPeaks,
)
# from snapred.meta.Config import Resource

# Define test data paths
spectrum_data_path = "/home/dzj/Documents/Work/csaps/DSP_58882_cal_CC_Column.nxs"
weights_data_path = "/home/dzj/Documents/Work/csaps/stripPeaksWeight_58882_Column.nxs"

def testSetup(self):
    testAlgo = SmoothDataExcludingPeaks()
    testAlgo.initialize()

def testProperties(self):
    assert self.testAlgo.getProperty("InputWorkspace").value == ""
    assert self.testAlgo.getProperty("nxsPath").value == ""
    assert self.testAlgo.getProperty("OutputWorkspace").value == ""