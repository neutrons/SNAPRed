import unittest

import numpy as np
from mantid.simpleapi import (
    ConvertUnits,
    mtd,
)
from util.diffraction_calibration_synthetic_data import SyntheticData

from snapred.backend.dao.ingredients.ArtificialNormalizationIngredients import ArtificialNormalizationIngredients
from snapred.backend.recipe.algorithm.CreateArtificialNormalizationAlgo import CreateArtificialNormalizationAlgo as Algo


class TestCreateArtificialNormalizationAlgo(unittest.TestCase):
    def setUp(self):
        self.inputs = SyntheticData()
        self.fakeIngredients = self.inputs.ingredients

        self.fakeRawData = "test_data_ws"
        self.fakeGroupingWorkspace = "test_grouping_ws"
        self.fakeMaskWorkspace = "test_mask_ws"
        self.inputs.generateWorkspaces(self.fakeRawData, self.fakeGroupingWorkspace, self.fakeMaskWorkspace)

        ConvertUnits(
            InputWorkspace=self.fakeRawData,
            OutputWorkspace=self.fakeRawData,
            Target="dSpacing",
        )

    def tearDown(self) -> None:
        mtd.clear()
        assert len(mtd.getObjectNames()) == 0
        return super().tearDown()

    def test_chop_ingredients(self):
        self.fakeIngredients = ArtificialNormalizationIngredients(
            peakWindowClippingSize=10,
            smoothingParameter=0.5,
            decreaseParameter=True,
            lss=True,
        )
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("OutputWorkspace", "test_output_ws")
        originalData = []
        inputWs = mtd[self.fakeRawData]
        for i in range(inputWs.getNumberHistograms()):
            originalData.append(inputWs.readY(i).copy())
        algo.execute()
        self.assertTrue(mtd.doesExist("test_output_ws"))  # noqa: PT009
        output_ws = mtd["test_output_ws"]
        for i in range(output_ws.getNumberHistograms()):
            dataY = output_ws.readY(i)
            self.assertNotEqual(list(dataY), list(originalData[i]), f"Data for histogram {i} was not modified")  # noqa: PT009
            self.assertTrue(np.any(dataY))  # noqa: PT009
        self.assertEqual(algo.peakWindowClippingSize, 10)  # noqa: PT009
        self.assertEqual(algo.smoothingParameter, 0.5)  # noqa: PT009
        self.assertTrue(algo.decreaseParameter)  # noqa: PT009
        self.assertTrue(algo.LSS)  # noqa: PT009

    def test_execute(self):
        self.fakeIngredients = ArtificialNormalizationIngredients(
            peakWindowClippingSize=10,
            smoothingParameter=0.5,
            decreaseParameter=True,
            lss=True,
        )
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("OutputWorkspace", "test_output_ws")
        assert algo.execute()

    def test_output_data_characteristics(self):
        self.fakeIngredients = ArtificialNormalizationIngredients(
            peakWindowClippingSize=10,
            smoothingParameter=0.5,
            decreaseParameter=True,
            lss=True,
        )
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("OutputWorkspace", "test_output_ws")
        algo.execute()
        output_ws = mtd["test_output_ws"]
        for i in range(output_ws.getNumberHistograms()):
            dataY = output_ws.readY(i)
            self.assertFalse(np.isnan(dataY).any(), f"Histogram {i} contains NaN values")  # noqa: PT009
            self.assertFalse(np.isinf(dataY).any(), f"Histogram {i} contains infinite values")  # noqa: PT009
