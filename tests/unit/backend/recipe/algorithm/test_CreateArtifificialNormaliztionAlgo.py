import json
import unittest

import numpy as np
from mantid.api import WorkspaceFactory
from mantid.simpleapi import *
from snapred.backend.recipe.algorithm.CreateArtificialNormalizationAlgo import (
    CreateArtificialNormalizationAlgo as ThisAlgo,
)


class TestCreateArtificialNormalizationAlgo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the workspaces and ingredients used in all tests."""
        # Create a simple input workspace for testing (mock diffraction data)
        cls.inputWS = WorkspaceFactory.create("Workspace2D", NVectors=1, XLength=10, YLength=10)
        cls.inputWS.setY(0, np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
        cls.inputWSName = "TestInputWorkspace"
        cls.outputWSName = "TestOutputWorkspace"
        AddWorkspace(cls.inputWS, cls.inputWSName)

        # Define a fake ingredients JSON for the test
        cls.fakeIngredients = {
            "peakWindowClippingSize": 2,
            "smoothingParameter": 0.5,
            "decreaseParameter": True,
            "lss": True,
        }
        cls.fakeIngredientsStr = json.dumps(cls.fakeIngredients)

    @classmethod
    def tearDownClass(cls):
        """Clean up workspaces created during the tests."""
        DeleteWorkspace(cls.inputWSName)
        DeleteWorkspace(cls.outputWSName)

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized correctly."""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredientsStr)
        algo.setProperty("InputWorkspace", self.inputWSName)
        algo.setProperty("OutputWorkspace", self.outputWSName)

        self.assertEqual(algo.getProperty("Ingredients").value, self.fakeIngredientsStr)  # noqa: PT009
        self.assertEqual(algo.getPropertyValue("InputWorkspace"), self.inputWSName)  # noqa: PT009
        self.assertEqual(algo.getPropertyValue("OutputWorkspace"), self.outputWSName)  # noqa: PT009

    def test_chop_ingredients(self):
        """Test that ingredients are correctly processed."""
        algo = ThisAlgo()
        algo.initialize()
        algo.chopInredients(self.fakeIngredientsStr)

        # Verify the extracted values
        self.assertEqual(algo.peakWindowClippingSize, self.fakeIngredients["peakWindowClippingSize"])  # noqa: PT009
        self.assertEqual(algo.smoothingParameter, self.fakeIngredients["smoothingParameter"])  # noqa: PT009
        self.assertEqual(algo.decreaseParameter, self.fakeIngredients["decreaseParameter"])  # noqa: PT009
        self.assertEqual(algo.LSS, self.fakeIngredients["lss"])  # noqa: PT009

    def test_peak_clipping(self):
        """Test the peak clipping logic on sample data."""
        algo = ThisAlgo()
        algo.initialize()

        # Test data
        data = np.array([1, 3, 2, 5, 4])
        clipped_data = algo.peakClip(data, winSize=2, decrese=True, LLS=False, smoothing=0)

        # Expected output after clipping
        expected_output = np.array([1, 1, 1, 1, 1])
        np.testing.assert_array_almost_equal(clipped_data, expected_output)

    def test_execution(self):
        """Test that the algorithm executes and modifies the output workspace."""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredientsStr)
        algo.setProperty("InputWorkspace", self.inputWSName)
        algo.setProperty("OutputWorkspace", self.outputWSName)

        # Execute the algorithm
        self.assertTrue(algo.execute())  # noqa: PT009

        # Check that the output workspace has been modified
        outputWS = mtd[self.outputWSName]
        outputY = outputWS.readY(0)

        # Verify that the output Y data has been clipped
        expected_output = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
        np.testing.assert_array_almost_equal(outputY, expected_output)
