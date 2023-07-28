import json
import socket
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import CompareWorkspaces, DeleteWorkspace, LoadNexusProcessed, mtd
    from snapred.backend.dao.ReductionIngredients import ReductionIngredients  # noqa: E402
    from snapred.backend.recipe.CalibrationReductionRecipe import CalibrationReductionRecipe  # noqa: E402
    from snapred.meta.Config import Config, Resource

    IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

    def setup():
        """Setup before all tests"""
        pass

    def teardown():
        """Teardown after all tests"""
        if not IS_ON_ANALYSIS_MACHINE:  # noqa: F821
            return
        # collect list of all workspaces
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                # just eat it if it doesnt exist
                print("ws doesnt exist!")

    @pytest.fixture(autouse=True)
    def _setup_teardown():
        """Setup before each test, teardown after each test"""
        setup()
        yield
        teardown()

    expected_outputs_files = Config["test.outputs.calibration.files"]
    outputs_root = Config["test.outputs.calibration.root"]

    def get_input_json():
        """Read input json file"""
        input_json = None
        with Resource.open("/inputs/calibration/input.json", "r") as f:
            input_json = f.read()
        return input_json

    def getSubstringContains(values, substring):
        return [value for value in values if substring in value][0]

    def compareWorkspaces(expected, actual):
        result, _ = CompareWorkspaces(expected, actual, Tolerance=0.007, CheckInstrument=False)
        assert result

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_happypath_calibration():
        # read input json file
        input_json = get_input_json()
        # create recipe
        recipe = CalibrationReductionRecipe()
        reductionIngredients = ReductionIngredients(**json.loads(input_json))
        recipe.executeRecipe(reductionIngredients)

        outputWorkspaces = []
        workspaces = mtd.getObjectNames()
        for workspace in workspaces:
            outputWorkspaces.append(workspace)
        # load expected output from file with mantid
        expectedOutputWorkspaces = []
        for output_file in expected_outputs_files:
            output = output_file.split(".")[0]
            expectedOutputWorkspaces.append(output)
            LoadNexusProcessed(Resource.getPath(outputs_root + output_file), OutputWorkspace=output)
            # ConvertToEventWorkspace(InputWorkspace=output+"2D", OutputWorkspace=output)

        # compare expected output with actual output
        # All
        expected = getSubstringContains(expectedOutputWorkspaces, "All")
        actual = getSubstringContains(outputWorkspaces, "All")
        compareWorkspaces(expected, actual)
        # Bank
        expected = getSubstringContains(expectedOutputWorkspaces, "Bank")
        actual = getSubstringContains(outputWorkspaces, "Bank")
        compareWorkspaces(expected, actual)
        # Column
        expected = getSubstringContains(expectedOutputWorkspaces, "Column")
        actual = getSubstringContains(outputWorkspaces, "Column")
        compareWorkspaces(expected, actual)
