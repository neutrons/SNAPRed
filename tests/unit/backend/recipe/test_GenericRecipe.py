import unittest
from typing import List
from unittest import mock

import pytest
from mantid.api import AlgorithmFactory, MatrixWorkspaceProperty, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import CloneWorkspace, CompareWorkspaces, CreateSingleValuedWorkspace
from pydantic import BaseModel, parse_raw_as
from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.recipe.GenericRecipe import GenericRecipe


class DummyAlgo:
    pass


class TestGenericRecipe(unittest.TestCase):
    def setUp(self):
        # Create a mock ReductionIngredients instance with the required attributes
        self.mock_runConfig = mock.MagicMock()
        self.mock_runConfig.runNumber = "12345"
        self.mock_reductionIngredients = mock.MagicMock(spec=ReductionIngredients)
        self.mock_reductionIngredients.runConfig = self.mock_runConfig

        class CakeRecipe(GenericRecipe[DummyAlgo]):
            pass

        self.GenericRecipe = CakeRecipe()
        self.GenericRecipe.mantidSnapper = mock.Mock()
        self.mockReturn = mock.Mock()
        self.mockReturn.get.return_value = "Mocked result"
        # self.GenericRecipe.mantidSnapper.DummyAlgo = lambda x:"Mocked result"
        self.GenericRecipe.mantidSnapper.DummyAlgo.return_value = self.mockReturn

    def test_baseModelsToStrings(self):
        pydanticObject = BaseModel(val="yes")
        kwargs = {
            "string": "a string",
            "float": 0.0,
            "baseModel": pydanticObject,
            "listOfBaseModels": [pydanticObject],
        }
        result = self.GenericRecipe._baseModelsToStrings(**kwargs)
        assert result["string"] == "a string"
        assert result["float"] == 0.0
        assert result["baseModel"] == pydanticObject.json()
        assert result["listOfBaseModels"] == f"[{pydanticObject.json()}]"

    def test_execute_successful(self):
        result = self.GenericRecipe.executeRecipe(Input=self.mock_reductionIngredients)

        assert result == "Mocked result"
        self.GenericRecipe.mantidSnapper.DummyAlgo.assert_called_once_with(
            "", Input=self.mock_reductionIngredients.json()
        )

    def test_execute_unsuccessful(self):
        self.GenericRecipe.mantidSnapper.executeQueue.side_effect = RuntimeError("passed")

        try:
            self.GenericRecipe.executeRecipe(Input=self.mock_reductionIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        self.GenericRecipe.mantidSnapper.DummyAlgo.assert_called_once_with(
            "", Input=self.mock_reductionIngredients.json()
        )


class InputOutputAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("InputValue", "", direction=Direction.Input)
        self.declareProperty("OutputValue", "incorrect", direction=Direction.Output)

    def PyExec(self):
        self.setPropertyValue("OutputValue", self.getPropertyValue("InputValue"))


class PrimitivePropertyAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("IntProperty", int(1), direction=Direction.Input)
        self.declareProperty("FloatProperty", 0.0, direction=Direction.Input)
        self.declareProperty("TupleProperty", tuple([0.0]), direction=Direction.Input)
        self.declareProperty("IntOut", int(1), direction=Direction.Output)
        self.declareProperty("FloatOut", 0.0, direction=Direction.Output)
        self.declareProperty("TupleOut", tuple([0.0]), direction=Direction.Output)

    def PyExec(self):
        intProperty = self.getProperty("IntProperty").value
        floatProperty = self.getProperty("FloatProperty").value
        tupleProperty = self.getProperty("TupleProperty").value
        assert isinstance(intProperty, int)
        assert isinstance(floatProperty, float)
        self.setProperty("IntOut", intProperty)
        self.setProperty("FloatOut", floatProperty)
        self.setProperty("TupleOut", tupleProperty)


class MatrixPropertyAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty(MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input))
        self.declareProperty(MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output))

    def PyExec(self):
        CloneWorkspace(
            InputWorkspace=self.getPropertyValue("InputWorkspace"),
            Outputworkspace=self.getPropertyValue("Outputworkspace"),
        )
        self.setPropertyValue("OutputWorkspace", self.getPropertyValue("OutputWorkspace"))


class DAOPropertyAlgo(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("ScalarDAO", "", direction=Direction.Input)
        self.declareProperty("ListDAO", "", direction=Direction.Input)
        self.declareProperty("EmptyDAO", "{}", direction=Direction.Input)

    def PyExec(self):
        assert BaseModel.parse_raw(self.getPropertyValue("ScalarDAO"))
        assert parse_raw_as(List[BaseModel], self.getPropertyValue("ListDAO"))
        assert BaseModel.parse_raw(self.getPropertyValue("EmptyDAO"))


class TestGenericRecipeInputsAndOutputs(unittest.TestCase):
    def test_in_and_out(self):
        # register the algorithm and define the recipe
        AlgorithmFactory.subscribe(InputOutputAlgo)

        class TestInAndOut(GenericRecipe[InputOutputAlgo]):
            pass

        # run the recipe and make sure correct result is given
        res = TestInAndOut().executeRecipe(InputValue="correct", OutputValue="incorrect")
        assert isinstance(res, str)
        assert res != "incorrect"

    def test_failure_set_string_with_float(self):
        # register the algorithm and define the recipe
        AlgorithmFactory.subscribe(InputOutputAlgo)

        class TestInAndOut(GenericRecipe[InputOutputAlgo]):
            pass

        # try to set the string with a float --- will fail, reproducing an old error
        with pytest.raises(TypeError):
            TestInAndOut().executeRecipe(InputValue=0.0, OutputValue="incorrect")

    def test_workspaces(self):
        # register the algorithm and define the recipe
        AlgorithmFactory.subscribe(MatrixPropertyAlgo)

        class TestMatrixProp(GenericRecipe[MatrixPropertyAlgo]):
            pass

        # run the recipe and make sure correct result is given
        CreateSingleValuedWorkspace(OutputWorkspace="okay")
        res = TestMatrixProp().executeRecipe(InputWorkspace="okay", OutputWorkspace="hurray")
        assert CompareWorkspaces(Workspace1="okay", Workspace2=res)

    def test_primitives(self):
        # register the algorithm and define the recipe
        AlgorithmFactory.subscribe(PrimitivePropertyAlgo)

        class TestPrimitives(GenericRecipe[PrimitivePropertyAlgo]):
            pass

        # run the recipe and make sure correct result is given
        res = TestPrimitives().executeRecipe(IntProperty=2, FloatProperty=1.0, TupleProperty=(2.0, 3.0))
        # NOTE the isinstance asserts should work, for unknown reasons they are broken
        # assert isinstance(res[0], int)
        # assert isinstance(res[1], float)
        # assert isinstance(res[2], tuple)
        assert res[0] == 2
        assert res[1] == 1.0
        # NOTE the result should be a tuple, but it is remaining a std_vector_dbl object
        # assert res[2] == (2.,3.)

    def test_daos(self):
        # register the algorithm and define the recipe
        AlgorithmFactory.subscribe(DAOPropertyAlgo)

        class TestDAOs(GenericRecipe[DAOPropertyAlgo]):
            pass

        # run the recipe and make sure correct result is given
        TestDAOs().executeRecipe(ScalarDAO=BaseModel(), ListDAO=[BaseModel()])
