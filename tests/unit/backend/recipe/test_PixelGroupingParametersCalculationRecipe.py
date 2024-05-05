import unittest.mock as mock
from typing import List

import pytest
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.meta.redantic import list_to_raw


@mock.patch("snapred.backend.recipe.PixelGroupingParametersCalculationRecipe.BinnedValue")
def test_execute_successful(mockBinnedValue):
    # mock algorithm execution result and output
    params = PixelGroupingParameters(
        groupID=1,
        isMasked=False,
        L2=10.0,
        twoTheta=3.14,
        azimuth=0.0,
        dResolution=Limit(minimum=0.1, maximum=1.0),
        dRelativeResolution=0.01,
    )
    mock_output_val = [params]
    mockAlgo = mock.Mock(return_value=list_to_raw(mock_output_val))

    # execute recipe with mocked input
    recipe = PixelGroupingParametersCalculationRecipe()
    recipe.mantidSnapper.PixelGroupingParametersCalculationAlgorithm = mockAlgo
    ingredients = mock.Mock(json=mock.Mock(return_value="good ingredients"))
    ingredients.nBinsAcrossPeakWidth = 7
    groceries = {
        "groupingWorkspace": "grouping workspace",
        "maskWorkspace": "mask workspace",
    }
    data = recipe.executeRecipe(ingredients, groceries)

    assert mockAlgo.call_count == 1
    assert isinstance(data, dict)
    assert data["result"]
    assert isinstance(data["parameters"], list)
    assert data["parameters"][0] == mock_output_val[0]
    assert data["tof"] == mockBinnedValue.return_value


def test_execute_unsuccessful():
    mockAlgo = mock.Mock(side_effect=RuntimeError("passed"))

    recipe = PixelGroupingParametersCalculationRecipe()
    recipe.mantidSnapper.PixelGroupingParametersCalculationAlgorithm = mockAlgo
    ingredients = mock.Mock()
    groceries = {
        "groupingWorkspace": "grouping workspace",
        "maskWorkspace": "mask workspace",
    }

    with pytest.raises(RuntimeError) as e:
        recipe.executeRecipe(ingredients, groceries)
    assert str(e.value) == "passed"  # noqa: PT017
    assert mockAlgo.call_count == 1


@mock.patch("snapred.backend.recipe.PixelGroupingParametersCalculationRecipe.parse_raw_as")
@mock.patch("snapred.backend.recipe.PixelGroupingParametersCalculationRecipe.BinnedValue")
def test_resolve_callback(BinnedValue, parse_raw_as):
    BinnedValue.return_value = "tof"
    parse_raw_as.return_value = [mock.Mock(dRelativeResolution=1.0)]
    mockAlgo = mock.Mock(return_value=mock.Mock(get=mock.Mock(return_value="done")))

    recipe = PixelGroupingParametersCalculationRecipe()
    recipe.mantidSnapper.PixelGroupingParametersCalculationAlgorithm = mockAlgo
    ingredients = mock.Mock(nBinsAcrossPeakWidth=10)
    groceries = {
        "groupingWorkspace": "grouping workspace",
        "maskWorkspace": "mask workspace",
    }
    data = recipe.executeRecipe(ingredients, groceries)
    assert data["result"]
    assert data["tof"] == BinnedValue.return_value
    assert data["parameters"] == parse_raw_as.return_value
    assert parse_raw_as.called_once_with(List[PixelGroupingParameters], "done")
