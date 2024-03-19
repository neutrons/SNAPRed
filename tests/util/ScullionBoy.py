# TODO this needs to be setup to better handle inputs


from typing import List
from unittest import mock

from pydantic import parse_raw_as
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.recipe.GenericRecipe import DetectorPeakPredictorRecipe
from snapred.meta.Config import Resource


class ScullionBoy:
    """
    The scullion boy is a poor substitute for a sous chef,
    but good enough if you need some mock ingredients.

    Should be able to mock out the SousChef.
    """

    def __init__(self):
        pass

    def prepCalibration(self, runNumber: str):  # noqa ARG002
        return mock.Mock()

    def prepInstrumentState(self, runNumber: str):  # noqa ARG002
        return mock.Mock()

    def prepRunConfig(self, runNumber: str):  # noqa ARG002
        return mock.Mock()

    def prepCalibrantSample(self, runNumber: str):  # noqa ARG002
        return mock.Mock()

    def prepFocusGroup(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return mock.Mock()

    def prepPixelGroup(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return mock.Mock()

    def prepCrystallographicInfo(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return mock.Mock()

    def prepPeakIngredients(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        if "good" in ingredients:
            path = Resource.getPath("/inputs/predict_peaks/input_good_ingredients.json")
        else:
            path = Resource.getPath("/inputs/predict_peaks/input_fake_ingredients.json")
        return PeakIngredients.parse_file(path)

    def prepDetectorPeaks(self, ingredients: FarmFreshIngredients) -> List[GroupPeakList]:
        try:
            peakList = DetectorPeakPredictorRecipe().executeRecipe(
                Ingredients=self.prepPeakIngredients(ingredients),
                PurgeDuplicates=ingredients.get("purge", True),
            )
            return parse_raw_as(List[GroupPeakList], peakList)
        except TypeError:
            return [mock.Mock()]

    def prepReductionIngredients(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return mock.Mock()

    def prepNormalizationIngredients(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return mock.Mock()

    def prepDiffractionCalibrationIngredients(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return mock.Mock()
