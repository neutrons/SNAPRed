# TODO this needs to be setup to better handle inputs


from typing import List
from unittest import mock

from pydantic import parse_raw_as
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import (
    DiffractionCalibrationIngredients,
    NormalizationIngredients,
    PeakIngredients,
    ReductionIngredients,
)
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.CalibrantSample import (
    CalibrantSamples,
    Geometry,
    Material,
)
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.recipe.GenericRecipe import DetectorPeakPredictorRecipe
from snapred.meta.Config import Resource


class SculleryBoy:
    """
    The scullery boy is a poor substitute for a sous chef,
    but good enough if you need some mock ingredients.

    Should be able to mock out the SousChef.
    """

    def __init__(self):
        pass

    def prepCalibration(self, runNumber: str):  # noqa ARG002
        return {mock.Mock()}

    def prepInstrumentState(self, runNumber: str):  # noqa ARG002
        return mock.Mock()

    def prepRunConfig(self, runNumber: str) -> RunConfig:
        return RunConfig(runNumber=runNumber)

    def prepCalibrantSample(self, calibrantSamplePath: str):  # noqa ARG002
        return CalibrantSamples(
            name="xtal",
            unique_id="123",
            geometry=self._prepGeometry(),
            material=self._prepMaterial(),
        )

    def prepFocusGroup(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return mock.Mock()

    def prepPixelGroup(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return PixelGroup(
            pixelGroupingParameters=[],
            nBinsAcrossPeakWidth=7,
            focusGroup={"name": "several", "definition": "/bread/coconut"},
            timeOfFlight={"minimum": 0, "maximum": 100, "binWidth": 1, "binnindMode": -1},
            binningMode=-1,
        )

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
            print("yup!")
            return parse_raw_as(List[GroupPeakList], peakList)
        except TypeError:
            return [mock.Mock(spec_set=GroupPeakList)]
        except AttributeError:
            return [mock.Mock(spec_set=GroupPeakList)]

    def prepReductionIngredients(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        path = Resource.getPath("/inputs/normalization/ReductionIngredients.json")
        return ReductionIngredients.parse_file(path)

    def prepNormalizationIngredients(self, ingredients: FarmFreshIngredients) -> NormalizationIngredients:
        return NormalizationIngredients(
            pixelGroup=self.prepPixelGroup(ingredients),
            calibrantSample=self.prepCalibrantSample("123"),
            detectorPeaks=self.prepDetectorPeaks(ingredients),
        )

    def prepDiffractionCalibrationIngredients(
        self, ingredients: FarmFreshIngredients
    ) -> DiffractionCalibrationIngredients:
        return DiffractionCalibrationIngredients(
            runConfig=self.prepRunConfig(ingredients.runNumber),
            pixelGroup=self.prepPixelGroup(ingredients),
            groupedPeakLists=self.prepDetectorPeaks(ingredients),
        )

    # these methods are not in SousChef, but are needed to build other things

    def _prepGeometry(self):
        return Geometry(
            shape="Sphere",
            radius=1.0,
        )

    def _prepMaterial(self):
        return Material(
            chemicalFormula="V-B",
            packingFraction=0.5,
            massDensity=1.0,
        )
