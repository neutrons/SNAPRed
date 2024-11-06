from typing import Dict, List
import pydantic

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import (
    DiffractionCalibrationIngredients,
    NormalizationIngredients,
    ReductionIngredients,
)
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.GenericRecipe import DetectorPeakPredictorRecipe
from snapred.meta.Config import Resource
from snapred.meta.redantic import parse_file_as

##
## Put test-related imports at the end, so that the normal non-test import sequence is unmodified.
##
from util.dao import DAOFactory
from util.mock_util import mock_instance_methods

from unittest import mock

@mock_instance_methods
class SculleryBoy:
    """
    The scullery boy is a poor substitute for a sous chef,
    but good enough if you need some mock ingredients.

    Should be able to mock out the SousChef.
    """
    # TODO: Why isn't this `class SculleryBoy(SousChef)`?!

    def __init__(
        self,
        prepPeakIngredientsFlags: Dict[str, bool] = {"useFakePeakValues": False},
        prepDetectorPeaksFlags: Dict[str, bool] = {"removeDuplicatePeaks": True},
    ):
        # Flag values were previously passed in using mocks:  this is not a recommended practice
        #   as it causes the mock to deviate from its "spec" type.
        self.prepPeakIngredientsFlags = prepPeakIngredientsFlags
        self.prepDetectorPeaksFlags = prepDetectorPeaksFlags

    def prepCalibration(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return DAOFactory.calibrationParameters(ingredients.runNumber, ingredients.useLiteMode)

    def prepInstrumentState(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return DAOFactory.default_instrument_state.copy()

    def prepRunConfig(self, runNumber: str) -> RunConfig:
        return RunConfig(runNumber=runNumber)

    def prepCalibrantSample(self, calibrantSamplePath: str):  # noqa ARG002
        return DAOFactory.sample_calibrant_sample

    def prepFocusGroup(self, ingredients: FarmFreshIngredients) -> FocusGroup:  # noqa ARG002
        return DAOFactory.focus_group_SNAP_column_lite.copy()

    def prepPixelGroup(self, ingredients: FarmFreshIngredients = None):  # noqa ARG002
        params = PixelGroupingParameters(
            groupID=1,
            isMasked=False,
            L2=10.0,
            twoTheta=3.14,
            azimuth=0.0,
            dResolution=Limit(minimum=0.1, maximum=1.0),
            dRelativeResolution=0.01,
        )

        return PixelGroup(
            pixelGroupingParameters=[params],
            nBinsAcrossPeakWidth=7,
            focusGroup=self.prepFocusGroup(ingredients),
            timeOfFlight={"minimum": 0.001, "maximum": 100, "binWidth": 1, "binnindMode": -1},
            binningMode=-1,
        )

    def prepCrystallographicInfo(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return DAOFactory.default_xtal_info.copy()

    def prepPeakIngredients(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        if not self.prepPeakIngredientsFlags["useFakePeakValues"]:
            return DAOFactory.good_peak_ingredients.copy()
        else:
            return DAOFactory.fake_peak_ingredients.copy()

    def prepDetectorPeaks(self, ingredients: FarmFreshIngredients, purgePeaks=False) -> List[GroupPeakList]:  # noqa: ARG002
        try:
            peakList = DetectorPeakPredictorRecipe().executeRecipe(
                Ingredients=self.prepPeakIngredients(ingredients),
                PurgeDuplicates=self.prepDetectorPeaksFlags["removeDuplicatePeaks"],
            )
            return pydantic.TypeAdapter(List[GroupPeakList]).validate_json(peakList)
        except (TypeError, AttributeError):
            return [mock.Mock(spec_set=GroupPeakList)]

    def prepReductionIngredients(
        self, _ingredients: FarmFreshIngredients, _combinedPixelMask: Optional[WorkspaceName] = None
    ):
        path = Resource.getPath("/inputs/calibration/ReductionIngredients.json")
        return parse_file_as(ReductionIngredients, path)

    def prepNormalizationIngredients(self, ingredients: FarmFreshIngredients) -> NormalizationIngredients:
        return NormalizationIngredients(
            pixelGroup=self.prepPixelGroup(ingredients),
            calibrantSample=self.prepCalibrantSample("123"),
            detectorPeaks=self.prepDetectorPeaks(ingredients),
        )

    def verifyCalibrationExists(self, runNumber: str, useLiteMode: bool) -> bool:  # noqa ARG002
        return True

    def prepDiffractionCalibrationIngredients(
        self, ingredients: FarmFreshIngredients
    ) -> DiffractionCalibrationIngredients:
        return DiffractionCalibrationIngredients(
            runConfig=self.prepRunConfig(ingredients.runNumber),
            pixelGroup=self.prepPixelGroup(ingredients),
            groupedPeakLists=self.prepDetectorPeaks(ingredients),
            maxChiSq=100.0,
        )
