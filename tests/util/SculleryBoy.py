# TODO this needs to be setup to better handle inputs


from typing import List
from unittest import mock

import pydantic
from snapred.backend.dao.calibration.Calibration import Calibration
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
    Atom,
    CalibrantSamples,
    Crystallography,
    Geometry,
    Material,
)
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.recipe.GenericRecipe import DetectorPeakPredictorRecipe
from snapred.meta.Config import Resource
from snapred.meta.redantic import parse_file_as


class SculleryBoy:
    """
    The scullery boy is a poor substitute for a sous chef,
    but good enough if you need some mock ingredients.

    Should be able to mock out the SousChef.
    """

    def __init__(self):
        pass

    def prepCalibration(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        calibration = parse_file_as(Calibration, Resource.getPath("inputs/calibration/CalibrationParameters.json"))
        calibration.seedRun = ingredients.runNumber
        calibration.useLiteMode = ingredients.useLiteMode
        return calibration

    def prepInstrumentState(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        return mock.Mock()

    def prepRunConfig(self, runNumber: str) -> RunConfig:
        return RunConfig(runNumber=runNumber)

    def prepCalibrantSample(self, calibrantSamplePath: str):  # noqa ARG002
        return CalibrantSamples(
            name="fake cylinder sample",
            unique_id="435elmst",
            geometry=self._prepGeometry(),
            material=self._prepMaterial(),
            crystallography=self._prepCrystallography(),
        )

    def prepFocusGroup(self, ingredients: FarmFreshIngredients) -> FocusGroup:  # noqa ARG002
        return FocusGroup(
            name="Natural",
            definition=Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
        )

    def prepPixelGroup(self, ingredients: FarmFreshIngredients = None):  # noqa ARG002
        return PixelGroup(
            pixelGroupingParameters=[],
            nBinsAcrossPeakWidth=7,
            focusGroup=self.prepFocusGroup(ingredients),
            timeOfFlight={"minimum": 0.001, "maximum": 100, "binWidth": 1, "binnindMode": -1},
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
            return pydantic.TypeAdapter(List[GroupPeakList]).validate_json(peakList)
        except TypeError:
            return [mock.Mock(spec_set=GroupPeakList)]
        except AttributeError:
            return [mock.Mock(spec_set=GroupPeakList)]

    def prepReductionIngredients(self, ingredients: FarmFreshIngredients):  # noqa ARG002
        path = Resource.getPath("/inputs/calibration/ReductionIngredients.json")
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
            maxChiSq=100.0,
        )

    # these methods are not in SousChef, but are needed to build other things

    def _prepGeometry(self):
        return Geometry(
            shape="Cylinder",
            radius=1.5,
            height=5.0,
        )

    def _prepMaterial(self):
        return Material(
            packingFraction=0.3,
            massDensity=1.0,
            chemicalFormula="V-B",
        )

    def _prepCrystallography(self):
        vanadiumAtom = Atom(
            symbol="V",
            coordinates=[0, 0, 0],
            siteOccupationFactor=0.5,
        )
        boronAtom = Atom(
            symbol="B",
            coordinates=[0, 1, 0],
            siteOccupationFactor=1.0,
        )
        return Crystallography(
            cifFile=Resource.getPath("inputs/crystalInfo/example.cif"),
            spaceGroup="I m -3 m",
            latticeParameters=[1, 2, 3, 4, 5, 6],
            atoms=[vanadiumAtom, boronAtom],
        )
