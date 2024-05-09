from typing import Dict, List, Tuple

from pydantic import parse_raw_as

from snapred.backend.dao import CrystallographicInfo, GroupPeakList, RunConfig
from snapred.backend.dao.calibration import Calibration
from snapred.backend.dao.ingredients import (
    DiffractionCalibrationIngredients,
    GroceryListItem,
    NormalizationIngredients,
    PeakIngredients,
    PixelGroupingIngredients,
    ReductionIngredients,
)
from snapred.backend.dao.request import FarmFreshIngredients
from snapred.backend.dao.state import FocusGroup, InstrumentState, PixelGroup
from snapred.backend.dao.state.CalibrantSample import CalibrantSamples
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.recipe.GenericRecipe import (
    DetectorPeakPredictorRecipe,
    PurgeOverlappingPeaksRecipe,
)
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.Service import Service
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class SousChef(Service):
    """
    It slices, it dices, and it knows how to make the more complicated ingredients.
    """

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.groceryService = GroceryService()
        self.groceryClerk = GroceryListItem.builder()
        self.dataFactoryService = DataFactoryService()
        self._pixelGroupCache: Dict[Tuple[str, bool, str], PixelGroup] = {}
        self._peaksCache: Dict[Tuple[str, bool, str, float, float, float], List[GroupPeakList]] = {}
        self._xtalCache: Dict[Tuple[str, float, float], CrystallographicInfo] = {}
        return

    @staticmethod
    def name():
        return "souschef"

    def prepCalibration(self, ingredients: FarmFreshIngredients) -> Calibration:
        calibration = self.dataFactoryService.getCalibrationState(ingredients.runNumber)
        calibration.instrumentState.fwhmMultipliers = ingredients.fwhmMultipliers
        return calibration

    def prepInstrumentState(self, ingredients: FarmFreshIngredients) -> InstrumentState:
        return self.prepCalibration(ingredients).instrumentState

    def prepRunConfig(self, runNumber: str) -> RunConfig:
        return self.dataFactoryService.getRunConfig(runNumber)

    def prepCalibrantSample(self, calibrantSamplePath: str) -> CalibrantSamples:
        return self.dataFactoryService.getCalibrantSample(calibrantSamplePath)

    def prepFocusGroup(self, ingredients: FarmFreshIngredients) -> FocusGroup:
        if self.dataFactoryService.fileExists(ingredients.focusGroup.definition):
            return ingredients.focusGroup
        else:
            groupingMap = self.dataFactoryService.getGroupingMap(ingredients.runNumber, ingredients.useLiteMode)
            return groupingMap.getMap(ingredients.useLiteMode)[ingredients.focusGroup.name]

    def prepPixelGroup(self, ingredients: FarmFreshIngredients) -> PixelGroup:
        groupingSchema = ingredients.focusGroup.name
        key = (ingredients.runNumber, ingredients.useLiteMode, groupingSchema)
        if key not in self._pixelGroupCache:
            focusGroup = self.prepFocusGroup(ingredients)
            instrumentState = self.prepInstrumentState(ingredients)
            pixelIngredients = PixelGroupingIngredients(
                instrumentState=instrumentState,
                nBinsAcrossPeakWidth=ingredients.nBinsAcrossPeakWidth,
            )
            self.groceryClerk.name("groupingWorkspace").fromRun(ingredients.runNumber).grouping(
                focusGroup.name
            ).useLiteMode(ingredients.useLiteMode).add()
            groceries = self.groceryService.fetchGroceryDict(self.groceryClerk.buildDict())
            data = PixelGroupingParametersCalculationRecipe().executeRecipe(pixelIngredients, groceries)

            self._pixelGroupCache[key] = PixelGroup(
                focusGroup=focusGroup,
                pixelGroupingParameters=data["parameters"],
                timeOfFlight=data["tof"],
                nBinsAcrossPeakWidth=ingredients.nBinsAcrossPeakWidth,
            )
        return self._pixelGroupCache[key]

    def _getInstrumentDefinitionFilename(self, useLiteMode: bool) -> str:
        if useLiteMode is True:
            return Config["instrument.lite.definition.file"]
        elif useLiteMode is False:
            return Config["instrument.native.definition.file"]

    def prepCrystallographicInfo(self, ingredients: FarmFreshIngredients) -> CrystallographicInfo:
        if not ingredients.cifPath:
            samplePath = ingredients.calibrantSamplePath.split("/")[-1].split(".")[0]
            ingredients.cifPath = self.dataFactoryService.getCifFilePath(samplePath)
        key = (ingredients.cifPath, ingredients.crystalDBounds.minimum, ingredients.crystalDBounds.maximum)
        if key not in self._xtalCache:
            self._xtalCache[key] = CrystallographicInfoService().ingest(*key)["crystalInfo"]
        return self._xtalCache[key]

    def prepPeakIngredients(self, ingredients: FarmFreshIngredients) -> PeakIngredients:
        return PeakIngredients(
            crystalInfo=self.prepCrystallographicInfo(ingredients),
            instrumentState=self.prepInstrumentState(ingredients),
            pixelGroup=self.prepPixelGroup(ingredients),
            peakIntensityThreshold=ingredients.peakIntensityThreshold,
        )

    def prepDetectorPeaks(self, ingredients: FarmFreshIngredients, purgePeaks=True) -> List[GroupPeakList]:
        # NOTE purging overlapping peaks is necessary for proper functioning inside the DiffCal process
        # this should not be user-settable, and therefore should not be included inside the FarmFreshIngredients list
        key = (
            ingredients.runNumber,
            ingredients.useLiteMode,
            ingredients.focusGroup.name,
            ingredients.crystalDBounds.minimum,
            ingredients.crystalDBounds.maximum,
            ingredients.fwhmMultipliers.left,
            ingredients.fwhmMultipliers.right,
            ingredients.peakIntensityThreshold,
            purgePeaks,
        )
        dMin = ingredients.crystalDBounds.minimum
        dMax = ingredients.crystalDBounds.maximum
        if key not in self._peaksCache:
            ingredients = self.prepPeakIngredients(ingredients)
            res = DetectorPeakPredictorRecipe().executeRecipe(
                Ingredients=ingredients,
            )
            if purgePeaks:
                res = PurgeOverlappingPeaksRecipe().executeRecipe(
                    Ingredients=ingredients,
                    DetectorPeaks=res,
                    dMin=dMin,
                    dMax=dMax,
                )
            self._peaksCache[key] = parse_raw_as(List[GroupPeakList], res)
        return self._peaksCache[key]

    def prepReductionIngredients(self, ingredients: FarmFreshIngredients) -> ReductionIngredients:
        return ReductionIngredients(
            reductionState=self.dataFactoryService.getReductionState(ingredients.runNumber, ingredients.useLiteMode),
            runConfig=self.prepRunConfig(ingredients.runNumber),
            pixelGroup=self.prepPixelGroup(ingredients),
        )

    def prepNormalizationIngredients(self, ingredients: FarmFreshIngredients) -> NormalizationIngredients:
        return NormalizationIngredients(
            pixelGroup=self.prepPixelGroup(ingredients),
            calibrantSample=self.prepCalibrantSample(ingredients.calibrantSamplePath),
            detectorPeaks=self.prepDetectorPeaks(ingredients, purgePeaks=False),
        )

    def prepDiffractionCalibrationIngredients(
        self, ingredients: FarmFreshIngredients
    ) -> DiffractionCalibrationIngredients:
        return DiffractionCalibrationIngredients(
            runConfig=self.prepRunConfig(ingredients.runNumber),
            pixelGroup=self.prepPixelGroup(ingredients),
            groupedPeakLists=self.prepDetectorPeaks(ingredients),
            peakFunction=ingredients.peakFunction,
            convergenceThreshold=ingredients.convergenceThreshold,
            maxOffset=ingredients.maxOffset,
        )
