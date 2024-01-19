import json
import time
from datetime import date
from functools import lru_cache
from typing import Dict, List, Tuple

from pydantic import parse_raw_as

from snapred.backend.dao import CrystallographicInfo, GroupPeakList
from snapred.backend.dao.calibration import Calibration
from snapred.backend.dao.ingredients import (
    DiffractionCalibrationIngredients,
    FarmFreshIngredients,
    GroceryListItem,
    NormalizationIngredients,
    PeakIngredients,
    PixelGroupingIngredients,
    ReductionIngredients,
)
from snapred.backend.dao.state import FocusGroup, InstrumentState, PixelGroup
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.recipe.GenericRecipe import (
    DetectorPeakPredictorRecipe,
)
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.Service import Service
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.redantic import list_to_raw


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
        self.__pixelGroupCache: Dict[Tuple[str, bool, str], PixelGroup] = {}
        self.__calibrationCache: Dict[str, Calibration] = {}
        self.__peaksCache: Dict[Tuple[str, bool, str, float], List[GroupPeakList]] = {}
        self.__xtalCache: Dict[Tuple[str, float, float], CrystallographicInfo] = {}
        self.registerPath("reduction", self.reduction)
        return

    @staticmethod
    def name():
        return "souschef"

    def getInstrumentState(self, runNumber) -> InstrumentState:
        if runNumber not in self.__calibrationCache:
            self.__calibrationCache[runNumber] = self.dataFactoryService.getCalibrationState(runNumber)
        return self.__calibrationCache[runNumber].instrumentState

    def groupingSchemaFromPath(self, path: str) -> str:
        return path.split("/")[-1].split("_")[-1].split(".")[0]

    def getFocusGroup(self, ingredients: FarmFreshIngredients) -> FocusGroup:
        definition = ingredients.groupingSchemaFilepath
        if not definition:
            definition = f'{Config["instrument.calibration.powder.grouping.home"]}/{ingredients.name}'
        return FocusGroup(
            name=ingredients.groupingSchema,
            definition=definition,
        )

    def getPixelGroup(self, ingredients: FarmFreshIngredients):
        focusGroup = self.getFocusGroup(ingredients)
        key = (ingredients.runNumber, ingredients.useLiteMode, ingredients.groupingSchema)
        if key not in self.__pixelGroupCache:
            instrumentState = self.getInstrumentState(ingredients.runNumber)
            ingredients = PixelGroupingIngredients(
                instrumentState=instrumentState,
                nBinsAcrossPeakWidth=ingredients.nBinsAcrossPeakWidth,
            )
            getGrouping = (
                self.groceryClerk.grouping(ingredients.groupingSchema)
                .useLiteMode(ingredients.useLiteMode)
                .source(InstrumentFilename=self._getInstrumentDefinitionFilename(ingredients.useLiteMode))
                .buildList()
            )
            groupingWS = self.groceryService.fetchGroceryList(getGrouping)[0]
            data = PixelGroupingParametersCalculationRecipe().executeRecipe(ingredients, groupingWS)
            self.__pixelGroupCache[key] = PixelGroup(
                focusGroup=focusGroup,
                pixelGroupingParameters=data["parameters"],
                timeOfFlight=data["tof"],
                nBinsAcrossPeakWidth=ingredients.nBinsAcrossPeakWidth,
            )
        return self.__pixelGroupCache[key]

    def _getInstrumentDefinitionFilename(self, useLiteMode: bool):
        if useLiteMode is True:
            return Config["instrument.lite.definition.file"]
        elif useLiteMode is False:
            return Config["instrument.native.definition.file"]

    def getCrystallographicInfo(self, ingredients: FarmFreshIngredients):
        key = (ingredients.cifPath, ingredients.dBounds.minimum, ingredients.dBounds.maximum)
        if key not in self.__xtalCache:
            self.__xtalCache[key] = CrystallographicInfoService().ingest(*key)["crystalInfo"]
        return self.__xtalCache[key]

    def getPeakIngredients(self, ingredients: FarmFreshIngredients) -> PeakIngredients:
        return PeakIngredients(
            crystalInfo=self.getCrystallographicInfo(ingredients),
            instrumentState=self.getInstrumentState(ingredients.runNumber),
            pixelGroup=self.getPixelGroup(ingredients),
            peakIntensityThreshold=ingredients.peakIntensityThreshold,
        )

    def getDetectorPeaks(self, ingredients: FarmFreshIngredients) -> List[GroupPeakList]:
        key = (
            ingredients.runNumber,
            ingredients.useLiteMode,
            ingredients.groupingSchema,
            ingredients.peakIntensityThreshold,
        )
        if key not in self.__peaksCache:
            ingredients = self.getPeakIngredients(ingredients)
            res = DetectorPeakPredictorRecipe().executeRecipe(Ingredients=ingredients)
            self.__peaksCache[key] = parse_raw_as(List[GroupPeakList], res)
        return self.__peaksCache[key]

    def getReductionIngredients(self, ingredients: FarmFreshIngredients) -> ReductionIngredients:
        from snapred.backend.data.DataFactoryService import DataFactoryService

        dataFactoryService = DataFactoryService()
        return ReductionIngredients(
            reductionState=dataFactoryService.getReductionState(ingredients.runNumber),
            runConfig=dataFactoryService.getRunConfig(ingredients.runNumber),
            pixelGroup=self.getPixelGroup(),
        )

    def getNormalizationIngredients(self, ingredients: FarmFreshIngredients) -> NormalizationIngredients:
        from snapred.backend.data.DataFactoryService import DataFactoryService

        calibrantSample = DataFactoryService().getCalibrantSample(ingredients.samplePath)
        return NormalizationIngredients(
            pixelGroup=self.getPixelGroup(ingredients),
            calibrantSample=calibrantSample,
            detectorPeaks=self.getDetectorPeaks(ingredients),
        )

    def getDiffractionCalibrationIngredients(
        self, ingredients: FarmFreshIngredients
    ) -> DiffractionCalibrationIngredients:
        pass
