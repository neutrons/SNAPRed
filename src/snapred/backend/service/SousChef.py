import os
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pydantic

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
from snapred.backend.dao.state.CalibrantSample import CalibrantSample
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.GenericRecipe import (
    DetectorPeakPredictorRecipe,
    PurgeOverlappingPeaksRecipe,
)
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.Service import Service
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)


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
        return

    @staticmethod
    def name():
        return "souschef"

    def logger(self):
        return logger

    def prepCalibration(self, ingredients: FarmFreshIngredients) -> Calibration:
        calibration = self.dataFactoryService.getCalibrationState(
            ingredients.runNumber, ingredients.useLiteMode, ingredients.alternativeState
        )
        # NOTE: This generates a new instrument state based on the appropriate SNAPInstPrm, as opposed to
        #       passing the previous instrument state forward.
        calibration.instrumentState = self.dataFactoryService.getDefaultInstrumentState(ingredients.runNumber)
        calibration.calibrantSamplePath = ingredients.calibrantSamplePath
        calibration.peakIntensityThreshold = self._getThresholdFromCalibrantSample(ingredients.calibrantSamplePath)
        calibration.instrumentState.fwhmMultipliers = ingredients.fwhmMultipliers
        return calibration

    def prepInstrumentState(self, ingredients: FarmFreshIngredients) -> InstrumentState:
        # check if a calibration exists else just use default state
        instrumentState = None
        if self.dataFactoryService.calibrationExists(
            ingredients.runNumber, ingredients.useLiteMode, ingredients.alternativeState
        ):
            calibration = self.prepCalibration(ingredients)
            instrumentState = calibration.instrumentState
        else:
            if ingredients.alternativeState is not None:
                raise ValueError(
                    (
                        f"Alternative calibration state {ingredients.alternativeState}",
                        f" not found for run {ingredients.runNumber}.",
                    )
                )
            mode = "lite" if ingredients.useLiteMode else "native"
            self.logger().info(
                f"No calibration found for run {ingredients.runNumber} in {mode} mode.  Using default instrument state."
            )
            instrumentState = self.dataFactoryService.getDefaultInstrumentState(ingredients.runNumber)

        return instrumentState

    def prepRunConfig(self, runNumber: str) -> RunConfig:
        return self.dataFactoryService.getRunConfig(runNumber)

    def prepCalibrantSample(self, calibrantSamplePath: str) -> CalibrantSample:
        return self.dataFactoryService.getCalibrantSample(calibrantSamplePath)

    def prepFocusGroup(self, ingredients: FarmFreshIngredients) -> FocusGroup:
        if self.dataFactoryService.fileExists(ingredients.focusGroup.definition):
            return ingredients.focusGroup
        else:
            groupingMap = self.dataFactoryService.getGroupingMap(ingredients.runNumber)
            return groupingMap.getMap(ingredients.useLiteMode)[ingredients.focusGroup.name]

    def prepPixelGroup(
        self, ingredients: FarmFreshIngredients, pixelMask: Optional[WorkspaceName] = None
    ) -> PixelGroup:
        groupingSchema = ingredients.focusGroup.name
        key = (
            ingredients.runNumber,
            ingredients.useLiteMode,
            groupingSchema,
            ingredients.calibrantSamplePath,
            pixelMask,
        )
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
            groceries = self.groceryService.fetchGroceryDict(self.groceryClerk.buildDict(), maskWorkspace=pixelMask)
            data = PixelGroupingParametersCalculationRecipe().executeRecipe(pixelIngredients, groceries)

            self._pixelGroupCache[key] = PixelGroup(
                focusGroup=focusGroup,
                pixelGroupingParameters=data["parameters"],
                timeOfFlight=data["tof"],
                nBinsAcrossPeakWidth=ingredients.nBinsAcrossPeakWidth,
            )
        return deepcopy(self._pixelGroupCache[key])

    def prepManyPixelGroups(
        self, ingredients: FarmFreshIngredients, pixelMask: Optional[WorkspaceName] = None
    ) -> List[PixelGroup]:
        pixelGroups = []
        ingredients_ = ingredients.model_copy()
        for focusGroup in ingredients.focusGroups:
            ingredients_.focusGroup = focusGroup
            pixelGroups.append(self.prepPixelGroup(ingredients_, pixelMask))
        return pixelGroups

    def _getInstrumentDefinitionFilename(self, useLiteMode: bool) -> str:
        if useLiteMode is True:
            return Config["instrument.lite.definition.file"]
        elif useLiteMode is False:
            return Config["instrument.native.definition.file"]

    def prepCrystallographicInfo(self, ingredients: FarmFreshIngredients) -> CrystallographicInfo:
        samplePath = Path(ingredients.calibrantSamplePath).stem
        ingredients.cifPath = self.dataFactoryService.getCifFilePath(samplePath)
        key = (
            ingredients.cifPath,
            ingredients.crystalDBounds.minimum,
            ingredients.crystalDBounds.maximum,
            ingredients.calibrantSamplePath,
        )
        return CrystallographicInfoService().ingest(*(key[:-1]))["crystalInfo"]

    def prepPeakIngredients(
        self, ingredients: FarmFreshIngredients, pixelMask: Optional[WorkspaceName] = None
    ) -> PeakIngredients:
        return PeakIngredients(
            crystalInfo=self.prepCrystallographicInfo(ingredients),
            instrumentState=self.prepInstrumentState(ingredients),
            pixelGroup=self.prepPixelGroup(ingredients, pixelMask=pixelMask),
            peakIntensityThreshold=ingredients.peakIntensityThreshold,
        )

    @staticmethod
    def parseGroupPeakList(src: str) -> List[GroupPeakList]:
        # Implemented as a separate method to facilitate testing
        return pydantic.TypeAdapter(List[GroupPeakList]).validate_json(src)

    def prepDetectorPeaks(
        self, ingredients: FarmFreshIngredients, purgePeaks=True, pixelMask: Optional[WorkspaceName] = None
    ) -> List[GroupPeakList]:
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
            ingredients.calibrantSamplePath,
            purgePeaks,
            pixelMask,
        )
        crystalDMin = ingredients.crystalDBounds.minimum
        crystalDMax = ingredients.crystalDBounds.maximum
        ingredients.peakIntensityThreshold = self._getThresholdFromCalibrantSample(ingredients.calibrantSamplePath)
        if key not in self._peaksCache:
            ingredients = self.prepPeakIngredients(ingredients, pixelMask=pixelMask)
            res = DetectorPeakPredictorRecipe().executeRecipe(
                Ingredients=ingredients,
            )

            if purgePeaks:
                res = PurgeOverlappingPeaksRecipe().executeRecipe(
                    Ingredients=ingredients,
                    DetectorPeaks=res,
                    crystalDMin=crystalDMin,
                    crystalDMax=crystalDMax,
                )

            self._peaksCache[key] = self.parseGroupPeakList(res)

        return deepcopy(self._peaksCache[key])

    def prepManyDetectorPeaks(
        self, ingredients: FarmFreshIngredients, pixelMask: Optional[WorkspaceName] = None
    ) -> List[List[GroupPeakList]]:
        # this also needs to check if it is in fact the default calibration
        if ingredients.calibrantSamplePath is None:
            mode = "lite" if ingredients.useLiteMode else "native"
            self.logger().debug(f"No calibrant sample found for run {ingredients.runNumber} in {mode} mode.")
            return None

        detectorPeaks = []
        ingredients_ = ingredients.model_copy()
        for focusGroup in ingredients.focusGroups:
            ingredients_.focusGroup = focusGroup
            detectorPeaks.append(self.prepDetectorPeaks(ingredients_, purgePeaks=False, pixelMask=pixelMask))
        return detectorPeaks

    # FFI = Farm Fresh Ingredients
    def _pullCalibrationRecordFFI(
        self,
        ingredients: FarmFreshIngredients,
    ) -> FarmFreshIngredients:
        if ingredients.versions.calibration is None:
            raise ValueError("Calibration version must be specified")
        calibrationRecord = self.dataFactoryService.getCalibrationRecord(
            ingredients.runNumber,
            ingredients.useLiteMode,
            ingredients.versions.calibration,
            ingredients.alternativeState,
        )
        if calibrationRecord is not None:
            ingredients.calibrantSamplePath = calibrationRecord.calculationParameters.calibrantSamplePath
            if ingredients.calibrantSamplePath:
                ingredients.cifPath = self.dataFactoryService.getCifFilePath(Path(ingredients.calibrantSamplePath).stem)
        return ingredients

    """
    def _pullManyCalibrationDetectorPeaks(
        self, ingredients: FarmFreshIngredients, runNumber: str, useLiteMode: bool
    ) -> FarmFreshIngredients:
        calibrationRecord = self.dataFactoryService.getCalibrationRecord(runNumber, useLiteMode)

        if ingredients.cifPath is None:
            ingredients = self._pullCalibrationRecordFFI(ingredients, runNumber, useLiteMode)

        detectorPeaks = None
        if calibrationRecord is not None:
            detectorPeaks = self.prepManyDetectorPeaks(ingredients)

        return detectorPeaks
    """

    # FFI = Farm Fresh Ingredients
    def _pullNormalizationRecordFFI(
        self,
        ingredients: FarmFreshIngredients,
    ) -> Tuple[FarmFreshIngredients, float, Optional[str]]:
        normalizationRecord = self.dataFactoryService.getNormalizationRecord(
            ingredients.runNumber, ingredients.useLiteMode, ingredients.versions.normalization
        )
        smoothingParameter = Config["calibration.parameters.default.smoothing"]
        calibrantSamplePath = None
        if normalizationRecord is not None:
            smoothingParameter = normalizationRecord.smoothingParameter
            calibrantSamplePath = normalizationRecord.normalizationCalibrantSamplePath
        # TODO: Should smoothing parameter be an ingredient?
        return ingredients, smoothingParameter, calibrantSamplePath

    def prepReductionIngredients(
        self, ingredients: FarmFreshIngredients, combinedPixelMask: Optional[WorkspaceName] = None
    ) -> ReductionIngredients:
        ingredients_ = ingredients.model_copy()
        # some of the reduction ingredients MUST match those used in the calibration/normalization processes
        ingredients_ = self._pullCalibrationRecordFFI(ingredients_)
        ingredients_, smoothingParameter, calibrantSamplePath = self._pullNormalizationRecordFFI(ingredients_)
        ingredients_.calibrantSamplePath = calibrantSamplePath

        return ReductionIngredients(
            runNumber=ingredients_.runNumber,
            useLiteMode=ingredients_.useLiteMode,
            timestamp=ingredients_.timestamp,
            pixelGroups=self.prepManyPixelGroups(ingredients_, combinedPixelMask),
            unmaskedPixelGroups=self.prepManyPixelGroups(ingredients_),
            smoothingParameter=smoothingParameter,
            calibrantSamplePath=ingredients_.calibrantSamplePath,
            peakIntensityThreshold=self._getThresholdFromCalibrantSample(ingredients_.calibrantSamplePath),
            detectorPeaksMany=self.prepManyDetectorPeaks(ingredients_, combinedPixelMask),
            keepUnfocused=ingredients_.keepUnfocused,
            convertUnitsTo=ingredients_.convertUnitsTo,
        )

    def verifyCalibrationExists(self, runNumber: str, useLiteMode: bool) -> bool:
        if not self.dataFactoryService.calibrationExists(runNumber, useLiteMode):
            recoveryData = {
                "runNumber": runNumber,
                "useLiteMode": useLiteMode,
            }
            raise RecoverableException(
                f"No calibration record found for run {runNumber}.",
                flags=RecoverableException.Type.STATE_UNINITIALIZED,
                data=recoveryData,
            )

    def prepNormalizationIngredients(self, ingredients: FarmFreshIngredients) -> NormalizationIngredients:
        # The calibration folder at the very least should be initialized with a default calibration
        self.verifyCalibrationExists(ingredients.runNumber, ingredients.useLiteMode)

        return NormalizationIngredients(
            pixelGroup=self.prepPixelGroup(ingredients),
            calibrantSample=self.prepCalibrantSample(ingredients.calibrantSamplePath),
            detectorPeaks=self.prepDetectorPeaks(ingredients, purgePeaks=False),
            instrumentState=self.prepInstrumentState(ingredients),
        )

    def prepDiffractionCalibrationIngredients(
        self, ingredients: FarmFreshIngredients
    ) -> DiffractionCalibrationIngredients:
        self.verifyCalibrationExists(ingredients.runNumber, ingredients.useLiteMode)

        return DiffractionCalibrationIngredients(
            runConfig=self.prepRunConfig(ingredients.runNumber),
            pixelGroup=self.prepPixelGroup(ingredients),
            groupedPeakLists=self.prepDetectorPeaks(ingredients),
            peakFunction=ingredients.peakFunction,
            convergenceThreshold=ingredients.convergenceThreshold,
            maxOffset=ingredients.maxOffset,
            maxChiSq=ingredients.maxChiSq,
        )

    def _getThresholdFromCalibrantSample(self, calibrantSamplePath: str) -> float:
        if calibrantSamplePath is None:
            return Config["constants.PeakIntensityFractionThreshold"]
        else:
            if not Path(calibrantSamplePath).is_absolute():
                samplePath: str = Config["samples.home"]
                calibrantSamplePath = os.path.join(samplePath, calibrantSamplePath)
            if not os.path.exists(calibrantSamplePath):
                raise FileNotFoundError(f"Calibrant sample file {calibrantSamplePath} does not exist.")
            calibrantSample = self.prepCalibrantSample(calibrantSamplePath)
            return calibrantSample.peakIntensityFractionThreshold
