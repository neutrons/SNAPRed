import re
import time
from typing import Any, Dict

from snapred.backend.dao import Limit
from snapred.backend.dao.ingredients import (
    GroceryListItem,
    NormalizationIngredients,
)
from snapred.backend.dao.normalization import (
    Normalization,
    NormalizationIndexEntry,
    NormalizationRecord,
)
from snapred.backend.dao.request import (
    FarmFreshIngredients,
    FocusSpectraRequest,
    NormalizationCalibrationRequest,
    NormalizationExportRequest,
    SmoothDataExcludingPeaksRequest,
    VanadiumCorrectionRequest,
)
from snapred.backend.dao.response.NormalizationResponse import NormalizationResponse
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.GenericRecipe import (
    FocusSpectraRecipe,
    RawVanadiumCorrectionRecipe,
    SmoothDataExcludingPeaksRecipe,
)
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)


@Singleton
class NormalizationService(Service):
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.groceryClerk = GroceryListItem.builder()
        self.diffractionCalibrationService = CalibrationService()
        self.sousChef = SousChef()
        self.registerPath("", self.normalization)
        self.registerPath("assessment", self.normalizationAssessment)
        self.registerPath("save", self.saveNormalization)
        self.registerPath("smooth", self.smoothDataExcludingPeaks)
        return

    @staticmethod
    def name():
        return "normalization"

    @FromString
    def normalization(self, request: NormalizationCalibrationRequest):
        groupingScheme = request.focusGroup.name

        # prepare ingredients
        cifPath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            crystalDBounds=Limit(minimum=request.crystalDMin, maximum=request.crystalDMax),
        )
        ingredients = self.sousChef.prepNormalizationIngredients(farmFresh)

        # outputWorkspace=wng.run().runNumber(request.runNumber).group(groupingScheme).auxilary("S+F-Vanadium").build()
        correctedVanadium = wng.rawVanadium().runNumber(request.runNumber).build()
        focusedVanadium = wng.run().runNumber(request.runNumber).group(groupingScheme).auxilary("S+F-Vanadium").build()
        # focusedVanadium = wng.focusedRawVanadium().runNumber(request.runNumber).group(groupingScheme).build()
        smoothedVanadium = wng.smoothedFocusedRawVanadium().runNumber(request.runNumber).group(groupingScheme).build()

        if (
            self.groceryService.workspaceDoesExist(correctedVanadium)
            and self.groceryService.workspaceDoesExist(focusedVanadium)
            and self.groceryService.workspaceDoesExist(smoothedVanadium)
        ):
            return NormalizationResponse(
                correctedVanadium=correctedVanadium,
                focusedVanadium=focusedVanadium,
                smoothedVanadium=smoothedVanadium,
                detectorPeaks=ingredients.detectorPeaks,
            ).dict()

        # gather needed groceries and ingredients
        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("backgroundWorkspace").neutron(request.backgroundRunNumber).useLiteMode(
            request.useLiteMode
        ).add()
        self.groceryClerk.name("groupingWorkspace").grouping(groupingScheme).useLiteMode(
            request.useLiteMode
        ).fromPrev().add()
        groceries = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
        )

        # 1. correctiom
        RawVanadiumCorrectionRecipe().executeRecipe(
            InputWorkspace=groceries["inputWorkspace"],
            BackgroundWorkspace=groceries["backgroundWorkspace"],
            Ingredients=ingredients,
            OutputWorkspace=correctedVanadium,
        )
        # 2. focus
        FocusSpectraRecipe().executeRecipe(
            InputWorkspace=correctedVanadium,
            GroupingWorkspace=groceries["groupingWorkspace"],
            Ingredients=ingredients.pixelGroup,
            OutputWorkspace=focusedVanadium,
        )
        # 3. smooth
        SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=focusedVanadium,
            DetectorPeaks=ingredients.detectorPeaks,
            SmoothingParameter=request.smoothingParameter,
            OutputWorkspace=smoothedVanadium,
        )
        # done
        return NormalizationResponse(
            correctedVanadium=correctedVanadium,
            focusedVanadium=focusedVanadium,
            smoothedVanadium=smoothedVanadium,
            detectorPeaks=ingredients.detectorPeaks,
        ).dict()

    @FromString
    def normalizationAssessment(self, request: NormalizationCalibrationRequest):
        calibration = self.dataFactoryService.getCalibrationState(request.runNumber)
        record = NormalizationRecord(
            runNumber=request.runNumber,
            backgroundRunNumber=request.backgroundRunNumber,
            smoothingParameter=request.smoothingParameter,
            calibration=calibration,
            dMin=request.crystalDMin,
        )
        return record

    @FromString
    def saveNormalization(self, request: NormalizationExportRequest):
        entry = request.normalizationIndexEntry
        normalizationRecord = request.normalizationRecord
        normalizationRecord = self.dataExportService.exportNormalizationRecord(normalizationRecord)
        entry.version = normalizationRecord.version
        self.saveNormalizationToIndex(entry)

    def saveNormalizationToIndex(self, entry: NormalizationIndexEntry):
        if entry.appliesTo is None:
            entry.appliesTo = ">" + entry.runNumber
        if entry.timestamp is None:
            entry.timestamp = int(round(time.time() * 1000))
        logger.info(f"Saving normalization index entry for Run Number {entry.runNumber}")
        self.dataExportService.exportNormalizationIndexEntry(entry)

    def vanadiumCorrection(self, request: VanadiumCorrectionRequest):
        cifPath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            crystalDBounds=Limit(minimum=request.crystalDMin, maximum=request.crystalDMax),
        )
        ingredients = self.sousChef.prepNormalizationIngredients(farmFresh)
        return RawVanadiumCorrectionRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            BackgroundWorkspace=request.backgroundWorkspace,
            Ingredients=ingredients,
            OutputWorkspace=request.outputWorkspace,
        )

    def focusSpectra(self, request: FocusSpectraRequest):
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
        )
        ingredients = self.sousChef.prepPixelGroup(farmFresh)
        return FocusSpectraRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            GroupingWorkspace=request.groupingWorkspace,
            Ingredients=ingredients,
            OutputWorkspace=request.outputWorkspace,
        )

    @FromString
    def smoothDataExcludingPeaks(self, request: SmoothDataExcludingPeaksRequest):
        cifPath = self.dataFactoryService.getCifFilePath(request.calibrantSamplePath.split("/")[-1].split(".")[0])
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            crystalDBounds=Limit(minimum=request.crystalDMin, maximum=request.crystalDMax),
        )
        peaks = self.sousChef.prepDetectorPeaks(farmFresh)

        SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            OutputWorkspace=request.outputWorkspace,
            DetectorPeaks=peaks,
            SmoothingParameter=request.smoothingParameter,
        )
        return peaks
