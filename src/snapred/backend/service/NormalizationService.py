import re
import time
from typing import Any, Dict

from snapred.backend.dao.ingredients import (
    GroceryListItem,
    NormalizationIngredients,
    PeakIngredients,
)
from snapred.backend.dao.normalization import (
    Normalization,
    NormalizationIndexEntry,
    NormalizationRecord,
)
from snapred.backend.dao.request import (
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
    DetectorPeakPredictorRecipe,
    FocusSpectraRecipe,
    RawVanadiumCorrectionRecipe,
    SmoothDataExcludingPeaksRecipe,
)
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


@Singleton
class NormalizationService(Service):
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.crystallographicInfoService = CrystallographicInfoService()
        self.groceryClerk = GroceryListItem.builder()
        self.sousChef = SousChef()
        self.diffractionCalibrationService = CalibrationService()
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
        groupingFile = request.groupingPath
        groupingScheme = groupingFile.split("/")[-1].split(".")[0].replace("SNAPFocGroup_", "")

        self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(request.useLiteMode).add()
        self.groceryClerk.name("backgroundWorkspace").neutron(request.backgroundRunNumber).useLiteMode(
            request.useLiteMode
        ).add()
        self.groceryClerk.name("groupingWorkspace").grouping(groupingScheme).useLiteMode(
            request.useLiteMode
        ).fromPrev().add()

        outputWorkspace = wng.run().runNumber(request.runNumber).group(groupingScheme).auxilary("S+F-Vanadium").build()
        correctedVanadium = wng.run().runNumber(request.runNumber).group(groupingScheme).auxilary("C-Vanadium").build()
        smoothedOutput = (
            wng.run()
            .runNumber(request.runNumber)
            .group(groupingScheme)
            .auxilary(f"{request.smoothingParameter}-s_{request.dMin}-dmin")
            .build()
        )

        if (
            self.groceryService.workspaceDoesExist(outputWorkspace)
            and self.groceryService.workspaceDoesExist(correctedVanadium)
            and self.groceryService.workspaceDoesExist(smoothedOutput)
        ):
            return NormalizationResponse(
                correctedVanadium=correctedVanadium,
                outputWorkspace=outputWorkspace,
                smoothedOutput=smoothedOutput,
            ).dict()

        groceries = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
            outputWorkspace=outputWorkspace,
            smoothedOutput=smoothedOutput,
        )

        # 0. get normalization ingredients
        ingredients = self.sousChef.getNormalizationIngredients()

        # 1. correction
        vanadiumCorrectionRequest = VanadiumCorrectionRequest(
            backgroundWorkspace=groceries["backgroundWorkspace"],
            inputWorkspace=groceries["inputWorkspace"],
            outputWorkspace=groceries["outputWorkspace"],
            ingredients=ingredients,
        )
        outputWorkspace = self.vanadiumCorrection(vanadiumCorrectionRequest)
        # clone output correctedVanadium
        correctedVanadiumWs = correctedVanadium
        self.groceryService.getCloneOfWorkspace(outputWorkspace, correctedVanadiumWs)
        # 2. focus
        requestFs = FocusSpectraRequest(
            inputWorkspace=correctedVanadiumWs,
            outputWorkspace=outputWorkspace,
            groupingWorkspace=groceries["groupingWorkspace"],
            pixelGroup=ingredients.pixelGroup,
        )
        outputWorkspace = self.focusSpectra(requestFs)
        # clone output focussedVanadium
        focussedVanadiumWs = "focussedCorrectedVanadium"
        self.groceryService.getCloneOfWorkspace(outputWorkspace, focussedVanadiumWs)
        # 3. smooth

        smoothRequest = SmoothDataExcludingPeaksRequest(
            inputWorkspace=focussedVanadiumWs,
            outputWorkspace=smoothedOutput,
            smoothingParameter=request.smoothingParameter,
            detectorPeaks=ingredients.detectorPeaks,
        )
        outputWorkspace = self.smoothDataExcludingPeaks(smoothRequest)

        return NormalizationResponse(
            correctedVanadium=correctedVanadiumWs, outputWorkspace=focussedVanadiumWs, smoothedOutput=outputWorkspace
        ).dict()

    @FromString
    def normalizationAssessment(self, request: NormalizationCalibrationRequest):
        normalization = self.dataFactoryService.getNormalizationState(request.runNumber)
        record = NormalizationRecord(
            runNumber=request.runNumber,
            backgroundRunNumber=request.backgroundRunNumber,
            smoothingParameter=request.smoothingParameter,
            normalization=normalization,
        )
        return record

    @FromString
    def saveNormalization(self, request: NormalizationExportRequest):
        entry = request.normalizationIndexEntry
        normalizationRecord = request.normalizationRecord
        normalizationRecord = self.dataExportService.exportNormalizationRecord(normalizationRecord)
        entry.version = normalizationRecord.version
        self.saveNormalizationToIndex(entry)

    @FromString
    def saveNormalizationToIndex(self, entry: NormalizationIndexEntry):
        if entry.appliesTo is None:
            entry.appliesTo = ">" + entry.runNumber
        if entry.timestamp is None:
            entry.timestamp = int(round(time.time() * 1000))
        logger.info(f"Saving normalization index entry for Run Number {entry.runNumber}")
        self.dataExportService.exportNormalizationIndexEntry(entry)

    @FromString
    def vanadiumCorrection(self, request: VanadiumCorrectionRequest):
        return RawVanadiumCorrectionRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            BackgroundWorkspace=request.backgroundWorkspace,
            Ingredients=request.ingredients,
            OutputWorkspace=request.outputWorkspace,
        )

    @FromString
    def focusSpectra(self, request: FocusSpectraRequest):
        return FocusSpectraRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            GroupingWorkspace=request.groupingWorkspace,
            Ingredients=request.pixelGroup,
            OutputWorkspace=request.outputWorkspace,
        )

    def _getDetectorPeaks(self, x):
        sampleFilePath = self.dataFactoryService.getCifFilePath((request.samplePath).split("/")[-1].split(".")[0])
        crystalInfo = self.crystallographicInfoService.ingest(sampleFilePath, request.dMin)["crystalInfo"]

        calibration = self.diffractionCalibrationService.getCalibration(request.runNumber, request.groupingPath)
        pixelGroup = self.diffractionCalibrationService.getPixelGroup(
            request.runNumber, request.groupingPath, request.useLiteMode, request.nBinsAcrossPeakWidth, calibration
        )

        ingredients = PeakIngredients(
            smoothingParameter=request.smoothingParameter,
            instrumentState=calibration.instrumentState,
            pixelGroup=pixelGroup,
            crystalInfo=crystalInfo,
        )
        return DetectorPeakPredictorRecipe().executeRecipe(Ingredients=ingredients)

    @FromString
    def smoothDataExcludingPeaks(self, request: SmoothDataExcludingPeaksRequest):
        return SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            OutputWorkspace=request.outputWorkspace,
            DetectorPeaks=request.detectorPeaks,
            SmoothingParameter=request.smoothingParameter,
        )
