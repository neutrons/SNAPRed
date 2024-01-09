import re
import time
from typing import Any, Dict

from snapred.backend.dao.ingredients import (
    DiffractionFocussingIngredients,
    GroceryListItem,
    NormalizationCalibrationIngredients,
    SmoothDataExcludingPeaksIngredients,
    VanadiumCorrectionIngredients,
)
from snapred.backend.dao.normalization import (
    Normalization,
    NormalizationIndexEntry,
    NormalizationRecord,
)
from snapred.backend.dao.request import (
    FocusSpectraIngredients,
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
from snapred.backend.recipe.CalibrationNormalizationRecipe import CalibrationNormalizationRecipe
from snapred.backend.recipe.GenericRecipe import (
    FocusSpectraRecipe,
    RawVanadiumCorrectionRecipe,
    SmoothDataExcludingPeaksRecipe,
)
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton

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
        self.diffractionCalibrationService = CalibrationService()
        self.registerPath("", self.normalization)
        self.registerPath("assessment", self.normalizationAssessment)
        self.registerPath("save", self.saveNormalization)
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
        groceries = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
            outputWorkspace="smoothedFocussedCorrectedVanadium",
            smoothedOutput="smoothedOutput",
        )

        # 1. correction
        vanadiumCorrectionRequest = VanadiumCorrectionRequest(
            samplePath=request.samplePath,
            runNumber=request.runNumber,
            groupingPath=request.groupingPath,
            useLiteMode=request.useLiteMode,
            backgroundWorkspace=groceries["backgroundWorkspace"],
            inputWorkspace=groceries["inputWorkspace"],
            outputWorkspace=groceries["outputWorkspace"],
        )
        outputWorkspace = self.vanadiumCorrection(vanadiumCorrectionRequest)
        # clone output correctedVanadium
        correctedVanadiumWs = "correctedVanadium"
        self.groceryService.getCloneOfWorkspace(outputWorkspace, correctedVanadiumWs)
        # 2. focus
        requestFs = FocusSpectraRequest(
                inputWorkspace=correctedVanadiumWs,
                groupingWorkspace=groceries["groupingWorkspace"],
                runNumber=request.runNumber,
                groupingPath=request.groupingPath,
                useLiteMode=request.useLiteMode,
                outputWorkspace=outputWorkspace
        )
        outputWorkspace = self.focusSpectra(requestFs)
        # clone output focussedVanadium
        focussedVanadiumWs = "focussedCorrectedVanadium"
        self.groceryService.getCloneOfWorkspace(outputWorkspace, focussedVanadiumWs)
        # 3. smooth

        smoothRequest = SmoothDataExcludingPeaksRequest(
            inputWorkspace=focussedVanadiumWs,
            outputWorkspace="smoothedOutput",
            samplePath=request.samplePath,
            groupingPath=request.groupingPath,
            useLiteMode=request.useLiteMode,
            runNumber=request.runNumber,
            smoothingParameter=request.smoothingParameter,
            dMin=request.dMin,
        )
        outputWorkspace = self.smoothDataExcludingPeaks(smoothRequest)

        return NormalizationResponse(
            correctedVanadium=correctedVanadiumWs,
            outputWorkspace=focussedVanadiumWs,
            smoothedOutput=outputWorkspace
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
        calibrantSample = self.dataFactoryService.getCalibrantSample(request.samplePath)

        calibration = self.diffractionCalibrationService.getCalibration(
            request.runNumber, request.groupingPath, request.useLiteMode
        )
        pixelGroup = calibration.instrumentState.pixelGroup
        reductionIngredients = self.dataFactoryService.getReductionIngredients(request.runNumber, pixelGroup)

        return RawVanadiumCorrectionRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            BackgroundWorkspace=request.backgroundWorkspace,
            Ingredients=reductionIngredients,
            CalibrantSample=calibrantSample,
            OutputWorkspace=request.outputWorkspace,
        )

    @FromString
    def focusSpectra(self, request: FocusSpectraRequest):
        calibration = self.diffractionCalibrationService.getCalibration(
            request.runNumber, request.groupingPath, request.useLiteMode
        )
        pixelGroup = calibration.instrumentState.pixelGroup
        reductionIngredients = self.dataFactoryService.getReductionIngredients(request.runNumber, pixelGroup)
        return FocusSpectraRecipe().executeRecipe(InputWorkspace=request.inputWorkspace,
                GroupingWorkspace=request.groupingWorkspace,
                Ingredients=reductionIngredients,
                OutputWorkspace=request.outputWorkspace)
    
    @FromString
    def smoothDataExcludingPeaks(self, request: SmoothDataExcludingPeaksRequest):
        sampleFilePath = self.dataFactoryService.getCifFilePath((request.samplePath).split("/")[-1].split(".")[0])
        crystalInfo = self.crystallographicInfoService.ingest(sampleFilePath, request.dMin)["crystalInfo"]

        calibration = self.diffractionCalibrationService.getCalibration(
            request.runNumber, request.groupingPath, request.useLiteMode
        )

        instrumentState = calibration.instrumentState
        ingredients = SmoothDataExcludingPeaksIngredients(
            smoothingParameter=request.smoothingParameter,
            instrumentState=instrumentState,
            crystalInfo=crystalInfo,
            dMin=request.dMin,
        )
        return SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace, OutputWorkspace=request.outputWorkspace, Ingredients=ingredients
        )
