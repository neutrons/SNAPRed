import re
import time
from typing import Any, Dict

from snapred.backend.dao.ingredients import (
    GroceryListItem,
    SmoothDataExcludingPeaksIngredients,
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

        # 1. correction
        vanadiumCorrectionRequest = VanadiumCorrectionRequest(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
            calibrantSamplePath=request.calibrantSamplePath,
            inputWorkspace=groceries["inputWorkspace"],
            backgroundWorkspace=groceries["backgroundWorkspace"],
            outputWorkspace=groceries["outputWorkspace"],
        )
        outputWorkspace = self.vanadiumCorrection(vanadiumCorrectionRequest)
        # clone output correctedVanadium
        correctedVanadiumWs = correctedVanadium
        self.groceryService.getCloneOfWorkspace(outputWorkspace, correctedVanadiumWs)
        # 2. focus
        requestFs = FocusSpectraRequest(
            inputWorkspace=correctedVanadiumWs,
            groupingWorkspace=groceries["groupingWorkspace"],
            runNumber=request.runNumber,
            focusGroup=request.focusGroup,
            useLiteMode=request.useLiteMode,
            outputWorkspace=outputWorkspace,
        )
        outputWorkspace = self.focusSpectra(requestFs)
        # clone output focussedVanadium
        focussedVanadiumWs = "focussedCorrectedVanadium"
        self.groceryService.getCloneOfWorkspace(outputWorkspace, focussedVanadiumWs)
        # 3. smooth

        smoothRequest = SmoothDataExcludingPeaksRequest(
            inputWorkspace=focussedVanadiumWs,
            outputWorkspace=smoothedOutput,
            calibrantSamplePath=request.calibrantSamplePath,
            focusGroup=request.focusGroup,
            useLiteMode=request.useLiteMode,
            runNumber=request.runNumber,
            smoothingParameter=request.smoothingParameter,
            dMin=request.dMin,
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
        calibrantSample = self.sousChef.prepCalibrantSample(request.calibrantSamplePath)

        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
        )
        reductionIngredients = self.sousChef.prepReductionIngredients(farmFresh)

        return RawVanadiumCorrectionRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            BackgroundWorkspace=request.backgroundWorkspace,
            Ingredients=reductionIngredients,
            CalibrantSample=calibrantSample,
            OutputWorkspace=request.outputWorkspace,
        )

    @FromString
    def focusSpectra(self, request: FocusSpectraRequest):
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroup=request.focusGroup,
        )
        reductionIngredients = self.sousChef.prepReductionIngredients(farmFresh)
        return FocusSpectraRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            GroupingWorkspace=request.groupingWorkspace,
            Ingredients=reductionIngredients,
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
        )
        peakIngredients = self.sousChef.prepPeakIngredients(farmFresh)

        # TODO this will be replaced with simply PeakIngredients
        ingredients = SmoothDataExcludingPeaksIngredients(
            smoothingParameter=request.smoothingParameter,
            instrumentState=peakIngredients.instrumentState,
            crystalInfo=peakIngredients.crystalInfo,
        )
        return SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace, OutputWorkspace=request.outputWorkspace, Ingredients=ingredients
        )
