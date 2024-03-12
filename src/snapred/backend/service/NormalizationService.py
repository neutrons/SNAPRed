import re
import time
from typing import Any, Dict

from snapred.backend.dao import Limit
from snapred.backend.dao.ingredients import (
    GroceryListItem,
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
    """
    Implements the NormalizationService class, handling the normalization process within SNAPRed.

    This service orchestrates various normalization tasks such as calibration and smoothing of
    scientific data, utilizing a range of data objects, services, and recipes. It is a pivotal
    component designed to streamline the normalization workflow, ensuring efficiency and accuracy
    across operations.

    Attributes:
        - dataFactoryService (DataFactoryService): Manages creation and retrieval of data objects for
          normalization tasks.
        - dataExportService (DataExportService): Enables the export of processed data to persistent
          storage systems.
        - groceryService (GroceryService): Interfaces with the data layer to fetch and manage
          normalization-relevant data.
        - groceryClerk (GroceryListItem.builder): Utilizes the builder pattern to assemble required
          data items for normalization processes.
        - diffractionCalibrationService (CalibrationService): Specializes in calibration tasks for
          diffraction data, ensuring precision and accuracy.
        - sousChef (SousChef): Prepares the necessary ingredients for the normalization recipe,
          optimizing the preparation phase.

    Key Operations:
        - Handles the entire normalization workflow, including correction, focusing, and smoothing
          of data.
        - Validates the consistency of instrument states across runs.
        - Assesses the outcomes of normalization, facilitating data quality evaluation.
        - Persists normalization data and metadata following user validation.
        - Manages the indexing of normalization records for efficient retrieval.

    Through its comprehensive suite of functionalities, the NormalizationService ensures that
    normalization processes are executed with high precision, contributing significantly to the
    integrity and reproducibility of scientific data analysis within the SNAPRed ecosystem.
    """

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
        if not self._sameStates(request.runNumber, request.backgroundRunNumber):
            raise ValueError("Run number and background run number must be of the same Instrument State.")

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
            peakIntensityThreshold=request.peakIntensityThreshold,
        )
        ingredients = self.sousChef.prepNormalizationIngredients(farmFresh)

        # prepare and check focus group workspaces -- see if grouping already calculated
        correctedVanadium = wng.rawVanadium().runNumber(request.runNumber).build()
        focusedVanadium = wng.run().runNumber(request.runNumber).group(groupingScheme).auxiliary("S+F-Vanadium").build()
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
        self.groceryClerk.name("groupingWorkspace").fromRun(request.runNumber).grouping(groupingScheme).useLiteMode(
            request.useLiteMode
        ).add()
        groceries = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
        )

        # NOTE: This used to point at other methods in this service to accomplish the same thing
        #       It looks like it got reverted accidentally?
        #       I'm leaving them as is but this should be fixed.
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

    def _sameStates(self, runnumber1, runnumber2):
        stateId1 = self.dataFactoryService.constructStateId(runnumber1)
        stateId2 = self.dataFactoryService.constructStateId(runnumber2)
        return stateId1 == stateId2

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
        normalizationRecord = self.dataExportService.exportNormalizationWorkspaces(normalizationRecord)
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
            peakIntensityThreshold=request.peakIntensityThreshold,
        )
        peaks = self.sousChef.prepDetectorPeaks(farmFresh)

        # execute recipe -- the output will be set by the algorithm
        SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            OutputWorkspace=request.outputWorkspace,
            DetectorPeaks=peaks,
            SmoothingParameter=request.smoothingParameter,
        )

        # we need the corrected vanadium workspace name to be in response
        # if this endpoint is being called, the vanadium already exists with the below name
        correctedVanadium = wng.rawVanadium().runNumber(request.runNumber).build()

        # return response
        return NormalizationResponse(
            correctedVanadium=correctedVanadium,
            focusedVanadium=request.inputWorkspace,
            smoothedVanadium=request.outputWorkspace,
            detectorPeaks=peaks,
        ).dict()
