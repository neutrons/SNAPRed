from pathlib import Path
from typing import Any, Dict

from snapred.backend.dao import Limit
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VersionState
from snapred.backend.dao.ingredients import (
    GroceryListItem,
)
from snapred.backend.dao.normalization import (
    Normalization,
)
from snapred.backend.dao.request import (
    CalculateNormalizationResidualRequest,
    CalibrationWritePermissionsRequest,
    CreateNormalizationRecordRequest,
    FarmFreshIngredients,
    FocusSpectraRequest,
    MatchRunsRequest,
    NormalizationExportRequest,
    NormalizationRequest,
    SmoothDataExcludingPeaksRequest,
    VanadiumCorrectionRequest,
)
from snapred.backend.dao.response.NormalizationResponse import NormalizationResponse
from snapred.backend.dao.WorkspaceMetadata import DiffcalStateMetadata, NormalizationStateMetadata, WorkspaceMetadata
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.GenericRecipe import (
    ConvertUnitsRecipe,
    FocusSpectraRecipe,
    MinusRecipe,
    RawVanadiumCorrectionRecipe,
    SmoothDataExcludingPeaksRecipe,
)
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.service.Service import Register, Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceName,
)
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.pointer import create_pointer
from snapred.meta.redantic import parse_obj_as

logger = snapredLogger.getLogger(__name__)


@Singleton
class NormalizationService(Service):
    """

    This service orchestrates various normalization tasks such as calibration and smoothing of
    scientific data, utilizing a range of data objects, services, and recipes. It is a pivotal
    component designed to streamline the normalization workflow, ensuring efficiency and accuracy across operations.

    """

    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.dataExportService = DataExportService()
        self.groceryService = GroceryService()
        self.groceryClerk = GroceryListItem.builder()
        self.diffractionCalibrationService = CalibrationService()
        self.sousChef = SousChef()
        return

    @staticmethod
    def name():
        return "normalization"

    @FromString
    @Register("")
    def normalization(self, request: NormalizationRequest):
        self.validateRequest(request)

        groupingScheme = request.focusGroup.name

        # prepare ingredients
        cifPath = self.dataFactoryService.getCifFilePath(Path(request.calibrantSamplePath).stem)
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroups=[request.focusGroup],
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            crystalDBounds=request.crystalDBounds,
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
        calVersion = self.dataFactoryService.getLatestApplicableCalibrationVersion(
            request.runNumber, request.useLiteMode
        )
        # gather needed groceries and ingredients
        if request.correctedVanadiumWs is None:
            self.groceryClerk.name("inputWorkspace").neutron(request.runNumber).useLiteMode(
                request.useLiteMode
            ).diffCalVersion(calVersion).dirty().add()
            self.groceryClerk.name("backgroundWorkspace").neutron(request.backgroundRunNumber).useLiteMode(
                request.useLiteMode
            ).diffCalVersion(calVersion).dirty().add()
        else:
            # check that the corrected vanadium workspaces exist already
            if request.correctedVanadiumWs != correctedVanadium:
                raise RuntimeError(
                    (
                        "Corrected vanadium of unexpected name provided. Is this workspace still compatible?"
                        f"{request.correctedVanadiumWs} vs {correctedVanadium}"
                    )
                )
            if not self.groceryService.workspaceDoesExist(correctedVanadium):
                raise RuntimeError(f"Supplied corrected vanadium {correctedVanadium} does not exist.")

        self.groceryClerk.name("groupingWorkspace").fromRun(request.runNumber).grouping(groupingScheme).useLiteMode(
            request.useLiteMode
        ).add()

        calRunNumber = self.dataFactoryService.getCalibrationRecord(
            request.runNumber, request.useLiteMode, calVersion
        ).runNumber

        self.groceryClerk.name("maskWorkspace").diffcal_mask(request.runNumber, calVersion).useLiteMode(
            request.useLiteMode
        ).add()

        groceries = self.groceryService.fetchGroceryDict(
            self.groceryClerk.buildDict(),
        )

        if request.correctedVanadiumWs is None:
            self._markWorkspaceMetadata(request, groceries["inputWorkspace"])
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

            # TODO: delete inputWorkspace and backgroundWorkspace as they are no longer needed
            self.groceryService.deleteWorkspace(groceries["inputWorkspace"])
            self.groceryService.deleteWorkspace(groceries["backgroundWorkspace"])
        # 1.5 Apply latest calibration before focussing, if unable to mark it as uncalibrated?
        # Apply diffcal and mask
        groceries["inputWorkspace"] = correctedVanadium
        groceries["outputWorkspace"] = focusedVanadium

        # focus and normalize by current
        ConvertUnitsRecipe().executeRecipe(
            InputWorkspace=groceries["inputWorkspace"],
            OutputWorkspace=groceries["outputWorkspace"],
            Target="dSpacing",
            EMode="Elastic",
        )

        groceries["inputWorkspace"] = focusedVanadium
        groceries["outputWorkspace"] = focusedVanadium

        ReductionGroupProcessingRecipe().cook(
            ReductionGroupProcessingRecipe.Ingredients(pixelGroup=ingredients.pixelGroup),
            groceries={
                "inputWorkspace": groceries["inputWorkspace"],
                "outputWorkspace": groceries["outputWorkspace"],
                "groupingWorkspace": groceries["groupingWorkspace"],
                "maskWorkspace": groceries["maskWorkspace"],
            },
        )

        # 2. focus
        # 3. smooth
        SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=focusedVanadium,
            DetectorPeaks=create_pointer(ingredients.detectorPeaks),
            SmoothingParameter=request.smoothingParameter,
            OutputWorkspace=smoothedVanadium,
        )
        # done
        return NormalizationResponse(
            correctedVanadium=correctedVanadium,
            focusedVanadium=focusedVanadium,
            smoothedVanadium=smoothedVanadium,
            detectorPeaks=ingredients.detectorPeaks,
            calibrationRunNumber=calRunNumber,
        ).dict()

    def _markWorkspaceMetadata(self, request: NormalizationRequest, workspace: WorkspaceName):
        calibrationState = (
            DiffcalStateMetadata.DEFAULT
            if ContinueWarning.Type.DEFAULT_DIFFRACTION_CALIBRATION in request.continueFlags
            else DiffcalStateMetadata.EXISTS
        )
        metadata = WorkspaceMetadata(diffcalState=calibrationState, normalizationState=NormalizationStateMetadata.UNSET)
        self.groceryService.writeWorkspaceMetadataAsTags(workspace, metadata)

    def validateRequest(self, request: NormalizationRequest):
        """
        Validate the normalization request.

        :param request: a normalization request
        :type request: NormalizationRequest
        """
        if not self._sameStates(request.runNumber, request.backgroundRunNumber):
            raise ValueError("Run number and background run number must be of the same Instrument State.")

        # This is a redundant call, but it is placed here to facilitate re-sequencing.
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=request.runNumber, continueFlags=request.continueFlags
        )
        self.validateWritePermissions(permissionsRequest)
        self._validateDiffractionCalibrationExists(request)

    def _validateDiffractionCalibrationExists(self, request: NormalizationRequest):
        continueFlags = ContinueWarning.Type.UNSET

        self.sousChef.verifyCalibrationExists(request.runNumber, request.useLiteMode)

        calVersion = self.dataFactoryService.getLatestApplicableCalibrationVersion(
            request.runNumber, request.useLiteMode
        )
        if calVersion is None:
            continueFlags = continueFlags | ContinueWarning.Type.DEFAULT_DIFFRACTION_CALIBRATION

        if request.continueFlags:
            continueFlags = continueFlags ^ (request.continueFlags & continueFlags)

        if continueFlags:
            raise ContinueWarning(
                "Only the default Diffraction Calibration data is available for this run.\n"
                "Normalizations may not be accurate to true state of the instrument, and will be marked as such.\n"
                "Continue anyway?",
                continueFlags,
            )

    @Register("validateWritePermissions")
    def validateWritePermissions(self, request: CalibrationWritePermissionsRequest):
        """
        Validate that the normalization-calibration workflow will be able to save its output.

        :param request: a write-permissions request containing the run number and existing continue flags
        :type request: CalibrationWritePermissionsRequest
        """
        # Note: this is split-out as a separate method so it can be checked as early as possible in the workflow.

        # check that the user has write permissions to the save directory
        if not self.checkWritePermissions(request.runNumber):
            raise RuntimeError(
                "<font size = "
                "2"
                " >"
                + "<p>It looks like you don't have permissions to write to "
                + f"<br><b>{self.getSavePath(request.runNumber)}</b>,<br>"
                + "which is a requirement in order to run the normalization-calibration workflow.</p>"
                + "<p>If this is something that you need to do, then you may need to change the "
                + "<br><b>instrument.calibration.powder.home</b> entry in SNAPRed's <b>application.yml</b> file.</p>"
                + "</font>"
            )

    def _sameStates(self, runnumber1, runnumber2):
        stateId1 = self.dataFactoryService.constructStateId(runnumber1)
        stateId2 = self.dataFactoryService.constructStateId(runnumber2)
        return stateId1[0] == stateId2[0]

    def checkWritePermissions(self, runNumber: str) -> bool:
        path = self.dataExportService.getCalibrationStateRoot(runNumber)
        return self.dataExportService.checkWritePermissions(path)

    def getSavePath(self, runNumber: str) -> Path:
        return self.dataExportService.getCalibrationStateRoot(runNumber)

    @FromString
    @Register("assessment")
    def normalizationAssessment(self, request: NormalizationRequest):
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            focusGroups=[request.focusGroup],
            useLiteMode=request.useLiteMode,
            calibrantSamplePath=request.calibrantSamplePath,
            fwhmMultipliers=request.fwhmMultipliers,
            crystalDBounds=request.crystalDBounds,
        )
        normalization = parse_obj_as(Normalization, self.sousChef.prepCalibration(farmFresh))

        createRecordRequest = CreateNormalizationRecordRequest(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            backgroundRunNumber=request.backgroundRunNumber,
            smoothingParameter=request.smoothingParameter,
            normalizationCalibrantSamplePath=request.calibrantSamplePath,
            calculationParameters=normalization,
            crystalDBounds=request.crystalDBounds,
            version=VersionState.NEXT,
        )
        return self.dataFactoryService.createNormalizationRecord(createRecordRequest)

    @FromString
    @Register("save")
    def saveNormalization(self, request: NormalizationExportRequest):
        """
        If no version is attached to the request, this will save at next version number
        """
        entry = self.dataFactoryService.createNormalizationIndexEntry(request.createIndexEntryRequest)
        record = self.dataFactoryService.createNormalizationRecord(request.createRecordRequest)
        version = entry.version

        # rename the workspaces to include version number
        savedWorkspaces = []
        for workspace in record.workspaceNames:
            newName = workspace + "_" + wnvf.formatVersion(version)
            self.groceryService.renameWorkspace(workspace, newName)
            savedWorkspaces.append(newName)
        record.workspaceNames = savedWorkspaces

        # save the objects at the indicated version
        self.dataExportService.exportNormalizationRecord(record, entry)
        self.dataExportService.exportNormalizationWorkspaces(record)

    def saveNormalizationToIndex(self, entry: IndexEntry):
        """
        Correct version must be attached to the entry.
        """
        if entry.appliesTo is None:
            entry.appliesTo = ">=" + entry.runNumber
        if entry.timestamp is None:
            entry.timestamp = self.dataExportService.getUniqueTimestamp()
        logger.info(f"Saving normalization index entry for Run Number {entry.runNumber}")
        self.dataExportService.exportNormalizationIndexEntry(entry)

    def vanadiumCorrection(self, request: VanadiumCorrectionRequest):
        cifPath = self.dataFactoryService.getCifFilePath(Path(request.calibrantSamplePath).stem)
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroups=[request.focusGroup],
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
            focusGroups=[request.focusGroup],
        )
        ingredients = self.sousChef.prepPixelGroup(farmFresh)
        return FocusSpectraRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            GroupingWorkspace=request.groupingWorkspace,
            Ingredients=ingredients,
            OutputWorkspace=request.outputWorkspace,
        )

    @FromString
    @Register("smooth")
    def smoothDataExcludingPeaks(self, request: SmoothDataExcludingPeaksRequest):
        cifPath = self.dataFactoryService.getCifFilePath(Path(request.calibrantSamplePath).stem)
        farmFresh = FarmFreshIngredients(
            runNumber=request.runNumber,
            useLiteMode=request.useLiteMode,
            focusGroups=[request.focusGroup],
            cifPath=cifPath,
            calibrantSamplePath=request.calibrantSamplePath,
            crystalDBounds=Limit(minimum=request.crystalDMin, maximum=request.crystalDMax),
        )
        peaks = self.sousChef.prepDetectorPeaks(farmFresh, purgePeaks=False)

        # execute recipe -- the output will be set by the algorithm
        SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=request.inputWorkspace,
            OutputWorkspace=request.outputWorkspace,
            DetectorPeaks=create_pointer(peaks),
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

    def matchRunsToNormalizationVersions(self, request: MatchRunsRequest) -> Dict[str, Any]:
        """
        For each run in the list, find the calibration version that applies to it
        """
        response = {}
        for runNumber in request.runNumbers:
            response[runNumber] = self.dataFactoryService.getLatestApplicableNormalizationVersion(
                runNumber, request.useLiteMode
            )
        return response

    @FromString
    @Register("fetchMatches")
    def fetchMatchingNormalizations(self, request: MatchRunsRequest):
        normalizations = self.matchRunsToNormalizationVersions(request)
        for runNumber in request.runNumbers:
            if normalizations.get(runNumber) is not None:
                self.groceryClerk.normalization(runNumber, normalizations[runNumber]).useLiteMode(
                    request.useLiteMode
                ).add()
        return set(self.groceryService.fetchGroceryList(self.groceryClerk.buildList())), normalizations

    @Register("calculateResidual")
    def calculateResidual(self, request: CalculateNormalizationResidualRequest):
        outputWorkspace = wng.normCalResidual().runNumber(request.runNumber).unit(wng.Units.DSP).build()
        MinusRecipe().executeRecipe(
            LHSWorkspace=request.dataWorkspace,
            RHSWorkspace=request.calculationWorkspace,
            OutputWorkspace=outputWorkspace,
        )
        return outputWorkspace
