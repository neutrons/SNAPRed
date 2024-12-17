from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients.ApplyNormalizationIngredients import ApplyNormalizationIngredients

# These are from the same `__init__` module, so for the moment, we require the full import specifications.
# (That is, not just "from snapred.backend.dao.ingredients import ...".)
from snapred.backend.dao.ingredients.ArtificialNormalizationIngredients import ArtificialNormalizationIngredients
from snapred.backend.dao.ingredients.EffectiveInstrumentIngredients import EffectiveInstrumentIngredients
from snapred.backend.dao.ingredients.GenerateFocussedVanadiumIngredients import GenerateFocussedVanadiumIngredients
from snapred.backend.dao.ingredients.PreprocessReductionIngredients import PreprocessReductionIngredients
from snapred.backend.dao.ingredients.ReductionGroupProcessingIngredients import ReductionGroupProcessingIngredients
from snapred.backend.dao.state.PixelGroup import PixelGroup


class ReductionIngredients(BaseModel):
    """Data class to hold the ingredients for each subrecipe of reduction and itself"""

    runNumber: str
    useLiteMode: bool
    timestamp: float

    pixelGroups: List[PixelGroup]
    unmaskedPixelGroups: List[PixelGroup]

    # these should come from calibration / normalization records
    # But will not exist if we proceed without calibration / normalization
    # NOTE: These are peaks for normalization, and thus should use the
    # Calibrant Sample for the Normalization
    detectorPeaksMany: Optional[List[List[GroupPeakList]]] = None
    smoothingParameter: Optional[float]
    calibrantSamplePath: Optional[str]
    peakIntensityThreshold: Optional[float]

    keepUnfocused: bool
    convertUnitsTo: str
    artificialNormalizationIngredients: Optional[ArtificialNormalizationIngredients] = None

    #
    # FACTORY methods to create sub-recipe ingredients:
    #
    def preprocess(self) -> PreprocessReductionIngredients:
        # At present, `PreprocessReductionIngredients` has no required parameters.
        return PreprocessReductionIngredients()

    def getDetectorPeaks(self, groupingIndex: int) -> List[GroupPeakList]:
        if self.detectorPeaksMany is None:
            return None
        return self.detectorPeaksMany[groupingIndex]

    def groupProcessing(self, groupingIndex: int) -> ReductionGroupProcessingIngredients:
        return ReductionGroupProcessingIngredients(pixelGroup=self.pixelGroups[groupingIndex])

    def generateFocussedVanadium(self, groupingIndex: int) -> GenerateFocussedVanadiumIngredients:
        return GenerateFocussedVanadiumIngredients(
            smoothingParameter=self.smoothingParameter,
            pixelGroup=self.pixelGroups[groupingIndex],
            detectorPeaks=self.getDetectorPeaks(groupingIndex),
            artificialNormalizationIngredients=self.artificialNormalizationIngredients,
        )

    def applyNormalization(self, groupingIndex: int) -> ApplyNormalizationIngredients:
        return ApplyNormalizationIngredients(
            pixelGroup=self.pixelGroups[groupingIndex],
        )

    def effectiveInstrument(self, groupingIndex: int) -> EffectiveInstrumentIngredients:
        return EffectiveInstrumentIngredients(unmaskedPixelGroup=self.unmaskedPixelGroups[groupingIndex])

    model_config = ConfigDict(
        extra="forbid",
    )
