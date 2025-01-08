from typing import Dict, List, Optional

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

    # Changed to dict keyed by group ID
    pixelGroups: Dict[int, PixelGroup]
    unmaskedPixelGroups: List[PixelGroup]

    # Peaks now stored in a dict keyed by group ID
    detectorPeaksMany: Optional[Dict[int, List[GroupPeakList]]] = None
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

    # Consider renaming `groupingIndex` to `groupID` for clarity.
    def getDetectorPeaks(self, groupID: int) -> Optional[List[GroupPeakList]]:
        if self.detectorPeaksMany is None:
            return None
        return self.detectorPeaksMany.get(groupID)

    def groupProcessing(self, groupID: int) -> ReductionGroupProcessingIngredients:
        return ReductionGroupProcessingIngredients(pixelGroup=self.pixelGroups[groupID])

    def generateFocussedVanadium(self, groupID: int) -> GenerateFocussedVanadiumIngredients:
        return GenerateFocussedVanadiumIngredients(
            smoothingParameter=self.smoothingParameter,
            pixelGroup=self.pixelGroups[groupID],
            detectorPeaks=self.getDetectorPeaks(groupID),
        )

    def applyNormalization(self, groupID: int) -> ApplyNormalizationIngredients:
        return ApplyNormalizationIngredients(
            pixelGroup=self.pixelGroups[groupID],
        )

    def effectiveInstrument(self, groupID: int) -> EffectiveInstrumentIngredients:
        return EffectiveInstrumentIngredients(unmaskedPixelGroup=self.unmaskedPixelGroups[groupID])

    model_config = ConfigDict(
        extra="forbid",
    )
