from typing import List

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.GroupPeakList import GroupPeakList

# These are from the same `__init__` module, so for the moment, we require the full import specifications.
# (That is, not just "from snapred.backend.dao.ingredients import ...".)
from snapred.backend.dao.ingredients.ApplyNormalizationIngredients import ApplyNormalizationIngredients
from snapred.backend.dao.ingredients.GenerateFocussedVanadiumIngredients import GenerateFocussedVanadiumIngredients
from snapred.backend.dao.ingredients.PreprocessReductionIngredients import PreprocessReductionIngredients
from snapred.backend.dao.ingredients.ReductionGroupProcessingIngredients import ReductionGroupProcessingIngredients
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class ReductionIngredients(BaseModel):
    """Data class to hold the ingredients for each subrecipe of reduction and itself"""

    maskList: List[WorkspaceName]
    pixelGroups: List[PixelGroup]
    detectorPeaksMany: List[List[GroupPeakList]]

    # these should come from calibration / normalization records
    smoothingParameter: float
    calibrantSamplePath: str
    peakIntensityThreshold: float

    keepUnfocused: bool
    convertUnitsTo: str

    #
    # FACTORY methods to create sub-recipe ingredients:
    #
    def preprocess(self) -> PreprocessReductionIngredients:
        # Note: at present, there are no required parameters.
        return PreprocessReductionIngredients(maskList=None)

    def groupProcessing(self, groupingIndex: int) -> ReductionGroupProcessingIngredients:
        return ReductionGroupProcessingIngredients(pixelGroup=self.pixelGroups[groupingIndex])

    def generateFocussedVanadium(self, groupingIndex: int) -> GenerateFocussedVanadiumIngredients:
        return GenerateFocussedVanadiumIngredients(
            smoothingParameter=self.smoothingParameter,
            pixelGroup=self.pixelGroups[groupingIndex],
            detectorPeaks=self.detectorPeaksMany[groupingIndex],
        )

    def applyNormalization(self, groupingIndex: int) -> ApplyNormalizationIngredients:
        return ApplyNormalizationIngredients(
            pixelGroup=self.pixelGroups[groupingIndex],
        )

    model_config = ConfigDict(
        extra="forbid",
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
