from pydantic import BaseModel

from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.state import InstrumentState
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.FocusGroup import FocusGroup


class NormalizationCalibrationIngredients(BaseModel):
    """Class to hold the ingredients necessary for normalization calibration algo."""

    reductionIngredients: ReductionIngredients
    backgroundReductionIngredients: ReductionIngredients
    focusGroup: FocusGroup
    calibrantSample: CalibrantSamples
    instrumentState: InstrumentState
    smoothDataIngredients: SmoothDataExcludingPeaksIngredients
