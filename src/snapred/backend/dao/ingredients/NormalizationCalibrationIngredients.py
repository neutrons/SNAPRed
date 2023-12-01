from pydantic import BaseModel

from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.state import InstrumentState
from snapred.backend.dao.state.CalibrantSample import CalibrantSamples
from snapred.backend.dao.state.FocusGroup import FocusGroup


class NormalizationCalibrationIngredients(BaseModel):
    """Class to hold the ingredients necessary for normalization calibration algo."""

    reductionIngredients: ReductionIngredients
    calibrantSample: CalibrantSamples
    smoothDataIngredients: SmoothDataExcludingPeaksIngredients
