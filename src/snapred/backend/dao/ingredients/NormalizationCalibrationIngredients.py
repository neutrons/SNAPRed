from pydantic import BaseModel

from snapred.backend.dao.ingredients.PeakIngredients import PeakIngredients
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.state.CalibrantSample import CalibrantSamples


class NormalizationCalibrationIngredients(BaseModel):
    """Class to hold the ingredients necessary for normalization calibration algo."""

    reductionIngredients: ReductionIngredients
    backgroundReductionIngredients: ReductionIngredients
    calibrantSample: CalibrantSamples
    smoothDataIngredients: PeakIngredients
