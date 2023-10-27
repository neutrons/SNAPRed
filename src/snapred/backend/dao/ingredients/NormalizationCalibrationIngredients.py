from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples


class NormalizationCalibrationIngredients(BaseModel):
    """Class to hold the ingredients necessary for normalization calibration algo."""

    run: RunConfig
    backgroundRun: RunConfig
    reductionIngredients: ReductionIngredients
    smoothDataIngredients: SmoothDataExcludingPeaksIngredients
    calibrationRecord: CalibrationRecord
    calibrantSample: CalibrantSamples
    calibrationWorkspace: str
