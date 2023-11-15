from pydantic import BaseModel

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state import InstrumentState
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.FocusGroup import FocusGroup


class NormalizationCalibrationIngredients(BaseModel):
    """Class to hold the ingredients necessary for normalization calibration algo."""

    run: RunConfig
    backgroundRun: RunConfig
    reductionIngredients: ReductionIngredients
    calibrationRecord: CalibrationRecord
    calibrantSample: CalibrantSamples
    focusGroup: FocusGroup
    instrumentState: InstrumentState
    calibrationWorkspace: str
