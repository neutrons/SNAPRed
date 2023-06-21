from pydantic import BaseModel, root_validator
from typing import Optional, List

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState

class FitMultiplePeaksIngredients(BaseModel):
    """Class to hold fit multiple peaks parameters"""

    instrumentState: InstrumentState
    crystalInfo: CrystallographicInfo
    peakType: Optional[str] = "Gaussian"
    inputWS: str
    showResult: Optional[bool] = False

    @root_validator(pre=True, allow_reuse=True)
    def validate_peak_type(cls, v):
        allowed_peak_types = ["AsymmetricPearsonVII", "BackToBackExponential", "Bk2BkExpConvPV",
                              "DeltaFunction", "ElasticDiffRotDiscreteCircle", "ElasticDiffSphere",
                              "ElasticIsoRotDiff", "ExamplePeakFunction", "Gaussian", "IkedaCarpenterPV",
                              "Lorentzian", "PseudoVoigt", "Voigt"]
        peakType = v.get("peakType")
        if peakType != None:
            if peakType.strip() not in allowed_peak_types:
                raise ValueError(f'{peakType} not found in {allowed_peak_types}')
        return v
