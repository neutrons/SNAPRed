from pydantic import BaseModel, root_validator

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.InstrumentState import InstrumentState


class FitMultiplePeaksIngredients(BaseModel):
    """Class to hold fit multiple peaks parameters"""

    instrumentState: InstrumentState
    crystalInfo: CrystallographicInfo
    peakType: str = "Gaussian"
    inputWorkspace: str
    showResult: bool = False

    @root_validator(pre=True, allow_reuse=True)
    def validate_peak_type(cls, v):
        allowed_peak_types = [
            "AsymmetricPearsonVII",
            "BackToBackExponential",
            "Bk2BkExpConvPV",
            "DeltaFunction",
            "ElasticDiffRotDiscreteCircle",
            "ElasticDiffSphere",
            "ElasticIsoRotDiff",
            "ExamplePeakFunction",
            "Gaussian",
            "IkedaCarpenterPV",
            "Lorentzian",
            "PseudoVoigt",
            "Voigt",
        ]
        peakType = v.get("peakType")
        if peakType is not None:
            if peakType.strip() not in allowed_peak_types:
                raise ValueError(f"{peakType} not found in {allowed_peak_types}")
        return v
