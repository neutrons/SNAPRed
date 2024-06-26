from typing import Any, List, Optional, Union

from pydantic import BaseModel, ConfigDict, field_validator

from snapred.backend.dao.Limit import Limit, Pair
from snapred.backend.dao.state import FocusGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum


class FarmFreshIngredients(BaseModel, extra="forbid"):
    """
    from these, the Sous Chef can make everything
    """

    # NOTE this class is a REQUEST object, despite its name
    # Do NOT use inside ingredients for algorithms

    runNumber: str
    version: Optional[str] = None
    useLiteMode: bool
    focusGroup: Union[FocusGroup, List[FocusGroup]]

    ## needs to be mandatory for diffcal
    cifPath: Optional[str] = None

    ## needs to be mandatory for normcal
    calibrantSamplePath: Optional[str] = None

    ## the below are not-so-fresh, being fiddly optional parameters with defaults
    convergenceThreshold: float = Config["calibration.diffraction.convergenceThreshold"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    peakIntensityThreshold: float = Config["calibration.diffraction.peakIntensityThreshold"]
    peakFunction: SymmetricPeakEnum = SymmetricPeakEnum[Config["calibration.diffraction.peakFunction"]]
    maxOffset: float = Config["calibration.diffraction.maximumOffset"]
    crystalDBounds: Limit[float] = Limit(
        minimum=Config["constants.CrystallographicInfo.crystalDMin"],
        maximum=Config["constants.CrystallographicInfo.crystalDMax"],
    )
    fwhmMultipliers: Pair[float] = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
    maxChiSq: Optional[float] = Config["constants.GroupDiffractionCalibration.MaxChiSq"]

    @field_validator("crystalDBounds", mode="before")
    @classmethod
    def validate_crystalDBounds(cls, v: Any) -> Limit[float]:
        if isinstance(v, dict):
            v = Limit[float](**v)
        if not isinstance(v, Limit[float]):
            # Coerce Generic[T]-derived type
            v = Limit[float](**v.dict())
        return v

    @field_validator("fwhmMultipliers", mode="before")
    @classmethod
    def validate_fwhmMultipliers(cls, v: Any) -> Pair[float]:
        if isinstance(v, dict):
            v = Pair[float](**v)
        if not isinstance(v, Pair[float]):
            # Coerce Generic[T]-derived type
            v = Pair[float](**v.dict())
        return v

    model_config = ConfigDict(extra="forbid")
