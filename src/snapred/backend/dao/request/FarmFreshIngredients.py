from typing import Any, List, NamedTuple, Optional

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

from snapred.backend.dao.indexing.Versioning import Version, VersionState
from snapred.backend.dao.Limit import Limit, Pair
from snapred.backend.dao.state import FocusGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum

# TODO: this declaration is duplicated in `ReductionRequest`.
Versions = NamedTuple("Versions", [("calibration", Version), ("normalization", Version)])


class FarmFreshIngredients(BaseModel):
    """
    from these, the Sous Chef can make everything
    """

    # NOTE this class is a REQUEST object, despite its name
    # Do NOT use inside ingredients for algorithms

    runNumber: str

    versions: Versions = Versions(VersionState.LATEST, VersionState.LATEST)

    # allow 'versions' to be accessed as a single version,
    #   or, to be accessed ambiguously
    @property
    def version(self) -> Optional[int]:
        if self.versions.calibration is not None and self.versions.normalization is not None:
            raise RuntimeError("accessing 'version' property when 'versions' is non-singular")
        return self.versions[0]

    @version.setter
    def version(self, v: Optional[int]):
        self.versions = Versions(v, None)

    useLiteMode: bool

    ## mandatory for reduction
    timestamp: Optional[float] = None

    ## needs to be mandatory for diffcal
    cifPath: Optional[str] = None

    ## needs to be mandatory for normcal
    calibrantSamplePath: Optional[str] = None

    ## needs to be mandatory for reduction
    keepUnfocused: Optional[bool] = None
    convertUnitsTo: Optional[str] = None

    ## the below are not-so-fresh, being fiddly optional parameters with defaults
    convergenceThreshold: float = Config["calibration.diffraction.convergenceThreshold"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    peakIntensityThreshold: Optional[float] = None
    peakFunction: SymmetricPeakEnum = SymmetricPeakEnum[Config["calibration.diffraction.peakFunction"]]
    maxOffset: float = Config["calibration.diffraction.maximumOffset"]
    crystalDBounds: Limit[float] = Limit(
        minimum=Config["constants.CrystallographicInfo.crystalDMin"],
        maximum=Config["constants.CrystallographicInfo.crystalDMax"],
    )
    fwhmMultipliers: Pair[float] = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
    maxChiSq: Optional[float] = Config["constants.GroupDiffractionCalibration.MaxChiSq"]

    focusGroups: Optional[List[FocusGroup]] = None

    # Allow 'focusGroups' to be accessed as a single 'focusGroup'
    @property
    def focusGroup(self) -> FocusGroup:
        if len(self.focusGroups) > 1:
            raise RuntimeError("accessing 'focusGroup' property when 'focusGroups' is non-singular")
        return self.focusGroups[0]

    @focusGroup.setter
    def focusGroup(self, fg: FocusGroup):
        if fg is None:
            raise ValidationError("'focusGroup' is None")
        self.focusGroups = [fg]

    @field_validator("versions")
    @classmethod
    def validate_versions(cls, v) -> Versions:
        if not isinstance(v, Versions):
            v = Versions(v)
        if v.calibration is None:
            raise ValueError("Calibration version must be specified")
        if v.normalization is None:
            raise ValueError("Normalization version must be specified")
        return v

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

    @model_validator(mode="before")
    @classmethod
    def validate_focusGroups(cls, v: Any):
        if isinstance(v, dict):
            if "focusGroup" in v:
                if "focusGroups" in v:
                    raise ValidationError("initialization with both 'focusGroup' and 'focusGroups' properties")
                fg = v["focusGroup"]
                if fg is None:
                    raise ValidationError("'focusGroup' is None")
                v["focusGroups"] = [fg]
                del v["focusGroup"]
        return v

    model_config = ConfigDict(extra="forbid", validate_assignment=True)
