from typing import Literal, Union

from pydantic import BaseModel, ConfigDict

from snapred.meta.Enum import StrEnum

UNSET: str = "unset"


class DiffcalStateMetadata(StrEnum):
    """Class to hold tags related to a workspace"""

    UNSET: str = UNSET  # the default condition before any settings
    DEFAULT: str = "default"  # the default condition before any settings
    EXISTS: str = "exists"  # the state exists and the corresponding .h5 file located
    ALTERNATE: str = "alternate"  # the user supplied an alternate .h5 that they believe is usable.
    NONE: str = "none"  # proceed using the defaul (IDF) geometry.


class NormalizationStateMetadata(StrEnum):
    """Class to hold tags related to a workspace"""

    UNSET: str = UNSET  # the default condition before any settings
    EXISTS: str = "exists"  # the state exists and the corresponding .h5 file located
    ALTERNATE: str = "alternate"  # the user supplied an alternate .h5 that they believe is usable
    NONE: str = "none"  # proceed without applying any normalization
    FAKE: str = "fake"  # proceed by creating a "fake vanadium"


class ParticleNormalizationMethod(StrEnum):
    """Class to hold tags related to a workspace"""

    UNSET: str = UNSET  # the default condition before any settings
    PROTON_CHARGE: str = "proton_charge"  # the state exists and the corresponding .h5 file located
    MONITOR_COUNTS: str = "monitor_counts"  # the user supplied an alternate .h5 that they believe is usable
    NONE: str = "none"  # proceed without applying any normalization


class WorkspaceMetadata(BaseModel):
    """Class to hold tags related to a workspace"""

    diffcalState: DiffcalStateMetadata = UNSET
    normalizationState: NormalizationStateMetadata = UNSET
    # NOTE: variables not allowed in type declarations
    liteDataCompressionTolerance: Union[float, Literal["unset"]] = UNSET
    normalizeByMonitorFactor: Union[float, Literal["unset"]] = UNSET
    normalizeByMonitorID: Union[int, Literal["unset"]] = UNSET
    particleNormalizationMethod: ParticleNormalizationMethod = UNSET

    model_config = ConfigDict(extra="forbid", use_enum_values=True)
