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


class WorkspaceMetadata(BaseModel):
    """Class to hold tags related to a workspace"""

    diffcalState: DiffcalStateMetadata = UNSET
    normalizationState: NormalizationStateMetadata = UNSET

    model_config = ConfigDict(extra="forbid", use_enum_values=True)
