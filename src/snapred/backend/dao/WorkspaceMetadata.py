from typing import Literal, get_args

from pydantic import BaseModel

# possible states for diffraction calibration

DIFFCAL_METADATA_STATE = Literal[
    "unset",  # the default condition before any settings
    "exists",  # the state exists and the corresponding .h5 file located
    "alternate",  # the user supplied an alternate .h5 that they believe is usable.
    "none",  # proceed using the defaul (IDF) geometry.
]

diffcal_metadata_state_list = list(get_args(DIFFCAL_METADATA_STATE))


# possible states for normalization

NORMCAL_METADATA_STATE = Literal[
    "unset",  # the default condition before any settings
    "exists",  # the state exists and the corresponding .h5 file located
    "alternate",  # the user supplied an alternate .h5 that they believe is usable
    "none",  # proceed without applying any normalization
    "fake",  # proceed by creating a "fake vanadium"
]

normcal_metadata_state_list = list(get_args(NORMCAL_METADATA_STATE))


class WorkspaceMetadata(BaseModel):
    """Class to hold tags related to a workspace"""

    diffcalState: DIFFCAL_METADATA_STATE = "unset"
    normalizationState: NORMCAL_METADATA_STATE = "unset"
