from snapred.meta.Config import Config
from snapred.meta.Enum import StrEnum


# NOTES:
#   * This should probably not be reconfigurable at runtime.
#     This would be liable to only cause indexing issues.
#   * `VERSION_START` is used as the DEFAULT version number.
#     For example, when diffraction calibration would otherwise
#     be missing and the default diffraction calibration is used.
def VERSION_START():
    return Config["version.start"]


class VersionState(StrEnum):
    DEFAULT = Config["version.friendlyName.default"]
    LATEST = "latest"
    NEXT = "next"
    # NOTE: This is only so we may read old saved IntrumentConfigs
    LEGACY_INST_PRM = "1.4"


""" TODO: remove this.
# I'm not sure why ci is failing without this, it doesn't seem to be used anywhere
VERSION_DEFAULT = VersionState.DEFAULT
"""

Version = int | VersionState
