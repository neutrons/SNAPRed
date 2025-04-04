from snapred.meta.Config import Config
from snapred.meta.Enum import StrEnum


# NOTE: This should probably not be reconfigurable at runtime.
#       This would be liable to only cause indexing issues.
def VERSION_START():
    return Config["version.start"]


class VersionState(StrEnum):
    DEFAULT = Config["version.friendlyName.default"]
    LATEST = "latest"
    NEXT = "next"
    # NOTE: This is only so we may read old saved IntrumentConfigs
    LEGACY_INST_PRM = "1.4"


# I'm not sure why ci is failing without this, it doesn't seem to be used anywhere
VERSION_DEFAULT = VersionState.DEFAULT

Version = int | VersionState

