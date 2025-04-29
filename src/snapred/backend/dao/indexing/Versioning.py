import numpy
from pydantic import BaseModel, ConfigDict, field_validator

from snapred.meta.Config import Config
from snapred.meta.Enum import StrEnum

# NOTE: This should probably not be reconfigurable at runtime.
#       This would be liable to only cause indexing issues.
VERSION_START = Config["version.start"]


class VersionState(StrEnum):
    DEFAULT = Config["version.friendlyName.default"]
    LATEST = "latest"
    NEXT = "next"
    # NOTE: This is only so we may read old saved IntrumentConfigs
    LEGACY_INST_PRM = "1.4"


# I'm not sure why ci is failing without this, it doesn't seem to be used anywhere
VERSION_DEFAULT = VersionState.DEFAULT

Version = int | VersionState


class VersionedObject(BaseModel):
    # Base class for all versioned DAO

    version: Version

    @field_validator("version", mode="before")
    def validate_version(cls, value: Version) -> Version:
        if value in VersionState.values():
            # Without an explicit conversion here, the value will end up being a string!
            return VersionState(value)

        if isinstance(value, str):
            raise ValueError(f"Version must be an int or {VersionState.values()}")

        if value is None:
            raise ValueError("Version must be specified")

        if isinstance(value, numpy.int64):
            # Conversion from HDF5 metadata.
            value = int(value)

        if value < VERSION_START:
            raise ValueError(f"Version must be greater than {VERSION_START}")

        return value

    # NOTE: This approach was taken because 'field_serializer' was checking against the
    #       INITIAL value of version for some reason.  This is a workaround.
    #
    def model_dump_json(self, *args, **kwargs):  # noqa ARG002
        if self.version in VersionState.values():
            raise ValueError(f"Version {self.version} must be flattened to an int before writing to JSON")
        return super().model_dump_json(*args, **kwargs)

    model_config = ConfigDict(
        strict=True,
        use_enum_values=True,
        validate_assignment=True,
    )
